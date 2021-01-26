package txmanager

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/celer-network/eth-services/client"
	"github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/celer-network/eth-services/types"
	"github.com/google/uuid"

	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
)

// TxBroadcaster monitors transactions that need to be broadcast, assigns nonces and ensures that
// at least one eth node somewhere has received the transaction successfully.
//
// This does not guarantee delivery! A whole host of other things can
// subsequently go wrong such as transactions being evicted from the mempool,
// eth nodes going offline etc. Responsibility for ensuring eventual inclusion
// into the chain falls on the shoulders of the TxConfirmer.
//
// What TxBroadcaster does guarantee is:
// - a monotonic series of increasing nonces for txes that can all eventually be confirmed if you retry enough times
// - transition of txes out of unstarted into either fatal_error or unconfirmed
// - existence of a saved tx_attempt
type TxBroadcaster interface {
	RegisterAccount(address gethCommon.Address) error

	AddTx(txID uuid.UUID,
		from gethCommon.Address,
		to gethCommon.Address,
		value *big.Int,
		encodedPayload []byte,
		gasLimit uint64,
	) error

	Start() error

	Stop() error

	Trigger()

	ProcessUnstartedTxs(account *models.Account) error
}

type txBroadcaster struct {
	ethClient client.Client
	store     store.Store
	keyStore  client.KeyStoreInterface
	config    *types.Config
	logger    types.Logger

	// trigger allows other goroutines to force TxBroadcaster to rescan the
	// database early (before the next poll interval)
	trigger chan struct{}
	chStop  chan struct{}
	wg      sync.WaitGroup

	lock sync.Mutex

	StartStopOnce
}

var _ TxBroadcaster = (*txBroadcaster)(nil)

// NewTxBroadcaster returns a new concrete TxBroadcaster
func NewTxBroadcaster(
	ethClient client.Client,
	store store.Store,
	keyStore client.KeyStoreInterface,
	config *types.Config) TxBroadcaster {
	return &txBroadcaster{
		ethClient: ethClient,
		store:     store,
		keyStore:  keyStore,
		config:    config,
		logger:    config.Logger,
		trigger:   make(chan struct{}, 1),
		chStop:    make(chan struct{}),
		wg:        sync.WaitGroup{},
	}
}

func (tb *txBroadcaster) RegisterAccount(address gethCommon.Address) error {
	account, err := tb.keyStore.GetAccountByAddress(address)
	if err != nil {
		return err
	}
	storedAccount := &models.Account{
		Address:   account.Address,
		NextNonce: -1,
	}
	return tb.store.PutAccount(storedAccount)
}

func (tb *txBroadcaster) AddTx(
	txID uuid.UUID,
	from gethCommon.Address,
	to gethCommon.Address,
	value *big.Int,
	encodedPayload []byte,
	gasLimit uint64,
) error {
	err := tb.store.AddTx(txID, from, to, value, encodedPayload, gasLimit)
	if err != nil {
		return err
	}
	tb.Trigger()
	return nil
}

func (tb *txBroadcaster) Start() error {
	if !tb.OkayToStart() {
		return errors.New("TxBroadcaster is already started")
	}

	tb.wg.Add(1)
	go tb.monitorTxs()

	return nil
}

func (tb *txBroadcaster) Stop() error {
	if !tb.OkayToStop() {
		return errors.New("TxBroadcaster is already stopped")
	}

	close(tb.chStop)
	tb.wg.Wait()

	return nil
}

func (tb *txBroadcaster) Trigger() {
	select {
	case tb.trigger <- struct{}{}:
	default:
	}
}

func (tb *txBroadcaster) monitorTxs() {
	defer tb.wg.Done()
	for {
		pollDBTimer := time.NewTimer(withJitter(tb.config.DBPollInterval))

		accounts, err := tb.store.GetAccounts()

		if err != nil {
			tb.logger.Error(errors.Wrap(err, "monitorTxs failed getting key"))
		} else {
			var wg sync.WaitGroup

			// It is safe to process separate accounts concurrently
			// NOTE: This design will block one account if another takes a really long time to execute
			wg.Add(len(accounts))
			for _, account := range accounts {
				go func(account *models.Account) {
					if err := tb.ProcessUnstartedTxs(account); err != nil {
						tb.logger.Errorw("Error in ProcessUnstartedTxs",
							"error", err,
						)
					}
					wg.Done()
				}(account)
			}
			wg.Wait()
		}

		select {
		case <-tb.chStop:
			// NOTE: See: https://godoc.org/time#Timer.Stop for an explanation of this pattern
			if !pollDBTimer.Stop() {
				<-pollDBTimer.C
			}
			return
		case <-tb.trigger:
			if !pollDBTimer.Stop() {
				<-pollDBTimer.C
			}
			continue
		case <-pollDBTimer.C:
			continue
		}
	}
}

func (tb *txBroadcaster) ProcessUnstartedTxs(account *models.Account) error {
	tb.lock.Lock()
	defer tb.lock.Unlock()
	return tb.processUnstartedTxs(account.Address)
}

// NOTE: This MUST NOT be run concurrently for the same address or it could
// result in undefined state or deadlocks.
// First handle any in_progress transactions left over from last time.
// Then keep looking up unstarted transactions and processing them until there are none remaining.
func (tb *txBroadcaster) processUnstartedTxs(fromAddress gethCommon.Address) error {
	var n uint = 0
	mark := time.Now()
	defer func() {
		if n > 0 {
			tb.logger.Debugw("TxBroadcaster: finished processUnstartedTxs",
				"address", fromAddress,
				"time", time.Since(mark),
				"n", n,
				"id", "eth_broadcaster",
			)
		}
	}()

	if err := tb.handleAnyInProgressTx(fromAddress); err != nil {
		return errors.Wrap(err, "processUnstartedTxs failed")
	}

	for {
		tx, err := tb.nextUnstartedTxWithNonce(fromAddress)
		if err != nil {
			return errors.Wrap(err, "processUnstartedTxs failed")
		}
		if tx == nil {
			return nil
		}
		n++
		attempt, err := newAttempt(tb.keyStore, tb.config, tx, tb.config.DefaultGasPrice)
		if err != nil {
			return errors.Wrap(err, "processUnstartedTxs failed")
		}
		if err := tb.saveInProgressTx(tx, attempt); err != nil {
			return errors.Wrap(err, "processUnstartedTxs failed")
		}

		if err := tb.handleInProgressTx(tx, attempt); err != nil {
			return errors.Wrap(err, "processUnstartedTxs failed")
		}
	}
}

// handleInProgressTx checks if there is any transaction
// in_progress and if so, finishes the job
func (tb *txBroadcaster) handleAnyInProgressTx(fromAddress gethCommon.Address) error {
	tx, err := tb.getInProgressTx(fromAddress)
	if err != nil {
		return errors.Wrap(err, "handleAnyInProgressTx failed")
	}
	if tx != nil {
		if err := tb.handleInProgressTx(tx, &tx.TxAttempts[0]); err != nil {
			return errors.Wrap(err, "handleAnyInProgressTx failed")
		}
	}
	return nil
}

// getInProgressTx returns either 0 or 1 transaction that was left in
// an unfinished state because something went screwy the last time. Most likely
// the program crashed in the middle of the ProcessUnstartedTxs loop.
// It may or may not have been broadcast to an eth node.
func (tb *txBroadcaster) getInProgressTx(fromAddress gethCommon.Address) (*models.Tx, error) {
	tx, err := tb.store.GetOneInProgressTx(fromAddress)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if len(tx.TxAttempts) != 1 || tx.TxAttempts[0].State != models.TxAttemptStateInProgress {
		return nil, errors.Errorf("invariant violation: expected in_progress transaction %v to have exactly one unsent attempt. "+
			"Your database is in an inconsistent state and the program will not function correctly until the problem is resolved", tx.ID)
	}
	return tx, errors.Wrap(err, "getInProgressTx failed")
}

// There can be at most one in_progress transaction per address.
// Here we complete the job that we didn't finish last time.
func (tb *txBroadcaster) handleInProgressTx(tx *models.Tx, attempt *models.TxAttempt) error {
	if tx.State != models.TxStateInProgress {
		return errors.Errorf("invariant violation: expected transaction %v to be in_progress, it was %s", tx.ID, tx.State)
	}

	ctx, cancel := context.WithTimeout(context.Background(), maxEthNodeRequestTime)
	defer cancel()
	sendError := sendTransaction(ctx, tb.ethClient, attempt, tb.logger)

	if sendError.Fatal() {
		tx.Error = sendError.Error()
		// Attempt is thrown away in this case; we don't need it since it never got accepted by a node
		return tb.saveFatallyErroredTransaction(tx)
	}

	if sendError.IsNonceTooLowError() || sendError.IsReplacementUnderpriced() {
		// There are three scenarios that this can happen:
		//
		// SCENARIO 1
		//
		// This is resuming a previous crashed run. In this scenario, it is
		// likely that our previous transaction was the one who was confirmed,
		// in which case we hand it off to the eth confirmer to get the
		// receipt.
		//
		// SCENARIO 2
		//
		// It is also possible that an external wallet can have messed with the
		// account and sent a transaction on this nonce.
		//
		// In this case, it is a human error since this is explicitly unsupported.
		//
		// If it turns out to have been an external wallet, we will never get a
		// receipt for this transaction and it will eventually be marked as
		// errored.
		//
		// The end result is that we will NOT SEND a transaction for this
		// nonce.
		//
		// SCENARIO 3
		//
		// The network/eth client can be assumed to have at-least-once delivery
		// behavior. It is possible that the eth client could have already
		// sent this exact same transaction even if this is our first time
		// calling SendTransaction().
		//
		// In all scenarios, the correct thing to do is assume success for now
		// and hand off to the eth confirmer to get the receipt (or mark as
		// failed).
		sendError = nil
	}

	if sendError.IsTerminallyUnderpriced() {
		return tb.tryAgainWithHigherGasPrice(sendError, tx, attempt)
	}

	if sendError.IsTemporarilyUnderpriced() {
		// If we can't even get the transaction into the mempool at all, assume
		// success (even though the transaction will never confirm) and hand
		// off to the TxConfirmer to bump gas periodically until we _can_ get
		// it in
		tb.logger.Infow("TxBroadcaster: Transaction temporarily underpriced", "TxID", tx.ID, "err", sendError.Error(), "gasPriceWei", attempt.GasPrice.String())
		sendError = nil
	}

	if sendError != nil {
		// Any other type of error is considered temporary or resolvable by the
		// human intervention, but will likely prevent other transactions from working.
		// Safest thing to do is bail out and wait for the next poll.
		return errors.Wrapf(sendError, "error while sending transaction %v", tx.ID)
	}

	return tb.saveUnconfirmed(tx, attempt)
}

// Finds next transaction in the queue, assigns a nonce, and moves it to "in_progress" state ready for broadcast.
// Returns nil if no transactions are in queue
func (tb *txBroadcaster) nextUnstartedTxWithNonce(fromAddress gethCommon.Address) (*models.Tx, error) {
	tx, err := tb.getNextUnstartedTx(fromAddress)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Finish. No more transactions left to process. Hoorah!
			return nil, nil
		}
		return nil, errors.Wrap(err, "getNextUnstartedTx failed")
	}
	tb.logger.Debugw("nextUnstartedTxWithNonce", "id", tx.ID, "state", tx.State)

	nonce, err := tb.getNextNonceWithInitialLoad(tx.FromAddress)
	if err != nil {
		return nil, err
	}
	tx.Nonce = nonce
	return tx, nil
}

func (tb *txBroadcaster) saveInProgressTx(tx *models.Tx, attempt *models.TxAttempt) error {
	if tx.State != models.TxStateUnstarted {
		return errors.Errorf("can only transition to in_progress from unstarted, transaction is currently %s", tx.State)
	}
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.New("attempt state must be in_progress")
	}
	tx.State = models.TxStateInProgress
	attempts := tx.TxAttempts
	attempts = append(attempts, *attempt)
	tx.TxAttempts = attempts
	err := tb.store.PutTx(tx)
	if err != nil {
		return errors.Wrap(err, "saveInProgressTx failed")
	}
	return nil
}

// Finds the next saved transaction that has yet to be broadcast from the given address
// TODO: Ordering
func (tb *txBroadcaster) getNextUnstartedTx(fromAddress gethCommon.Address) (*models.Tx, error) {
	return tb.store.GetNextUnstartedTx(fromAddress)
}

func (tb *txBroadcaster) saveUnconfirmed(tx *models.Tx, attempt *models.TxAttempt) error {
	if tx.State != models.TxStateInProgress {
		return errors.Errorf("can only transition to unconfirmed from in_progress, transaction is currently %s", tx.State)
	}
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.New("attempt must be in in_progress state")
	}
	tb.logger.Debugw("TxBroadcaster: successfully broadcast transaction", "TxID", tx.ID, "txHash", attempt.Hash.Hex())
	tx.State = models.TxStateUnconfirmed
	// Update state
	for i, currAttempt := range tx.TxAttempts {
		if bytes.Equal(currAttempt.ID[:], attempt.ID[:]) {
			tx.TxAttempts[i].State = models.TxAttemptStateBroadcast
			break
		}
	}
	err := tb.store.PutTx(tx)
	if err != nil {
		return errors.Wrap(err, "saveUnconfirmed failed to save tx")
	}
	// BEGIN DEBUG
	txs, _ := tb.store.GetTxs(tx.FromAddress)
	for _, tx := range txs {
		tb.logger.Debugw("saveUnconfirmed all txs", "id", tx.ID, "state", tx.State)
	}
	// END DEBUG

	err = tb.store.SetNextNonce(tx.FromAddress, tx.Nonce+1)
	if err != nil {
		return errors.Wrap(err, "saveUnconfirmed failed to update nonce")
	}
	return nil
}

func (tb *txBroadcaster) tryAgainWithHigherGasPrice(sendError *client.SendError, tx *models.Tx, attempt *models.TxAttempt) error {
	bumpedGasPrice, err := BumpGas(tb.config, attempt.GasPrice)
	if err != nil {
		return errors.Wrap(err, "tryAgainWithHigherGasPrice failed")
	}
	tb.logger.Errorw(fmt.Sprintf("default gas price %v wei was rejected by the eth node for being too low. "+
		"Eth node returned: '%s'. "+
		"Bumping to %v wei and retrying. ACTION REQUIRED: This is a configuration error. "+
		"Consider increasing DefaultGasPrice", tb.config.DefaultGasPrice, sendError.Error(), bumpedGasPrice), "err", err)
	if bumpedGasPrice.Cmp(attempt.GasPrice) == 0 && bumpedGasPrice.Cmp(tb.config.MaxGasPrice) == 0 {
		return errors.Errorf("Hit gas price bump ceiling, will not bump further. This is a terminal error")
	}
	replacementAttempt, err := newAttempt(tb.keyStore, tb.config, tx, bumpedGasPrice)
	if err != nil {
		return errors.Wrap(err, "tryAgainWithHigherGasPrice failed")
	}

	if err := saveReplacementInProgressAttempt(tb.store, tx, attempt, replacementAttempt); err != nil {
		return errors.Wrap(err, "tryAgainWithHigherGasPrice failed")
	}
	return tb.handleInProgressTx(tx, replacementAttempt)
}

func (tb *txBroadcaster) saveFatallyErroredTransaction(tx *models.Tx) error {
	if tx.State != models.TxStateInProgress {
		return errors.Errorf("can only transition to fatal_error from in_progress, transaction is currently %s", tx.State)
	}
	if tx.Error == "" {
		return errors.New("expected error field to be set")
	}
	tb.logger.Errorw("TxBroadcaster: fatal error sending transaction", "TxID", tx.ID, "error", tx.Error)
	tx.Nonce = -1
	tx.State = models.TxStateFatalError
	// Clear TxAttempts
	tx.TxAttempts = nil
	err := tb.store.PutTx(tx)
	if err != nil {
		return errors.Wrap(err, "saveFatallyErroredTransaction failed to save tx")
	}
	return nil
}

// getNextNonceWithInitialLoad returns account.NextNonce for the given address
// It loads it from the database, or if this is a brand new key, queries the eth node for the latest nonce
func (tb *txBroadcaster) getNextNonceWithInitialLoad(address gethCommon.Address) (int64, error) {
	nonce, err := tb.store.GetNextNonce(address)
	if err != nil {
		return 0, err
	}
	if nonce != -1 {
		return nonce, nil
	}
	return tb.loadAndSaveNonce(address)
}

func (tb *txBroadcaster) loadAndSaveNonce(address gethCommon.Address) (int64, error) {
	tb.logger.Debugw("TxBroadcaster: loading next nonce from eth node", "address", address.Hex())
	nonce, err := tb.loadInitialNonceFromEthClient(address)
	if err != nil {
		return 0, errors.Wrap(err, "loadAndSaveNonce failed to loadInitialNonceFromEthClient")
	}
	account, err := tb.store.GetAccount(address)
	if err != nil {
		return 0, errors.Wrap(err, "loadAndSaveNonce failed to get account")
	}
	account.NextNonce = int64(nonce)
	err = tb.store.PutAccount(account)
	if err != nil {
		return 0, errors.Wrap(err, "loadAndSaveNonce failed to put account")
	}
	if nonce == 0 {
		tb.logger.Infow(
			fmt.Sprintf("TxBroadcaster: first use of address %s, starting from nonce 0", address.Hex()),
			"address", address.Hex(),
			"nextNonce", nonce,
		)
	} else {
		tb.logger.Warnw(fmt.Sprintf("TxBroadcaster: address %s has been used before. Starting from nonce %v."+
			" Please note that using the accounts with an external wallet is NOT SUPPORTED and can lead to missed or stuck transactions.",
			address.Hex(), nonce),
			"address", address.Hex(),
			"nextNonce", nonce,
		)
	}

	return int64(nonce), nil
}

func (tb *txBroadcaster) loadInitialNonceFromEthClient(account gethCommon.Address) (nextNonce uint64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), maxEthNodeRequestTime)
	defer cancel()
	nextNonce, err = tb.ethClient.PendingNonceAt(ctx, account)
	return nextNonce, errors.WithStack(err)
}