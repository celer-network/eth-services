package txmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/celer-network/eth-services/client"
	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/celer-network/eth-services/subscription"
	"github.com/celer-network/eth-services/types"

	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/multierr"
)

var (
	// ErrCouldNotGetReceipt is the error string we save if we reach our finality depth for a confirmed transaction without ever getting a receipt
	// This most likely happened because an external wallet used the account for this nonce
	ErrCouldNotGetReceipt = "could not get receipt"
)

type EthConfirmer interface {
	subscription.HeadTrackable

	GetTx(fromAddress gethCommon.Address, txID uuid.UUID) (*models.Tx, error)
}

// EthConfirmer is a broad service which performs four different tasks in sequence on every new longest chain
// Step 1: Mark that all currently pending transaction attempts were broadcast before this block
// Step 2: Check pending transactions for receipts
// Step 3: See if any transactions have exceeded the gas bumping block threshold and, if so, bump them
// Step 4: Check confirmed transactions to make sure they are still in the longest chain (reorg protection)

type ethConfirmer struct {
	ethClient client.Client
	store     esStore.Store
	keyStore  client.KeyStoreInterface
	config    *types.Config
	logger    types.Logger

	lock sync.Mutex
}

func NewEthConfirmer(
	ethClient client.Client,
	store esStore.Store,
	keyStore client.KeyStoreInterface,
	config *types.Config) EthConfirmer {
	return &ethConfirmer{
		ethClient: ethClient,
		store:     store,
		keyStore:  keyStore,
		config:    config,
		logger:    config.Logger,
	}
}

var _ EthConfirmer = (*ethConfirmer)(nil)

func (ec *ethConfirmer) GetTx(fromAddress gethCommon.Address, txID uuid.UUID) (*models.Tx, error) {
	return ec.store.GetTx(fromAddress, txID)
}

// Do nothing on connect, simply wait for the next head
func (ec *ethConfirmer) Connect(*models.Head) error {
	return nil
}

func (ec *ethConfirmer) Disconnect() {
	// pass
}

func (ec *ethConfirmer) OnNewLongestChain(ctx context.Context, head models.Head) {
	if err := ec.ProcessHead(ctx, head); err != nil {
		ec.logger.Errorw("EthConfirmer error",
			"err", err,
		)
	}
}

// ProcessHead takes all required transactions for the confirmer on a new head
func (ec *ethConfirmer) ProcessHead(ctx context.Context, head models.Head) error {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	return ec.processHead(ctx, head)
}

// NOTE: This SHOULD NOT be run concurrently or it could behave badly
func (ec *ethConfirmer) processHead(ctx context.Context, head models.Head) error {
	if err := ec.store.SetBroadcastBeforeBlockNum(head.Number); err != nil {
		return errors.Wrap(err, "SetBroadcastBeforeBlockNum failed")
	}

	mark := time.Now()

	if err := ec.CheckForReceipts(ctx, head.Number); err != nil {
		return errors.Wrap(err, "CheckForReceipts failed")
	}

	ec.logger.Debugw("EthConfirmer: finished CheckForReceipts",
		"headNum", head.Number,
		"time", time.Since(mark),
		"id", "eth_confirmer",
	)
	mark = time.Now()

	accounts, err := ec.store.GetAccounts()
	if err != nil {
		return errors.Wrap(err, "could not fetch keys")
	}
	if err := ec.BumpGasWhereNecessary(ctx, accounts, head.Number); err != nil {
		return errors.Wrap(err, "BumpGasWhereNecessary failed")
	}

	ec.logger.Debugw("EthConfirmer: finished BumpGasWhereNecessary",
		"headNum", head.Number,
		"time", time.Since(mark),
		"id", "eth_confirmer",
	)
	mark = time.Now()

	defer func() {
		ec.logger.Debugw("EthConfirmer: finished EnsureConfirmedTransactionsInLongestChain",
			"headNum", head.Number,
			"time", time.Since(mark),
			"id", "eth_confirmer",
		)
	}()

	return errors.Wrap(ec.EnsureConfirmedTxsInLongestChain(ctx, accounts, &head), "EnsureConfirmedTransactionsInLongestChain failed")
}

// receiptFetcherWorkerCount is the max number of concurrently executing
// workers that will fetch receipts for eth transactions
const receiptFetcherWorkerCount = 10

func (ec *ethConfirmer) CheckForReceipts(ctx context.Context, blockNum int64) error {
	txs, err := ec.store.GetTxsRequiringReceiptFetch()
	if err != nil {
		return errors.Wrap(err, "getTxsRequiringReceiptFetch failed")
	}
	if len(txs) == 0 {
		return nil
	}

	ec.logger.Debugf("EthConfirmer: fetching receipt for %v transactions", len(txs))

	ec.concurrentlyFetchReceipts(ctx, txs)

	if err := ec.store.MarkConfirmedMissingReceipt(); err != nil {
		return errors.Wrap(err, "unable to mark Txs as 'confirmed_missing_receipt'")
	}

	if err := ec.markOldTxsMissingReceiptAsErrored(ctx, blockNum); err != nil {
		return errors.Wrap(err, "unable to confirm buried unconfirmed Txes")
	}

	return nil
}

func (ec *ethConfirmer) concurrentlyFetchReceipts(ctx context.Context, txs []*models.Tx) {
	var wg sync.WaitGroup
	wg.Add(receiptFetcherWorkerCount)
	chTxs := make(chan *models.Tx)
	for i := 0; i < receiptFetcherWorkerCount; i++ {
		go ec.fetchReceipts(ctx, chTxs, &wg)
	}
	for _, tx := range txs {
		chTxs <- tx
	}
	close(chTxs)
	wg.Wait()

}

func (ec *ethConfirmer) fetchReceipts(ctx context.Context, chTxs <-chan *models.Tx, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		tx, ok := <-chTxs
		if !ok {
			return
		}
		for i, attempt := range tx.TxAttempts {
			// NOTE: This could conceivably be optimized even further at the
			// expense of slightly higher load for the remote eth node, by
			// batch requesting all receipts at once
			receipt, err := ec.fetchReceipt(ctx, attempt.Hash)
			if client.IsParityQueriedReceiptTooEarly(err) || (receipt != nil && receipt.BlockNumber == nil) {
				ec.logger.Debugw("EthConfirmer#fetchReceipts: got receipt for transaction but it's still in the mempool and not included in a block yet",
					"txHash", attempt.Hash.Hex(),
				)
				break
			} else if err != nil {
				ec.logger.Errorw("EthConfirmer#fetchReceipts: fetchReceipt failed",
					"txHash", attempt.Hash.Hex(),
					"err", err,
				)
				break
			}
			if receipt != nil {
				ec.logger.Debugw("EthConfirmer#fetchReceipts: got receipt for transaction",
					"txHash", attempt.Hash.Hex(),
					"blockNumber", receipt.BlockNumber,
				)
				if receipt.TxHash != attempt.Hash {
					ec.logger.Errorf("EthConfirmer#fetchReceipts: invariant violation, expected receipt with hash %s to have same hash as attempt with hash %s", receipt.TxHash.Hex(), attempt.Hash.Hex())
					break
				}
				serializedReceipt, err := serializeReceipt(receipt)
				if err != nil {
					ec.logger.Errorw("EthConfirmer#fetchReceipts: serializeReceipt failed",
						"err", err,
					)
				}
				tx.TxAttempts[i].Receipts = append(tx.TxAttempts[i].Receipts, *serializedReceipt)
				tx.State = models.TxStateConfirmed
				err = ec.store.PutTx(tx)
				if err != nil {
					ec.logger.Errorw("EthConfirmer#fetchReceipts: saveReceipt failed",
						"err", err,
					)
				}
				break
			} else {
				ec.logger.Debugw("EthConfirmer#fetchReceipts: still waiting for receipt",
					"txHash", attempt.Hash.Hex(),
					"txAttemptID", attempt.ID,
					"txID", tx.ID,
				)
			}
		}
	}
}

func (ec *ethConfirmer) fetchReceipt(ctx context.Context, hash gethCommon.Hash) (*gethTypes.Receipt, error) {
	ctx, cancel := context.WithTimeout(ctx, maxEthNodeRequestTime)
	defer cancel()
	receipt, err := ec.ethClient.TransactionReceipt(ctx, hash)
	if err != nil && err.Error() == "not found" {
		return nil, nil
	}
	return receipt, err
}

func serializeReceipt(receipt *gethTypes.Receipt) (*models.Receipt, error) {
	if receipt.BlockNumber == nil {
		return nil, errors.Errorf("receipt was missing block number: %#v", receipt)
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return nil, errors.Wrap(err, "serializeReceipt failed to marshal")
	}
	serializedReceipt := models.Receipt{
		Receipt:          receiptJSON,
		TxHash:           receipt.TxHash,
		BlockHash:        receipt.BlockHash,
		BlockNumber:      receipt.BlockNumber.Int64(),
		TransactionIndex: receipt.TransactionIndex,
	}
	return &serializedReceipt, nil
}

func (ec *ethConfirmer) markOldTxsMissingReceiptAsErrored(ctx context.Context, blockNum int64) error {
	cutoff := blockNum - ec.config.FinalityDepth
	if cutoff <= 0 {
		return nil
	}
	// TODO: log missing receipt error?
	return ec.store.MarkOldTxsMissingReceiptAsErrored(cutoff)
}

func (ec *ethConfirmer) BumpGasWhereNecessary(ctx context.Context, accounts []*models.Account, blockHeight int64) error {
	var wg sync.WaitGroup

	// It is safe to process separate accounts concurrently
	// NOTE: This design will block one account if another takes a really long time to execute
	wg.Add(len(accounts))
	errors := []error{}
	var errMu sync.Mutex
	for _, account := range accounts {
		go func(fromAddress gethCommon.Address) {
			if err := ec.bumpGasWhereNecessary(ctx, fromAddress, blockHeight); err != nil {
				errMu.Lock()
				errors = append(errors, err)
				errMu.Unlock()
				ec.logger.Errorw("Error in BumpGasWhereNecessary",
					"error", err,
					"fromAddress", fromAddress,
				)
			}
			wg.Done()
		}(account.Address)
	}

	wg.Wait()

	return multierr.Combine(errors...)
}

func (ec *ethConfirmer) bumpGasWhereNecessary(ctx context.Context, address gethCommon.Address, blockHeight int64) error {
	if err := ec.handleAllInProgressAttempts(ctx, address, blockHeight); err != nil {
		return errors.Wrap(err, "handleAllInProgressAttempts failed")
	}

	threshold := ec.config.GasBumpThreshold
	depth := ec.config.GasBumpTxDepth
	txs, err := ec.store.GetTxsRequiringNewAttempt(address, blockHeight, threshold, depth)
	if err != nil {
		return errors.Wrap(err, "GetTxsRequiringNewAttempt failed")
	}
	if len(txs) > 0 {
		ec.logger.Debugf("EthConfirmer: Bumping gas for %v transactions", len(txs))
	}
	for _, tx := range txs {
		attempt, err := ec.newAttemptWithGasBump(tx)
		if err != nil {
			return errors.Wrap(err, "newAttemptWithGasBump failed")
		}

		if err := ec.saveInProgressAttempt(tx, attempt); err != nil {
			return errors.Wrap(err, "saveInProgressAttempt failed")
		}

		if err := ec.handleInProgressAttempt(ctx, tx, attempt, blockHeight); err != nil {
			return errors.Wrap(err, "handleInProgressTxAttempt failed")
		}
	}
	return nil
}

// handleAllInProgressAttempts handles "in_progress" attempts were left behind after a
// crash/restart and may or may not have been sent. Ensure they get on-chain so we can fetch a
// receipt.
func (ec *ethConfirmer) handleAllInProgressAttempts(ctx context.Context, address gethCommon.Address, blockHeight int64) error {
	attempts, err := ec.store.GetInProgressAttempts(address)
	if err != nil {
		return errors.Wrap(err, "GetInProgressTxAttempts failed")
	}
	for _, attempt := range attempts {
		tx, err := ec.store.GetTx(address, attempt.TxID)
		if err != nil {
			return errors.Wrap(err, "handleInProgressAttempt cannot get tx")
		}
		if err := ec.handleInProgressAttempt(ctx, tx, attempt, blockHeight); err != nil {
			return errors.Wrap(err, "handleInProgressAttempt failed")
		}
	}
	return nil
}

func (ec *ethConfirmer) newAttemptWithGasBump(tx *models.Tx) (attempt *models.TxAttempt, err error) {
	var bumpedGasPrice *big.Int
	numAttempts := len(tx.TxAttempts)
	if numAttempts > 0 {
		previousAttempt := tx.TxAttempts[numAttempts-1]
		if previousAttempt.State == models.TxAttemptStateInsufficientEth {
			// Do not create a new attempt if we ran out of eth last time since bumping gas is pointless
			// Instead try to resubmit the same attempt at the same price, in the hope that the wallet was funded since our last attempt
			previousAttempt.State = models.TxAttemptStateInProgress
			return &previousAttempt, nil
		}
		previousGasPrice := previousAttempt.GasPrice
		bumpedGasPrice, err = BumpGas(ec.config, &previousGasPrice)
		if err != nil {
			ec.logger.Errorw("Failed to bump gas",
				"err", err,
				"txID", tx.ID,
				"txHash", attempt.Hash,
				"originalGasPrice", previousGasPrice.String(),
				"maxGasPrice", ec.config.MaxGasPrice,
			)
			// Do not create a new attempt if bumping gas would put us over the limit or cause some other problem
			// Instead try to resubmit the previous attempt, and keep resubmitting until its accepted
			previousAttempt.BroadcastBeforeBlockNum = -1
			previousAttempt.State = models.TxAttemptStateInProgress
			return &previousAttempt, nil
		}
	} else {
		ec.logger.Errorf("Invariant violation: Tx %v was unconfirmed but didn't have any attempts. "+
			"Falling back to default gas price instead.", tx.ID)
		bumpedGasPrice = ec.config.DefaultGasPrice
	}
	return newAttempt(ec.keyStore, ec.config, tx, bumpedGasPrice)
}

func (ec *ethConfirmer) saveInProgressAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.New("saveInProgressAttempt failed: attempt state must be in_progress")
	}
	return ec.saveAttempt(tx, attempt)
}

func (ec *ethConfirmer) handleInProgressAttempt(ctx context.Context, tx *models.Tx, attempt *models.TxAttempt, blockHeight int64) error {
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.Errorf("invariant violation: expected TxAttempt %v to be in_progress, it was %s", attempt.ID, attempt.State)
	}

	sendError := sendTransaction(ctx, ec.ethClient, attempt, ec.logger)

	if sendError.IsTerminallyUnderpriced() {
		// This should really not ever happen in normal operation since we
		// already bumped above the required minimum in EthBroadcaster.
		//
		// It could conceivably happen if the remote eth node changed it's configuration.
		bumpedGasPrice, err := BumpGas(ec.config, &attempt.GasPrice)
		if err != nil {
			return errors.Wrap(err, "could not bump gas for terminally underpriced transaction")
		}
		ec.logger.Errorf("gas price %v wei was rejected by the eth node for being too low. "+
			"Eth node returned: '%s'. "+
			"Bumping to %v wei and retrying. "+
			"ACTION REQUIRED: You should consider increasing DefaultGasPrice", attempt.GasPrice, sendError.Error(), bumpedGasPrice)
		replacementAttempt, err := newAttempt(ec.keyStore, ec.config, tx, bumpedGasPrice)
		if err != nil {
			return errors.Wrap(err, "newAttempt failed")
		}

		if err := saveReplacementInProgressAttempt(ec.store, tx, attempt, replacementAttempt); err != nil {
			return errors.Wrap(err, "saveReplacementInProgressAttempt failed")
		}
		return ec.handleInProgressAttempt(ctx, tx, replacementAttempt, blockHeight)
	}

	if sendError.IsTemporarilyUnderpriced() {
		// Most likely scenario here is a parity node that is rejecting
		// low-priced transactions due to mempool pressure
		//
		// In that case, the safest thing to do is to pretend the transaction
		// was accepted and continue the normal gas bumping cycle until we can
		// get it into the mempool
		ec.logger.Infow("EthConfirmer: Transaction temporarily underpriced",
			"txID", tx.ID,
			"attemptID", attempt.ID,
			"err", sendError.Error(),
			"gasPriceWei", attempt.GasPrice.String(),
		)
		sendError = nil
	}

	if sendError.Fatal() {
		// This is an invariant violation. The EthBroadcaster can never create an TxAttempt that
		// will fatally error. The only scenario imaginable where this might take place is if
		// geth / parity have been updated between broadcasting and confirming steps.
		ec.logger.Errorw("invariant violation: fatal error while re-attempting transaction",
			"txID", tx.ID,
			"err", sendError,
			"signedRawTx", hexutil.Encode(attempt.SignedRawTx),
			"blockHeight", blockHeight,
		)
		// This will loop continuously on every new head so it must be handled manually.
		return ec.deleteInProgressAttempt(tx, attempt)
	}

	if sendError.IsNonceTooLowError() {
		// Nonce too low indicated that a transaction at this nonce was confirmed already.
		// Assume success and hand off to the next cycle to fetch a receipt and mark confirmed.
		sendError = nil
	}

	if sendError.IsReplacementUnderpriced() {
		// Our system constraints guarantee that the attempt referenced in this
		// function has the highest gas price of all attempts.
		//
		// Thus, there are only two possible scenarios where this can happen.
		//
		// 1. Our gas bump was insufficient compared to our previous attempt
		// 2. An external wallet used the account to manually send a transaction
		// at a higher gas price
		//
		// In this case the simplest and most robust way to recover is to ignore
		// this attempt and wait until the next bump threshold is reached in
		// order to bump again.
		ec.logger.Errorw(fmt.Sprintf("EthConfirmer: replacement transaction underpriced at %v wei for Tx %v. "+
			"Eth node returned error: '%s'. "+
			"Either you have set ETH_GAS_BUMP_PERCENT (currently %v%%) too low or an external wallet used this account. "+
			"Please note that using your node's private keys outside of eth-service is NOT SUPPORTED and can lead to missed transactions.",
			attempt.GasPrice.Int64(), tx.ID, sendError.Error(), ec.config.GasBumpPercent), "err", sendError)

		// Assume success and hand off to the next cycle.
		sendError = nil
	}

	if sendError.IsInsufficientEth() {
		ec.logger.Errorw(fmt.Sprintf("EthConfirmer: TxAttempt %v (hash 0x%x) at gas price (%s Wei) was rejected due to insufficient eth. "+
			"The eth node returned %s. "+
			"ACTION REQUIRED: Wallet with address 0x%x is OUT OF FUNDS",
			attempt.ID, attempt.Hash, attempt.GasPrice.String(), sendError.Error(), tx.FromAddress,
		), "err", sendError)
		return ec.saveInsufficientEthAttempt(tx, attempt)
	}

	if sendError == nil {
		return ec.saveSentAttempt(tx, attempt)
	}

	// Any other type of error is considered temporary or resolvable manually. The node may have it
	// in the mempool so we must keep the attempt (leave it in_progress). Safest thing to do is bail
	// out and wait for the next head.
	return errors.Wrapf(sendError, "unexpected error sending Tx %v with hash %s", tx.ID, attempt.Hash.Hex())
}

func (ec *ethConfirmer) deleteInProgressAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.New("deleteInProgressAttempt: expected attempt state to be in_progress")
	}
	return ec.deleteAttempt(tx, attempt)
}

func (ec *ethConfirmer) saveSentAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.New("expected state to be in_progress")
	}
	attempt.State = models.TxAttemptStateBroadcast
	return ec.saveAttempt(tx, attempt)
}

func (ec *ethConfirmer) saveInsufficientEthAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	if !(attempt.State == models.TxAttemptStateInProgress || attempt.State == models.TxAttemptStateInsufficientEth) {
		return errors.New("expected state to be either in_progress or insufficient_eth")
	}
	attempt.State = models.TxAttemptStateInsufficientEth
	return ec.saveAttempt(tx, attempt)
}

func (ec *ethConfirmer) saveAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	appendAttempt := true
	for i, currAttempt := range tx.TxAttempts {
		if uuid.Equal(currAttempt.ID, attempt.ID) {
			appendAttempt = false
			tx.TxAttempts[i] = *attempt
			break
		}
	}
	if appendAttempt {
		tx.TxAttempts = append(tx.TxAttempts, *attempt)
	}
	return errors.Wrap(ec.store.PutTx(tx), "saveAttempt failed")
}

func (ec *ethConfirmer) deleteAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	index := -1
	for i, currAttempt := range tx.TxAttempts {
		if uuid.Equal(currAttempt.ID, attempt.ID) {
			index = i
			break
		}
	}
	if index != -1 {
		copy(tx.TxAttempts[index:], tx.TxAttempts[index+1:])
		tx.TxAttempts = tx.TxAttempts[:len(tx.TxAttempts)-1]
		return errors.Wrap(ec.store.PutTx(tx), "deleteAttempt failed")
	}
	return errors.New("deleteAttempt: attempt not found")
}

// EnsureConfirmedTxsInLongestChain finds all confirmed Txs up to the depth
// of the given chain and ensures that every one has a receipt with a block hash that is
// in the given chain.
//
// If any of the confirmed transactions does not have a receipt in the chain, it has been
// re-org'd out and will be rebroadcast.
func (ec *ethConfirmer) EnsureConfirmedTxsInLongestChain(ctx context.Context, accounts []*models.Account, head *models.Head) error {
	txs, err := ec.store.GetTxsConfirmedAtOrAboveBlockHeight(head.EarliestInChain().Number)
	if err != nil {
		return errors.Wrap(err, "getTxsConfirmedAtOrAboveBlockHeight failed")
	}

	for _, tx := range txs {
		if !hasReceiptInLongestChain(tx, head) {
			if err := ec.markForRebroadcast(tx); err != nil {
				return errors.Wrapf(err, "markForRebroadcast failed for tx %v", tx.ID)
			}
		}
	}

	// It is safe to process separate accounts concurrently
	// NOTE: This design will block one key if another takes a really long time to execute
	var wg sync.WaitGroup
	errors := []error{}
	var errMu sync.Mutex
	wg.Add(len(accounts))
	for _, account := range accounts {
		go func(fromAddress gethCommon.Address) {
			if err := ec.handleAllInProgressAttempts(ctx, fromAddress, head.Number); err != nil {
				errMu.Lock()
				errors = append(errors, err)
				errMu.Unlock()
				ec.logger.Errorw("Error in BumpGasWhereNecessary", "error", err, "fromAddress", fromAddress)
			}

			wg.Done()
		}(account.Address)
	}

	wg.Wait()

	return multierr.Combine(errors...)
}

func hasReceiptInLongestChain(tx *models.Tx, head *models.Head) bool {
	for {
		for _, attempt := range tx.TxAttempts {
			for _, receipt := range attempt.Receipts {
				if receipt.BlockHash == head.Hash && receipt.BlockNumber == head.Number {
					return true
				}
			}
		}
		if head.Parent == nil {
			return false
		}
		head = head.Parent
	}
}

func (ec *ethConfirmer) markForRebroadcast(tx *models.Tx) error {
	numAttempts := len(tx.TxAttempts)
	if numAttempts == 0 {
		return errors.Errorf("invariant violation: expected Tx %v to have at least one attempt", tx.ID)
	}

	// Rebroadcast the one with the highest gas price
	attempt := tx.TxAttempts[numAttempts-1]

	// Put it back in progress and delete all receipts (they do not apply to the new chain)
	deleteAllReceipts(tx)
	err := unconfirmTx(tx)
	if err != nil {
		return errors.Wrapf(err, "unconfirmTx failed for tx %v", tx.ID)
	}
	err = unbroadcastAttempt(tx, &attempt)
	if err != nil {
		return errors.Wrapf(err, "unbroadcastAttempt failed for tx %v", tx.ID)
	}
	err = ec.store.PutTx(tx)
	return errors.Wrap(err, "markForRebroadcast failed")
}

func deleteAllReceipts(tx *models.Tx) {
	for i := range tx.TxAttempts {
		tx.TxAttempts[i].Receipts = make([]models.Receipt, 0)
	}
}

func unconfirmTx(tx *models.Tx) error {
	if tx.State != models.TxStateConfirmed {
		return errors.New("expected Tx.State to be confirmed")
	}
	tx.State = models.TxStateUnconfirmed
	return nil
}

func unbroadcastAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	if attempt.State != models.TxAttemptStateBroadcast {
		return errors.New("expected TxAttempt.State to be broadcast")
	}
	for i, currAttempt := range tx.TxAttempts {
		if uuid.Equal(currAttempt.ID, attempt.ID) {
			tx.TxAttempts[i].State = models.TxAttemptStateInProgress
			tx.TxAttempts[i].BroadcastBeforeBlockNum = -1
		}
	}
	return nil
}
