package txmanager

import (
	"bytes"
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
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

var (
	// ErrCouldNotGetReceipt is the error string we save if we reach our finality depth for a confirmed transaction without ever getting a receipt
	// This most likely happened because an external wallet used the account for this nonce
	ErrCouldNotGetReceipt = "could not get receipt"
)

type TxConfirmer interface {
	subscription.HeadTrackable

	// Methods for testing
	CheckForReceipts(ctx context.Context, blockNum int64) error
	BumpGasWhereNecessary(ctx context.Context, accounts []*models.Account, blockHeight int64) error
	EnsureConfirmedTxsInLongestChain(ctx context.Context, accounts []*models.Account, head *models.Head) error
	SetKeyStore(keyStore client.KeyStoreInterface)
}

// TxConfirmer is a broad service which performs four different tasks in sequence on every new longest chain
// Step 1: Mark that all currently pending transaction attempts were broadcast before this block
// Step 2: Check pending transactions for receipts
// Step 3: See if any transactions have exceeded the gas bumping block threshold and, if so, bump them
// Step 4: Check confirmed transactions to make sure they are still in the longest chain (reorg protection)

type txConfirmer struct {
	ethClient client.Client
	store     esStore.Store
	keyStore  client.KeyStoreInterface
	config    *types.Config
	logger    types.Logger

	lock sync.Mutex
}

func NewTxConfirmer(
	ethClient client.Client,
	store esStore.Store,
	keyStore client.KeyStoreInterface,
	config *types.Config) TxConfirmer {
	return &txConfirmer{
		ethClient: ethClient,
		store:     store,
		keyStore:  keyStore,
		config:    config,
		logger:    config.Logger,
	}
}

var _ TxConfirmer = (*txConfirmer)(nil)

// Do nothing on connect, simply wait for the next head
func (tc *txConfirmer) Connect(*models.Head) error {
	return nil
}

func (tc *txConfirmer) Disconnect() {
	// pass
}

func (tc *txConfirmer) OnNewLongestChain(ctx context.Context, head *models.Head) {
	if err := tc.ProcessHead(ctx, head); err != nil {
		tc.logger.Errorw("TxConfirmer error",
			"err", err,
		)
	}
}

// ProcessHead takes all required transactions for the confirmer on a new head
func (tc *txConfirmer) ProcessHead(ctx context.Context, head *models.Head) error {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	return tc.processHead(ctx, head)
}

// NOTE: This SHOULD NOT be run concurrently or it could behave badly
func (tc *txConfirmer) processHead(ctx context.Context, head *models.Head) error {
	if err := tc.store.SetBroadcastBeforeBlockNum(head.Number); err != nil {
		return errors.Wrap(err, "SetBroadcastBeforeBlockNum failed")
	}

	mark := time.Now()

	if err := tc.CheckForReceipts(ctx, head.Number); err != nil {
		return errors.Wrap(err, "CheckForReceipts failed")
	}

	tc.logger.Debugw("TxConfirmer: finished CheckForReceipts",
		"headNum", head.Number,
		"time", time.Since(mark),
		"id", "eth_confirmer",
	)
	mark = time.Now()

	accounts, err := tc.store.GetAccounts()
	if err != nil {
		return errors.Wrap(err, "could not fetch keys")
	}
	if err := tc.BumpGasWhereNecessary(ctx, accounts, head.Number); err != nil {
		return errors.Wrap(err, "BumpGasWhereNecessary failed")
	}

	tc.logger.Debugw("TxConfirmer: finished BumpGasWhereNecessary",
		"headNum", head.Number,
		"time", time.Since(mark),
		"id", "eth_confirmer",
	)
	mark = time.Now()

	defer func() {
		tc.logger.Debugw("TxConfirmer: finished EnsureConfirmedTransactionsInLongestChain",
			"headNum", head.Number,
			"time", time.Since(mark),
			"id", "eth_confirmer",
		)
	}()

	return errors.Wrap(tc.EnsureConfirmedTxsInLongestChain(ctx, accounts, head), "EnsureConfirmedTransactionsInLongestChain failed")
}

// receiptFetcherWorkerCount is the max number of concurrently executing
// workers that will fetch receipts for eth transactions
const receiptFetcherWorkerCount = 10

func (tc *txConfirmer) CheckForReceipts(ctx context.Context, blockNum int64) error {
	txs, err := tc.store.GetTxsRequiringReceiptFetch()
	if err != nil {
		return errors.Wrap(err, "GetTxsRequiringReceiptFetch failed")
	}
	if len(txs) == 0 {
		return nil
	}

	tc.logger.Debugf("TxConfirmer: fetching receipt for %v transactions", len(txs))

	tc.concurrentlyFetchReceipts(ctx, txs)

	if err := tc.store.MarkConfirmedMissingReceipt(); err != nil {
		return errors.Wrap(err, "unable to mark Txs as 'confirmed_missing_receipt'")
	}

	if err := tc.markOldTxsMissingReceiptAsErrored(ctx, blockNum); err != nil {
		return errors.Wrap(err, "unable to confirm buried unconfirmed txs")
	}

	return nil
}

func (tc *txConfirmer) SetKeyStore(keyStore client.KeyStoreInterface) {
	tc.keyStore = keyStore
}

func (tc *txConfirmer) concurrentlyFetchReceipts(ctx context.Context, txs []*models.Tx) {
	var wg sync.WaitGroup
	wg.Add(receiptFetcherWorkerCount)
	chTxs := make(chan *models.Tx)
	for i := 0; i < receiptFetcherWorkerCount; i++ {
		go tc.fetchReceipts(ctx, chTxs, &wg)
	}
	for _, tx := range txs {
		chTxs <- tx
	}
	close(chTxs)
	wg.Wait()

}

func (tc *txConfirmer) fetchReceipts(ctx context.Context, chTxs <-chan *models.Tx, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		tx, ok := <-chTxs
		if !ok {
			return
		}
		// Iterate from the attempt with the highest gas price
		for _, attemptID := range tx.TxAttemptIDs {
			attempt, err := tc.store.GetTxAttempt(attemptID)
			if err != nil {
				tc.logger.Errorw("TxConfirmer#fetchReceipts: GetTxAttempt failed",
					"txHash", attempt.Hash.Hex(),
					"err", err,
				)
				break
			}
			// NOTE: This could conceivably be optimized even further at the
			// expense of slightly higher load for the remote eth node, by
			// batch requesting all receipts at once
			receipt, err := tc.fetchReceipt(ctx, attempt.Hash)
			if client.IsParityQueriedReceiptTooEarly(err) || (receipt != nil && receipt.BlockNumber == nil) {
				tc.logger.Debugw("TxConfirmer#fetchReceipts: got receipt for transaction but it's still in the mempool and not included in a block yet",
					"txHash", attempt.Hash.Hex(),
				)
				break
			} else if err != nil {
				tc.logger.Errorw("TxConfirmer#fetchReceipts: fetchReceipt failed",
					"txHash", attempt.Hash.Hex(),
					"err", err,
				)
				break
			}
			if receipt != nil {
				tc.logger.Debugw("TxConfirmer#fetchReceipts: got receipt for transaction",
					"txHash", attempt.Hash.Hex(),
					"blockNumber", receipt.BlockNumber,
				)
				if receipt.TxHash != attempt.Hash {
					tc.logger.Errorf("TxConfirmer#fetchReceipts: invariant violation, expected receipt with hash %s to have same hash as attempt with hash %s", receipt.TxHash.Hex(), attempt.Hash.Hex())
					break
				}
				txReceipt, err := buildTxReceipt(receipt)
				if err != nil {
					tc.logger.Errorw("TxConfirmer#fetchReceipts: buildTxReceipt failed",
						"err", err,
					)
				}
				err = tc.saveReceipt(tx, attempt, txReceipt)
				if err != nil {
					tc.logger.Errorw("TxConfirmer#fetchReceipts: saveReceipt failed",
						"err", err,
					)
				}
				break
			} else {
				tc.logger.Debugw("TxConfirmer#fetchReceipts: still waiting for receipt",
					"txHash", attempt.Hash.Hex(),
					"txAttemptID", attempt.ID,
					"txID", tx.ID,
				)
			}
		}
	}
}

func (tc *txConfirmer) fetchReceipt(ctx context.Context, hash gethCommon.Hash) (*gethTypes.Receipt, error) {
	ctx, cancel := context.WithTimeout(ctx, maxEthNodeRequestTime)
	defer cancel()
	receipt, err := tc.ethClient.TransactionReceipt(ctx, hash)
	if err != nil && err.Error() == "not found" {
		return nil, nil
	}
	return receipt, err
}

func buildTxReceipt(receipt *gethTypes.Receipt) (*models.TxReceipt, error) {
	if receipt.BlockNumber == nil {
		return nil, errors.Errorf("receipt was missing block number: %#v", receipt)
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return nil, errors.Wrap(err, "serializeReceipt failed to marshal")
	}
	txReceipt := models.TxReceipt{
		ID:               uuid.New(),
		Receipt:          receiptJSON,
		TxHash:           receipt.TxHash,
		BlockHash:        receipt.BlockHash,
		BlockNumber:      receipt.BlockNumber.Int64(),
		TransactionIndex: receipt.TransactionIndex,
	}
	return &txReceipt, nil
}

func (tc *txConfirmer) saveReceipt(
	tx *models.Tx,
	attempt *models.TxAttempt,
	receipt *models.TxReceipt,
) error {
	insert := true
	for _, receiptID := range attempt.TxReceiptIDs {
		currReceipt, getReceiptErr := tc.store.GetTxReceipt(receiptID)
		if getReceiptErr != nil {
			return getReceiptErr
		}
		if bytes.Equal(currReceipt.BlockHash[:], receipt.BlockHash[:]) &&
			bytes.Equal(currReceipt.TxHash[:], receipt.TxHash[:]) {
			// Conflict here shouldn't be possible because there should only ever
			// be one receipt for a Tx, and if it exists then the transaction
			// is marked confirmed which means we can never get here.
			// However, even so, it still shouldn't be an error to re-insert a receipt we already have.
			insert = false
			break
		}
	}

	if insert {
		putReceiptErr := tc.store.PutTxReceipt(receipt)
		if putReceiptErr != nil {
			return putReceiptErr
		}
		attempt.TxReceiptIDs = append(attempt.TxReceiptIDs, receipt.ID)
		putAttemptErr := tc.store.PutTxAttempt(attempt)
		if putAttemptErr != nil {
			return putAttemptErr
		}
	}
	tx.State = models.TxStateConfirmed
	return tc.store.PutTx(tx)
}

func (tc *txConfirmer) markOldTxsMissingReceiptAsErrored(ctx context.Context, blockNum int64) error {
	cutoff := blockNum - tc.config.FinalityDepth
	if cutoff <= 0 {
		return nil
	}
	// TODO: log missing receipt error?
	return tc.store.MarkOldTxsMissingReceiptAsErrored(cutoff)
}

func (tc *txConfirmer) BumpGasWhereNecessary(ctx context.Context, accounts []*models.Account, blockHeight int64) error {
	var wg sync.WaitGroup

	// It is safe to process separate accounts concurrently
	// NOTE: This design will block one account if another takes a really long time to execute
	wg.Add(len(accounts))
	errors := []error{}
	var errMu sync.Mutex
	for _, account := range accounts {
		go func(fromAddress gethCommon.Address) {
			if err := tc.bumpGasWhereNecessary(ctx, fromAddress, blockHeight); err != nil {
				errMu.Lock()
				errors = append(errors, err)
				errMu.Unlock()
				tc.logger.Errorw("Error in BumpGasWhereNecessary",
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

func (tc *txConfirmer) bumpGasWhereNecessary(ctx context.Context, address gethCommon.Address, blockHeight int64) error {
	if err := tc.handleAllInProgressAttempts(ctx, address, blockHeight); err != nil {
		return errors.Wrap(err, "handleAllInProgressAttempts failed")
	}

	threshold := tc.config.GasBumpThreshold
	depth := tc.config.GasBumpTxDepth
	txs, err := tc.store.GetTxsRequiringNewAttempt(address, blockHeight, threshold, depth)
	if err != nil {
		return errors.Wrap(err, "GetTxsRequiringNewAttempt failed")
	}
	if len(txs) > 0 {
		tc.logger.Debugf("TxConfirmer: Bumping gas for %v transactions", len(txs))
	}
	for _, tx := range txs {
		attempt, err := tc.newAttemptWithGasBump(tx)
		if err != nil {
			return errors.Wrap(err, "newAttemptWithGasBump failed")
		}

		if err := tc.saveInProgressAttempt(tx, attempt); err != nil {
			return errors.Wrap(err, "saveInProgressAttempt failed")
		}

		if err := tc.handleInProgressAttempt(ctx, tx, attempt, blockHeight); err != nil {
			return errors.Wrap(err, "handleInProgressTxAttempt failed")
		}
	}
	return nil
}

// handleAllInProgressAttempts handles "in_progress" attempts were left behind after a
// crash/restart and may or may not have been sent. Ensure they get on-chain so we can fetch a
// receipt.
func (tc *txConfirmer) handleAllInProgressAttempts(ctx context.Context, address gethCommon.Address, blockHeight int64) error {
	attempts, err := tc.store.GetInProgressAttempts(address)
	if err != nil {
		return errors.Wrap(err, "GetInProgressTxAttempts failed")
	}
	for _, attempt := range attempts {
		tx, err := tc.store.GetTx(attempt.TxID)
		if err != nil {
			return errors.Wrap(err, "handleInProgressAttempt cannot get tx")
		}
		if err := tc.handleInProgressAttempt(ctx, tx, attempt, blockHeight); err != nil {
			return errors.Wrap(err, "handleInProgressAttempt failed")
		}
	}
	return nil
}

func (tc *txConfirmer) newAttemptWithGasBump(tx *models.Tx) (attempt *models.TxAttempt, err error) {
	errStr := "newAttemptWithGasBump failed"
	var bumpedGasPrice *big.Int
	numAttempts := len(tx.TxAttemptIDs)
	if numAttempts > 0 {
		// Get attempt with highest gas price
		previousAttempt, getPreviousAttemptErr := tc.store.GetTxAttempt(tx.TxAttemptIDs[0])
		if getPreviousAttemptErr != nil {
			return nil, errors.Wrap(getPreviousAttemptErr, errStr)
		}
		if previousAttempt.State == models.TxAttemptStateInsufficientEth {
			// Do not create a new attempt if we ran out of eth last time since bumping gas is pointless
			// Instead try to resubmit the same attempt at the same price, in the hope that the wallet was funded since our last attempt
			previousAttempt.State = models.TxAttemptStateInProgress
			return previousAttempt, nil
		}
		previousGasPrice := previousAttempt.GasPrice
		bumpedGasPrice, err = BumpGas(tc.config, previousGasPrice)
		if err != nil {
			tc.logger.Errorw("Failed to bump gas",
				"err", err,
				"txID", tx.ID,
				"txHash", previousAttempt.Hash,
				"originalGasPrice", previousGasPrice.String(),
				"maxGasPrice", tc.config.MaxGasPrice,
			)
			// Do not create a new attempt if bumping gas would put us over the limit or cause some other problem
			// Instead try to resubmit the previous attempt, and keep resubmitting until its accepted
			previousAttempt.BroadcastBeforeBlockNum = -1
			previousAttempt.State = models.TxAttemptStateInProgress
			return previousAttempt, nil
		}
	} else {
		tc.logger.Errorf("Invariant violation: Tx %v was unconfirmed but didn't have any attempts. "+
			"Falling back to default gas price instead.", tx.ID)
		bumpedGasPrice = tc.config.DefaultGasPrice
	}
	return newAttempt(tc.keyStore, tc.config, tx, bumpedGasPrice)
}

func (tc *txConfirmer) saveInProgressAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	errStr := "saveInProgressAttempt failed"
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.Wrap(errors.New("expect attempt state to be in_progress"), errStr)
	}
	err := tc.store.PutTxAttempt(attempt)
	if err != nil {
		return errors.Wrap(err, errStr)
	}
	err = tc.store.AddOrUpdateAttempt(tx, attempt)
	if err != nil {
		return errors.Wrap(err, errStr)
	}
	return nil
}

func (tc *txConfirmer) handleInProgressAttempt(ctx context.Context, tx *models.Tx, attempt *models.TxAttempt, blockHeight int64) error {
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.Errorf("invariant violation: expected TxAttempt %v to be in_progress, it was %s", attempt.ID, attempt.State)
	}

	sendError := sendTx(ctx, tc.ethClient, attempt, tc.logger)

	if sendError.IsTerminallyUnderpriced() {
		// This should really not ever happen in normal operation since we
		// already bumped above the required minimum in TxBroadcaster.
		//
		// It could conceivably happen if the remote eth node changed it's configuration.
		bumpedGasPrice, err := BumpGas(tc.config, attempt.GasPrice)
		if err != nil {
			return errors.Wrap(err, "could not bump gas for terminally underpriced transaction")
		}
		tc.logger.Errorf("gas price %v wei was rejected by the eth node for being too low. "+
			"Eth node returned: '%s'. "+
			"Bumping to %v wei and retrying. "+
			"ACTION REQUIRED: You should consider increasing DefaultGasPrice", attempt.GasPrice, sendError.Error(), bumpedGasPrice)
		replacementAttempt, err := newAttempt(tc.keyStore, tc.config, tx, bumpedGasPrice)
		if err != nil {
			return errors.Wrap(err, "newAttempt failed")
		}

		if err := saveReplacementInProgressAttempt(tc.store, tx, attempt, replacementAttempt); err != nil {
			return errors.Wrap(err, "saveReplacementInProgressAttempt failed")
		}
		return tc.handleInProgressAttempt(ctx, tx, replacementAttempt, blockHeight)
	}

	if sendError.IsTemporarilyUnderpriced() {
		// Most likely scenario here is a parity node that is rejecting
		// low-priced transactions due to mempool pressure
		//
		// In that case, the safest thing to do is to pretend the transaction
		// was accepted and continue the normal gas bumping cycle until we can
		// get it into the mempool
		tc.logger.Infow("TxConfirmer: Transaction temporarily underpriced",
			"txID", tx.ID,
			"attemptID", attempt.ID,
			"err", sendError.Error(),
			"gasPriceWei", attempt.GasPrice.String(),
		)
		sendError = nil
	}

	if sendError.Fatal() {
		// This is an invariant violation. The TxBroadcaster can never create an TxAttempt that
		// will fatally error. The only scenario imaginable where this might take place is if
		// geth / parity have been updated between broadcasting and confirming steps.
		tc.logger.Errorw("invariant violation: fatal error while re-attempting transaction",
			"txID", tx.ID,
			"err", sendError,
			"signedRawTx", hexutil.Encode(attempt.SignedRawTx),
			"blockHeight", blockHeight,
		)
		// This will loop continuously on every new head so it must be handled manually.
		return tc.deleteInProgressAttempt(tx, attempt)
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
		tc.logger.Errorw(fmt.Sprintf("TxConfirmer: replacement transaction underpriced at %v wei for Tx %v. "+
			"Eth node returned error: '%s'. "+
			"Either you have set ETH_GAS_BUMP_PERCENT (currently %v%%) too low or an external wallet used this account. "+
			"Please note that using your node's private keys outside of eth-service is NOT SUPPORTED and can lead to missed transactions.",
			attempt.GasPrice.Int64(), tx.ID, sendError.Error(), tc.config.GasBumpPercent), "err", sendError)

		// Assume success and hand off to the next cycle.
		sendError = nil
	}

	if sendError.IsInsufficientEth() {
		tc.logger.Errorw(fmt.Sprintf("TxConfirmer: TxAttempt %v (hash 0x%x) at gas price (%s Wei) was rejected due to insufficient eth. "+
			"The eth node returned %s. "+
			"ACTION REQUIRED: Wallet with address 0x%x is OUT OF FUNDS",
			attempt.ID, attempt.Hash, attempt.GasPrice.String(), sendError.Error(), tx.FromAddress,
		), "err", sendError)
		return tc.saveInsufficientEthAttempt(tx, attempt)
	}

	if sendError == nil {
		return tc.saveBroadcastAttempt(tx, attempt)
	}

	// Any other type of error is considered temporary or resolvable manually. The node may have it
	// in the mempool so we must keep the attempt (leave it in_progress). Safest thing to do is bail
	// out and wait for the next head.
	return errors.Wrapf(sendError, "unexpected error sending Tx %v with hash %s", tx.ID, attempt.Hash.Hex())
}

func (tc *txConfirmer) deleteInProgressAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	errStr := "deleteInProgressAttempt failed"
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.Wrap(errors.New("expected attempt state to be in_progress"), errStr)
	}
	return errors.Wrap(tc.deleteAttempt(tx, attempt), errStr)
}

func (tc *txConfirmer) saveBroadcastAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	errStr := "saveBroadcastAttempt failed"
	if attempt.State != models.TxAttemptStateInProgress {
		return errors.Wrap(errors.New("expected attempt state to be in_progress"), errStr)
	}
	attempt.State = models.TxAttemptStateBroadcast
	return errors.Wrap(tc.store.PutTxAttempt(attempt), errStr)
}

func (tc *txConfirmer) saveInsufficientEthAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	errStr := "saveInsufficientEthAttempt failed"
	if !(attempt.State == models.TxAttemptStateInProgress || attempt.State == models.TxAttemptStateInsufficientEth) {
		return errors.Wrap(errors.New("expected attempt state to be either in_progress or insufficient_eth"), errStr)
	}
	attempt.State = models.TxAttemptStateInsufficientEth
	return errors.Wrap(tc.store.PutTxAttempt(attempt), errStr)
}

func (tc *txConfirmer) deleteAttempt(tx *models.Tx, attempt *models.TxAttempt) error {
	errStr := "deleteAttempt failed"
	// Update TxAttemptIDs
	index := -1
	for i, currAttemptID := range tx.TxAttemptIDs {
		if bytes.Equal(currAttemptID[:], attempt.ID[:]) {
			index = i
			break
		}
	}
	if index == -1 {
		return errors.Wrap(errors.New("attempt not found"), errStr)
	}
	copy(tx.TxAttemptIDs[index:], tx.TxAttemptIDs[index+1:])
	tx.TxAttemptIDs = tx.TxAttemptIDs[:len(tx.TxAttemptIDs)-1]
	putTxErr := tc.store.PutTx(tx)
	if putTxErr != nil {
		return errors.Wrap(putTxErr, errStr)
	}
	// Remove receipts if any
	for _, receiptID := range attempt.TxReceiptIDs {
		deleteReceiptErr := tc.store.DeleteTxReceipt(receiptID)
		if deleteReceiptErr != nil {
			return errors.Wrap(deleteReceiptErr, errStr)
		}
	}
	deleteAttemptErr := tc.store.DeleteTxAttempt(attempt.ID)
	if deleteAttemptErr != nil {
		return errors.Wrap(deleteAttemptErr, errStr)
	}
	return nil
}

// EnsureConfirmedTxsInLongestChain finds all confirmed Txs up to the depth
// of the given chain and ensures that every one has a receipt with a block hash that is
// in the given chain.
//
// If any of the confirmed transactions does not have a receipt in the chain, it has been
// re-org'd out and will be rebroadcast.
//
// TODO: Mark txs finalized if confirmed for long enough.
func (tc *txConfirmer) EnsureConfirmedTxsInLongestChain(ctx context.Context, accounts []*models.Account, head *models.Head) error {
	txs, err := tc.store.GetTxsConfirmedAtOrAboveBlockHeight(head.EarliestInChain().Number)
	if err != nil {
		return errors.Wrap(err, "getTxsConfirmedAtOrAboveBlockHeight failed")
	}

	for _, tx := range txs {
		hasReceipt, hasReceiptErr := tc.hasReceiptInLongestChain(tx, head)
		if hasReceiptErr != nil {
			return errors.Wrap(hasReceiptErr, "hasReceiptInLongestChain failed")

		}
		if !hasReceipt {
			if err := tc.markForRebroadcast(tx); err != nil {
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
			if err := tc.handleAllInProgressAttempts(ctx, fromAddress, head.Number); err != nil {
				errMu.Lock()
				errors = append(errors, err)
				errMu.Unlock()
				tc.logger.Errorw("Error in BumpGasWhereNecessary", "error", err, "fromAddress", fromAddress)
			}

			wg.Done()
		}(account.Address)
	}

	wg.Wait()

	return multierr.Combine(errors...)
}

func (tc *txConfirmer) hasReceiptInLongestChain(tx *models.Tx, head *models.Head) (bool, error) {
	for {
		for _, attemptID := range tx.TxAttemptIDs {
			attempt, getAttemptErr := tc.store.GetTxAttempt(attemptID)
			if getAttemptErr != nil {
				return false, getAttemptErr
			}
			for _, receiptID := range attempt.TxReceiptIDs {
				receipt, getReceiptErr := tc.store.GetTxReceipt(receiptID)
				if getReceiptErr != nil {
					return false, getReceiptErr
				}
				if receipt.BlockHash == head.Hash && receipt.BlockNumber == head.Number {
					return true, nil
				}
			}
		}
		if head.Parent == nil {
			return false, nil
		}
		head = head.Parent
	}
}

func (tc *txConfirmer) markForRebroadcast(tx *models.Tx) error {
	numAttempts := len(tx.TxAttemptIDs)
	if numAttempts == 0 {
		return errors.Errorf("invariant violation: expected Tx %v to have at least one attempt", tx.ID)
	}

	// Rebroadcast the one with the highest gas price
	attemptID := tx.TxAttemptIDs[0]
	attempt, err := tc.store.GetTxAttempt(attemptID)
	if err != nil {
		return err
	}

	// Put it back in progress and delete all receipts (they do not apply to the new chain)
	err = tc.deleteAllReceipts(tx)
	if err != nil {
		return err
	}
	err = tc.unconfirmTx(tx)
	if err != nil {
		return errors.Wrapf(err, "unconfirmTx failed for tx %v", tx.ID)
	}
	err = tc.unbroadcastAttempt(attempt)
	if err != nil {
		return errors.Wrapf(err, "unbroadcastAttempt failed for tx %v", tx.ID)
	}
	return errors.Wrap(err, "markForRebroadcast failed")
}

func (tc *txConfirmer) deleteAllReceipts(tx *models.Tx) error {
	for _, attemptID := range tx.TxAttemptIDs {
		attempt, getAttemptErr := tc.store.GetTxAttempt(attemptID)
		if getAttemptErr != nil {
			return getAttemptErr
		}
		for _, receiptID := range attempt.TxReceiptIDs {
			deleteErr := tc.store.DeleteTxReceipt(receiptID)
			if deleteErr != nil {
				return deleteErr
			}
		}
		attempt.TxReceiptIDs = make([]uuid.UUID, 0)
		putAttemptErr := tc.store.PutTxAttempt(attempt)
		if putAttemptErr != nil {
			return putAttemptErr
		}
	}
	return nil
}

func (tc *txConfirmer) unconfirmTx(tx *models.Tx) error {
	if tx.State != models.TxStateConfirmed {
		return errors.New("expected Tx.State to be confirmed")
	}
	tx.State = models.TxStateUnconfirmed
	return tc.store.PutTx(tx)
}

func (tc *txConfirmer) unbroadcastAttempt(attempt *models.TxAttempt) error {
	if attempt.State != models.TxAttemptStateBroadcast {
		return errors.New("expected TxAttempt.State to be broadcast")
	}
	attempt.State = models.TxAttemptStateInProgress
	attempt.BroadcastBeforeBlockNum = -1
	return tc.store.PutTxAttempt(attempt)
}
