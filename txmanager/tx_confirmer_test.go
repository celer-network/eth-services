package txmanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/celer-network/eth-services/client"
	"github.com/celer-network/eth-services/internal/mocks"
	esTesting "github.com/celer-network/eth-services/internal/testing"
	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/txmanager"
	"github.com/pkg/errors"

	"github.com/celer-network/eth-services/store/models"

	gethAccounts "github.com/ethereum/go-ethereum/accounts"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func mustAddTxToAccount(t *testing.T, store esStore.Store, fromAddress gethCommon.Address, tx *models.Tx) {
	t.Helper()

	account, err := store.GetAccount(fromAddress)
	require.NoError(t, err)
	account.TxIDs = append(account.TxIDs, tx.ID)
	require.NoError(t, store.PutAccount(account))
}

func mustInsertUnstartedTx(t *testing.T, store esStore.Store, fromAddress gethCommon.Address) {
	t.Helper()

	tx := esTesting.NewTx(t, fromAddress)
	tx.State = models.TxStateUnstarted
	require.NoError(t, store.PutTx(tx))
	mustAddTxToAccount(t, store, fromAddress, tx)
}

func newBroadcastTxAttempt(t *testing.T, txID uuid.UUID, store esStore.Store, gasPrice ...int64) *models.TxAttempt {
	t.Helper()

	attempt := esTesting.NewTxAttempt(t, txID)
	attempt.State = models.TxAttemptStateBroadcast
	if len(gasPrice) > 0 {
		gp := gasPrice[0]
		attempt.GasPrice = big.NewInt(gp)
	}
	return attempt
}

func mustInsertInProgressTx(t *testing.T, store esStore.Store, nonce int64, fromAddress gethCommon.Address) models.Tx {
	t.Helper()

	tx := esTesting.NewTx(t, fromAddress)
	tx.State = models.TxStateInProgress
	tx.Nonce = nonce
	require.NoError(t, store.PutTx(tx))
	mustAddTxToAccount(t, store, fromAddress, tx)
	return *tx
}

func mustInsertConfirmedTx(t *testing.T, store esStore.Store, nonce int64, fromAddress gethCommon.Address) models.Tx {
	t.Helper()

	tx := esTesting.NewTx(t, fromAddress)
	tx.State = models.TxStateConfirmed
	tx.Nonce = nonce
	require.NoError(t, store.PutTx(tx))
	mustAddTxToAccount(t, store, fromAddress, tx)
	return *tx
}

func TestTxConfirmer_SetBroadcastBeforeBlockNum(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	require.NoError(t, os.RemoveAll(config.KeysDir))
	keyStore := client.NewInsecureKeyStore(config.KeysDir)
	_, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, 0)
	ethClient := new(mocks.Client)

	txmanager.NewTxConfirmer(ethClient, store, keyStore, config)

	tx := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, 0, fromAddress)

	headNum := int64(9000)
	var err error

	t.Run("saves block num to unconfirmed TxAttempts without one", func(t *testing.T) {
		// Do the thing
		require.NoError(t, store.SetBroadcastBeforeBlockNum(headNum))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)
		require.Len(t, tx.TxAttemptIDs, 1)
		attempt, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)

		assert.Equal(t, int64(9000), attempt.BroadcastBeforeBlockNum)
	})

	t.Run("does not change TxAttempts that already have BroadcastBeforeBlockNum set", func(t *testing.T) {
		n := int64(42)
		attempt := newBroadcastTxAttempt(t, tx.ID, store, 2)
		attempt.BroadcastBeforeBlockNum = n
		require.NoError(t, store.PutTxAttempt(attempt))
		esTesting.MustAddAttemptToTx(t, store, tx.ID, attempt)

		// Do the thing
		require.NoError(t, store.SetBroadcastBeforeBlockNum(headNum))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)
		require.Len(t, tx.TxAttemptIDs, 2)
		// tx.TxAttemptIDs[0] now points to the second attempt because its GasPrice is higher
		attempt, err = store.GetTxAttempt(tx.TxAttemptIDs[0])

		assert.Equal(t, int64(42), attempt.BroadcastBeforeBlockNum)
	})
}

func TestTxConfirmer_CheckForReceipts(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	require.NoError(t, os.RemoveAll(config.KeysDir))
	keyStore := client.NewInsecureKeyStore(config.KeysDir)
	_, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore)
	ethClient := new(mocks.Client)

	tc := txmanager.NewTxConfirmer(ethClient, store, keyStore, config)

	nonce := int64(0)
	var err error
	ctx := context.Background()
	blockNum := int64(0)

	t.Run("only finds Txs in unconfirmed state", func(t *testing.T) {
		esTesting.MustInsertFatalErrorTx(t, store, fromAddress)
		mustInsertInProgressTx(t, store, nonce, fromAddress)
		nonce++
		esTesting.MustInsertConfirmedTxWithAttempt(t, store, nonce, 1, fromAddress)
		nonce++
		mustInsertUnstartedTx(t, store, fromAddress)

		// Do the thing
		require.NoError(t, tc.CheckForReceipts(ctx, blockNum))
		// No calls
		ethClient.AssertExpectations(t)
	})

	tx1 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	require.Len(t, tx1.TxAttemptIDs, 1)
	attempt1_1, err := store.GetTxAttempt(tx1.TxAttemptIDs[0])
	require.NoError(t, err)
	require.Len(t, attempt1_1.TxReceiptIDs, 0)

	t.Run("fetches receipt for an unconfirmed Tx", func(t *testing.T) {
		// Transaction not confirmed yet, receipt is nil
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_1.Hash
		})).Return(nil, errors.New("not found")).Once()

		// Do the thing
		require.NoError(t, tc.CheckForReceipts(ctx, blockNum))

		tx1, err = store.GetTx(tx1.ID)
		require.NoError(t, err)
		require.Len(t, tx1.TxAttemptIDs, 1)
		attempt1_1, err := store.GetTxAttempt(tx1.TxAttemptIDs[0])
		require.NoError(t, err)
		require.Len(t, attempt1_1.TxReceiptIDs, 0)

		ethClient.AssertExpectations(t)
	})

	t.Run("saves nothing if returned receipt does not match the attempt", func(t *testing.T) {
		gethReceipt := gethTypes.Receipt{
			TxHash:           esTesting.NewHash(),
			BlockHash:        esTesting.NewHash(),
			BlockNumber:      big.NewInt(42),
			TransactionIndex: uint(1),
		}

		// First transaction confirmed
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_1.Hash
		})).Return(&gethReceipt, nil).Once()

		// Do the thing
		// No error because it is merely logged
		require.NoError(t, tc.CheckForReceipts(ctx, blockNum))

		tx, err := store.GetTx(tx1.ID)
		require.NoError(t, err)
		assert.Len(t, tx.TxAttemptIDs, 1)
		attempt, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		require.Len(t, attempt.TxReceiptIDs, 0)
	})

	tx2 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	require.Len(t, tx2.TxAttemptIDs, 1)
	attempt2_1, err := store.GetTxAttempt(tx2.TxAttemptIDs[0])
	require.NoError(t, err)
	require.Len(t, attempt2_1.TxReceiptIDs, 0)

	t.Run("saves eth_receipt and marks Tx as confirmed when geth client returns valid receipt", func(t *testing.T) {
		gethReceipt := gethTypes.Receipt{
			TxHash:           attempt1_1.Hash,
			BlockHash:        esTesting.NewHash(),
			BlockNumber:      big.NewInt(42),
			TransactionIndex: uint(1),
		}

		// First transaction confirmed
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_1.Hash
		})).Return(&gethReceipt, nil).Once()
		// Second transaction still unconfirmed
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt2_1.Hash
		})).Return(nil, errors.New("not found")).Once()

		// Do the thing
		require.NoError(t, tc.CheckForReceipts(ctx, blockNum))

		// Check that the receipt was saved
		tx, err := store.GetTx(tx1.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateConfirmed, tx.State)
		assert.Len(t, tx.TxAttemptIDs, 1)
		attempt1_1, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		require.Len(t, attempt1_1.TxReceiptIDs, 1)

		txReceipt, err := store.GetTxReceipt(attempt1_1.TxReceiptIDs[0])
		require.NoError(t, err)

		assert.Equal(t, gethReceipt.TxHash, txReceipt.TxHash)
		assert.Equal(t, gethReceipt.BlockHash, txReceipt.BlockHash)
		assert.Equal(t, gethReceipt.BlockNumber.Int64(), txReceipt.BlockNumber)
		assert.Equal(t, gethReceipt.TransactionIndex, txReceipt.TransactionIndex)

		receiptJSON, err := json.Marshal(gethReceipt)
		require.NoError(t, err)

		assert.JSONEq(t, string(receiptJSON), string(txReceipt.Receipt))

		ethClient.AssertExpectations(t)
	})

	t.Run("fetches and saves receipts for several attempts in gas price order", func(t *testing.T) {
		attempt2_2 := newBroadcastTxAttempt(t, tx2.ID, store)
		attempt2_2.GasPrice = big.NewInt(10)

		attempt2_3 := newBroadcastTxAttempt(t, tx2.ID, store)
		attempt2_3.GasPrice = big.NewInt(20)

		// Insert order deliberately reversed to test sorting by gas price
		require.NoError(t, store.PutTxAttempt(attempt2_3))
		require.NoError(t, store.PutTxAttempt(attempt2_2))
		esTesting.MustAddAttemptToTx(t, store, tx2.ID, attempt2_3)
		esTesting.MustAddAttemptToTx(t, store, tx2.ID, attempt2_2)

		// Most expensive attempt still unconfirmed
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt2_3.Hash
		})).Return(nil, errors.New("not found")).Once()

		gethReceipt := gethTypes.Receipt{
			TxHash:           attempt2_2.Hash,
			BlockHash:        esTesting.NewHash(),
			BlockNumber:      big.NewInt(42),
			TransactionIndex: uint(1),
		}
		// Second most expensive attempt is confirmed
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt2_2.Hash
		})).Return(&gethReceipt, nil).Once()

		// Do the thing
		require.NoError(t, tc.CheckForReceipts(ctx, blockNum))

		ethClient.AssertExpectations(t)

		// Check that the state was updated
		tx, err := store.GetTx(tx2.ID)
		require.NoError(t, err)

		require.Equal(t, models.TxStateConfirmed, tx.State)
		require.Len(t, tx.TxAttemptIDs, 3)
	})

	tx3 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	attempt3_1, err := store.GetTxAttempt(tx3.TxAttemptIDs[0])
	require.NoError(t, err)
	nonce++

	t.Run("ignores error that comes from querying parity too early", func(t *testing.T) {
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt3_1.Hash
		})).Return(nil, errors.New("missing required field 'transactionHash' for Log")).Once()

		// Do the thing
		require.NoError(t, tc.CheckForReceipts(ctx, blockNum))

		// No receipt, but no error either
		tx, err := store.GetTx(tx3.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx.State)
		assert.Len(t, tx.TxAttemptIDs, 1)
		attempt3_1, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.Len(t, attempt3_1.TxReceiptIDs, 0)
	})

	t.Run("ignores partially hydrated receipt that comes from querying parity too early", func(t *testing.T) {
		receipt := gethTypes.Receipt{
			TxHash: attempt3_1.Hash,
		}
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt3_1.Hash
		})).Return(&receipt, nil).Once()

		// Do the thing
		require.NoError(t, tc.CheckForReceipts(ctx, blockNum))

		// No receipt, but no error either
		tx, err := store.GetTx(tx3.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx.State)
		assert.Len(t, tx.TxAttemptIDs, 1)
		attempt3_1, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.Len(t, attempt3_1.TxReceiptIDs, 0)
	})

	t.Run("handles case where TxReceipt already exists somehow", func(t *testing.T) {
		txReceipt := esTesting.MustInsertTxReceipt(t, store, 42, esTesting.NewHash(), attempt3_1.Hash, attempt3_1)

		gethReceipt := gethTypes.Receipt{
			TxHash:           attempt3_1.Hash,
			BlockHash:        txReceipt.BlockHash,
			BlockNumber:      big.NewInt(txReceipt.BlockNumber),
			TransactionIndex: txReceipt.TransactionIndex,
		}
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt3_1.Hash
		})).Return(&gethReceipt, nil).Once()

		// Do the thing
		require.NoError(t, tc.CheckForReceipts(ctx, blockNum))

		// Check that the receipt was unchanged
		tx, err := store.GetTx(tx3.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateConfirmed, tx.State)
		assert.Len(t, tx.TxAttemptIDs, 1)
		attempt3_1, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		require.Len(t, attempt3_1.TxReceiptIDs, 1)

		txReceipt, err = store.GetTxReceipt(attempt3_1.TxReceiptIDs[0])
		require.NoError(t, err)

		assert.Equal(t, gethReceipt.TxHash, txReceipt.TxHash)
		assert.Equal(t, gethReceipt.BlockHash, txReceipt.BlockHash)
		assert.Equal(t, gethReceipt.BlockNumber.Int64(), txReceipt.BlockNumber)
		assert.Equal(t, gethReceipt.TransactionIndex, txReceipt.TransactionIndex)

		ethClient.AssertExpectations(t)
	})
}

func TestTxConfirmer_CheckForReceipts_confirmed_missing_receipt(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	config.FinalityDepth = 50
	require.NoError(t, os.RemoveAll(config.KeysDir))
	keyStore := client.NewInsecureKeyStore(config.KeysDir)
	_, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, 0)
	ethClient := new(mocks.Client)

	tc := txmanager.NewTxConfirmer(ethClient, store, keyStore, config)
	ctx := context.Background()

	// STATE
	// Txs with nonce 0 has two attempts (broadcast before block 21 and 41) the first of which will get a receipt
	// Txs with nonce 1 has two attempts (broadcast before block 21 and 41) neither of which will ever get a receipt
	// Txs with nonce 2 has an attempt (broadcast before block 41) that will not get a receipt on the first try but will get one later
	// Txs with nonce 3 has an attempt (broadcast before block 41) that has been confirmed in block 42
	// All other attempts were broadcast before block 41
	b := int64(21)

	tx0 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, 0, fromAddress)
	require.Len(t, tx0.TxAttemptIDs, 1)
	attempt0_1, err := store.GetTxAttempt(tx0.TxAttemptIDs[0])
	require.NoError(t, err)
	require.Len(t, attempt0_1.TxReceiptIDs, 0)
	attempt0_2 := newBroadcastTxAttempt(t, tx0.ID, store)
	// Of course it didn't confirm... we didn't pay anything for gas!
	attempt0_2.GasPrice = big.NewInt(0)
	attempt0_2.BroadcastBeforeBlockNum = b
	require.NoError(t, store.PutTxAttempt(attempt0_2))
	esTesting.MustAddAttemptToTx(t, store, tx0.ID, attempt0_2)

	tx1 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, 1, fromAddress)
	require.Len(t, tx1.TxAttemptIDs, 1)
	attempt1_1, err := store.GetTxAttempt(tx1.TxAttemptIDs[0])
	require.NoError(t, err)
	require.Len(t, attempt1_1.TxReceiptIDs, 0)
	attempt1_2 := newBroadcastTxAttempt(t, tx1.ID, store)
	// Of course it didn't confirm... we didn't pay anything for gas!
	attempt1_2.GasPrice = big.NewInt(0)
	attempt1_2.BroadcastBeforeBlockNum = b
	require.NoError(t, store.PutTxAttempt(attempt1_2))
	esTesting.MustAddAttemptToTx(t, store, tx1.ID, attempt1_2)

	tx2 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, 2, fromAddress)
	require.Len(t, tx2.TxAttemptIDs, 1)
	attempt2_1, err := store.GetTxAttempt(tx2.TxAttemptIDs[0])
	require.NoError(t, err)
	require.Len(t, attempt2_1.TxReceiptIDs, 0)

	tx3 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, 3, fromAddress)
	require.Len(t, tx3.TxAttemptIDs, 1)
	attempt3_1, err := store.GetTxAttempt(tx3.TxAttemptIDs[0])
	require.NoError(t, err)
	require.Len(t, attempt3_1.TxReceiptIDs, 0)

	// Force set BroadcastBeforeBlockNum
	err = store.SetBroadcastBeforeBlockNum(41)
	require.NoError(t, err)

	t.Run("marks buried Txs as 'confirmed_missing_receipt'", func(t *testing.T) {
		gethReceipt0 := gethTypes.Receipt{
			TxHash:           attempt0_1.Hash,
			BlockHash:        esTesting.NewHash(),
			BlockNumber:      big.NewInt(42),
			TransactionIndex: uint(1),
		}
		gethReceipt3 := gethTypes.Receipt{
			TxHash:           attempt3_1.Hash,
			BlockHash:        esTesting.NewHash(),
			BlockNumber:      big.NewInt(42),
			TransactionIndex: uint(1),
		}
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt3_1.Hash
		})).Return(&gethReceipt3, nil).Once()
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt2_1.Hash
		})).Return(nil, errors.New("not found")).Once()
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_1.Hash
		})).Return(nil, errors.New("not found")).Once()
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_2.Hash
		})).Return(nil, errors.New("not found")).Once()
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt0_1.Hash
		})).Return(&gethReceipt0, nil).Once()

		// PERFORM
		// Block num of 43 is one higher than the receipt (as would generally be expected)
		require.NoError(t, tc.CheckForReceipts(ctx, 43))

		ethClient.AssertExpectations(t)

		// Expected state is that the "top" Tx is now confirmed, with the
		// two below it "confirmed_missing_receipt" and the "bottom" Tx also confirmed
		tx3, err = store.GetTx(tx3.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx3.State)

		attempt3_1, err := store.GetTxAttempt(tx3.TxAttemptIDs[0])
		require.NoError(t, err)
		txReceipt, err := store.GetTxReceipt(attempt3_1.TxReceiptIDs[0])
		require.NoError(t, err)
		require.Equal(t, gethReceipt3.BlockHash, txReceipt.BlockHash)

		tx2, err = store.GetTx(tx2.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmedMissingReceipt, tx2.State)
		tx1, err = store.GetTx(tx1.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmedMissingReceipt, tx1.State)

		tx0, err = store.GetTx(tx0.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx0.State)

		attempt0_1, err = store.GetTxAttempt(tx0.TxAttemptIDs[0])
		require.NoError(t, err)
		txReceipt, err = store.GetTxReceipt(attempt0_1.TxReceiptIDs[0])
		require.NoError(t, err)
		require.Equal(t, gethReceipt0.BlockHash, txReceipt.BlockHash)
	})

	// STATE
	// Txs with nonce 0 is confirmed
	// Txs with nonce 1 is confirmed_missing_receipt
	// Txs with nonce 2 is confirmed_missing_receipt
	// Txs with nonce 3 is confirmed

	t.Run("marks Txs with state 'confirmed_missing_receipt' as 'confirmed' if a receipt finally shows up", func(t *testing.T) {
		gethReceipt := gethTypes.Receipt{
			TxHash:           attempt2_1.Hash,
			BlockHash:        esTesting.NewHash(),
			BlockNumber:      big.NewInt(43),
			TransactionIndex: uint(1),
		}
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt2_1.Hash
		})).Return(&gethReceipt, nil).Once()
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_1.Hash
		})).Return(nil, errors.New("not found")).Once()
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_2.Hash
		})).Return(nil, errors.New("not found")).Once()

		// PERFORM
		// Block num of 44 is one higher than the receipt (as would generally be expected)
		require.NoError(t, tc.CheckForReceipts(ctx, 44))

		ethClient.AssertExpectations(t)

		// Expected state is that the "top" two Txs are now confirmed, with the
		// one below it still "confirmed_missing_receipt" and the bottom one remains confirmed
		tx3, err = store.GetTx(tx3.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx3.State)
		tx2, err = store.GetTx(tx2.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx2.State)

		attempt2_1, err := store.GetTxAttempt(tx2.TxAttemptIDs[0])
		require.NoError(t, err)
		txReceipt, err := store.GetTxReceipt(attempt2_1.TxReceiptIDs[0])
		require.NoError(t, err)
		require.Equal(t, gethReceipt.BlockHash, txReceipt.BlockHash)

		tx1, err = store.GetTx(tx1.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmedMissingReceipt, tx1.State)
		tx0, err = store.GetTx(tx0.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx0.State)
	})

	// STATE
	// Txs with nonce 0 is confirmed
	// Txs with nonce 1 is confirmed_missing_receipt
	// Txs with nonce 2 is confirmed
	// Txs with nonce 3 is confirmed

	t.Run("continues to leave Txs with state 'confirmed_missing_receipt' unchanged if at least one attempt is above config.FinalityDepth", func(t *testing.T) {
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_1.Hash
		})).Return(nil, errors.New("not found")).Once()
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_2.Hash
		})).Return(nil, errors.New("not found")).Once()

		// PERFORM
		// Block num of 80 puts the first attempt (21) below threshold but second attempt (41) still above
		require.NoError(t, tc.CheckForReceipts(ctx, 80))

		ethClient.AssertExpectations(t)

		// Expected state is that the "top" two Txs are now confirmed, with the
		// one below it still "confirmed_missing_receipt" and the bottom one remains confirmed
		tx3, err = store.GetTx(tx3.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx3.State)
		tx2, err = store.GetTx(tx2.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx2.State)
		tx1, err = store.GetTx(tx1.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmedMissingReceipt, tx1.State)
		tx0, err = store.GetTx(tx0.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx0.State)
	})

	// STATE
	// Txs with nonce 0 is confirmed
	// Txs with nonce 1 is confirmed_missing_receipt
	// Txs with nonce 2 is confirmed
	// Txs with nonce 3 is confirmed

	t.Run("marks Txs with state 'confirmed_missing_receipt' as 'errored' if a receipt fails to show up and all attempts are buried deeper than ETH_FINALITY_DEPTH", func(t *testing.T) {
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_1.Hash
		})).Return(nil, errors.New("not found")).Once()
		ethClient.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(txHash gethCommon.Hash) bool {
			return txHash == attempt1_2.Hash
		})).Return(nil, errors.New("not found")).Once()

		// PERFORM
		// Block num of 100 puts the first attempt (21) and second attempt (41) below threshold
		require.NoError(t, tc.CheckForReceipts(ctx, 100))

		ethClient.AssertExpectations(t)

		// Expected state is that the "top" two Txs are now confirmed, with the
		// one below it marked as "fatal_error" and the bottom one remains confirmed
		tx3, err = store.GetTx(tx3.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx3.State)
		tx2, err = store.GetTx(tx2.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx2.State)
		tx1, err = store.GetTx(tx1.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateFatalError, tx1.State)
		tx0, err = store.GetTx(tx0.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateConfirmed, tx0.State)
	})
}

func TestTxConfirmer_GetTxsRequiringNewAttempt(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	config.FinalityDepth = 50
	require.NoError(t, os.RemoveAll(config.KeysDir))
	keyStore := client.NewInsecureKeyStore(config.KeysDir)
	_, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, 0)

	currentHead := int64(30)
	gasBumpThreshold := int64(10)
	tooNew := int64(21)
	onTheMoney := int64(20)
	oldEnough := int64(19)
	nonce := int64(0)

	mustInsertConfirmedTx(t, store, nonce, fromAddress)
	nonce++

	_, otherAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore)

	t.Run("returns nothing when there are no transactions", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		assert.Len(t, txs, 0)
	})

	mustInsertInProgressTx(t, store, nonce, fromAddress)
	nonce++

	t.Run("returns nothing when the transaction is in_progress", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		assert.Len(t, txs, 0)
	})

	// This one has BroadcastBeforeBlockNum set as nil... which can happen, but it should be ignored
	esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++

	t.Run("ignores unconfirmed transactions with nil BroadcastBeforeBlockNum", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		assert.Len(t, txs, 0)
	})

	tx1 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	attempt1_1, err := store.GetTxAttempt(tx1.TxAttemptIDs[0])
	require.NoError(t, err)
	attempt1_1.BroadcastBeforeBlockNum = tooNew
	require.NoError(t, store.PutTxAttempt(attempt1_1))
	attempt1_2 := newBroadcastTxAttempt(t, tx1.ID, store)
	attempt1_2.BroadcastBeforeBlockNum = onTheMoney
	attempt1_2.GasPrice = big.NewInt(30000)
	require.NoError(t, store.PutTxAttempt(attempt1_2))
	esTesting.MustAddAttemptToTx(t, store, tx1.ID, attempt1_2)

	t.Run("returns nothing when the transaction is unconfirmed with an attempt that is recent", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		assert.Len(t, txs, 0)
	})

	tx2 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	attempt2_1, err := store.GetTxAttempt(tx2.TxAttemptIDs[0])
	attempt2_1.BroadcastBeforeBlockNum = tooNew
	require.NoError(t, store.PutTxAttempt(attempt2_1))

	t.Run("returns nothing when the transaction has attempts that are too new", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		assert.Len(t, txs, 0)
	})

	txWithoutAttempts := esTesting.NewTx(t, fromAddress)
	txWithoutAttempts.Nonce = nonce
	txWithoutAttempts.State = models.TxStateUnconfirmed
	require.NoError(t, store.PutTx(txWithoutAttempts))
	mustAddTxToAccount(t, store, fromAddress, txWithoutAttempts)
	nonce++

	t.Run("does nothing if the transaction is from a different address than the one given", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(otherAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		assert.Len(t, txs, 0)
	})

	t.Run("returns the transaction if it is unconfirmed and has no attempts (note that this is an invariant violation, but we handle it anyway)", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		require.Len(t, txs, 1)
		assert.Equal(t, txWithoutAttempts.ID, txs[0].ID)
	})

	tx3 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	attempt3_1, err := store.GetTxAttempt(tx3.TxAttemptIDs[0])
	require.NoError(t, err)
	attempt3_1.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt3_1))

	t.Run("returns the transaction if it is unconfirmed with an attempt that is older than gasBumpThreshold blocks", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		require.Len(t, txs, 2)
		assert.Equal(t, txWithoutAttempts.ID, txs[0].ID)
		assert.Equal(t, tx3.ID, txs[1].ID)
	})

	t.Run("does not return more transactions the requested limit", func(t *testing.T) {
		// Unconfirmed txes in DB are:
		// (unnamed) (nonce 2)
		// tx1 (nonce 3)
		// tx2 (nonce 4)
		// txWithoutAttempts (nonce 5)
		// tx3 (nonce 6) - ready for bump
		// tx4 (nonce 7) - ready for bump
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 4)
		require.NoError(t, err)

		require.Len(t, txs, 1) // returns txWithoutAttempts only - eligible for gas bumping because it technically doesn't have any attempts within gasBumpThreshold blocks
		assert.Equal(t, txWithoutAttempts.ID, txs[0].ID)

		txs, err = store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 5)
		require.NoError(t, err)

		require.Len(t, txs, 2) // includes txWithoutAttempts, tx3 and tx4
		assert.Equal(t, txWithoutAttempts.ID, txs[0].ID)
		assert.Equal(t, tx3.ID, txs[1].ID)

		// Zero limit disables it
		txs, err = store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 0)
		require.NoError(t, err)

		require.Len(t, txs, 2) // includes txWithoutAttempts, tx3 and tx4
	})

	tx4 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	attempt4_1, err := store.GetTxAttempt(tx4.TxAttemptIDs[0])
	require.NoError(t, err)
	attempt4_1.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt4_1))

	t.Run("ignores pending transactions for another key", func(t *testing.T) {
		// Re-use etx3 nonce for another key, it should not affect the results for this key
		txOther := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, tx3.Nonce, otherAddress)
		aOther, err := store.GetTxAttempt(txOther.TxAttemptIDs[0])
		require.NoError(t, err)
		aOther.BroadcastBeforeBlockNum = oldEnough
		require.NoError(t, store.PutTxAttempt(aOther))

		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 6)
		require.NoError(t, err)

		require.Len(t, txs, 3) // includes txWithoutAttempts, tx3 and tx4
		assert.Equal(t, txWithoutAttempts.ID, txs[0].ID)
		assert.Equal(t, tx3.ID, txs[1].ID)
		assert.Equal(t, tx4.ID, txs[2].ID)
	})

	attempt3_2 := newBroadcastTxAttempt(t, tx3.ID, store)
	attempt3_2.BroadcastBeforeBlockNum = oldEnough
	attempt3_2.GasPrice = big.NewInt(30000)
	require.NoError(t, store.PutTxAttempt(attempt3_2))
	esTesting.MustAddAttemptToTx(t, store, tx3.ID, attempt3_2)

	t.Run("returns the transaction if it is unconfirmed with two attempts that are older than gasBumpThreshold blocks", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		require.Len(t, txs, 3)
		assert.Equal(t, txWithoutAttempts.ID, txs[0].ID)
		assert.Equal(t, tx3.ID, txs[1].ID)
		assert.Equal(t, tx4.ID, txs[2].ID)
	})

	attempt3_3 := newBroadcastTxAttempt(t, tx3.ID, store)
	attempt3_3.BroadcastBeforeBlockNum = tooNew
	attempt3_3.GasPrice = big.NewInt(40000)
	require.NoError(t, store.PutTxAttempt(attempt3_3))
	esTesting.MustAddAttemptToTx(t, store, tx3.ID, attempt3_3)

	t.Run("does not return the transaction if it has some older but one newer attempt", func(t *testing.T) {
		txs, err := store.GetTxsRequiringNewAttempt(fromAddress, currentHead, gasBumpThreshold, 10)
		require.NoError(t, err)

		require.Len(t, txs, 2)
		assert.Equal(t, txWithoutAttempts.ID, txs[0].ID)
		assert.Equal(t, tx4.ID, txs[1].ID)
	})
}

func TestTxConfirmer_BumpGasWhereNecessary(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	config.MaxGasPrice = big.NewInt(500000000000) // 500 Gwei
	ethClient := new(mocks.Client)
	// Use a mock KeyStore for this test
	keyStore := new(mocks.KeyStoreInterface)

	tc := txmanager.NewTxConfirmer(ethClient, store, keyStore, config)
	currentHead := int64(30)
	oldEnough := int64(19)
	nonce := int64(0)

	otherAccount := esTesting.MustInsertRandomAccount(t, store)
	account := esTesting.MustInsertRandomAccount(t, store)
	fromAddress := account.Address
	accounts := []*models.Account{account, otherAccount}

	keyStore.On("GetAccountByAddress", fromAddress).
		Return(gethAccounts.Account{Address: fromAddress}, nil)

	t.Run("does nothing if no transactions require bumping", func(t *testing.T) {
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))
	})

	tx := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	attempt1_1, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
	require.NoError(t, err)
	attempt1_1.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt1_1))

	t.Run("returns on keystore error", func(t *testing.T) {
		// simulate transaction that is somehow impossible to sign
		keyStore.On("SignTx", mock.Anything,
			mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
				return gethTx.Nonce() == uint64(tx.Nonce)
			}),
			mock.Anything).Return(nil, errors.New("signing error")).Once()

		// Do the thing
		err = tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead)
		require.Error(t, err)
		require.Contains(t, err.Error(), "signing error")

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)
		require.Equal(t, models.TxStateUnconfirmed, tx.State)

		require.Len(t, tx.TxAttemptIDs, 1)

		keyStore.AssertExpectations(t)
	})

	keyStore = new(mocks.KeyStoreInterface)
	keyStore.On("GetAccountByAddress", fromAddress).
		Return(gethAccounts.Account{Address: fromAddress}, nil)
	tc.SetKeyStore(keyStore)

	t.Run("does nothing and continues on fatal error", func(t *testing.T) {
		ethTx := gethTypes.Transaction{}
		keyStore.On("SignTx",
			mock.AnythingOfType("accounts.Account"),
			mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
				if gethTx.Nonce() != uint64(tx.Nonce) {
					return false
				}
				ethTx = *gethTx
				return true
			}),
			mock.MatchedBy(func(chainID *big.Int) bool {
				return chainID.Cmp(config.ChainID) == 0
			})).Return(&ethTx, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return gethTx.Nonce() == uint64(tx.Nonce)
		})).Return(errors.New("exceeds block gas limit")).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)

		require.Len(t, tx.TxAttemptIDs, 1)

		keyStore.AssertExpectations(t)
	})

	keyStore = new(mocks.KeyStoreInterface)
	tc.SetKeyStore(keyStore)
	keyStore.On("GetAccountByAddress", fromAddress).
		Return(gethAccounts.Account{Address: fromAddress}, nil)
	var attempt1_2 *models.TxAttempt

	t.Run("creates new attempt with higher gas price if transaction has an attempt older than threshold", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(25000000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt1_1.GasPrice.Int64())

		ethTx := gethTypes.Transaction{}
		keyStore.On("SignTx",
			mock.AnythingOfType("accounts.Account"),
			mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
				if expectedBumpedGasPrice.Cmp(tx.GasPrice()) != 0 {
					return false
				}
				ethTx = *tx
				return true
			}),
			mock.MatchedBy(func(chainID *big.Int) bool {
				return chainID.Cmp(config.ChainID) == 0
			})).Return(&ethTx, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return expectedBumpedGasPrice.Cmp(tx.GasPrice()) == 0
		})).Return(nil).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)

		require.Len(t, tx.TxAttemptIDs, 2)
		// attempt1_1 has lower gas price
		require.Equal(t, attempt1_1.ID, tx.TxAttemptIDs[1])

		// Got the new attempt
		// attempt1_2 has higher gas price
		attempt1_2, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, expectedBumpedGasPrice.Int64(), attempt1_2.GasPrice.Int64())
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt1_2.State)

		ethClient.AssertExpectations(t)
	})

	t.Run("does nothing if there is an attempt without BroadcastBeforeBlockNum set", func(t *testing.T) {
		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)

		require.Len(t, tx.TxAttemptIDs, 2)
	})

	attempt1_2.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt1_2))
	var attempt1_3 *models.TxAttempt

	t.Run("creates new attempt with higher gas price if transaction is already in mempool (e.g. due to previous crash before we could save the new attempt)", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(30000000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt1_2.GasPrice.Int64())

		ethTx := gethTypes.Transaction{}
		keyStore.On("SignTx",
			mock.AnythingOfType("accounts.Account"),
			mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
				if int64(gethTx.Nonce()) != tx.Nonce || expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) != 0 {
					return false
				}
				ethTx = *gethTx
				return true
			}),
			mock.Anything).Return(&ethTx, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return expectedBumpedGasPrice.Cmp(tx.GasPrice()) == 0
		})).Return(fmt.Errorf("known transaction: %s", ethTx.Hash().Hex())).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)

		require.Len(t, tx.TxAttemptIDs, 3)
		require.Equal(t, attempt1_1.ID, tx.TxAttemptIDs[2])
		require.Equal(t, attempt1_2.ID, tx.TxAttemptIDs[1])

		// Got the new attempt
		attempt1_3, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, expectedBumpedGasPrice.Int64(), attempt1_3.GasPrice.Int64())
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt1_3.State)

		keyStore.AssertExpectations(t)
		ethClient.AssertExpectations(t)
	})

	attempt1_3.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt1_3))
	var attempt1_4 *models.TxAttempt

	t.Run("saves new attempt even for transaction that has already been confirmed (nonce already used)", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(36000000000) // Check config.GasBumpPercent and config.GasBumpWei
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt1_2.GasPrice.Int64())

		ethTx := gethTypes.Transaction{}
		receipt := gethTypes.Receipt{BlockNumber: big.NewInt(40)}
		keyStore.On("SignTx",
			mock.AnythingOfType("accounts.Account"),
			mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
				if int64(gethTx.Nonce()) != tx.Nonce || expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) != 0 {
					return false
				}
				ethTx = *gethTx
				receipt.TxHash = gethTx.Hash()
				return true
			}),
			mock.Anything).Return(&ethTx, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return expectedBumpedGasPrice.Cmp(tx.GasPrice()) == 0
		})).Return(errors.New("nonce too low")).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx.State)

		// Got the new attempt
		attempt1_4, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, expectedBumpedGasPrice.Int64(), attempt1_4.GasPrice.Int64())

		require.Len(t, tx.TxAttemptIDs, 4)
		require.Equal(t, attempt1_1.ID, tx.TxAttemptIDs[3])
		require.Equal(t, attempt1_2.ID, tx.TxAttemptIDs[2])
		require.Equal(t, attempt1_3.ID, tx.TxAttemptIDs[1])
		require.Equal(t, attempt1_4.ID, tx.TxAttemptIDs[0])
		attempt1_1, err = store.GetTxAttempt(tx.TxAttemptIDs[3])
		require.NoError(t, err)
		attempt1_2, err = store.GetTxAttempt(tx.TxAttemptIDs[2])
		require.NoError(t, err)
		attempt1_3, err = store.GetTxAttempt(tx.TxAttemptIDs[1])
		require.NoError(t, err)
		require.Equal(t, models.TxAttemptStateBroadcast, attempt1_1.State)
		require.Equal(t, models.TxAttemptStateBroadcast, attempt1_2.State)
		require.Equal(t, models.TxAttemptStateBroadcast, attempt1_3.State)
		require.Equal(t, models.TxAttemptStateBroadcast, attempt1_4.State)

		ethClient.AssertExpectations(t)
		keyStore.AssertExpectations(t)
	})

	// Mark original tx as confirmed so we won't pick it up any more
	tx.State = models.TxStateConfirmed
	require.NoError(t, store.PutTx(tx))

	tx2 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	attempt2_1, err := store.GetTxAttempt(tx2.TxAttemptIDs[0])
	require.NoError(t, err)
	attempt2_1.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt2_1))
	var attempt2_2 *models.TxAttempt

	t.Run("saves in_progress attempt on temporary error and returns error", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(25000000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt2_1.GasPrice.Int64())

		ethTx := gethTypes.Transaction{}
		n := tx2.Nonce
		keyStore.On("SignTx",
			mock.AnythingOfType("accounts.Account"),
			mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
				if int64(gethTx.Nonce()) != n || expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) != 0 {
					return false
				}
				ethTx = *gethTx
				return true
			}),
			mock.Anything).Return(&ethTx, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return int64(gethTx.Nonce()) == n && expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(errors.New("some network error")).Once()

		// Do the thing
		err = tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead)
		require.Error(t, err)
		require.Contains(t, err.Error(), "some network error")

		tx2, err = store.GetTx(tx2.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx2.State)

		// Old attempt is untouched
		require.Len(t, tx2.TxAttemptIDs, 2)
		require.Equal(t, attempt2_1.ID, tx2.TxAttemptIDs[1])
		attempt2_1, err = store.GetTxAttempt(tx2.TxAttemptIDs[1])
		require.NoError(t, err)
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt2_1.State)
		assert.Equal(t, oldEnough, attempt2_1.BroadcastBeforeBlockNum)

		// New in_progress attempt saved
		attempt2_2, err = store.GetTxAttempt(tx2.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, models.TxAttemptStateInProgress, attempt2_2.State)
		assert.Equal(t, int64(-1), attempt2_2.BroadcastBeforeBlockNum)

		// Do it again and move the attempt into "broadcast"
		n = tx2.Nonce
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return int64(gethTx.Nonce()) == n && expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(nil).Once()

		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		// Attempt marked "broadcast"
		tx2, err = store.GetTx(tx2.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx2.State)

		// New in_progress attempt saved
		require.Len(t, tx2.TxAttemptIDs, 2)
		require.Equal(t, attempt2_2.ID, tx2.TxAttemptIDs[0])
		attempt2_2, err = store.GetTxAttempt(tx2.TxAttemptIDs[0])
		require.NoError(t, err)
		require.Equal(t, models.TxAttemptStateBroadcast, attempt2_2.State)
		assert.Equal(t, int64(-1), attempt2_2.BroadcastBeforeBlockNum)

		ethClient.AssertExpectations(t)
		keyStore.AssertExpectations(t)
	})

	// Set BroadcastBeforeBlockNum again so the next test will pick it up
	attempt2_2.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt2_2))

	t.Run("assumes that 'nonce too low' error means success", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(30000000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt2_1.GasPrice.Int64())

		ethTx := gethTypes.Transaction{}
		n := tx2.Nonce
		keyStore.On("SignTx",
			mock.AnythingOfType("accounts.Account"),
			mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
				if int64(gethTx.Nonce()) != n || expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) != 0 {
					return false
				}
				ethTx = *gethTx
				return true
			}),
			mock.Anything).Return(&ethTx, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return int64(gethTx.Nonce()) == n && expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(errors.New("nonce too low")).Once()

		// Creates new attempt as normal if currentHead is not high enough
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))
		tx2, err = store.GetTx(tx2.ID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStateUnconfirmed, tx2.State)

		// One new attempt saved
		require.Len(t, tx2.TxAttemptIDs, 3)
		attempt2_1, err = store.GetTxAttempt(tx2.TxAttemptIDs[2])
		require.NoError(t, err)
		attempt2_2, err = store.GetTxAttempt(tx2.TxAttemptIDs[1])
		require.NoError(t, err)
		attempt2_3, err := store.GetTxAttempt(tx2.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt2_1.State)
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt2_2.State)
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt2_3.State)

		ethClient.AssertExpectations(t)
		keyStore.AssertExpectations(t)
	})

	// Original tx is confirmed so we won't pick it up any more
	tx3 := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	attempt3_1, err := store.GetTxAttempt(tx3.TxAttemptIDs[0])
	require.NoError(t, err)
	attempt3_1.BroadcastBeforeBlockNum = oldEnough
	attempt3_1.GasPrice = big.NewInt(35000000000)
	require.NoError(t, store.PutTxAttempt(attempt3_1))
	var attempt3_2 *models.TxAttempt

	t.Run("saves attempt anyway if replacement transaction is underpriced because the bumped gas price is insufficiently higher than the previous one", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(42000000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt3_1.GasPrice.Int64())

		ethTx := gethTypes.Transaction{}
		keyStore.On("SignTx",
			mock.AnythingOfType("accounts.Account"),
			mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
				if int64(gethTx.Nonce()) != tx3.Nonce || expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) != 0 {
					return false
				}
				ethTx = *gethTx
				return true
			}),
			mock.Anything).Return(&ethTx, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return int64(gethTx.Nonce()) == tx3.Nonce && expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(errors.New("replacement transaction underpriced")).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx3, err = store.GetTx(tx3.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx3.State)

		require.Len(t, tx3.TxAttemptIDs, 2)
		require.Equal(t, attempt3_1.ID, tx3.TxAttemptIDs[1])
		attempt3_2, err = store.GetTxAttempt(tx3.TxAttemptIDs[0])

		assert.Equal(t, expectedBumpedGasPrice.Int64(), attempt3_2.GasPrice.Int64())

		keyStore.AssertExpectations(t)
		ethClient.AssertExpectations(t)
	})

	attempt3_2.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt3_2))
	var attempt3_3 *models.TxAttempt

	t.Run("handles case where transaction is already known somehow", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(50400000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt3_1.GasPrice.Int64())

		ethTx := gethTypes.Transaction{}
		keyStore.On("SignTx",
			mock.AnythingOfType("accounts.Account"),
			mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
				if int64(gethTx.Nonce()) != tx3.Nonce || expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) != 0 {
					return false
				}
				ethTx = *gethTx
				return true
			}),
			mock.Anything).Return(&ethTx, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return int64(gethTx.Nonce()) == tx3.Nonce && expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(fmt.Errorf("known transaction: %s", ethTx.Hash().Hex())).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx3, err = store.GetTx(tx3.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx3.State)

		require.Len(t, tx3.TxAttemptIDs, 3)
		attempt3_3, err = store.GetTxAttempt(tx3.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, expectedBumpedGasPrice.Int64(), attempt3_3.GasPrice.Int64())

		ethClient.AssertExpectations(t)
		keyStore.AssertExpectations(t)
	})

	attempt3_3.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt3_3))
	var attempt3_4 *models.TxAttempt

	t.Run("pretends it was accepted and continues the cycle if rejected for being temporarily underpriced", func(t *testing.T) {
		// This happens if parity is rejecting transactions that are not priced high enough to even get into the mempool at all
		// It should pretend it was accepted into the mempool and hand off to the next cycle to continue bumping gas as normal
		temporarilyUnderpricedError := "There are too many transactions in the queue. Your transaction was dropped due to limit. Try increasing the fee."

		expectedBumpedGasPrice := big.NewInt(60480000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt3_2.GasPrice.Int64())

		ethTx := gethTypes.Transaction{}
		keyStore.On("SignTx",
			mock.AnythingOfType("accounts.Account"),
			mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
				if int64(gethTx.Nonce()) != tx3.Nonce || expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) != 0 {
					return false
				}
				ethTx = *gethTx
				return true
			}),
			mock.Anything).Return(&ethTx, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return int64(gethTx.Nonce()) == tx3.Nonce && expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(errors.New(temporarilyUnderpricedError)).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx3, err = store.GetTx(tx3.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx3.State)

		require.Len(t, tx3.TxAttemptIDs, 4)
		attempt3_4, err = store.GetTxAttempt(tx3.TxAttemptIDs[0])
		assert.Equal(t, expectedBumpedGasPrice.Int64(), attempt3_4.GasPrice.Int64())

		ethClient.AssertExpectations(t)
		keyStore.AssertExpectations(t)
	})

	attempt3_4.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt3_4))

	t.Run("resubmits at the old price and does not create a new attempt if one of the bumped transactions would exceed MaxGasPrice", func(t *testing.T) {
		// Set price such that the next bump will exceed MaxGasPrice
		// Existing gas price is: 60480000000
		gasPrice := attempt3_4.GasPrice
		config.MaxGasPrice = big.NewInt(60500000000)

		// We already submitted at this price, now its time to bump and submit again but since we
		// simply resubmitted rather than increasing gas price, geth already knows about this tx.
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return int64(gethTx.Nonce()) == tx3.Nonce && gasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(errors.New("already known")).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx3, err = store.GetTx(tx3.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx3.State)

		// No new tx attempts
		require.Len(t, tx3.TxAttemptIDs, 4)
		attempt3_4, err = store.GetTxAttempt(tx3.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, gasPrice.Int64(), attempt3_4.GasPrice.Int64())

		ethClient.AssertExpectations(t)
		keyStore.AssertExpectations(t)
	})

	attempt3_4.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt3_4))

	t.Run("resubmits at the old price and does not create a new attempt if the current price is exactly MaxGasPrice", func(t *testing.T) {
		// Set price such that the current price is already at MaxGasPrice
		// Existing gas price is: 60480000000
		gasPrice := attempt3_4.GasPrice
		config.MaxGasPrice = big.NewInt(60480000000)

		// We already submitted at this price, now its time to bump and submit again but since we
		// simply resubmitted rather than increasing gas price, geth already knows about this tx.
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return int64(gethTx.Nonce()) == tx3.Nonce && gasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(errors.New("already known")).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx3, err = store.GetTx(tx3.ID)
		require.NoError(t, err)

		assert.Equal(t, models.TxStateUnconfirmed, tx3.State)

		// No new tx attempts
		require.Len(t, tx3.TxAttemptIDs, 4)
		attempt3_4, err := store.GetTxAttempt(tx3.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, gasPrice.Int64(), attempt3_4.GasPrice.Int64())

		ethClient.AssertExpectations(t)
		keyStore.AssertExpectations(t)
	})

	keyStore.AssertExpectations(t)
	ethClient.AssertExpectations(t)
}

func TestTxConfirmer_BumpGasWhereNecessary_WhenOutOfEth(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	require.NoError(t, os.RemoveAll(config.KeysDir))
	keyStore := client.NewInsecureKeyStore(config.KeysDir)
	account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, 0)
	ethClient := new(mocks.Client)
	accounts := []*models.Account{account}

	tc := txmanager.NewTxConfirmer(ethClient, store, keyStore, config)
	currentHead := int64(30)
	oldEnough := int64(19)
	nonce := int64(0)

	tx := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, nonce, fromAddress)
	nonce++
	attempt1_1, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
	require.NoError(t, err)
	attempt1_1.BroadcastBeforeBlockNum = oldEnough
	require.NoError(t, store.PutTxAttempt(attempt1_1))
	var attempt1_2 *models.TxAttempt

	insufficientEthError := errors.New("insufficient funds for gas * price + value")

	t.Run("saves attempt with state 'insufficient_eth' if eth node returns this error", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(25000000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt1_1.GasPrice.Int64())

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(insufficientEthError).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)

		require.Len(t, tx.TxAttemptIDs, 2)
		require.Equal(t, attempt1_1.ID, tx.TxAttemptIDs[1])

		// Got the new attempt
		attempt1_2, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, expectedBumpedGasPrice.Int64(), attempt1_2.GasPrice.Int64())
		assert.Equal(t, models.TxAttemptStateInsufficientEth, attempt1_2.State)
		assert.Equal(t, int64(-1), attempt1_2.BroadcastBeforeBlockNum)

		ethClient.AssertExpectations(t)
	})

	t.Run("does not bump gas when previous error was 'out of eth', instead resubmits existing transaction", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(25000000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt1_1.GasPrice.Int64())

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(insufficientEthError).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)

		// New attempt was NOT created
		require.Len(t, tx.TxAttemptIDs, 2)

		// The attempt is still "out of eth"
		attempt1_2, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, expectedBumpedGasPrice.Int64(), attempt1_2.GasPrice.Int64())
		assert.Equal(t, models.TxAttemptStateInsufficientEth, attempt1_2.State)

		ethClient.AssertExpectations(t)
	})

	t.Run("saves the attempt as broadcast after node wallet has been topped up with sufficient balance", func(t *testing.T) {
		expectedBumpedGasPrice := big.NewInt(25000000000)
		require.Greater(t, expectedBumpedGasPrice.Int64(), attempt1_1.GasPrice.Int64())

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			return expectedBumpedGasPrice.Cmp(gethTx.GasPrice()) == 0
		})).Return(nil).Once()

		// Do the thing
		require.NoError(t, tc.BumpGasWhereNecessary(context.TODO(), accounts, currentHead))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)

		// New attempt was NOT created
		require.Len(t, tx.TxAttemptIDs, 2)

		// Attempt is now 'broadcast'
		attempt1_2, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, expectedBumpedGasPrice.Int64(), attempt1_2.GasPrice.Int64())
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt1_2.State)

		ethClient.AssertExpectations(t)
	})
}

func TestTxConfirmer_EnsureConfirmedTransactionsInLongestChain(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	require.NoError(t, os.RemoveAll(config.KeysDir))
	keyStore := client.NewInsecureKeyStore(config.KeysDir)
	account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, 0)
	ethClient := new(mocks.Client)
	accounts := []*models.Account{account}

	tc := txmanager.NewTxConfirmer(ethClient, store, keyStore, config)

	head := &models.Head{
		Hash:   esTesting.NewHash(),
		Number: 10,
		Parent: &models.Head{
			Hash:   esTesting.NewHash(),
			Number: 9,
			Parent: &models.Head{
				Number: 8,
				Hash:   esTesting.NewHash(),
				Parent: nil,
			},
		},
	}

	t.Run("does nothing if there aren't any transactions", func(t *testing.T) {
		require.NoError(t, tc.EnsureConfirmedTxsInLongestChain(context.TODO(), accounts, head))
	})

	t.Run("does nothing to unconfirmed transactions", func(t *testing.T) {
		tx := esTesting.MustInsertUnconfirmedTxWithBroadcastAttempt(t, store, 0, fromAddress)

		// Do the thing
		require.NoError(t, tc.EnsureConfirmedTxsInLongestChain(context.TODO(), accounts, head))

		tx, err := store.GetTx(tx.ID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStateUnconfirmed, tx.State)
	})

	t.Run("does nothing to confirmed transactions with receipts within head height of the chain and included in the chain", func(t *testing.T) {
		tx := esTesting.MustInsertConfirmedTxWithAttempt(t, store, 2, 1, fromAddress)
		attempt, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		esTesting.MustInsertTxReceipt(t, store, head.Number, head.Hash, attempt.Hash, attempt)

		// Do the thing
		require.NoError(t, tc.EnsureConfirmedTxsInLongestChain(context.TODO(), accounts, head))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStateConfirmed, tx.State)
	})

	t.Run("does nothing to confirmed transactions that only have receipts older than the start of the chain", func(t *testing.T) {
		tx := esTesting.MustInsertConfirmedTxWithAttempt(t, store, 3, 1, fromAddress)
		attempt, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		// Add receipt that is older than the lowest block of the chain
		esTesting.MustInsertTxReceipt(t, store, head.Parent.Parent.Number-1, esTesting.NewHash(), attempt.Hash, attempt)

		// Do the thing
		require.NoError(t, tc.EnsureConfirmedTxsInLongestChain(context.TODO(), accounts, head))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStateConfirmed, tx.State)
	})

	t.Run("unconfirms and rebroadcasts transactions that have receipts within head height of the chain but not included in the chain", func(t *testing.T) {
		tx := esTesting.MustInsertConfirmedTxWithAttempt(t, store, 4, 1, fromAddress)
		attempt, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		// Include one within head height but a different block hash
		esTesting.MustInsertTxReceipt(t, store, head.Parent.Number, esTesting.NewHash(), attempt.Hash, attempt)

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			aTx, err := attempt.GetSignedTx()
			require.NoError(t, err)
			// Keeps gas price and nonce the same
			return aTx.GasPrice().Cmp(gethTx.GasPrice()) == 0 && aTx.Nonce() == gethTx.Nonce()
		})).Return(nil).Once()

		// Do the thing
		require.NoError(t, tc.EnsureConfirmedTxsInLongestChain(context.TODO(), accounts, head))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStateUnconfirmed, tx.State)
		require.Len(t, tx.TxAttemptIDs, 1)
		attempt, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt.State)

		ethClient.AssertExpectations(t)
	})

	t.Run("unconfirms and rebroadcasts transactions that have receipts within head height of chain but not included in the chain even if a receipt exists older than the start of the chain", func(t *testing.T) {
		tx := esTesting.MustInsertConfirmedTxWithAttempt(t, store, 5, 1, fromAddress)
		attempt, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		// Add receipt that is older than the lowest block of the chain
		esTesting.MustInsertTxReceipt(t, store, head.Parent.Parent.Number-1, esTesting.NewHash(), attempt.Hash, attempt)
		// Include one within head height but a different block hash
		esTesting.MustInsertTxReceipt(t, store, head.Parent.Number, esTesting.NewHash(), attempt.Hash, attempt)

		ethClient.On("SendTransaction", mock.Anything, mock.Anything).Return(nil).Once()

		// Do the thing
		require.NoError(t, tc.EnsureConfirmedTxsInLongestChain(context.TODO(), accounts, head))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStateUnconfirmed, tx.State)
		require.Len(t, tx.TxAttemptIDs, 1)
		attempt, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt.State)

		ethClient.AssertExpectations(t)
	})

	t.Run("if more than one attempt has a receipt (unlikely but allowed within constraints of system, and possible in the event of forks) unconfirms and rebroadcasts only the attempt with the highest gas price", func(t *testing.T) {
		tx := esTesting.MustInsertConfirmedTxWithAttempt(t, store, 6, 1, fromAddress)
		require.Len(t, tx.TxAttemptIDs, 1)
		// Sanity check to assert the included attempt has the lowest gas price
		attempt1, err := store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		require.Less(t, attempt1.GasPrice.Int64(), int64(30000))

		attempt2 := newBroadcastTxAttempt(t, tx.ID, store, 30000)
		attempt2.SignedRawTx = hexutil.MustDecode("0xf88c8301f3a98503b9aca000832ab98094f5fff180082d6017036b771ba883025c654bc93580a4daa6d556000000000000000000000000000000000000000000000000000000000000000026a0f25601065ee369b6470c0399a2334afcfbeb0b5c8f3d9a9042e448ed29b5bcbda05b676e00248b85faf4dd889f0e2dcf91eb867e23ac9eeb14a73f9e4c14972cdf")
		attempt3 := newBroadcastTxAttempt(t, tx.ID, store, 40000)
		attempt3.SignedRawTx = hexutil.MustDecode("0xf88c8301f3a88503b9aca0008316e36094151445852b0cfdf6a4cc81440f2af99176e8ad0880a4daa6d556000000000000000000000000000000000000000000000000000000000000000026a0dcb5a7ad52b96a866257134429f944c505820716567f070e64abb74899803855a04c13eff2a22c218e68da80111e1bb6dc665d3dea7104ab40ff8a0275a99f630d")
		require.NoError(t, store.PutTxAttempt(attempt2))
		require.NoError(t, store.PutTxAttempt(attempt3))
		require.NoError(t, store.AddOrUpdateAttempt(tx, attempt2))
		require.NoError(t, store.AddOrUpdateAttempt(tx, attempt3))

		// Receipt is within head height but a different block hash
		esTesting.MustInsertTxReceipt(t, store, head.Parent.Number, esTesting.NewHash(), attempt2.Hash, attempt2)
		// Receipt is within head height but a different block hash
		esTesting.MustInsertTxReceipt(t, store, head.Parent.Number, esTesting.NewHash(), attempt3.Hash, attempt3)

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(gethTx *gethTypes.Transaction) bool {
			s, err := attempt3.GetSignedTx()
			require.NoError(t, err)
			return gethTx.Hash() == s.Hash()
		})).Return(nil).Once()

		// Do the thing
		require.NoError(t, tc.EnsureConfirmedTxsInLongestChain(context.TODO(), accounts, head))

		tx, err = store.GetTx(tx.ID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStateUnconfirmed, tx.State)
		require.Len(t, tx.TxAttemptIDs, 3)
		attempt1, err = store.GetTxAttempt(tx.TxAttemptIDs[2])
		require.NoError(t, err)
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt1.State)
		attempt2, err = store.GetTxAttempt(tx.TxAttemptIDs[1])
		require.NoError(t, err)
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt2.State)
		attempt3, err = store.GetTxAttempt(tx.TxAttemptIDs[0])
		require.NoError(t, err)
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt3.State)

		ethClient.AssertExpectations(t)
	})
}
