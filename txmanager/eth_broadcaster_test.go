package txmanager_test

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/celer-network/eth-services/client"
	"github.com/celer-network/eth-services/internal/mocks"
	esTesting "github.com/celer-network/eth-services/internal/testing"
	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/celer-network/eth-services/txmanager"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/require"

	gethCommon "github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
)

const (
	keyDir = "../internal/fixtures/keys"
)

func TestEthBroadcaster_ProcessUnstartedTxs_Success(t *testing.T) {
	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	require.NoError(t, os.RemoveAll(config.KeysDir))

	keyStore := client.NewInsecureKeyStore(config.KeysDir)
	account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, 0)
	ethClient := new(mocks.Client)
	eb := txmanager.NewEthBroadcaster(ethClient, store, keyStore, config)

	toAddress := gethCommon.HexToAddress("0x6C03DDA95a2AEd917EeCc6eddD4b9D16E6380411")

	encodedPayload := []byte{1, 2, 3}
	value := *big.NewInt(142)
	gasLimit := uint64(242)

	t.Run("no txes at all", func(t *testing.T) {
		require.NoError(t, eb.ProcessUnstartedTxs(account))
	})

	t.Run("txes exist for a different from address", func(t *testing.T) {
		_, otherAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore)

		tx := &models.Tx{
			FromAddress:    otherAddress,
			ToAddress:      toAddress,
			EncodedPayload: encodedPayload,
			Value:          value,
			GasLimit:       gasLimit,
			State:          models.TxStateUnstarted,
		}
		require.NoError(t, store.PutTx(tx))

		require.NoError(t, eb.ProcessUnstartedTxs(account))
	})

	t.Run("existing txes with unconfirmed or error states", func(t *testing.T) {
		nonce := int64(342)
		errStr := "some error"

		txUnconfirmed := &models.Tx{
			Nonce:          nonce,
			FromAddress:    fromAddress,
			ToAddress:      toAddress,
			EncodedPayload: encodedPayload,
			Value:          value,
			GasLimit:       gasLimit,
			Error:          "",
			State:          models.TxStateUnconfirmed,
		}
		txWithError := &models.Tx{
			Nonce:          -1,
			FromAddress:    fromAddress,
			ToAddress:      toAddress,
			EncodedPayload: encodedPayload,
			Value:          value,
			GasLimit:       gasLimit,
			Error:          errStr,
			State:          models.TxStateFatalError,
		}

		require.NoError(t, store.PutTx(txUnconfirmed))
		require.NoError(t, store.PutTx(txWithError))

		require.NoError(t, eb.ProcessUnstartedTxs(account))
	})

	t.Run("sends 1 tx", func(t *testing.T) {
		tx := &models.Tx{
			FromAddress:    fromAddress,
			ToAddress:      toAddress,
			EncodedPayload: []byte{42, 42, 0},
			Value:          value,
			GasLimit:       gasLimit,
			State:          models.TxStateUnstarted,
		}
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(ethTx *gethTypes.Transaction) bool {
			if ethTx.Nonce() != uint64(0) {
				return false
			}
			require.Equal(t, config.ChainID, ethTx.ChainId())
			require.Equal(t, gasLimit, ethTx.Gas())
			require.Equal(t, config.DefaultGasPrice, ethTx.GasPrice())
			require.Equal(t, toAddress, *ethTx.To())
			require.Equal(t, value.String(), ethTx.Value().String())
			require.Equal(t, tx.EncodedPayload, ethTx.Data())
			return true
		})).Return(nil).Once()

		require.NoError(t, store.PutTx(tx))

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check tx and it's attempt
		tx, err := store.GetTx(tx.FromAddress, tx.ID)
		require.NoError(t, err)
		assert.Empty(t, tx.Error)
		require.NotNil(t, tx.FromAddress)
		assert.Equal(t, fromAddress, tx.FromAddress)
		require.NotNil(t, tx.Nonce)
		assert.Equal(t, int64(0), tx.Nonce)
		assert.Len(t, tx.TxAttempts, 1)

		attempt := tx.TxAttempts[0]

		assert.Equal(t, tx.ID, attempt.TxID)
		assert.Equal(t, config.DefaultGasPrice.String(), attempt.GasPrice.String())

		_, err = attempt.GetSignedTx()
		require.NoError(t, err)
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt.State)
		require.Len(t, attempt.Receipts, 0)

		ethClient.AssertExpectations(t)
	})

	// TODO: Add multiple tx test with ordering
}

func TestEthBroadcaster_AssignsNonceOnFirstRun(t *testing.T) {
	var err error
	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	require.NoError(t, os.RemoveAll(config.KeysDir))

	keyStore := client.NewInsecureKeyStore(config.KeysDir)
	account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore)

	ethClient := new(mocks.Client)
	eb := txmanager.NewEthBroadcaster(ethClient, store, keyStore, config)

	toAddress := gethCommon.HexToAddress("0x6C03DDA95a2AEd917EeCc6eddD4b9D16E6380411")
	gasLimit := uint64(242)

	// Insert new account to test we only update the intended one
	dummyAccount := esTesting.MustInsertRandomAccount(t, store)

	tx := &models.Tx{
		FromAddress:    fromAddress,
		ToAddress:      toAddress,
		EncodedPayload: []byte{42, 42, 0},
		Value:          *big.NewInt(0),
		GasLimit:       gasLimit,
		State:          models.TxStateUnstarted,
		Nonce:          -1,
	}
	require.NoError(t, store.PutTx(tx))

	t.Run("when eth node returns error", func(t *testing.T) {
		ethClient.On("PendingNonceAt", mock.Anything, mock.MatchedBy(func(account gethCommon.Address) bool {
			return account.Hex() == fromAddress.Hex()
		})).Return(uint64(0), errors.New("something exploded")).Once()

		// First attempt errored
		err = eb.ProcessUnstartedTxs(account)
		require.Error(t, err)
		require.Contains(t, err.Error(), "something exploded")

		// Check tx that it has no nonce assigned
		tx, err = store.GetTx(tx.FromAddress, tx.ID)
		require.NoError(t, err)

		assert.Equal(t, int64(-1), tx.Nonce)

		// Check to make sure all keys still don't have a nonce assigned
		accounts, err := store.GetAccounts()
		require.NoError(t, err)
		count := 0
		for _, account := range accounts {
			if account.NextNonce == -1 {
				count++
			}
		}
		assert.Equal(t, 2, count)

		ethClient.AssertExpectations(t)
	})

	t.Run("when eth node returns nonce", func(t *testing.T) {
		ethNodeNonce := uint64(42)

		ethClient.On("PendingNonceAt", mock.Anything, mock.MatchedBy(func(account gethCommon.Address) bool {
			return account.Hex() == fromAddress.Hex()
		})).Return(ethNodeNonce, nil).Once()
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == ethNodeNonce
		})).Return(nil).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check tx that it has the correct nonce assigned
		tx, err = store.GetTx(tx.FromAddress, tx.ID)
		require.NoError(t, err)

		assert.NotEqual(t, -1, tx.Nonce)
		require.Equal(t, int64(ethNodeNonce), tx.Nonce)

		// Check account to make sure it has correct nonce assigned
		accounts, err := store.GetAccounts()
		require.NoError(t, err)
		var changed *models.Account
		var unchanged *models.Account
		if bytes.Equal(accounts[0].Address.Bytes(), account.Address.Bytes()) {
			changed = accounts[0]
			unchanged = accounts[1]
		} else {
			changed = accounts[1]
			unchanged = accounts[0]
		}

		require.Equal(t, int64(43), changed.NextNonce)

		// The dummy account did not get updated
		require.Equal(t, dummyAccount.Address, unchanged.Address)
		assert.Equal(t, int64(-1), unchanged.NextNonce)

		ethClient.AssertExpectations(t)
	})
}

func TestEthBroadcaster_ProcessUnstartedTxs_ResumingFromCrash(t *testing.T) {
	nextNonce := int64(916714082576372851)

	// TODO: Add test to make sure not more than one in_progress tx

	t.Run("previous run assigned nonce but never broadcast", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)
		require.NoError(t, os.RemoveAll(config.KeysDir))
		keyStore := client.NewInsecureKeyStore(config.KeysDir)
		account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, nextNonce)
		ethClient := new(mocks.Client)

		eb := txmanager.NewEthBroadcaster(ethClient, store, keyStore, config)

		// Crashed right after we save the nonce to the tx so accounts.NextNonce has not been incremented yet
		nonce := nextNonce
		inProgressTx := esTesting.MustInsertInProgressTxWithAttempt(t, store, nextNonce, fromAddress)

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == uint64(nonce)
		})).Return(nil).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check it was saved correctly with its attempt
		tx, err := store.GetTx(inProgressTx.FromAddress, inProgressTx.ID)
		require.NoError(t, err)

		assert.Equal(t, "", tx.Error)
		assert.Len(t, tx.TxAttempts, 1)
		assert.Equal(t, models.TxAttemptStateBroadcast, tx.TxAttempts[0].State)

		ethClient.AssertExpectations(t)
	})

	t.Run("previous run assigned nonce and broadcast but it fatally errored before we could save", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)
		require.NoError(t, os.RemoveAll(config.KeysDir))
		keyStore := client.NewInsecureKeyStore(config.KeysDir)
		account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, nextNonce)
		ethClient := new(mocks.Client)

		eb := txmanager.NewEthBroadcaster(ethClient, store, keyStore, config)

		// Crashed right after we save the nonce to the tx so accounts.NextNonce has not been incremented yet
		nonce := nextNonce
		inProgressTx := esTesting.MustInsertInProgressTxWithAttempt(t, store, nextNonce, fromAddress)

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == uint64(nonce)
		})).Return(errors.New("exceeds block gas limit")).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check it was saved correctly with its attempt
		tx, err := store.GetTx(inProgressTx.FromAddress, inProgressTx.ID)
		require.NoError(t, err)

		assert.NotEqual(t, "", tx.Error)
		assert.Equal(t, "exceeds block gas limit", tx.Error)
		assert.Len(t, tx.TxAttempts, 0)

		ethClient.AssertExpectations(t)
	})

	t.Run("previous run assigned nonce and broadcast and is now in mempool", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)
		require.NoError(t, os.RemoveAll(config.KeysDir))
		keyStore := client.NewInsecureKeyStore(config.KeysDir)
		account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, nextNonce)
		ethClient := new(mocks.Client)

		eb := txmanager.NewEthBroadcaster(ethClient, store, keyStore, config)

		// Crashed right after we save the nonce to the tx so accounts.NextNonce has not been incremented yet
		nonce := nextNonce
		inProgressTx := esTesting.MustInsertInProgressTxWithAttempt(t, store, nextNonce, fromAddress)

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == uint64(nonce)
		})).Return(errors.New("known transaction: a1313bd99a81fb4d8ad1d2e90b67c6b3fa77545c990d6251444b83b70b6f8980")).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check it was saved correctly with its attempt
		tx, err := store.GetTx(inProgressTx.FromAddress, inProgressTx.ID)
		require.NoError(t, err)

		assert.Equal(t, "", tx.Error)
		assert.Len(t, tx.TxAttempts, 1)

		ethClient.AssertExpectations(t)
	})

	t.Run("previous run assigned nonce and broadcast and now the transaction has been confirmed", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)
		require.NoError(t, os.RemoveAll(config.KeysDir))
		keyStore := client.NewInsecureKeyStore(config.KeysDir)
		account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, nextNonce)
		ethClient := new(mocks.Client)

		eb := txmanager.NewEthBroadcaster(ethClient, store, keyStore, config)

		// Crashed right after we save the nonce to the tx so accounts.NextNonce has not been incremented yet
		nonce := nextNonce
		inProgressTx := esTesting.MustInsertInProgressTxWithAttempt(t, store, nextNonce, fromAddress)

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == uint64(nonce)
		})).Return(errors.New("nonce too low")).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check it was saved correctly with its attempt
		tx, err := store.GetTx(inProgressTx.FromAddress, inProgressTx.ID)
		require.NoError(t, err)

		assert.Equal(t, "", tx.Error)
		assert.Len(t, tx.TxAttempts, 1)

		ethClient.AssertExpectations(t)
	})

	t.Run("previous run assigned nonce and then failed to reach node for some reason and node is still down", func(t *testing.T) {
		failedToReachNodeError := context.DeadlineExceeded
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)
		require.NoError(t, os.RemoveAll(config.KeysDir))
		keyStore := client.NewInsecureKeyStore(config.KeysDir)
		account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, nextNonce)
		ethClient := new(mocks.Client)

		eb := txmanager.NewEthBroadcaster(ethClient, store, keyStore, config)

		// Crashed right after we save the nonce to the tx so accounts.NextNonce has not been incremented yet
		nonce := nextNonce
		inProgressTx := esTesting.MustInsertInProgressTxWithAttempt(t, store, nextNonce, fromAddress)

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == uint64(nonce)
		})).Return(failedToReachNodeError).Once()

		// Do the thing
		err := eb.ProcessUnstartedTxs(account)
		require.Error(t, err)
		assert.Contains(t, err.Error(), failedToReachNodeError.Error())

		// Check it was left in the unfinished state
		tx, err := store.GetTx(inProgressTx.FromAddress, inProgressTx.ID)
		require.NoError(t, err)

		assert.Equal(t, nextNonce, tx.Nonce)
		assert.Equal(t, "", tx.Error)
		assert.Len(t, tx.TxAttempts, 1)

		ethClient.AssertExpectations(t)
	})

	t.Run("previous run assigned nonce and broadcast transaction then crashed and rebooted with a different configured gas price", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)
		require.NoError(t, os.RemoveAll(config.KeysDir))
		keyStore := client.NewInsecureKeyStore(config.KeysDir)
		account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, nextNonce)
		ethClient := new(mocks.Client)

		// Configured gas price changed
		config.DefaultGasPrice = big.NewInt(500000000000)

		eb := txmanager.NewEthBroadcaster(ethClient, store, keyStore, config)

		// Crashed right after we save the nonce to the tx so accounts.NextNonce has not been incremented yet
		nonce := nextNonce
		inProgressTx := esTesting.MustInsertInProgressTxWithAttempt(t, store, nextNonce, fromAddress)
		require.Len(t, inProgressTx.TxAttempts, 1)
		attempt := inProgressTx.TxAttempts[0]

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			// Ensure that the gas price is the same as the original attempt
			s, e := attempt.GetSignedTx()
			require.NoError(t, e)
			return tx.Nonce() == uint64(nonce) && tx.GasPrice().Int64() == s.GasPrice().Int64()
		})).Return(errors.New("known transaction: a1313bd99a81fb4d8ad1d2e90b67c6b3fa77545c990d6251444b83b70b6f8980")).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check it was saved correctly with its attempt
		tx, err := store.GetTx(inProgressTx.FromAddress, inProgressTx.ID)
		require.NoError(t, err)

		assert.Equal(t, "", tx.Error)
		assert.Len(t, tx.TxAttempts, 1)
		attempt = tx.TxAttempts[0]
		s, err := attempt.GetSignedTx()
		require.NoError(t, err)
		assert.Equal(t, int64(342), s.GasPrice().Int64())
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt.State)

		ethClient.AssertExpectations(t)
	})
}

func getLocalNextNonce(t *testing.T, store esStore.Store, fromAddress gethCommon.Address) uint64 {
	n, err := store.GetNextNonce(fromAddress)
	require.NoError(t, err)
	require.NotNil(t, n)
	return uint64(n)
}

// Note that all of these tests share the same database, and ordering matters.
// This in order to more deeply test ProcessUnstartedTxs over
// multiple runs with previous errors in the database.
func TestEthBroadcaster_ProcessUnstartedTxs_Errors(t *testing.T) {
	var err error
	toAddress := gethCommon.HexToAddress("0x6C03DDA95a2AEd917EeCc6eddD4b9D16E6380411")
	value := big.NewInt(142)
	gasLimit := uint64(242)
	encodedPayload := []byte{0, 1}

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)
	require.NoError(t, os.RemoveAll(config.KeysDir))
	keyStore := client.NewInsecureKeyStore(config.KeysDir)
	account, fromAddress := esTesting.MustAddRandomAccountToKeystore(t, store, keyStore, 0)
	ethClient := new(mocks.Client)

	eb := txmanager.NewEthBroadcaster(ethClient, store, keyStore, config)

	t.Run("if external wallet sent a transaction from the account and now the nonce is one higher than it should be and we got replacement underpriced then we assume a previous transaction of ours was the one that succeeded, and hand off to EthConfirmer", func(t *testing.T) {
		tx := &models.Tx{
			ID:             uuid.NewV4(),
			FromAddress:    fromAddress,
			ToAddress:      toAddress,
			EncodedPayload: encodedPayload,
			Value:          *value,
			GasLimit:       gasLimit,
			State:          models.TxStateUnstarted,
		}
		require.NoError(t, store.PutTx(tx))

		// First send, replacement underpriced
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == uint64(0)
		})).Return(errors.New("replacement transaction underpriced")).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		ethClient.AssertExpectations(t)

		// Check that the transaction was saved correctly with its attempt
		// We assume success and hand off to eth confirmer to eventually mark it as failed
		tx, err = store.GetTx(tx.FromAddress, tx.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(0), tx.Nonce)
		assert.Equal(t, "", tx.Error)
		assert.Len(t, tx.TxAttempts, 1)

		// Check that the local nonce was incremented by one
		finalNextNonce, err := store.GetNextNonce(fromAddress)
		require.NoError(t, err)
		require.Equal(t, int64(1), finalNextNonce)
	})

	t.Run("geth client returns an error in the fatal errors category", func(t *testing.T) {
		fatalErrorExample := "exceeds block gas limit"
		localNextNonce := getLocalNextNonce(t, store, fromAddress)

		tx := &models.Tx{
			ID:             uuid.NewV4(),
			FromAddress:    fromAddress,
			ToAddress:      toAddress,
			EncodedPayload: encodedPayload,
			Value:          *value,
			GasLimit:       gasLimit,
			State:          models.TxStateUnstarted,
		}
		require.NoError(t, store.PutTx(tx))

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == localNextNonce
		})).Return(errors.New(fatalErrorExample)).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check it was saved correctly with its attempt
		tx, err = store.GetTx(tx.FromAddress, tx.ID)
		require.NoError(t, err)

		require.Equal(t, int64(-1), tx.Nonce)
		assert.Contains(t, tx.Error, "exceeds block gas limit")
		assert.Len(t, tx.TxAttempts, 0)

		// Check that the key had its nonce reset
		account, err = store.GetAccount(account.Address)
		// Saved NextNonce must be the same as before because this transaction
		// was not accepted by the eth node and never can be
		require.NotNil(t, account.NextNonce)
		require.Equal(t, int64(localNextNonce), account.NextNonce)

		ethClient.AssertExpectations(t)
	})

	t.Run("eth client call fails with an unexpected random error (e.g. insufficient funds)", func(t *testing.T) {
		retryableErrorExample := "insufficient funds for transfer"
		localNextNonce := getLocalNextNonce(t, store, fromAddress)

		tx := &models.Tx{
			ID:             uuid.NewV4(),
			FromAddress:    fromAddress,
			ToAddress:      toAddress,
			EncodedPayload: encodedPayload,
			Value:          *value,
			GasLimit:       gasLimit,
			State:          models.TxStateUnstarted,
		}
		require.NoError(t, store.PutTx(tx))

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == localNextNonce
		})).Return(errors.New(retryableErrorExample)).Once()

		// Do the thing
		err = eb.ProcessUnstartedTxs(account)
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("error while sending transaction %v: insufficient funds for transfer", tx.ID))

		// Check it was saved correctly with its attempt
		tx, err = store.GetTx(tx.FromAddress, tx.ID)
		require.NoError(t, err)

		require.NotEqual(t, int64(-1), tx.Nonce)
		assert.Equal(t, "", tx.Error)
		assert.Equal(t, models.TxStateInProgress, tx.State)
		assert.Len(t, tx.TxAttempts, 1)
		attempt := tx.TxAttempts[0]
		assert.Equal(t, models.TxAttemptStateInProgress, attempt.State)

		ethClient.AssertExpectations(t)

		// Now on the second run, it is successful
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == localNextNonce
		})).Return(nil).Once()

		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check it was saved correctly with its attempt
		tx, err = store.GetTx(tx.FromAddress, tx.ID)
		require.NoError(t, err)

		require.NotEqual(t, int64(-1), tx.Nonce)
		assert.Equal(t, "", tx.Error)
		assert.Equal(t, models.TxStateUnconfirmed, tx.State)
		assert.Len(t, tx.TxAttempts, 1)
		attempt = tx.TxAttempts[0]
		assert.Equal(t, models.TxAttemptStateBroadcast, attempt.State)

		ethClient.AssertExpectations(t)
	})

	t.Run("eth node returns underpriced transaction", func(t *testing.T) {
		// This happens if a transaction's gas price is below the minimum
		// configured for the transaction pool.
		// This is a configuration error, since it means they set the base gas level too low.
		underpricedError := "transaction underpriced"
		localNextNonce := getLocalNextNonce(t, store, fromAddress)

		tx := &models.Tx{
			ID:             uuid.NewV4(),
			FromAddress:    fromAddress,
			ToAddress:      toAddress,
			EncodedPayload: encodedPayload,
			Value:          *value,
			GasLimit:       gasLimit,
			State:          models.TxStateUnstarted,
		}
		require.NoError(t, store.PutTx(tx))

		// First was underpriced
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == localNextNonce && tx.GasPrice().Cmp(config.DefaultGasPrice) == 0
		})).Return(errors.New(underpricedError)).Once()

		// Second with gas bump was still underpriced
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == localNextNonce && tx.GasPrice().Cmp(big.NewInt(25000000000)) == 0
		})).Return(errors.New(underpricedError)).Once()

		// Third succeeded
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == localNextNonce && tx.GasPrice().Cmp(big.NewInt(30000000000)) == 0
		})).Return(nil).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		ethClient.AssertExpectations(t)

		// Check it was saved correctly with its attempt
		tx, err = store.GetTx(tx.FromAddress, tx.ID)
		require.NoError(t, err)

		require.NotEqual(t, int64(-1), tx.Nonce)
		assert.Equal(t, "", tx.Error)
		assert.Len(t, tx.TxAttempts, 1)
		attempt := tx.TxAttempts[0]
		assert.Equal(t, big.NewInt(30000000000).String(), attempt.GasPrice.String())
	})

	txUnfinished := &models.Tx{
		ID:             uuid.NewV4(),
		FromAddress:    fromAddress,
		ToAddress:      toAddress,
		EncodedPayload: encodedPayload,
		Value:          *value,
		GasLimit:       gasLimit,
		State:          models.TxStateUnstarted,
	}
	require.NoError(t, store.PutTx(txUnfinished))

	t.Run("failed to reach node for some reason", func(t *testing.T) {
		failedToReachNodeError := context.DeadlineExceeded
		localNextNonce := getLocalNextNonce(t, store, fromAddress)

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == localNextNonce
		})).Return(failedToReachNodeError).Once()

		// Do the thing
		err = eb.ProcessUnstartedTxs(account)
		require.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("error while sending transaction %v: context deadline exceeded", txUnfinished.ID))

		// Check it was left in the unfinished state
		tx, err := store.GetTx(txUnfinished.FromAddress, txUnfinished.ID)
		require.NoError(t, err)

		assert.NotEqual(t, int64(-1), tx.Nonce)
		assert.Equal(t, "", tx.Error)
		assert.Equal(t, models.TxStateInProgress, tx.State)
		assert.Len(t, tx.TxAttempts, 1)
		assert.Equal(t, models.TxAttemptStateInProgress, tx.TxAttempts[0].State)

		ethClient.AssertExpectations(t)
	})

	t.Run("eth node returns temporarily underpriced transaction", func(t *testing.T) {
		// This happens if parity is rejecting transactions that are not priced high enough to even get into the mempool at all
		// It should pretend it was accepted into the mempool and hand off to ethConfirmer to bump gas as normal
		temporarilyUnderpricedError := "There are too many transactions in the queue. Your transaction was dropped due to limit. Try increasing the fee."
		localNextNonce := getLocalNextNonce(t, store, fromAddress)

		// Re-use the previously unfinished transaction, no need to insert new

		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == localNextNonce
		})).Return(errors.New(temporarilyUnderpricedError)).Once()

		// Do the thing
		require.NoError(t, eb.ProcessUnstartedTxs(account))

		// Check it was saved correctly with its attempt
		tx, err := store.GetTx(txUnfinished.FromAddress, txUnfinished.ID)
		require.NoError(t, err)

		require.NotEqual(t, int64(-1), tx.Nonce)
		assert.Equal(t, "", tx.Error)
		assert.Len(t, tx.TxAttempts, 1)
		attempt := tx.TxAttempts[0]
		assert.Equal(t, big.NewInt(20000000000).String(), attempt.GasPrice.String())

		ethClient.AssertExpectations(t)
	})

	t.Run("eth node returns underpriced transaction and bumping gas doesn't increase it", func(t *testing.T) {
		// This happens if a transaction's gas price is below the minimum
		// configured for the transaction pool.
		// This is a configuration error, since it means they set the base gas level too low.
		underpricedError := "transaction underpriced"
		localNextNonce := getLocalNextNonce(t, store, fromAddress)
		// Mess up the config and set the bump to zero
		config.GasBumpWei = big.NewInt(0)
		config.GasBumpPercent = 0

		tx := &models.Tx{
			ID:             uuid.NewV4(),
			FromAddress:    fromAddress,
			ToAddress:      toAddress,
			EncodedPayload: encodedPayload,
			Value:          *value,
			GasLimit:       gasLimit,
			State:          models.TxStateUnstarted,
		}
		require.NoError(t, store.PutTx(tx))

		// First was underpriced
		ethClient.On("SendTransaction", mock.Anything, mock.MatchedBy(func(tx *gethTypes.Transaction) bool {
			return tx.Nonce() == localNextNonce && tx.GasPrice().Cmp(config.DefaultGasPrice) == 0
		})).Return(errors.New(underpricedError)).Once()

		// Do the thing
		err := eb.ProcessUnstartedTxs(account)
		require.Error(t, err)
		require.Contains(t, err.Error(), "bumped gas price of 20000000000 is equal to original gas price of 20000000000. ACTION REQUIRED: This is a configuration error, you must increase either GasBumpPercent or GasBumpWei")

		ethClient.AssertExpectations(t)
	})
}

// TODO: Add tests
