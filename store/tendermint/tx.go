package tendermint

import (
	"math/big"
	"sort"

	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	errStrDecodeTx = "could not decode Tx"
)

var (
	prefixTx = []byte("tx")
)

func (store *TMStore) AddTx(
	txID uuid.UUID,
	fromAddress common.Address,
	toAddress common.Address,
	encodedPayload []byte,
	value *big.Int,
	gasLimit uint64,
) error {
	account, err := store.GetAccount(fromAddress)
	if err != nil {
		return err
	}
	tx := models.Tx{
		ID:             txID,
		FromAddress:    fromAddress,
		ToAddress:      toAddress,
		EncodedPayload: encodedPayload,
		Value:          value,
		GasLimit:       gasLimit,
		State:          models.TxStateUnstarted,
	}
	err = store.PutTx(&tx)
	if err != nil {
		return err
	}
	account.PendingTxIDs = append(account.PendingTxIDs, txID)
	return store.PutAccount(account)
}

func (store *TMStore) PutTx(tx *models.Tx) error {
	return set(store.nsTx, tx.ID[:], tx)
}

func (store *TMStore) GetTx(id uuid.UUID) (*models.Tx, error) {
	var tx models.Tx
	err := get(store.nsTx, id[:], &tx)
	if err != nil {
		return nil, err
	}
	return &tx, nil
}

func (store *TMStore) GetOneInProgressTx(fromAddress common.Address) (*models.Tx, error) {
	account, err := store.GetAccount(fromAddress)
	if err != nil {
		return nil, err
	}
	var inProgressTx *models.Tx
	for _, txID := range account.PendingTxIDs {
		tx, getTxErr := store.GetTx(txID)
		if getTxErr != nil {
			return nil, getTxErr
		}
		if tx.State == models.TxStateInProgress {
			inProgressTx = tx
			break
		}

	}
	if inProgressTx == nil {
		return nil, esStore.ErrNotFound
	}
	return inProgressTx, nil
}

func (store *TMStore) GetNextUnstartedTx(fromAddress common.Address) (*models.Tx, error) {
	account, err := store.GetAccount(fromAddress)
	if err != nil {
		return nil, err
	}
	var unstartedTx *models.Tx
	for _, txID := range account.PendingTxIDs {
		tx, getTxErr := store.GetTx(txID)
		if getTxErr != nil {
			return nil, getTxErr
		}
		if tx.State == models.TxStateUnstarted {
			unstartedTx = tx
			break
		}
	}
	if unstartedTx == nil {
		return nil, esStore.ErrNotFound
	}
	return unstartedTx, nil
}

func (store *TMStore) GetTxsRequiringReceiptFetch() ([]*models.Tx, error) {
	var txs []*models.Tx
	iter, err := store.nsTx.Iterator(nil, nil)
	if err != nil {
		return nil, toCreateIterError(err)
	}
	var iterError error
	for ; iter.Valid(); iter.Next() {
		var tx models.Tx
		value := iter.Value()
		unmarshalErr := msgpack.Unmarshal(value, &tx)
		if unmarshalErr != nil {
			iterError = toDecodeTxError(err)
			break
		}
		if tx.State == models.TxStateUnconfirmed || tx.State == models.TxStateConfirmedMissingReceipt {
			txs = append(txs, &tx)
		}
	}
	iter.Close()
	if iterError != nil {
		return nil, iterError
	}
	return txs, nil
}

func (store *TMStore) SetBroadcastBeforeBlockNum(blockNum int64) error {
	iter, err := store.nsTxAttempt.Iterator(nil, nil)
	if err != nil {
		return toCreateIterError(err)
	}
	var iterError error
	for ; iter.Valid(); iter.Next() {
		var attempt models.TxAttempt
		value := iter.Value()
		unmarshalErr := msgpack.Unmarshal(value, &attempt)
		if unmarshalErr != nil {
			iterError = toDecodeTxAttemptError(err)
			break
		}
		if attempt.State == models.TxAttemptStateBroadcast && attempt.BroadcastBeforeBlockNum == -1 {
			attempt.BroadcastBeforeBlockNum = blockNum
			putErr := store.PutTxAttempt(&attempt)
			if putErr != nil {
				return putErr
			}
		}
	}
	iter.Close()
	if iterError != nil {
		return iterError
	}
	return nil
}

func (store *TMStore) MarkConfirmedMissingReceipt() error {
	accounts, err := store.GetAccounts()
	if err != nil {
		return err
	}
	for _, account := range accounts {
		// Get max nonce for confirmed Txs
		var txs []*models.Tx
		var maxNonce int64 = -1
		for _, txID := range account.PendingTxIDs {
			tx, getTxErr := store.GetTx(txID)
			if getTxErr != nil {
				return getTxErr
			}
			if tx.State == models.TxStateConfirmed && tx.Nonce > maxNonce {
				maxNonce = tx.Nonce
			}
			txs = append(txs, tx)
		}

		// Set to confirmed_missing_receipt for stale unconfirmed Txs
		var txsToUpdate []*models.Tx
		for _, tx := range txs {
			if tx.State == models.TxStateUnconfirmed && tx.Nonce < maxNonce {
				tx.State = models.TxStateConfirmedMissingReceipt
				txsToUpdate = append(txsToUpdate, tx)
			}
		}
		for _, tx := range txsToUpdate {
			err = store.PutTx(tx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (store *TMStore) MarkOldTxsMissingReceiptAsErrored(cutoff int64) error {
	accounts, err := store.GetAccounts()
	if err != nil {
		return err
	}
	for _, account := range accounts {
		var txsToUpdate []*models.Tx
		for _, txID := range account.PendingTxIDs {
			tx, getTxErr := store.GetTx(txID)
			if getTxErr != nil {
				return getTxErr
			}
			if tx.State == models.TxStateConfirmedMissingReceipt {
				var maxAttemptBroadcastBeforeBlockNum int64 = -1
				for _, attemptID := range tx.TxAttemptIDs {
					attempt, getAttemptErr := store.GetTxAttempt(attemptID)
					if getAttemptErr != nil {
						return getAttemptErr
					}
					if attempt.BroadcastBeforeBlockNum > maxAttemptBroadcastBeforeBlockNum {
						maxAttemptBroadcastBeforeBlockNum = attempt.BroadcastBeforeBlockNum
					}
				}
				if maxAttemptBroadcastBeforeBlockNum != int64(-1) &&
					maxAttemptBroadcastBeforeBlockNum < cutoff {
					tx.State = models.TxStateFatalError
					tx.Nonce = -1
					tx.Error = esStore.ErrCouldNotGetReceipt.Error()
					txsToUpdate = append(txsToUpdate, tx)
				}
			}
		}
		for _, tx := range txsToUpdate {
			err = store.PutTx(tx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (store *TMStore) GetTxsRequiringNewAttempt(
	address common.Address,
	blockNum int64,
	gasBumpThreshold int64,
	depth int,
) ([]*models.Tx, error) {
	account, err := store.GetAccount(address)
	if err != nil {
		return nil, err
	}
	var txs []*models.Tx
	for _, txID := range account.PendingTxIDs {
		tx, getTxErr := store.GetTx(txID)
		if getTxErr != nil {
			return nil, getTxErr
		}
		if tx.State != models.TxStateUnconfirmed {
			continue
		}
		includeTx := true
		for _, attemptID := range tx.TxAttemptIDs {
			attempt, getAttemptErr := store.GetTxAttempt(attemptID)
			if getAttemptErr != nil {
				return nil, getAttemptErr
			}
			includeAttempt := attempt.State != models.TxAttemptStateInsufficientEth &&
				(attempt.State != models.TxAttemptStateBroadcast ||
					attempt.BroadcastBeforeBlockNum == int64(-1) ||
					attempt.BroadcastBeforeBlockNum > blockNum-gasBumpThreshold)
			if !includeAttempt {
				includeTx = false
				break
			}
		}
		if includeTx {
			txs = append(txs, tx)
		}
	}

	// Sort txs by ascending nonce
	sort.Slice(txs, func(i int, j int) bool {
		return txs[i].Nonce < txs[j].Nonce
	})

	if depth > 0 && depth < len(txs) {
		txs = txs[0:depth]
	}
	return txs, nil
}

func (store *TMStore) GetTxsConfirmedAtOrAboveBlockHeight(blockNum int64) ([]*models.Tx, error) {
	var allTxs []*models.Tx
	accounts, err := store.GetAccounts()
	if err != nil {
		return nil, err
	}
	for _, account := range accounts {
		var txs []*models.Tx
		for _, txID := range account.PendingTxIDs {
			tx, getTxErr := store.GetTx(txID)
			if getTxErr != nil {
				return nil, getTxErr
			}
			if tx.State != models.TxStateConfirmed && tx.State != models.TxStateConfirmedMissingReceipt {
				continue
			}
			// TODO: Just checking the last attempt should suffice
			includeTx := false
			for i := len(tx.TxAttemptIDs) - 1; i >= 0; i-- {
				attemptID := tx.TxAttemptIDs[i]
				attempt, getAttemptErr := store.GetTxAttempt(attemptID)
				if getAttemptErr != nil {
					return nil, getAttemptErr
				}
				if attempt.State != models.TxAttemptStateBroadcast {
					continue
				}
				for j := len(attempt.ReceiptIDs) - 1; j >= 0; j-- {
					receiptID := attempt.ReceiptIDs[j]
					receipt, getReceiptErr := store.GetTxReceipt(receiptID)
					if getReceiptErr != nil {
						return nil, getReceiptErr
					}
					if receipt.BlockNumber >= blockNum {
						includeTx = true
						break
					}
				}
				if includeTx {
					break
				}
			}
			if includeTx {
				txs = append(txs, tx)
			}
		}

		// Sort txs by ascending nonce
		sort.Slice(txs, func(i int, j int) bool {
			return txs[i].Nonce < txs[j].Nonce
		})
		allTxs = append(allTxs, txs...)
	}
	return allTxs, nil
}

func (store *TMStore) GetInProgressAttempts(address common.Address) ([]*models.TxAttempt, error) {
	account, err := store.GetAccount(address)
	if err != nil {
		return nil, err
	}
	var attempts []*models.TxAttempt
	for _, txID := range account.PendingTxIDs {
		tx, getTxErr := store.GetTx(txID)
		if getTxErr != nil {
			return nil, err
		}
		if tx.State == models.TxStateConfirmed || tx.State == models.TxStateConfirmedMissingReceipt ||
			tx.State == models.TxStateUnconfirmed {
			for _, attemptID := range tx.TxAttemptIDs {
				attempt, getAttemptErr := store.GetTxAttempt(attemptID)
				if getAttemptErr != nil {
					return nil, err
				}
				if attempt.State == models.TxAttemptStateInProgress {
					attempts = append(attempts, attempt)
				}

			}
		}

	}
	return attempts, nil
}

func (store *TMStore) IsTxConfirmedAtOrBeforeBlockNumber(txID uuid.UUID, blockNumber int64) (bool, error) {
	tx, err := store.GetTx(txID)
	if err != nil {
		return false, err
	}
	if tx.State != models.TxStateConfirmed && tx.State != models.TxStateConfirmedMissingReceipt {
		return false, nil
	}
	// TODO: Just checking the last attempt should suffice
	isConfirmed := false
	for i := len(tx.TxAttemptIDs) - 1; i >= 0; i-- {
		attemptID := tx.TxAttemptIDs[i]
		attempt, getAttemptErr := store.GetTxAttempt(attemptID)
		if getAttemptErr != nil {
			return false, getAttemptErr
		}
		if attempt.State != models.TxAttemptStateBroadcast {
			continue
		}
		for j := len(attempt.ReceiptIDs) - 1; j >= 0; j-- {
			receiptID := attempt.ReceiptIDs[j]
			receipt, getReceiptErr := store.GetTxReceipt(receiptID)
			if getReceiptErr != nil {
				return false, getReceiptErr
			}
			if receipt.BlockNumber <= blockNumber {
				isConfirmed = true
				break
			}
		}
		if isConfirmed {
			break
		}
	}
	return isConfirmed, nil
}

func toDecodeTxError(err error) error {
	return errors.Wrap(err, errStrDecodeTx)
}
