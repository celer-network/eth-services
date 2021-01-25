package tendermint

import (
	"math/big"
	"sort"

	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	tmDB "github.com/tendermint/tm-db"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	errStrDecodeTx      = "could not decode tx"
	errStrDecodeAccount = "could not decode account"
)

var (
	prefixAccount     = []byte("acct")
	prefixTx          = []byte("tx")
	prefixCompletedTx = []byte("ctx")
	prefixFailedTx    = []byte("ftx")

	keyLastTx = []byte("ltx")
)

func (store *TMStore) PutAccount(account *models.Account) error {
	return set(store.nsAccount, account.Address.Bytes(), account)
}

func (store *TMStore) GetAccount(fromAddress common.Address) (*models.Account, error) {
	var account models.Account
	err := get(store.nsAccount, fromAddress.Bytes(), &account)
	if err != nil {
		return nil, err
	}
	return &account, nil
}

func (store *TMStore) GetAccounts() ([]*models.Account, error) {
	iter, err := store.nsAccount.Iterator(nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, errStrCreateIter)
	}
	var accounts []*models.Account
	for ; iter.Valid(); iter.Next() {
		value := iter.Value()
		var account models.Account
		unmarshalErr := msgpack.Unmarshal(value, &account)
		if unmarshalErr != nil {
			return nil, toDecodeAccountError(err)
		}
		accounts = append(accounts, &account)
	}
	if len(accounts) == 0 {
		return nil, esStore.ErrNotFound
	}
	return accounts, nil
}

func (store *TMStore) AddTx(
	txID uuid.UUID,
	fromAddress common.Address,
	toAddress common.Address,
	value *big.Int,
	encodedPayload []byte,
	gasLimit uint64,
) error {
	tx := models.Tx{
		ID:             txID,
		FromAddress:    fromAddress,
		ToAddress:      toAddress,
		EncodedPayload: encodedPayload,
		Value:          value,
		GasLimit:       gasLimit,
		State:          models.TxStateUnstarted,
	}
	return store.PutTx(&tx)
}

func (store *TMStore) PutTx(tx *models.Tx) error {
	nsTxAddr := tmDB.NewPrefixDB(store.nsTx, tx.FromAddress.Bytes())
	return set(nsTxAddr, tx.ID[:], tx)
}

func (store *TMStore) GetTx(fromAddress common.Address, id uuid.UUID) (*models.Tx, error) {
	nsTxAddr := tmDB.NewPrefixDB(store.nsTx, fromAddress.Bytes())
	var tx models.Tx
	err := get(nsTxAddr, id[:], &tx)
	if err != nil {
		return nil, err
	}
	return &tx, nil
}

func (store *TMStore) GetTxs(fromAddress common.Address) ([]*models.Tx, error) {
	nsTxAddr := tmDB.NewPrefixDB(store.nsTx, fromAddress.Bytes())
	var txs []*models.Tx
	iter, err := nsTxAddr.Iterator(nil, nil)
	if err != nil {
		return nil, toCreateIterError(err)
	}
	var iterError error
	for ; iter.Valid(); iter.Next() {
		value := iter.Value()
		var tx models.Tx
		unmarshalErr := msgpack.Unmarshal(value, &tx)
		if unmarshalErr != nil {
			iterError = toDecodeTxError(err)
			break
		}
		txs = append(txs, &tx)
	}
	iter.Close()
	if iterError != nil {
		return nil, iterError
	}
	if len(txs) == 0 {
		return nil, esStore.ErrNotFound
	}
	return txs, nil
}

func (store *TMStore) GetOneInProgressTx(fromAddress common.Address) (*models.Tx, error) {
	nsTxAddr := tmDB.NewPrefixDB(store.nsTx, fromAddress.Bytes())
	iter, err := nsTxAddr.Iterator(nil, nil)
	if err != nil {
		return nil, toCreateIterError(err)
	}
	var inProgressTx *models.Tx
	var iterError error
	for ; iter.Valid(); iter.Next() {
		value := iter.Value()
		var tx models.Tx
		unmarshalErr := msgpack.Unmarshal(value, &tx)
		if unmarshalErr != nil {
			iterError = toDecodeTxError(err)
			break
		}
		if tx.State == models.TxStateInProgress {
			inProgressTx = &tx
			break
		}
	}
	iter.Close()
	if iterError != nil {
		return nil, iterError
	}
	if inProgressTx == nil {
		return nil, esStore.ErrNotFound
	}
	return inProgressTx, nil
}

func (store *TMStore) GetNextNonce(address common.Address) (int64, error) {
	var account models.Account
	err := get(store.nsAccount, address.Bytes(), &account)
	if err != nil {
		return 0, err
	}
	return account.NextNonce, nil
}

func (store *TMStore) SetNextNonce(address common.Address, nextNonce int64) error {
	var account models.Account
	err := get(store.nsAccount, address.Bytes(), &account)
	if err != nil {
		return err
	}
	account.NextNonce = nextNonce
	err = set(store.nsAccount, address.Bytes(), &account)
	if err != nil {
		return err
	}
	return nil
}

func (store *TMStore) GetNextUnstartedTx(fromAddress common.Address) (*models.Tx, error) {
	nsTxAddr := tmDB.NewPrefixDB(store.nsTx, fromAddress.Bytes())
	iter, err := nsTxAddr.Iterator(nil, nil)
	if err != nil {
		return nil, toCreateIterError(err)
	}
	var unstartedTx *models.Tx
	var iterError error
	for ; iter.Valid(); iter.Next() {
		var tx models.Tx
		value := iter.Value()
		unmarshalErr := msgpack.Unmarshal(value, &tx)
		if unmarshalErr != nil {
			iterError = toDecodeTxError(err)
			break
		}
		if tx.State == models.TxStateUnstarted {
			unstartedTx = &tx
			break
		}
	}
	iter.Close()
	if iterError != nil {
		return nil, iterError
	}
	if unstartedTx == nil {
		return nil, esStore.ErrNotFound
	}
	return unstartedTx, nil
}

func (store *TMStore) GetTxsRequiringReceiptFetch() ([]*models.Tx, error) {
	accounts, err := store.GetAccounts()
	if err != nil {
		return nil, err
	}
	var txs []*models.Tx
	for _, account := range accounts {
		nsTxAddr := tmDB.NewPrefixDB(store.nsTx, account.Address.Bytes())
		iter, err := nsTxAddr.Iterator(nil, nil)
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
	}
	return txs, nil
}

func (store *TMStore) SetBroadcastBeforeBlockNum(blockNum int64) error {
	accounts, err := store.GetAccounts()
	if err != nil {
		return err
	}
	// Get all Txs
	var txs []*models.Tx
	for _, account := range accounts {
		nsTxAddr := tmDB.NewPrefixDB(store.nsTx, account.Address.Bytes())
		iter, err := nsTxAddr.Iterator(nil, nil)
		if err != nil {
			return toCreateIterError(err)
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
			txs = append(txs, &tx)
		}
		iter.Close()
		if iterError != nil {
			return iterError
		}
	}
	for _, tx := range txs {
		for _, attempt := range tx.TxAttempts {
			if attempt.State == models.TxAttemptStateBroadcast && attempt.BroadcastBeforeBlockNum == -1 {
				attempt.BroadcastBeforeBlockNum = blockNum
				putErr := store.PutTx(tx)
				if putErr != nil {
					return putErr
				}
			}
		}
	}
	return nil
}

func (store *TMStore) MarkConfirmedMissingReceipt() error {
	accounts, err := store.GetAccounts()
	if err != nil {
		return err
	}
	for _, account := range accounts {
		nsTxAddr := tmDB.NewPrefixDB(store.nsTx, account.Address.Bytes())
		// Get max nonce for confirmed Txs
		iter, err := nsTxAddr.Iterator(nil, nil)
		if err != nil {
			return toCreateIterError(err)
		}
		var maxNonce int64 = -1
		var iterError error
		for ; iter.Valid(); iter.Next() {
			var tx models.Tx
			value := iter.Value()
			unmarshalErr := msgpack.Unmarshal(value, &tx)
			if unmarshalErr != nil {
				iterError = toDecodeTxError(err)
				break
			}
			if tx.State == models.TxStateConfirmed && tx.Nonce > maxNonce {
				maxNonce = tx.Nonce
			}
		}
		iter.Close()
		if iterError != nil {
			return iterError
		}

		// Set to confirmed_missing_receipt for stale unconfirmed Txs
		iter, err = nsTxAddr.Iterator(nil, nil)
		if err != nil {
			return toCreateIterError(err)
		}

		var txsToUpdate []*models.Tx
		for ; iter.Valid(); iter.Next() {
			var tx models.Tx
			value := iter.Value()
			unmarshalErr := msgpack.Unmarshal(value, &tx)
			if unmarshalErr != nil {
				iterError = toDecodeTxError(err)
				break
			}
			if tx.State == models.TxStateUnconfirmed && tx.Nonce < maxNonce {
				tx.State = models.TxStateConfirmedMissingReceipt
				txsToUpdate = append(txsToUpdate, &tx)
			}
		}
		iter.Close()
		if iterError != nil {
			return iterError
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
		nsTxAddr := tmDB.NewPrefixDB(store.nsTx, account.Address.Bytes())
		// Get max nonce for confirmed Txs
		iter, err := nsTxAddr.Iterator(nil, nil)
		if err != nil {
			return toCreateIterError(err)
		}
		var iterError error
		var txsToUpdate []*models.Tx
		for ; iter.Valid(); iter.Next() {
			var tx models.Tx
			value := iter.Value()
			unmarshalErr := msgpack.Unmarshal(value, &tx)
			if unmarshalErr != nil {
				iterError = toDecodeTxError(err)
				break
			}
			if tx.State == models.TxStateConfirmedMissingReceipt {
				var maxAttemptBroadcastBeforeBlockNum int64 = -1
				for _, attempt := range tx.TxAttempts {
					if attempt.BroadcastBeforeBlockNum > maxAttemptBroadcastBeforeBlockNum {
						maxAttemptBroadcastBeforeBlockNum = attempt.BroadcastBeforeBlockNum
					}
				}
				if maxAttemptBroadcastBeforeBlockNum != int64(-1) &&
					maxAttemptBroadcastBeforeBlockNum < cutoff {
					tx.State = models.TxStateFatalError
					tx.Nonce = -1
					tx.Error = esStore.ErrCouldNotGetReceipt.Error()
					txsToUpdate = append(txsToUpdate, &tx)
				}
			}
		}
		iter.Close()
		if iterError != nil {
			return iterError
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

func (store *TMStore) GetTxsRequiringNewAttempt(address common.Address, blockNum int64, gasBumpThreshold int64, depth int) ([]*models.Tx, error) {
	nsTxAddr := tmDB.NewPrefixDB(store.nsTx, address.Bytes())
	iter, err := nsTxAddr.Iterator(nil, nil)
	if err != nil {
		return nil, toCreateIterError(err)
	}
	var iterError error
	var txs []*models.Tx
	for ; iter.Valid(); iter.Next() {
		var tx models.Tx
		value := iter.Value()
		unmarshalErr := msgpack.Unmarshal(value, &tx)
		if unmarshalErr != nil {
			iterError = toDecodeTxError(err)
			break
		}
		if tx.State != models.TxStateUnconfirmed {
			continue
		}
		includeTx := true
		for _, attempt := range tx.TxAttempts {
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
			txs = append(txs, &tx)
		}
	}
	iter.Close()
	if iterError != nil {
		return nil, iterError
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
		nsTxAddr := tmDB.NewPrefixDB(store.nsTx, account.Address.Bytes())
		iter, err := nsTxAddr.Iterator(nil, nil)
		if err != nil {
			return nil, toCreateIterError(err)
		}
		var iterError error
		var txs []*models.Tx
		for ; iter.Valid(); iter.Next() {
			var tx models.Tx
			value := iter.Value()
			unmarshalErr := msgpack.Unmarshal(value, &tx)
			if unmarshalErr != nil {
				iterError = toDecodeTxError(err)
				break
			}
			if tx.State != models.TxStateConfirmed && tx.State != models.TxStateConfirmedMissingReceipt {
				continue
			}
			// TODO: Just checking the last attempt should suffice
			includeTx := false
			for i := len(tx.TxAttempts) - 1; i >= 0; i-- {
				attempt := tx.TxAttempts[i]
				if attempt.State != models.TxAttemptStateBroadcast {
					continue
				}
				for j := len(attempt.Receipts) - 1; j >= 0; j-- {
					receipt := attempt.Receipts[j]
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
				txs = append(txs, &tx)
			}
		}
		iter.Close()
		if iterError != nil {
			return nil, iterError
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
	nsTxAddr := tmDB.NewPrefixDB(store.nsTx, address.Bytes())
	iter, err := nsTxAddr.Iterator(nil, nil)
	if err != nil {
		return nil, toCreateIterError(err)
	}
	var iterError error
	var attempts []*models.TxAttempt
	for ; iter.Valid(); iter.Next() {
		var tx models.Tx
		value := iter.Value()
		unmarshalErr := msgpack.Unmarshal(value, &tx)
		if unmarshalErr != nil {
			iterError = toDecodeTxError(err)
			break
		}
		if tx.State == models.TxStateConfirmed || tx.State == models.TxStateConfirmedMissingReceipt ||
			tx.State == models.TxStateUnconfirmed {
			for _, attempt := range tx.TxAttempts {
				if attempt.State == models.TxAttemptStateInProgress {
					attempts = append(attempts, &attempt)
				}
			}
		}
	}
	iter.Close()
	if iterError != nil {
		return nil, iterError
	}
	return attempts, nil
}

func toDecodeTxError(err error) error {
	return errors.Wrap(err, errStrDecodeTx)
}

func toDecodeAccountError(err error) error {
	return errors.Wrap(err, errStrDecodeAccount)
}
