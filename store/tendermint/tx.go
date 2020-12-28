package tendermint

import (
	"math/big"

	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	tmdb "github.com/tendermint/tm-db"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	prefixAccount     = []byte("acct")
	prefixTx          = []byte("tx")
	prefixCompletedTx = []byte("ctx")
	prefixFailedTx    = []byte("ftx")

	keyLastTx = []byte("ltx")
)

func (store *TMStore) PutAccount(account *models.Account) error {
	return set(store.nsAccount, account.Address.Bytes(), &account)
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
		return nil, errors.Wrap(err, "could not create iterator")
	}
	var accounts []*models.Account
	for iter.Valid() {
		value := iter.Value()
		var account models.Account
		unmarshalErr := msgpack.Unmarshal(value, &account)
		if unmarshalErr != nil {
			return nil, errors.Wrap(err, "could not decode account")
		}
		accounts = append(accounts, &account)
		iter.Next()
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
	encodedPayload []byte,
	gasLimit uint64,
) error {
	tx := models.Tx{
		ID:             txID,
		FromAddress:    fromAddress,
		ToAddress:      toAddress,
		EncodedPayload: encodedPayload,
		Value:          *big.NewInt(0),
		GasLimit:       gasLimit,
		State:          models.TxStateUnstarted,
	}
	nsTxAddr := tmdb.NewPrefixDB(store.nsTx, fromAddress.Bytes())
	return set(nsTxAddr, fromAddress.Bytes(), tx)
}

func (store *TMStore) PutTx(tx *models.Tx) error {
	nsTxAddr := tmdb.NewPrefixDB(store.nsTx, tx.FromAddress.Bytes())
	return set(nsTxAddr, tx.ID.Bytes(), &tx)
}

func (store *TMStore) GetTx(fromAddress common.Address, id uuid.UUID) (*models.Tx, error) {
	nsTxAddr := tmdb.NewPrefixDB(store.nsTx, fromAddress.Bytes())
	var tx models.Tx
	err := get(nsTxAddr, id.Bytes(), &tx)
	if err != nil {
		return nil, err
	}
	return &tx, nil
}

func (store *TMStore) GetTxs(fromAddress common.Address) ([]*models.Tx, error) {
	nsTxAddr := tmdb.NewPrefixDB(store.nsTx, fromAddress.Bytes())
	var txs []*models.Tx
	iter, err := nsTxAddr.Iterator(nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not create iterator")
	}
	var iterError error
	for iter.Valid() {
		value := iter.Value()
		var tx models.Tx
		unmarshalErr := msgpack.Unmarshal(value, &tx)
		if unmarshalErr != nil {
			iterError = errors.Wrap(err, "could not decode tx")
			break
		}
		txs = append(txs, &tx)
		iter.Next()
	}
	err = iter.Close()
	if err != nil {
		return nil, errors.Wrap(err, "could not close iterator")
	}
	if iterError != nil {
		return nil, iterError
	}
	if len(txs) == 0 {
		return nil, esStore.ErrNotFound
	}
	return txs, nil
}

func (store *TMStore) GetOneInProgressTx(fromAddress common.Address) (*models.Tx, error) {
	nsTxAddr := tmdb.NewPrefixDB(store.nsTx, fromAddress.Bytes())
	iter, err := nsTxAddr.Iterator(nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not create iterator")
	}
	var inProgressTx *models.Tx
	var iterError error
	for iter.Valid() {
		value := iter.Value()
		var tx models.Tx
		unmarshalErr := msgpack.Unmarshal(value, &tx)
		if unmarshalErr != nil {
			iterError = errors.Wrap(err, "could not decode tx")
			break
		}
		if tx.State == models.TxStateInProgress {
			inProgressTx = &tx
			break
		}
		iter.Next()
	}
	err = iter.Close()
	if err != nil {
		return nil, errors.Wrap(err, "could not close iterator")
	}
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
	nsTxAddr := tmdb.NewPrefixDB(store.nsTx, fromAddress.Bytes())
	iter, err := nsTxAddr.Iterator(nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not create iterator")
	}
	var unstartedTx *models.Tx
	var iterError error
	for iter.Valid() {
		var tx models.Tx
		value := iter.Value()
		unmarshalErr := msgpack.Unmarshal(value, &tx)
		if unmarshalErr != nil {
			iterError = errors.Wrap(err, "could not decode tx")
			break
		}
		if tx.State == models.TxStateUnstarted {
			unstartedTx = &tx
			break
		}
		iter.Next()
	}
	err = iter.Close()
	if err != nil {
		return nil, errors.Wrap(err, "could not close iterator")
	}
	if iterError != nil {
		return nil, iterError
	}
	if unstartedTx == nil {
		return nil, esStore.ErrNotFound
	}
	return unstartedTx, nil
}
