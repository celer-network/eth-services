package tendermint

import (
	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	errStrDecodeAccount = "could not decode Account"
)

var (
	prefixAccount = []byte("act")
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

func toDecodeAccountError(err error) error {
	return errors.Wrap(err, errStrDecodeAccount)
}
