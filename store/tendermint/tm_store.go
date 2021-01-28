package tendermint

import (
	"github.com/celer-network/eth-services/store"
	"github.com/pkg/errors"
	tmDB "github.com/tendermint/tm-db"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	errStrCreateIter = "could not create iterator"
)

// TMStore is a Store implementation using Tendermint tm-db
type TMStore struct {
	nsHead      *tmDB.PrefixDB
	nsAccount   *tmDB.PrefixDB
	nsTx        *tmDB.PrefixDB
	nsTxAttempt *tmDB.PrefixDB
	nsTxReceipt *tmDB.PrefixDB
	nsJob       *tmDB.PrefixDB
}

var _ store.Store = (*TMStore)(nil)

// NewTMStore creates a new TMStore
func NewTMStore(db tmDB.DB) *TMStore {
	return &TMStore{
		nsHead:      tmDB.NewPrefixDB(db, prefixHead),
		nsAccount:   tmDB.NewPrefixDB(db, prefixAccount),
		nsTx:        tmDB.NewPrefixDB(db, prefixTx),
		nsTxAttempt: tmDB.NewPrefixDB(db, prefixTxAttempt),
		nsTxReceipt: tmDB.NewPrefixDB(db, prefixReceipt),
		nsJob:       tmDB.NewPrefixDB(db, prefixJob),
	}
}

// get will retrieve the binary data under the given key from the DB and decode it into the given
// entity. The provided entity needs to be a pointer to an initialized entity of the correct type.
func get(db tmDB.DB, key []byte, entity interface{}) error {
	value, err := db.Get(key)
	if err != nil {
		return errors.Wrap(err, "could not get data")
	}
	if value == nil {
		return store.ErrNotFound
	}
	err = msgpack.Unmarshal(value, entity)
	if err != nil {
		return errors.Wrap(err, "could not decode data")
	}
	return nil
}

// set will encode the given entity using MessagePack and will insert the resulting binary data in
// the DB under the provided key.
func set(db tmDB.DB, key []byte, entity interface{}) error {
	val, err := msgpack.Marshal(entity)
	if err != nil {
		return errors.Wrap(err, "could not encode entity")
	}
	err = db.Set(key, val)
	if err != nil {
		return errors.Wrap(err, "could not store data")
	}
	return nil
}

func concatKeys(parts ...[]byte) []byte {
	var res []byte
	for _, p := range parts {
		res = append(res, p...)
	}
	return res
}

func toCreateIterError(err error) error {
	return errors.Wrap(err, errStrCreateIter)
}
