package tendermint

import (
	"github.com/celer-network/eth-services/store"
	"github.com/pkg/errors"
	tmdb "github.com/tendermint/tm-db"
	"github.com/vmihailenco/msgpack/v5"
)

// TMStore is a Store implementation using Tendermint tm-db
type TMStore struct {
	nsHead *tmdb.PrefixDB
	nsTask *tmdb.PrefixDB
	nsTx   *tmdb.PrefixDB
}

var _ store.Store = (*TMStore)(nil)

// NewTMStore creates a new TMStore
func NewTMStore(db tmdb.DB) *TMStore {
	return &TMStore{
		nsHead: tmdb.NewPrefixDB(db, prefixHead),
		nsTask: tmdb.NewPrefixDB(db, prefixEthTask),
		nsTx:   tmdb.NewPrefixDB(db, prefixEthTx),
	}
}

// get will retrieve the binary data under the given key from the DB and decode it into the given
// entity. The provided entity needs to be a pointer to an initialized entity of the correct type.
func get(db tmdb.DB, key []byte, entity interface{}) error {
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
func set(db tmdb.DB, key []byte, entity interface{}) error {
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
