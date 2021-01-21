package tendermint

import (
	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	errStrDecodeHead = "could not decode head"
)

var (
	prefixHead = []byte("hd")

	keyLastHead = []byte("lhd")
)

// InsertHead inserts a block head
func (store *TMStore) InsertHead(head models.Head) error {
	lastHead, err := store.LastHead()
	if err != nil {
		return err
	}
	if lastHead == nil || head.Number >= lastHead.Number {
		setErr := set(store.nsHead, keyLastHead, head)
		if setErr != nil {
			return errors.Wrap(setErr, "error updating last head")
		}
	}
	return set(store.nsHead, head.Hash.Bytes(), head)
}

// LastHead returns the head with the highest number. In the case of ties (e.g.
// due to re-org) it returns the most recently seen head entry.
func (store *TMStore) LastHead() (*models.Head, error) {
	var lastHead models.Head
	err := get(store.nsHead, keyLastHead, &lastHead)
	if err != nil {
		if errors.Is(err, esStore.ErrNotFound) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "could not get last head")
	}
	return &lastHead, nil
}

// FirstHead returns the head with the lowest number. Only for testing.
func (store *TMStore) FirstHead() (*models.Head, error) {
	iter, err := store.nsHead.Iterator(nil, nil)
	if err != nil {
		return nil, toCreateIterError(err)
	}
	var firstHead *models.Head = nil
	var iterError error
	for ; iter.Valid(); iter.Next() {
		value := iter.Value()
		var head models.Head
		unmarshalErr := msgpack.Unmarshal(value, &head)
		if unmarshalErr != nil {
			iterError = toDecodeHeadError(err)
			break
		}
		if firstHead == nil || head.Number < firstHead.Number {
			firstHead = &head
		}
	}
	iter.Close()
	if iterError != nil {
		return nil, iterError
	}
	return firstHead, nil
}

// HeadByHash fetches the head with the given hash from the db, returns nil if none exists.
func (store *TMStore) HeadByHash(hash common.Hash) (*models.Head, error) {
	var head models.Head
	err := get(store.nsHead, hash.Bytes(), &head)
	if err != nil {
		if errors.Is(err, esStore.ErrNotFound) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "could not get head")
	}
	return &head, nil
}

// TrimOldHeads deletes heads such that only the top N block numbers remain.
func (store *TMStore) TrimOldHeads(depth int64) error {
	lastHead, err := store.LastHead()
	if err != nil {
		return err
	}
	highestNumber := lastHead.Number
	iter, err := store.nsHead.Iterator(nil, nil)
	if err != nil {
		return toCreateIterError(err)
	}
	toTrim := make([][]byte, 0)
	var iterError error
	for ; iter.Valid(); iter.Next() {
		value := iter.Value()
		var head models.Head
		unmarshalErr := msgpack.Unmarshal(value, &head)
		if unmarshalErr != nil {
			iterError = toDecodeHeadError(err)
			break
		}
		if highestNumber-head.Number >= depth {
			toTrim = append(toTrim, head.Hash.Bytes())
		}
	}
	iter.Close()
	if iterError != nil {
		return iterError
	}
	for _, key := range toTrim {
		deleteErr := store.nsHead.Delete(key)
		if deleteErr != nil {
			return errors.Wrap(err, "could not delete head")
		}
	}
	return nil
}

// Chain returns the chain of heads starting at hash and up to lookback parents.
func (store *TMStore) Chain(hash common.Hash, lookback int64) (models.Head, error) {
	var firstHead *models.Head
	var prevHead *models.Head
	currHash := hash
	for i := 0; i < int(lookback); i++ {
		head, headErr := store.HeadByHash(currHash)
		if headErr != nil {
			return models.Head{}, errors.Wrap(headErr, "could not get head")
		}
		if head == nil {
			// Chain is shorter than specified lookback
			break
		}
		if firstHead == nil {
			firstHead = head
		} else {
			prevHead.Parent = head
		}
		prevHead = head
		currHash = prevHead.ParentHash
	}
	if firstHead == nil {
		return models.Head{}, esStore.ErrNotFound
	}
	return *firstHead, nil
}

func toDecodeHeadError(err error) error {
	return errors.Wrap(err, errStrDecodeHead)
}
