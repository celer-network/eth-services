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
	prefixHead         = []byte("hd")
	prefixLastHeadHash = []byte("plh")
	keyLastHeadHash    = []byte("klh")
)

// InsertHead inserts a block head
func (store *TMStore) InsertHead(head *models.Head) error {
	lastHead, err := store.LastHead()
	if err != nil {
		return err
	}
	// Stores the hash of the last head.
	if lastHead == nil || head.Number >= lastHead.Number {
		err = set(store.nsLastHeadHash, keyLastHeadHash, &(head.Hash))
		if err != nil {
			return errors.Wrap(err, "error updating last head hash")
		}
	}
	return set(store.nsHead, head.Hash.Bytes(), &head)
}

// LastHead returns the head with the highest number. In the case of ties (e.g.
// due to re-org) it returns the most recently seen head entry.
func (store *TMStore) LastHead() (*models.Head, error) {
	var lastHeadHash common.Hash
	err := get(store.nsLastHeadHash, keyLastHeadHash, &lastHeadHash)
	if err != nil {
		if errors.Is(err, esStore.ErrNotFound) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "could not get last head")
	}
	lastHead, err := store.HeadByHash(lastHeadHash)
	if err != nil {
		return nil, err
	}
	return lastHead, nil
}

// FirstHead returns the head with the lowest number. Only for testing.
func (store *TMStore) FirstHead() (*models.Head, error) {
	iter, err := store.nsHead.Iterator(nil, nil)
	if err != nil {
		return nil, toCreateIterError(err)
	}
	defer iter.Close()
	var firstHead *models.Head = nil
	for ; iter.Valid(); iter.Next() {
		value := iter.Value()
		var head models.Head
		err = msgpack.Unmarshal(value, &head)
		if err != nil {
			return nil, toDecodeHeadError(err)
		}
		if firstHead == nil || head.Number < firstHead.Number {
			firstHead = &head
		}
	}
	return firstHead, nil
}

// HeadByHash fetches the head with the given hash from the db, returns nil if none exists.
func (store *TMStore) HeadByHash(hash common.Hash) (*models.Head, error) {
	var head = models.Head{}
	err := get(store.nsHead, hash.Bytes(), &head)
	if err != nil {
		if errors.Is(err, esStore.ErrNotFound) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "could not get head")
	}
	return &head, nil
}

// TrimOldHeads deletes "depth" number of heads such that only the top N block numbers remain.
func (store *TMStore) TrimOldHeads(depth int64) error {
	lastHead, err := store.LastHead()
	if err != nil {
		if errors.Is(err, esStore.ErrNotFound) {
			return nil
		}
		return err
	}
	// lastHead should not be nil by now
	highestNumber := lastHead.Number
	iter, err := store.nsHead.Iterator(nil, nil)
	if err != nil {
		return toCreateIterError(err)
	}
	defer iter.Close()
	toTrim := make([][]byte, 0)
	for ; iter.Valid(); iter.Next() {
		value := iter.Value()
		var head models.Head
		err = msgpack.Unmarshal(value, &head)
		if err != nil {
			return toDecodeHeadError(err)
		}
		if highestNumber-head.Number >= depth {
			toTrim = append(toTrim, head.Hash.Bytes())
		}
	}
	for _, key := range toTrim {
		err = store.nsHead.Delete(key)
		if err != nil {
			return errors.Wrap(err, "could not delete head")
		}
	}
	return nil
}

// Chain returns the chain of heads starting at hash and up to lookback parents.
func (store *TMStore) Chain(hash common.Hash, lookback int64) (*models.Head, error) {
	var firstHead *models.Head
	var prevHead *models.Head
	currHash := hash
	for i := 0; i < int(lookback); i++ {
		head, headErr := store.HeadByHash(currHash)
		if headErr != nil {
			return nil, errors.Wrap(headErr, "could not get head")
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
		return nil, esStore.ErrNotFound
	}
	return firstHead, nil
}

func toDecodeHeadError(err error) error {
	return errors.Wrap(err, errStrDecodeHead)
}
