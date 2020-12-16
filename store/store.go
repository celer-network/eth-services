package store

import (
	"github.com/pkg/errors"

	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
	uuid "github.com/satori/go.uuid"
)

var (
	ErrNotFound = errors.New("value not found")
)

type Store interface {
	// InsertHead inserts a block head
	InsertHead(head models.Head) error

	// LastHead returns the head with the highest number. In the case of ties (e.g.
	// due to re-org) it returns the most recently seen head entry.
	LastHead() (*models.Head, error)

	// FirstHead returns the head with the lowest number. Only for testing.
	FirstHead() (*models.Head, error)

	// HeadByHash fetches the head with the given hash from the db, returns nil if none exists.
	HeadByHash(hash common.Hash) (*models.Head, error)

	// TrimOldHeads deletes heads such that only the top N block numbers remain.
	TrimOldHeads(depth uint64) error

	// Chain returns the chain of heads starting at hash and up to lookback parents.
	Chain(hash common.Hash, lookback uint64) (models.Head, error)

	InsertEthTask(
		taskRunID uuid.UUID,
		fromAddress common.Address,
		toAddress common.Address,
		encodedPayload []byte,
		gasLimit uint64,
	) error
}
