package store

import (
	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
	uuid "github.com/satori/go.uuid"
)

type Store interface {
	// InsertHead inserts a block head
	InsertHead(models.Head) error

	// LastHead returns the head with the highest number. In the case of ties (e.g.
	// due to re-org) it returns the most recently seen head entry.
	LastHead() (*models.Head, error)

	// HeadByHash fetches the head with the given hash from the db, returns nil if none exists.
	HeadByHash(common.Hash) (*models.Head, error)

	// TrimOldHeads deletes heads such that only the top N block numbers remain.
	TrimOldHeads(uint) error

	// Chain returns the chain of heads starting at hash and up to lookback parents.
	Chain(common.Hash, uint) (models.Head, error)

	InsertEthTaskRunTx(
		taskRunID uuid.UUID,
		fromAddress common.Address,
		toAddress common.Address,
		encodedPayload []byte,
		gasLimit uint64,
	) error
}
