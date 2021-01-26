package tendermint

import (
	"log"

	"github.com/celer-network/eth-services/store/models"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	errStrDecodeTxAttempt = "could not decode TxAttempt"
)

var (
	prefixTxAttempt = []byte("tat")
)

func (store *TMStore) PutTxAttempt(attempt *models.TxAttempt) error {
	// BEGIN DEBUG
	log.Println("PutTxAttempt", attempt.ID)
	// END DEBUG
	return set(store.nsTxAttempt, attempt.ID[:], attempt)
}

func (store *TMStore) GetTxAttempt(id uuid.UUID) (*models.TxAttempt, error) {
	var attempt models.TxAttempt
	err := get(store.nsTxAttempt, id[:], &attempt)
	if err != nil {
		return nil, err
	}
	return &attempt, nil
}

func (store *TMStore) DeleteTxAttempt(id uuid.UUID) error {
	return store.nsTxAttempt.Delete(id[:])
}

func toDecodeTxAttemptError(err error) error {
	return errors.Wrap(err, errStrDecodeTxAttempt)
}
