package tendermint

import (
	"github.com/celer-network/eth-services/store/models"
	"github.com/google/uuid"
)

var (
	prefixReceipt = []byte("rct")
)

func (store *TMStore) GetReceipt(id uuid.UUID) (*models.Receipt, error) {
	var receipt models.Receipt
	err := get(store.nsReceipt, id[:], &receipt)
	if err != nil {
		return nil, err
	}
	return &receipt, nil
}

func (store *TMStore) PutReceipt(receipt *models.Receipt) error {
	return set(store.nsReceipt, receipt.ID[:], receipt)
}

func (store *TMStore) DeleteReceipt(id uuid.UUID) error {
	return store.nsReceipt.Delete(id[:])
}
