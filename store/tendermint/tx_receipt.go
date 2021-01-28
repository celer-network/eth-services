package tendermint

import (
	"github.com/celer-network/eth-services/store/models"
	"github.com/google/uuid"
)

var (
	prefixReceipt = []byte("rct")
)

func (store *TMStore) GetTxReceipt(id uuid.UUID) (*models.TxReceipt, error) {
	var receipt models.TxReceipt
	err := get(store.nsTxReceipt, id[:], &receipt)
	if err != nil {
		return nil, err
	}
	return &receipt, nil
}

func (store *TMStore) PutTxReceipt(receipt *models.TxReceipt) error {
	return set(store.nsTxReceipt, receipt.ID[:], receipt)
}

func (store *TMStore) DeleteTxReceipt(id uuid.UUID) error {
	return store.nsTxReceipt.Delete(id[:])
}
