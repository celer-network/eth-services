package tendermint

import (
	"bytes"
	"sort"

	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
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

func (store *TMStore) GetAttemptsForTx(tx *models.Tx) ([]*models.TxAttempt, error) {
	var attempts []*models.TxAttempt
	for _, attemptID := range tx.TxAttemptIDs {
		attempt, getAttemptErr := store.GetTxAttempt(attemptID)
		if getAttemptErr != nil {
			return nil, getAttemptErr
		}
		attempts = append(attempts, attempt)
	}
	return attempts, nil
}

func (store *TMStore) AddAttemptToTx(tx *models.Tx, attempt *models.TxAttempt) error {
	attemptIDs := tx.TxAttemptIDs
	update := false
	for _, attemptID := range attemptIDs {
		if bytes.Equal(attemptID[:], attempt.ID[:]) {
			update = true
			break
		}
	}
	if !update {
		attemptIDs = append(attemptIDs, attempt.ID)
	}
	tx.TxAttemptIDs = attemptIDs
	tx, err := store.sortAttemptsByGasPriceForTx(tx)
	if err != nil {
		return err
	}
	return store.PutTx(tx)
}

func (store *TMStore) ReplaceAttemptInTx(tx *models.Tx, oldAttempt *models.TxAttempt, newAttempt *models.TxAttempt) error {
	attemptIDs := tx.TxAttemptIDs
	removeIndex := -1
	for i, attemptID := range attemptIDs {
		if bytes.Equal(attemptID[:], oldAttempt.ID[:]) {
			removeIndex = i
			break
		}
	}
	if removeIndex == -1 {
		return errors.New("old attempt not found")
	}
	copy(attemptIDs[removeIndex:], attemptIDs[removeIndex+1:])
	attemptIDs = attemptIDs[:len(attemptIDs)-1]
	attemptIDs = append(attemptIDs, newAttempt.ID)
	tx.TxAttemptIDs = attemptIDs
	tx, err := store.sortAttemptsByGasPriceForTx(tx)
	if err != nil {
		return err
	}
	return store.PutTx(tx)
}

func (store *TMStore) sortAttemptsByGasPriceForTx(tx *models.Tx) (*models.Tx, error) {
	attempts, err := store.GetAttemptsForTx(tx)
	if err != nil {
		return nil, err
	}

	// Sort txs by descending GasPrice
	sort.Slice(attempts, func(i int, j int) bool {
		return attempts[i].GasPrice.Cmp(attempts[j].GasPrice) == 1
	})

	// Reconstruct attemptIDs
	var attemptIDs []uuid.UUID
	for _, attempt := range attempts {
		attemptIDs = append(attemptIDs, attempt.ID)
	}
	tx.TxAttemptIDs = attemptIDs
	return tx, nil
}

func (store *TMStore) DeleteTxAttempt(id uuid.UUID) error {
	return store.nsTxAttempt.Delete(id[:])
}

func (store *TMStore) GetInProgressAttempts(address common.Address) ([]*models.TxAttempt, error) {
	account, err := store.GetAccount(address)
	if err != nil {
		return nil, err
	}
	var attempts []*models.TxAttempt
	for _, txID := range account.PendingTxIDs {
		tx, getTxErr := store.GetTx(txID)
		if getTxErr != nil {
			return nil, err
		}
		if tx.State == models.TxStateConfirmed || tx.State == models.TxStateConfirmedMissingReceipt ||
			tx.State == models.TxStateUnconfirmed {
			for _, attemptID := range tx.TxAttemptIDs {
				attempt, getAttemptErr := store.GetTxAttempt(attemptID)
				if getAttemptErr != nil {
					return nil, err
				}
				if attempt.State == models.TxAttemptStateInProgress {
					attempts = append(attempts, attempt)
				}

			}
		}
	}
	return attempts, nil
}

func toDecodeTxAttemptError(err error) error {
	return errors.Wrap(err, errStrDecodeTxAttempt)
}
