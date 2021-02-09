package txmanager

import (
	"context"
	"encoding/json"
	"sync"

	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	esTypes "github.com/celer-network/eth-services/types"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/google/uuid"
)

type JobHandler func(jobID uuid.UUID, receipt *gethTypes.Receipt) error

type jobMonitor struct {
	store  esStore.Store
	config *esTypes.Config
	jobs   map[uuid.UUID]JobHandler

	lock sync.Mutex
}

func newJobMonitor(store esStore.Store, config *esTypes.Config) *jobMonitor {
	return &jobMonitor{
		store:  store,
		config: config,
		jobs:   make(map[uuid.UUID]JobHandler),
	}
}

func (m *jobMonitor) Connect(*models.Head) error {
	return nil
}

func (m *jobMonitor) Disconnect() {
	// pass
}

func (m *jobMonitor) OnNewLongestChain(ctx context.Context, head *models.Head) {
	m.lock.Lock()
	for jobID, handler := range m.jobs {
		currJobID := jobID
		currHandler := handler
		go func() {
			m.handleJob(head, currJobID, currHandler)
		}()
	}
	m.lock.Unlock()
}

func (m *jobMonitor) handleJob(head *models.Head, jobID uuid.UUID, handler JobHandler) {
	blockNumber := head.Number
	if blockNumber == -1 {
		return
	}
	threshold := blockNumber - m.config.FinalityDepth
	if threshold < 0 {
		return
	}
	job, err := m.store.GetJob(jobID)
	if err != nil {
		m.config.Logger.Error(err)
		m.deleteMonitor(jobID)
		return
	}
	confirmed, err := m.store.IsTxConfirmedAtOrBeforeBlockNumber(job.TxID, threshold)
	if err != nil {
		m.config.Logger.Error(err)
		m.deleteMonitor(jobID)
		return
	}
	tx, err := m.store.GetTx(job.TxID)
	if err != nil {
		m.config.Logger.Error(err)
		m.deleteMonitor(jobID)
		return
	}
	if confirmed {
		for _, attemptID := range tx.TxAttemptIDs {
			attempt, getAttemptErr := m.store.GetTxAttempt(attemptID)
			if getAttemptErr != nil {
				m.config.Logger.Error(getAttemptErr)
				m.deleteMonitor(jobID)
				return
			}
			if attempt.State != models.TxAttemptStateBroadcast || len(attempt.TxReceiptIDs) == 0 {
				continue
			}
			// TODO: Check if looping necessary
			receiptID := attempt.TxReceiptIDs[len(attempt.TxReceiptIDs)-1]
			receipt, getReceiptErr := m.store.GetTxReceipt(receiptID)
			if getReceiptErr != nil {
				m.config.Logger.Error(err)
				m.deleteMonitor(jobID)
				return
			}
			var ethReceipt gethTypes.Receipt
			marshalErr := json.Unmarshal(receipt.Receipt, &ethReceipt)
			if marshalErr != nil {
				m.config.Logger.Error(marshalErr)
				m.deleteMonitor(jobID)
				return
			}
			handler(jobID, &ethReceipt)
			m.deleteMonitor(jobID)
			return
		}
	}
}

func (m *jobMonitor) deleteMonitor(jobID uuid.UUID) {
	m.lock.Lock()
	delete(m.jobs, jobID)
	m.lock.Unlock()
}
