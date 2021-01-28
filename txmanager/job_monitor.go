package txmanager

import (
	"context"
	"encoding/json"
	"sync"

	esStore "github.com/celer-network/eth-services/store"
	esStoreModels "github.com/celer-network/eth-services/store/models"
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

func (m *jobMonitor) Connect(*esStoreModels.Head) error {
	return nil
}

func (m *jobMonitor) Disconnect() {
	// pass
}

func (m *jobMonitor) OnNewLongestChain(ctx context.Context, head esStoreModels.Head) {
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

func (m *jobMonitor) handleJob(head esStoreModels.Head, jobID uuid.UUID, handler JobHandler) {
	blockNumber := head.Number
	if blockNumber == -1 {
		return
	}
	threshold := blockNumber - m.config.FinalityDepth
	// BEGIN DEBUG
	m.config.Logger.Debugw("handleJob", "blockNumber", blockNumber, "threshold", threshold)
	// END DEBUG
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
	// BEGIN DEBUG
	m.config.Logger.Debugw("confirmed status", "confirmed", confirmed)
	// END DEBUG
	if confirmed {
		attempt, getAttemptErr := m.store.GetTxAttempt(tx.TxAttemptIDs[len(tx.TxAttemptIDs)-1])
		if getAttemptErr != nil {
			m.config.Logger.Error(getAttemptErr)
			m.deleteMonitor(jobID)
			return
		}
		receipt, getReceiptErr := m.store.GetTxReceipt(attempt.ReceiptIDs[len(attempt.ReceiptIDs)-1])
		if getReceiptErr != nil {
			m.config.Logger.Error(getReceiptErr)
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

func (m *jobMonitor) deleteMonitor(jobID uuid.UUID) {
	m.lock.Lock()
	delete(m.jobs, jobID)
	m.lock.Unlock()
}
