package txmanager

import (
	"context"
	"math/big"
	"time"

	esClient "github.com/celer-network/eth-services/client"
	esStore "github.com/celer-network/eth-services/store"
	esStoreModels "github.com/celer-network/eth-services/store/models"
	"github.com/celer-network/eth-services/subscription"
	"github.com/celer-network/eth-services/types"
	esTypes "github.com/celer-network/eth-services/types"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
)

type TxManager interface {
	Start() error

	RegisterAccount(address gethCommon.Address) error

	AddTx(
		from gethCommon.Address,
		to gethCommon.Address,
		value *big.Int,
		encodedPayload []byte,
		gasLimit uint64,
	) (uuid.UUID, error)

	IsTxConfirmedAtOrBeforeBlockNumber(txID uuid.UUID, blockNumber int64) (bool, error)

	AddJob(txID uuid.UUID, metadata []byte) (uuid.UUID, error)

	MonitorJob(jobID uuid.UUID, handler func(job *esStoreModels.Job, tx *esStoreModels.Tx) error)

	DeleteJob(jobID uuid.UUID) error

	GetUnhandledJobIDs() ([]uuid.UUID, error)
}

type txManager struct {
	store  esStore.Store
	config *types.Config

	headTracker *subscription.HeadTracker
	broadcaster TxBroadcaster
	confirmer   TxConfirmer

	blockNumberRecorder *blockNumberRecorder
}

var _ TxManager = (*txManager)(nil)

func NewTxManager(
	ethClient esClient.Client,
	store esStore.Store,
	keyStore esClient.KeyStoreInterface,
	config *esTypes.Config,
) (TxManager, error) {
	broadcaster := NewTxBroadcaster(ethClient, store, keyStore, config)
	confirmer := NewTxConfirmer(ethClient, store, keyStore, config)
	blockNumberRecorder := newBlockNumberRecorder()
	headTracker :=
		subscription.NewHeadTracker(
			ethClient,
			store,
			config,
			[]subscription.HeadTrackable{confirmer, blockNumberRecorder},
		)
	return &txManager{
		store:  store,
		config: config,

		broadcaster:         broadcaster,
		confirmer:           confirmer,
		headTracker:         headTracker,
		blockNumberRecorder: blockNumberRecorder,
	}, nil
}

func (txm *txManager) Start() error {
	err := txm.headTracker.Start()
	if err != nil {
		return err
	}
	return txm.broadcaster.Start()
}

func (txm *txManager) RegisterAccount(address gethCommon.Address) error {
	return txm.broadcaster.RegisterAccount(address)
}

func (txm *txManager) AddTx(
	fromAddress gethCommon.Address,
	to gethCommon.Address,
	value *big.Int,
	encodedPayload []byte,
	gasLimit uint64,
) (uuid.UUID, error) {
	txID := uuid.New()
	err := txm.broadcaster.AddTx(txID, fromAddress, to, value, encodedPayload, gasLimit)
	if err != nil {
		return uuid.Nil, err
	}
	return txID, nil
}

func (txm *txManager) IsTxConfirmedAtOrBeforeBlockNumber(txID uuid.UUID, blockNumber int64) (bool, error) {
	tx, err := txm.confirmer.GetTx(txID)
	if err != nil {
		return false, err
	}
	if tx.State != esStoreModels.TxStateConfirmed && tx.State != esStoreModels.TxStateConfirmedMissingReceipt {
		return false, nil
	}
	// TODO: Just checking the last attempt should suffice
	isConfirmed := false
	for i := len(tx.TxAttemptIDs) - 1; i >= 0; i-- {
		attemptID := tx.TxAttemptIDs[i]
		attempt, getAttemptErr := txm.store.GetTxAttempt(attemptID)
		if getAttemptErr != nil {
			return false, getAttemptErr
		}
		if attempt.State != esStoreModels.TxAttemptStateBroadcast {
			continue
		}
		for j := len(attempt.ReceiptIDs) - 1; j >= 0; j-- {
			receiptID := attempt.ReceiptIDs[j]
			receipt, getReceiptErr := txm.store.GetReceipt(receiptID)
			if getReceiptErr != nil {
				return false, getReceiptErr
			}
			if receipt.BlockNumber <= blockNumber {
				isConfirmed = true
				break
			}
		}
		if isConfirmed {
			break
		}
	}
	return isConfirmed, nil
}

type blockNumberRecorder struct {
	blockNumber int64
}

func newBlockNumberRecorder() *blockNumberRecorder {
	return &blockNumberRecorder{
		blockNumber: -1,
	}
}

func (r *blockNumberRecorder) Connect(*esStoreModels.Head) error {
	return nil
}

func (r *blockNumberRecorder) Disconnect() {
	// pass
}

func (r *blockNumberRecorder) OnNewLongestChain(ctx context.Context, head esStoreModels.Head) {
	r.blockNumber = head.Number
}

func (txm *txManager) AddJob(txID uuid.UUID, metadata []byte) (uuid.UUID, error) {
	jobID := uuid.New()
	job := &esStoreModels.Job{
		ID:       jobID,
		TxID:     txID,
		Metadata: metadata,
		State:    esStoreModels.JobStateUnhandled,
	}
	err := txm.store.PutJob(job)
	if err != nil {
		return uuid.Nil, err
	}
	return jobID, nil
}

func (txm *txManager) MonitorJob(jobID uuid.UUID, handler func(job *esStoreModels.Job, tx *esStoreModels.Tx) error) {
	go func() {
		// TODO: Add stop mechanism?
		jobPoller := time.NewTicker(withJitter(txm.config.BlockTime / 2))
		for {
			select {
			case <-jobPoller.C:
				blockNumber := txm.blockNumberRecorder.blockNumber
				if blockNumber == -1 {
					continue
				}
				threshold := blockNumber - txm.config.FinalityDepth
				if threshold < 0 {
					continue
				}
				job, err := txm.store.GetJob(jobID)
				if err != nil {
					txm.config.Logger.Error(err)
					return
				}
				confirmed, err := txm.IsTxConfirmedAtOrBeforeBlockNumber(job.TxID, threshold)
				if err != nil {
					txm.config.Logger.Error(err)
					return
				}
				tx, err := txm.store.GetTx(job.TxID)
				if err != nil {
					txm.config.Logger.Error(err)
					return
				}
				txm.config.Logger.Debugw("confirmed status", "confirmed", confirmed)
				if confirmed {
					handler(job, tx)
					return
				}
			}
		}
	}()
}

func (txm *txManager) DeleteJob(jobID uuid.UUID) error {
	return txm.store.DeleteJob(jobID)
}

func (txm *txManager) GetUnhandledJobIDs() ([]uuid.UUID, error) {
	return txm.store.GetUnhandledJobIDs()
}
