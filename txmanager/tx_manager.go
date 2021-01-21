package txmanager

import (
	"math/big"

	esClient "github.com/celer-network/eth-services/client"
	esStore "github.com/celer-network/eth-services/store"
	esStoreModels "github.com/celer-network/eth-services/store/models"
	"github.com/celer-network/eth-services/subscription"
	esTypes "github.com/celer-network/eth-services/types"
	gethCommon "github.com/ethereum/go-ethereum/common"
	uuid "github.com/satori/go.uuid"
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
	) error

	IsTxConfirmedAtOrBeforeBlockNumber(
		fromAddress gethCommon.Address,
		txID uuid.UUID,
		blockNumber int64,
	) (bool, error)
}

type txManager struct {
	headTracker *subscription.HeadTracker
	broadcaster EthBroadcaster
	confirmer   EthConfirmer
}

var _ TxManager = (*txManager)(nil)

func NewTxManager(
	ethClient esClient.Client,
	store esStore.Store,
	keyStore esClient.KeyStoreInterface,
	config *esTypes.Config,
) (TxManager, error) {
	broadcaster := NewEthBroadcaster(ethClient, store, keyStore, config)
	confirmer := NewEthConfirmer(ethClient, store, keyStore, config)
	headTracker := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{confirmer})
	return &txManager{
		broadcaster: broadcaster,
		confirmer:   confirmer,
		headTracker: headTracker,
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
) error {
	txID := uuid.NewV4()
	return txm.broadcaster.AddTx(txID, fromAddress, to, value, encodedPayload, gasLimit)
}

func (txm *txManager) IsTxConfirmedAtOrBeforeBlockNumber(
	fromAddress gethCommon.Address,
	txID uuid.UUID,
	blockNumber int64,
) (bool, error) {
	tx, err := txm.confirmer.GetTx(fromAddress, txID)
	if err != nil {
		return false, err
	}
	if tx.State != esStoreModels.TxStateConfirmed && tx.State != esStoreModels.TxStateConfirmedMissingReceipt {
		return false, nil
	}
	// TODO: Just checking the last attempt should suffice
	isConfirmed := false
	for i := len(tx.TxAttempts) - 1; i >= 0; i-- {
		attempt := tx.TxAttempts[i]
		if attempt.State != esStoreModels.TxAttemptStateBroadcast {
			continue
		}
		for j := len(attempt.Receipts) - 1; j >= 0; j-- {
			receipt := attempt.Receipts[j]
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
