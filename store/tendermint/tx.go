package tendermint

import (
	"math/big"

	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
	uuid "github.com/satori/go.uuid"
)

var (
	prefixEthTask = []byte("etk")
	prefixEthTx   = []byte("etx")
)

func (store *TMStore) InsertEthTask(
	taskID uuid.UUID,
	fromAddress common.Address,
	toAddress common.Address,
	encodedPayload []byte,
	gasLimit uint64,
) error {
	ethTx := models.EthTx{
		FromAddress:    fromAddress,
		ToAddress:      toAddress,
		EncodedPayload: encodedPayload,
		Value:          big.NewInt(0),
		GasLimit:       gasLimit,
		State:          models.EthTxUnstarted,
	}
	ethTask := models.EthTask{
		TaskID: taskID,
	}
	// TODO: Set
	set(store.nsTask, taskID.Bytes(), ethTask)
	set(store.nsTx, fromAddress.Bytes(), ethTx)
	return nil
}
