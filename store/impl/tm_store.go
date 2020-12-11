package impl

import (
	"math/big"

	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
	uuid "github.com/satori/go.uuid"
	tmdb "github.com/tendermint/tm-db"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/pkg/errors"
)

var (
	prefixHead    = []byte("hd")
	prefixEthTask = []byte("etk")
	prefixEthTx   = []byte("etx")

	separator = []byte("|")
)

type TMStore struct {
	db tmdb.DB
}

func (store *TMStore) InsertHead(head *models.Head) error {
	// TODO: Set
	return store.set(prefixHead, head)
}

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
	store.set(concatKeys(prefixEthTask, taskID.Bytes()), ethTask)
	store.set(concatKeys(prefixEthTx, fromAddress.Bytes()), ethTx)
	return nil
}

func (store *TMStore) set(key []byte, entity interface{}) error {
	val, err := msgpack.Marshal(entity)
	if err != nil {
		return errors.Wrap(err, "could not encode entity")
	}
	err = store.db.Set(key, val)
	if err != nil {
		return errors.Wrap(err, "could not store data")
	}
	return nil
}

func concatKeys(prefix []byte, key []byte) []byte {
	if prefix != nil {
		return append(append(prefix, separator...), key...)
	}
	return key
}
