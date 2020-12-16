package models

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/pkg/errors"

	uuid "github.com/satori/go.uuid"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

type EthTxState string
type EthTxAttemptState string

const (
	EthTxUnstarted               = EthTxState("unstarted")
	EthTxInProgress              = EthTxState("in_progress")
	EthTxFatalError              = EthTxState("fatal_error")
	EthTxUnconfirmed             = EthTxState("unconfirmed")
	EthTxConfirmed               = EthTxState("confirmed")
	EthTxConfirmedMissingReceipt = EthTxState("confirmed_missing_receipt")

	EthTxAttemptInProgress      = EthTxAttemptState("in_progress")
	EthTxAttemptInsufficientEth = EthTxAttemptState("insufficient_eth")
	EthTxAttemptBroadcast       = EthTxAttemptState("broadcast")
)

type Account struct {
	ID      int64
	Address common.Address
	// This is the nonce that should be used for the next transaction.
	// Conceptually equivalent to geth's `PendingNonceAt` but more reliable
	// because we have a better view of our own transactions
	NextNonce *int64
}

type EthTask struct {
	TaskID  uuid.UUID
	EthTxID int64
	EthTx   EthTx
}

type EthTx struct {
	ID             int64
	Nonce          *int64
	FromAddress    common.Address
	ToAddress      common.Address
	EncodedPayload []byte
	Value          *big.Int
	GasLimit       uint64
	Error          *string
	BroadcastAt    *time.Time
	CreatedAt      time.Time
	State          EthTxState
	EthTxAttempts  []EthTxAttempt
}

func (e EthTx) GetError() error {
	if e.Error == nil {
		return nil
	}
	return errors.New(*e.Error)
}

// GetID allows EthTx to be used as jsonapi.MarshalIdentifier
func (e EthTx) GetID() string {
	return fmt.Sprintf("%d", e.ID)
}

type EthTxAttempt struct {
	ID                      int64
	EthTxID                 int64
	EthTx                   EthTx
	GasPrice                *big.Int
	SignedRawTx             []byte
	Hash                    common.Hash
	BroadcastBeforeBlockNum *int64
	State                   EthTxAttemptState
	EthReceipts             []EthReceipt
}

type EthReceipt struct {
	ID               int64
	TxHash           common.Hash
	BlockHash        common.Hash
	BlockNumber      int64
	TransactionIndex uint
	Receipt          []byte
}

// GetSignedTx decodes the SignedRawTx into a types.Transaction struct
func (a EthTxAttempt) GetSignedTx() (*types.Transaction, error) {
	s := rlp.NewStream(bytes.NewReader(a.SignedRawTx), 0)
	signedTx := new(types.Transaction)
	if err := signedTx.DecodeRLP(s); err != nil {
		return nil, errors.Wrap(err, "Could not decode RLP")
	}
	return signedTx, nil
}

// Head represents a BlockNumber, BlockHash.
type Head struct {
	Hash       common.Hash
	Number     uint64
	ParentHash common.Hash
	Timestamp  time.Time

	Parent *Head // not persisted, filled in Chain()
}

// NewHead returns a Head instance.
func NewHead(number *big.Int, blockHash common.Hash, parentHash common.Hash, timestamp uint64) Head {
	return Head{
		Number:     number.Uint64(),
		Hash:       blockHash,
		ParentHash: parentHash,
		Timestamp:  time.Unix(int64(timestamp), 0),
	}
}

// EarliestInChain recurses through parents until it finds the earliest one
func (h Head) EarliestInChain() Head {
	for {
		if h.Parent != nil {
			h = *h.Parent
		} else {
			break
		}
	}
	return h
}

// ChainLength returns the length of the chain followed by recursively looking up parents
func (h Head) ChainLength() uint64 {
	l := uint64(1)

	for {
		if h.Parent != nil {
			l++
			h = *h.Parent
		} else {
			break
		}
	}
	return l
}

// String returns a string representation of this number.
func (h *Head) String() string {
	return h.ToInt().String()
}

// ToInt return the height as a *big.Int. Also handles nil by returning nil.
func (h *Head) ToInt() *big.Int {
	if h == nil {
		return nil
	}
	return new(big.Int).SetUint64(h.Number)
}

// GreaterThan compares BlockNumbers and returns true if the receiver BlockNumber is greater than
// the supplied BlockNumber
func (h *Head) GreaterThan(r *Head) bool {
	if h == nil {
		return false
	}
	if h != nil && r == nil {
		return true
	}
	return h.Number > r.Number
}

// NextInt returns the next BlockNumber as big.int, or nil if nil to represent latest.
func (h *Head) NextInt() *big.Int {
	if h == nil {
		return nil
	}
	return new(big.Int).Add(h.ToInt(), big.NewInt(1))
}

func (h *Head) UnmarshalJSON(bs []byte) error {
	type head struct {
		Hash       common.Hash    `json:"hash"`
		Number     *hexutil.Big   `json:"number"`
		ParentHash common.Hash    `json:"parentHash"`
		Timestamp  hexutil.Uint64 `json:"timestamp"`
	}

	var jsonHead head
	err := json.Unmarshal(bs, &jsonHead)
	if err != nil {
		return err
	}

	if jsonHead.Number == nil {
		*h = Head{}
		return nil
	}

	h.Hash = jsonHead.Hash
	h.Number = (*big.Int)(jsonHead.Number).Uint64()
	h.ParentHash = jsonHead.ParentHash
	h.Timestamp = time.Unix(int64(jsonHead.Timestamp), 0).UTC()
	return nil
}

func (h *Head) MarshalJSON() ([]byte, error) {
	type head struct {
		Hash       *common.Hash    `json:"hash,omitempty"`
		Number     *hexutil.Big    `json:"number,omitempty"`
		ParentHash *common.Hash    `json:"parentHash,omitempty"`
		Timestamp  *hexutil.Uint64 `json:"timestamp,omitempty"`
	}

	var jsonHead head
	if h.Hash != (common.Hash{}) {
		jsonHead.Hash = &h.Hash
	}
	jsonHead.Number = (*hexutil.Big)(big.NewInt(int64(h.Number)))
	if h.ParentHash != (common.Hash{}) {
		jsonHead.ParentHash = &h.ParentHash
	}
	if h.Timestamp != (time.Time{}) {
		t := hexutil.Uint64(h.Timestamp.UTC().Unix())
		jsonHead.Timestamp = &t
	}
	return json.Marshal(jsonHead)
}

// WeiPerEth is amount of Wei currency units in one Eth.
var WeiPerEth = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

type Log = types.Log

var emptyHash = common.Hash{}

// Unconfirmed returns true if the transaction is not confirmed.
func ReceiptIsUnconfirmed(txr *types.Receipt) bool {
	return txr == nil || txr.TxHash == emptyHash || txr.BlockNumber == nil
}
