package store

import (
	"math/big"

	"github.com/pkg/errors"

	"github.com/celer-network/eth-services/store/models"
	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
)

var (
	ErrNotFound           = errors.New("value not found")
	ErrCouldNotGetReceipt = errors.New("could not get receipt")
)

// Store defines the interface for the storage layer
type Store interface {
	// InsertHead inserts a block head
	InsertHead(head models.Head) error

	// LastHead returns the head with the highest number. In the case of ties (e.g.
	// due to re-org) it returns the most recently seen head entry.
	LastHead() (*models.Head, error)

	// FirstHead returns the head with the lowest number. Only for testing.
	FirstHead() (*models.Head, error)

	// HeadByHash fetches the head with the given hash from the db, returns nil if none exists.
	HeadByHash(hash common.Hash) (*models.Head, error)

	// TrimOldHeads deletes heads such that only the top N block numbers remain.
	TrimOldHeads(depth int64) error

	// Chain returns the chain of heads starting at hash and up to lookback parents.
	Chain(hash common.Hash, lookback int64) (models.Head, error)

	GetAccount(address common.Address) (*models.Account, error)
	// GetAccounts gets the list of accounts
	GetAccounts() ([]*models.Account, error)
	PutAccount(account *models.Account) error

	GetTx(id uuid.UUID) (*models.Tx, error)
	PutTx(tx *models.Tx) error

	GetTxAttempt(id uuid.UUID) (*models.TxAttempt, error)
	PutTxAttempt(attempt *models.TxAttempt) error
	DeleteTxAttempt(id uuid.UUID) error

	GetAttemptsForTx(tx *models.Tx) ([]*models.TxAttempt, error)
	AddAttemptToTx(tx *models.Tx, attempt *models.TxAttempt) error
	ReplaceAttemptInTx(tx *models.Tx, oldAttempt *models.TxAttempt, newAttempt *models.TxAttempt) error

	GetTxReceipt(id uuid.UUID) (*models.TxReceipt, error)
	PutTxReceipt(receipt *models.TxReceipt) error
	DeleteTxReceipt(id uuid.UUID) error

	AddTx(
		txID uuid.UUID,
		fromAddress common.Address,
		toAddress common.Address,
		encodedPayload []byte,
		value *big.Int,
		gasLimit uint64,
	) error

	GetOneInProgressTx(fromAddress common.Address) (*models.Tx, error)

	GetNextNonce(address common.Address) (int64, error)

	SetNextNonce(address common.Address, nextNonce int64) error

	GetNextUnstartedTx(fromAddress common.Address) (*models.Tx, error)

	GetTxsRequiringReceiptFetch() ([]*models.Tx, error)

	SetBroadcastBeforeBlockNum(blockNum int64) error

	// MarkConfirmedMissingReceipt
	// It is possible that we can fail to get a receipt for all TxAttempts
	// even though a transaction with this nonce has long since been confirmed (we
	// know this because transactions with higher nonces HAVE returned a receipt).
	//
	// This can probably only happen if an external wallet used the account (or
	// conceivably because of some bug in the remote eth node that prevents it
	// from returning a receipt for a valid transaction).
	//
	// In this case we mark these transactions as 'confirmed_missing_receipt' to
	// prevent gas bumping.
	//
	// We will continue to try to fetch a receipt for these attempts until all
	// attempts are below the finality depth from current head.
	MarkConfirmedMissingReceipt() error

	// MarkOldTxsMissingReceiptAsErrored
	//
	// Once a Tx has all of its attempts broadcast before some cutoff threshold,
	// we mark it as fatally errored (never sent).
	//
	// Any 'confirmed_missing_receipt' Tx with all attempts older than this block height will be
	// marked as errored. We will not try to query for receipts for this transaction any more.
	MarkOldTxsMissingReceiptAsErrored(cutoff int64) error

	// GetTxsRequiringNewAttempt returns transactions that have all attempts which are unconfirmed
	// for at least gasBumpThreshold blocks, limited by the depth limit on pending transactions.
	GetTxsRequiringNewAttempt(address common.Address, blockNum int64, gasBumpThreshold int64, depth int) ([]*models.Tx, error)

	GetTxsConfirmedAtOrAboveBlockHeight(blockNum int64) ([]*models.Tx, error)

	GetInProgressAttempts(address common.Address) ([]*models.TxAttempt, error)

	IsTxConfirmedAtOrBeforeBlockNumber(txID uuid.UUID, blockNumber int64) (bool, error)

	GetJob(jobID uuid.UUID) (*models.Job, error)
	PutJob(job *models.Job) error
	DeleteJob(jobID uuid.UUID) error
	GetUnhandledJobIDs() ([]uuid.UUID, error)
}
