package testing

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"flag"
	"math/big"
	mathRand "math/rand"
	"testing"

	"github.com/celer-network/eth-services/client"
	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/google/uuid"
	pbormanUUID "github.com/pborman/uuid"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
	"github.com/urfave/cli"
)

const (
	Password = "password"
)

func NewRandomInt64() int64 {
	id := mathRand.Int63()
	return id
}

// NewHash return random Keccak256
func NewHash() common.Hash {
	return common.BytesToHash(randomBytes(32))
}

// NewAddress return a random new address
func NewAddress() common.Address {
	return common.BytesToAddress(randomBytes(20))
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func Random32Byte() (b [32]byte) {
	copy(b[:], randomBytes(32))
	return b
}

// MustJSONSet uses sjson.Set to set a path in a JSON string and returns the string
// See https://github.com/tidwall/sjson
func MustJSONSet(t *testing.T, json, path string, value interface{}) string {
	json, err := sjson.Set(json, path, value)
	require.NoError(t, err)
	return json
}

// MustJSONDel uses sjson.Delete to remove a path from a JSON string and returns the string
func MustJSONDel(t *testing.T, json, path string) string {
	json, err := sjson.Delete(json, path)
	require.NoError(t, err)
	return json
}

func EmptyCLIContext() *cli.Context {
	set := flag.NewFlagSet("test", 0)
	return cli.NewContext(nil, set, nil)
}

func NewTx(t *testing.T, fromAddress common.Address) *models.Tx {
	t.Helper()

	return &models.Tx{
		ID:             uuid.New(),
		Nonce:          -1,
		FromAddress:    fromAddress,
		ToAddress:      NewAddress(),
		EncodedPayload: []byte{1, 2, 3},
		Value:          big.NewInt(142),
		GasLimit:       uint64(1000000000),
	}
}

func MustInsertUnconfirmedTxWithBroadcastAttempt(t *testing.T, store esStore.Store, nonce int64, fromAddress common.Address) *models.Tx {
	t.Helper()

	tx := NewTx(t, fromAddress)
	n := nonce
	tx.Nonce = n
	tx.State = models.TxStateUnconfirmed
	attempt := NewTxAttempt(t, tx.ID)
	ethTx := types.NewTransaction(uint64(nonce), NewAddress(), big.NewInt(142), 242, big.NewInt(342), []byte{1, 2, 3})
	rlp := new(bytes.Buffer)
	require.NoError(t, ethTx.EncodeRLP(rlp))

	attempt.SignedRawTx = rlp.Bytes()
	attempt.State = models.TxAttemptStateBroadcast
	require.NoError(t, store.PutTxAttempt(attempt))

	tx.TxAttemptIDs = append(tx.TxAttemptIDs, attempt.ID)
	require.NoError(t, store.PutTx(tx))
	account, err := store.GetAccount(tx.FromAddress)
	require.NoError(t, err)
	account.PendingTxIDs = append(account.PendingTxIDs, tx.ID)
	require.NoError(t, store.PutAccount(account))
	return tx
}

func mustInsertConfirmedTxWithAttempt(t *testing.T, store esStore.Store, nonce int64, broadcastBeforeBlockNum int64, fromAddress common.Address) *models.Tx {
	t.Helper()

	tx := NewTx(t, fromAddress)
	tx.Nonce = nonce
	tx.State = models.TxStateConfirmed

	attempt := NewTxAttempt(t, tx.ID)
	attempt.BroadcastBeforeBlockNum = broadcastBeforeBlockNum
	attempt.State = models.TxAttemptStateBroadcast
	require.NoError(t, store.PutTxAttempt(attempt))

	tx.TxAttemptIDs = append(tx.TxAttemptIDs, attempt.ID)
	require.NoError(t, store.PutTx(tx))
	account, err := store.GetAccount(tx.FromAddress)
	require.NoError(t, err)
	account.PendingTxIDs = append(account.PendingTxIDs, tx.ID)
	require.NoError(t, store.PutAccount(account))
	return tx
}

func MustInsertInProgressTxWithAttempt(t *testing.T, store esStore.Store, nonce int64, fromAddress common.Address) *models.Tx {
	t.Helper()

	tx := NewTx(t, fromAddress)
	tx.Nonce = nonce
	tx.State = models.TxStateInProgress
	attempt := NewTxAttempt(t, tx.ID)
	ethTx := types.NewTransaction(uint64(nonce), NewAddress(), big.NewInt(142), 242, big.NewInt(342), []byte{1, 2, 3})
	rlp := new(bytes.Buffer)
	require.NoError(t, ethTx.EncodeRLP(rlp))

	attempt.SignedRawTx = rlp.Bytes()
	attempt.State = models.TxAttemptStateInProgress
	require.NoError(t, store.PutTxAttempt(attempt))

	tx.TxAttemptIDs = append(tx.TxAttemptIDs, attempt.ID)
	require.NoError(t, store.PutTx(tx))
	account, err := store.GetAccount(tx.FromAddress)
	require.NoError(t, err)
	account.PendingTxIDs = append(account.PendingTxIDs, tx.ID)
	require.NoError(t, store.PutAccount(account))
	return tx
}

func NewTxAttempt(t *testing.T, txID uuid.UUID) *models.TxAttempt {
	t.Helper()

	gasPrice := big.NewInt(1)
	return &models.TxAttempt{
		ID:       uuid.New(),
		TxID:     txID,
		GasPrice: gasPrice,
		// Just a random signed raw tx that decodes correctly
		// Ignore all actual values
		SignedRawTx: hexutil.MustDecode("0xf889808504a817c8008307a12094000000000000000000000000000000000000000080a400000000000000000000000000000000000000000000000000000000000000000000000025a0838fe165906e2547b9a052c099df08ec891813fea4fcdb3c555362285eb399c5a070db99322490eb8a0f2270be6eca6e3aedbc49ff57ef939cf2774f12d08aa85e"),
		Hash:        NewHash(),
	}
}

func MustInsertFatalErrorTx(t *testing.T, store esStore.Store, fromAddress common.Address) *models.Tx {
	tx := NewTx(t, fromAddress)
	errStr := "something exploded"
	tx.Error = errStr
	tx.State = models.TxStateFatalError

	require.NoError(t, store.PutTx(tx))
	account, err := store.GetAccount(tx.FromAddress)
	require.NoError(t, err)
	account.PendingTxIDs = append(account.PendingTxIDs, tx.ID)
	require.NoError(t, store.PutAccount(account))
	return tx
}

func MustAddRandomAccountToKeystore(
	t testing.TB,
	store esStore.Store,
	keyStore client.KeyStoreInterface,
	opts ...interface{},
) (account *models.Account, address common.Address) {
	t.Helper()

	account, keyJSONBytes := MustGenerateRandomAccount(t, opts...)
	err := keyStore.Unlock(Password)
	require.NoError(t, err)
	MustAddAccountToKeyStore(t, account, keyJSONBytes, store, keyStore)
	return account, account.Address
}

func MustAddAccountToKeyStore(
	t testing.TB,
	account *models.Account,
	keyJSONBytes []byte,
	store esStore.Store,
	keyStore client.KeyStoreInterface,
) {
	t.Helper()

	err := keyStore.Unlock(Password)
	require.NoError(t, err)
	_, err = keyStore.Import(keyJSONBytes, Password)
	require.NoError(t, err)
	require.NoError(t, store.PutAccount(account))
}

// MustInsertRandomAccount inserts a randomly generated (not cryptographically secure) account for testing
// If using this with the keystore, it should be called before the keystore loads keys from the database
func MustInsertRandomAccount(t testing.TB, store esStore.Store, opts ...interface{}) *models.Account {
	t.Helper()

	account, _ := MustGenerateRandomAccount(t, opts...)

	require.NoError(t, store.PutAccount(account))
	return account
}

func MustGenerateRandomAccount(t testing.TB, opts ...interface{}) (account *models.Account, keyJSONBytes []byte) {
	t.Helper()

	privateKeyECDSA, err := ecdsa.GenerateKey(crypto.S256(), rand.Reader)
	require.NoError(t, err)
	id := pbormanUUID.NewRandom()
	k := &keystore.Key{
		Id:         id,
		Address:    crypto.PubkeyToAddress(privateKeyECDSA.PublicKey),
		PrivateKey: privateKeyECDSA,
	}
	keyJSONBytes, err = keystore.EncryptKey(k, Password, client.FastScryptParams.N, client.FastScryptParams.P)
	require.NoError(t, err)

	var nextNonce *int64
	for _, opt := range opts {
		switch v := opt.(type) {
		case int:
			i := int64(v)
			nextNonce = &i
		case int64:
			nextNonce = &v
		default:
			t.Fatalf("unrecognized option type: %T", v)
		}
	}
	var nonce int64
	if nextNonce == nil {
		nonce = -1
	} else {
		nonce = *nextNonce
	}

	account = &models.Account{
		Address:        k.Address,
		NextNonce:      nonce,
		PendingTxIDs:   make([]uuid.UUID, 0),
		CompletedTxIDs: make([]uuid.UUID, 0),
		ErroredTxIDs:   make([]uuid.UUID, 0),
	}
	return account, keyJSONBytes
}
