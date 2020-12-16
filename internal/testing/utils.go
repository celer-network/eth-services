package testing

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"
	"time"

	eslogger "github.com/celer-network/eth-services/logger"
	"github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/celer-network/eth-services/store/tendermint"
	"github.com/celer-network/eth-services/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	tmdb "github.com/tendermint/tm-db"
	"go.uber.org/zap"
)

// NewStore creates a new Store for testing
func NewStore(t testing.TB) store.Store {
	t.Helper()

	return tendermint.NewTMStore(tmdb.NewMemDB())
}

// NewConfig creates a new Config for testing
func NewConfig(t testing.TB) *types.Config {
	t.Helper()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	return &types.Config{
		Logger:                   eslogger.NewZapLogger(logger.Sugar()),
		BlockTime:                time.Second,
		RPCURL:                   nil,
		SecondaryRPCURLs:         nil,
		ChainID:                  big.NewInt(883),
		HeadTrackerHistoryDepth:  12,
		HeadTrackerMaxBufferSize: 100,
		FinalityDepth:            12,

		DefaultGasPrice: 10 * 1e18,
	}
}

// NewHash return random Keccak256
func NewHash() common.Hash {
	return common.BytesToHash(randomBytes(32))
}

// Head given the value convert it into an Head
func Head(val interface{}) *models.Head {
	var h models.Head
	time := uint64(0)
	switch t := val.(type) {
	case int:
		h = models.NewHead(big.NewInt(int64(t)), NewHash(), NewHash(), time)
	case uint64:
		h = models.NewHead(big.NewInt(int64(t)), NewHash(), NewHash(), time)
	case int64:
		h = models.NewHead(big.NewInt(t), NewHash(), NewHash(), time)
	case *big.Int:
		h = models.NewHead(t, NewHash(), NewHash(), time)
	default:
		panic(fmt.Sprintf("Could not convert %v of type %T to Head", val, val))
	}
	return &h
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
