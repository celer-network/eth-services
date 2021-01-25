package testing

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	esLogger "github.com/celer-network/eth-services/logger"
	"github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/celer-network/eth-services/store/tendermint"
	"github.com/celer-network/eth-services/types"
	"github.com/stretchr/testify/require"
	tmDB "github.com/tendermint/tm-db"
	"go.uber.org/zap"
)

// NewStore creates a new Store for testing
func NewStore(t testing.TB) store.Store {
	t.Helper()

	return tendermint.NewTMStore(tmDB.NewMemDB())
}

// NewConfig creates a new Config for testing
func NewConfig(t testing.TB) *types.Config {
	t.Helper()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defaultGasPrice := big.NewInt(20000000000) // 20 gwei
	return &types.Config{
		Logger:                   esLogger.NewZapLogger(logger.Sugar()),
		BlockTime:                time.Second,
		RPCURL:                   nil,
		SecondaryRPCURLs:         nil,
		ChainID:                  big.NewInt(883),
		HeadTrackerHistoryDepth:  12,
		HeadTrackerMaxBufferSize: 100,
		FinalityDepth:            12,

		KeysDir:         "/tmp/eth-service-test/keys",
		DefaultGasPrice: defaultGasPrice,
		MaxGasPrice:     new(big.Int).Mul(defaultGasPrice, big.NewInt(10)),
		GasBumpWei:      big.NewInt(5000000000),
		GasBumpPercent:  11,
	}
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
