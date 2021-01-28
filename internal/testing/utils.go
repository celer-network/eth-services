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
	return &types.Config{
		Logger:                   esLogger.NewZapLogger(logger.Sugar()),
		BlockTime:                time.Second,
		RPCURL:                   nil,
		SecondaryRPCURLs:         nil,
		ChainID:                  big.NewInt(883),
		HeadTrackerHistoryDepth:  100,
		HeadTrackerMaxBufferSize: 3,
		FinalityDepth:            50,
		KeysDir:                  "/tmp/eth-service-test/keys",
		DefaultGasPrice:          big.NewInt(20000000000),
		MaxGasPrice:              big.NewInt(1500000000000),
		GasBumpWei:               big.NewInt(5000000000),
		GasBumpPercent:           20,
		GasBumpThreshold:         3,
		GasBumpTxDepth:           10,
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
