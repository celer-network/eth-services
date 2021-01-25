package types

import (
	"math/big"
	"net/url"
	"time"
)

type Config struct {
	Logger    Logger
	BlockTime time.Duration

	// Primary RPC URL, must begin with ws:// or wss://
	RPCURL           *url.URL
	SecondaryRPCURLs []*url.URL
	ChainID          *big.Int

	HeadTrackerHistoryDepth  int64
	HeadTrackerMaxBufferSize int
	FinalityDepth            int64

	DBPollInterval time.Duration

	KeysDir         string
	DefaultGasPrice *big.Int
	MaxGasPrice     *big.Int
	GasBumpPercent  uint64
	GasBumpWei      *big.Int

	// Number of elapsed blocks to trigger gas bumping
	GasBumpThreshold int64
	GasBumpTxDepth   int
}
