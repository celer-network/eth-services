package types

import (
	"math/big"
	"net/url"
	"time"
)

type Config struct {
	Logger           Logger
	BlockTime        time.Duration
	RPCURL           *url.URL
	SecondaryRPCURLs []*url.URL
	ChainID          *big.Int

	HeadTrackerHistoryDepth  uint64
	HeadTrackerMaxBufferSize int
	FinalityDepth            uint64

	DBPollInterval time.Duration

	KeysDir         string
	DefaultGasPrice *big.Int
	MaxGasPrice     *big.Int
	GasBumpPercent  uint64
	GasBumpWei      *big.Int
}
