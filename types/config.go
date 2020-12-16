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

	DefaultGasPrice uint64
}
