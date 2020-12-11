package types

import (
	"math/big"
	"net/url"
	"time"
)

type Options struct {
	Logger           Logger
	BlockTime        time.Duration
	RPCURL           *url.URL
	SecondaryRPCURLs []*url.URL
	ChainID          *big.Int

	HeadTrackerHistoryDepth  uint
	HeadTrackerMaxBufferSize int
	FinalityDepth            int

	DefaultGasPrice uint64
}
