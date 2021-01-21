package client

import (
	"context"
	"math/big"
	"net/url"
	"strings"
	"sync"

	"github.com/celer-network/eth-services/store/models"
	esTypes "github.com/celer-network/eth-services/types"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
)

//go:generate mockery --name Client --output ../internal/mocks/ --case=underscore
//go:generate mockery --name GethClient --output ../internal/mocks/ --case=underscore
//go:generate mockery --name RPCClient --output ../internal/mocks/ --case=underscore
//go:generate mockery --name Subscription --output ../internal/mocks/ --case=underscore

// Client is the interface used to interact with an ethereum node.
type Client interface {
	GethClient

	Dial(ctx context.Context) error
	Close()

	SendRawTx(bytes []byte) (common.Hash, error)
	Call(result interface{}, method string, args ...interface{}) error
	CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error

	HeaderByNumber(ctx context.Context, n *big.Int) (*models.Head, error)
	SubscribeNewHead(ctx context.Context, ch chan<- *models.Head) (ethereum.Subscription, error)
}

// GethClient is an interface that represents go-ethereum's own ethclient
// https://github.com/ethereum/go-ethereum/blob/master/ethclient/ethclient.go
type GethClient interface {
	ChainID(ctx context.Context) (*big.Int, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
	PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error)
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
	BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
	SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error)
	EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error)
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error)
}

// RPCClient is an interface that represents go-ethereum's own rpc.Client.
// https://github.com/ethereum/go-ethereum/blob/master/rpc/client.go
type RPCClient interface {
	Call(result interface{}, method string, args ...interface{}) error
	CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error
	BatchCallContext(ctx context.Context, b []rpc.BatchElem) error
	EthSubscribe(ctx context.Context, channel interface{}, args ...interface{}) (ethereum.Subscription, error)
	Close()
}

// Subscription is an interface for mock generation. It is identical to `ethereum.Subscription`.
type Subscription interface {
	Err() <-chan error
	Unsubscribe()
}

// Impl implements the ethereum Client interface using a CallerSubscriber instance.
type Impl struct {
	GethClient
	RPCClient
	url                  *url.URL // For reestablishing the connection after a disconnect
	SecondaryGethClients []GethClient
	SecondaryRPCClients  []RPCClient
	secondaryURLs        []*url.URL
	mocked               bool
	logger               esTypes.Logger
}

var _ Client = (*Impl)(nil)

// NewImpl creates a new client implementation
func NewImpl(config *esTypes.Config) (*Impl, error) {
	rpcURL := config.RPCURL
	if rpcURL.Scheme != "ws" && rpcURL.Scheme != "wss" {
		return nil, errors.Errorf("Ethereum URL scheme must be websocket: %s", rpcURL.String())
	}

	secondaryRPCURLs := config.SecondaryRPCURLs
	for _, url := range secondaryRPCURLs {
		if url.Scheme != "http" && url.Scheme != "https" {
			return nil, errors.Errorf("secondary Ethereum RPC URL scheme must be http(s): %s", url.String())
		}
	}
	return &Impl{url: rpcURL, secondaryURLs: secondaryRPCURLs, logger: config.Logger}, nil
}

func (client *Impl) Dial(ctx context.Context) error {
	client.logger.Debugw("eth.Client#Dial(...)")
	if client.mocked {
		return nil
	} else if client.RPCClient != nil || client.GethClient != nil {
		panic("eth.Client.Dial(...) should only be called once during the application's lifetime.")
	}

	rpcClient, err := rpc.DialContext(ctx, client.url.String())
	if err != nil {
		return err
	}
	client.RPCClient = &rpcClientWrapper{rpcClient}
	client.GethClient = ethclient.NewClient(rpcClient)

	client.SecondaryGethClients = []GethClient{}
	client.SecondaryRPCClients = []RPCClient{}
	for _, url := range client.secondaryURLs {
		secondaryRPCClient, err := rpc.DialContext(ctx, url.String())
		if err != nil {
			return err
		}
		client.SecondaryRPCClients = append(client.SecondaryRPCClients, &rpcClientWrapper{secondaryRPCClient})
		client.SecondaryGethClients = append(client.SecondaryGethClients, ethclient.NewClient(secondaryRPCClient))
	}
	return nil
}

// SendRawTx sends a signed transaction to the transaction pool.
func (client *Impl) SendRawTx(bytes []byte) (common.Hash, error) {
	client.logger.Debugw("eth.Client#SendRawTx(...)",
		"bytes", bytes,
	)
	result := common.Hash{}
	err := client.RPCClient.Call(&result, "eth_sendRawTransaction", hexutil.Encode(bytes))
	return result, err
}

// TransactionReceipt wraps the GethClient's `TransactionReceipt` method so that we can ignore the
// error that arises when we're talking to a Parity node that has no receipt yet.
func (client *Impl) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	client.logger.Debugw("eth.Client#TransactionReceipt(...)",
		"txHash", txHash,
	)
	receipt, err := client.GethClient.TransactionReceipt(ctx, txHash)
	if err != nil && strings.Contains(err.Error(), "missing required field") {
		return nil, ethereum.NotFound
	}
	return receipt, err
}

func (client *Impl) ChainID(ctx context.Context) (*big.Int, error) {
	client.logger.Debugw("eth.Client#ChainID(...)")
	return client.GethClient.ChainID(ctx)
}

// SendTransaction also uses the secondary HTTP RPC URL if set
func (client *Impl) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	client.logger.Debugw("eth.Client#SendTransaction(...)",
		"tx", tx,
	)

	for _, gethClient := range client.SecondaryGethClients {
		// Parallel send to secondary node
		client.logger.Tracew("eth.SecondaryClient#SendTransaction(...)", "tx", tx)

		var wg sync.WaitGroup
		defer wg.Wait()
		wg.Add(1)
		go func(gethClient GethClient) {
			defer wg.Done()
			err := NewSendError(gethClient.SendTransaction(ctx, tx))
			if err == nil || err.IsNonceTooLowError() || err.IsTransactionAlreadyInMempool() {
				// Nonce too low or transaction known errors are expected since
				// the primary SendTransaction may well have succeeded already
				return
			}
			client.logger.Warnf("secondary eth client returned error", "err", err, "tx", tx)
		}(gethClient)
	}

	return client.GethClient.SendTransaction(ctx, tx)
}

func (client *Impl) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	client.logger.Debugw("eth.Client#PendingNonceAt(...)",
		"account", account,
	)
	return client.GethClient.PendingNonceAt(ctx, account)
}

func (client *Impl) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	client.logger.Debugw("eth.Client#PendingCodeAt(...)",
		"account", account,
	)
	return client.GethClient.PendingCodeAt(ctx, account)
}

func (client *Impl) EstimateGas(ctx context.Context, call ethereum.CallMsg) (gas uint64, err error) {
	client.logger.Debugw("eth.Client#EstimateGas(...)",
		"call", call,
	)
	return client.GethClient.EstimateGas(ctx, call)
}

func (client *Impl) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	client.logger.Debugw("eth.Client#SuggestGasPrice()")
	return client.GethClient.SuggestGasPrice(ctx)
}

func (client *Impl) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	client.logger.Debugw("eth.Client#BlockByNumber(...)",
		"number", number,
	)
	return client.GethClient.BlockByNumber(ctx, number)
}

func (client *Impl) HeaderByNumber(ctx context.Context, number *big.Int) (*models.Head, error) {
	client.logger.Debugw("eth.Client#HeaderByNumber(...)",
		"number", number,
	)
	var head *models.Head
	err := client.RPCClient.CallContext(ctx, &head, "eth_getBlockByNumber", toBlockNumArg(number), false)
	if err == nil && head == nil {
		err = ethereum.NotFound
	}
	return head, err
}

func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	return hexutil.EncodeBig(number)
}

func (client *Impl) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	client.logger.Debug("eth.Client#BalanceAt(...)",
		"account", account,
		"blockNumber", blockNumber,
	)
	return client.GethClient.BalanceAt(ctx, account, blockNumber)
}

func (client *Impl) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	client.logger.Debugw("eth.Client#FilterLogs(...)",
		"q", q,
	)
	return client.GethClient.FilterLogs(ctx, q)
}

func (client *Impl) SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	client.logger.Debugw("eth.Client#SubscribeFilterLogs(...)",
		"q", q,
	)
	return client.GethClient.SubscribeFilterLogs(ctx, q, ch)
}

func (client *Impl) SubscribeNewHead(ctx context.Context, ch chan<- *models.Head) (ethereum.Subscription, error) {
	client.logger.Debugw("eth.Client#SubscribeNewHead(...)")
	return client.RPCClient.EthSubscribe(ctx, ch, "newHeads")
}

// TODO: remove this wrapper type once EthMock is no longer in use in upstream.
type rpcClientWrapper struct {
	*rpc.Client
}

func (w *rpcClientWrapper) EthSubscribe(ctx context.Context, channel interface{}, args ...interface{}) (ethereum.Subscription, error) {
	return w.Client.EthSubscribe(ctx, channel, args...)
}

func (client *Impl) Call(result interface{}, method string, args ...interface{}) error {
	client.logger.Debugw("eth.Client#Call(...)",
		"method", method,
		"args", args,
	)
	return client.RPCClient.Call(result, method, args...)
}

func (client *Impl) CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	client.logger.Debugw("eth.Client#Call(...)",
		"method", method,
		"args", args,
	)
	return client.RPCClient.CallContext(ctx, result, method, args...)
}
