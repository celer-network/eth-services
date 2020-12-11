package subscription

import (
	"context"
	"fmt"
	"time"

	"github.com/celer-network/eth-services/client"
	"github.com/celer-network/eth-services/store/models"
	"github.com/celer-network/eth-services/types"

	ethereum "github.com/ethereum/go-ethereum"
)

// Unsubscriber is the interface for all subscriptions, allowing one to unsubscribe.
type Unsubscriber interface {
	Unsubscribe()
}

// ManagedSubscription encapsulates the connecting, backfilling, and clean up of an
// ethereum node subscription.
type ManagedSubscription struct {
	logSubscriber   client.Client
	logs            chan models.Log
	ethSubscription ethereum.Subscription
	callback        func(models.Log)
	logger          types.Logger
}

// NewManagedSubscription subscribes to the ethereum node with the passed filter
// and delegates incoming logs to callback.
func NewManagedSubscription(
	logSubscriber client.Client,
	filter ethereum.FilterQuery,
	callback func(models.Log),
) (*ManagedSubscription, error) {
	ctx := context.Background()
	logs := make(chan models.Log)
	es, err := logSubscriber.SubscribeFilterLogs(ctx, filter, logs)
	if err != nil {
		return nil, err
	}

	sub := &ManagedSubscription{
		logSubscriber:   logSubscriber,
		callback:        callback,
		logs:            logs,
		ethSubscription: es,
	}
	go sub.listenToLogs(filter)
	return sub, nil
}

// Unsubscribe closes channels and cleans up resources.
func (sub ManagedSubscription) Unsubscribe() {
	if sub.ethSubscription != nil {
		timedUnsubscribe(sub.ethSubscription, sub.logger)
	}
	close(sub.logs)
}

// timedUnsubscribe attempts to unsubscribe but aborts abruptly after a time delay
// unblocking the application. This is an effort to mitigate the occasional
// indefinite block described here from go-ethereum.
func timedUnsubscribe(unsubscriber Unsubscriber, logger types.Logger) {
	unsubscribed := make(chan struct{})
	go func() {
		unsubscriber.Unsubscribe()
		close(unsubscribed)
	}()
	select {
	case <-unsubscribed:
	case <-time.After(100 * time.Millisecond):
		logger.Warningf("Subscription %T Unsubscribe timed out.", unsubscriber)
	}
}

func (sub ManagedSubscription) listenToLogs(q ethereum.FilterQuery) {
	backfilledSet := sub.backfillLogs(q)
	for {
		select {
		case log, open := <-sub.logs:
			if !open {
				return
			}
			if _, present := backfilledSet[log.BlockHash.String()]; !present {
				sub.callback(log)
			}
		case err, ok := <-sub.ethSubscription.Err():
			if ok {
				sub.logger.Errorf(fmt.Sprintf("Error in log subscription: %s", err.Error()), "err", err)
			}
		}
	}
}

// Manually retrieve old logs since SubscribeFilterLogs(ctx, filter, chLogs) only returns newly
// imported blocks: https://github.com/ethereum/go-ethereum/wiki/RPC-PUB-SUB#logs
// Therefore TxManager.FilterLogs does a one time retrieval of old logs.
func (sub ManagedSubscription) backfillLogs(q ethereum.FilterQuery) map[string]bool {
	backfilledSet := map[string]bool{}
	if q.FromBlock == nil {
		return backfilledSet
	}

	logs, err := sub.logSubscriber.FilterLogs(context.TODO(), q)
	if err != nil {
		sub.logger.Errorf("Unable to backfill logs", "err", err, "fromBlock", q.FromBlock.String(), "toBlock", q.ToBlock.String())
		return backfilledSet
	}

	for _, log := range logs {
		backfilledSet[log.BlockHash.String()] = true
		sub.callback(log)
	}
	return backfilledSet
}
