package subscription_test

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/celer-network/eth-services/internal/mocks"
	esTesting "github.com/celer-network/eth-services/internal/testing"
	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/celer-network/eth-services/subscription"
	ethereum "github.com/ethereum/go-ethereum"
	gethCommon "github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/onsi/gomega"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHeadTracker_New(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)

	sub := new(mocks.Subscription)
	ethClient := new(mocks.Client)
	ethClient.On("ChainID", mock.Anything).Return(config.ChainID, nil)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)
	sub.On("Err").Return(nil)

	assert.Nil(t, store.InsertHead(esTesting.Head(1)))
	last := esTesting.Head(16)
	assert.Nil(t, store.InsertHead(last))
	assert.Nil(t, store.InsertHead(esTesting.Head(10)))

	ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{})
	assert.Nil(t, ht.Start())
	assert.Equal(t, last.Number, ht.HighestSeenHead().Number)
}

func TestHeadTracker_Save_InsertsAndTrims(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)

	config.HeadTrackerHistoryDepth = 100

	ethClient := new(mocks.Client)
	ethClient.On("ChainID", mock.Anything).Return(config.ChainID, nil)

	for i := 0; i < 200; i++ {
		assert.Nil(t, store.InsertHead(esTesting.Head(i)))
	}

	ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{})

	h := esTesting.Head(200)
	require.NoError(t, ht.Save(h))
	assert.Equal(t, big.NewInt(200), ht.HighestSeenHead().ToInt())

	firstHead, err := store.FirstHead()
	require.NoError(t, err)
	assert.Equal(t, big.NewInt(101), firstHead.ToInt())

	lastHead, err := store.LastHead()
	require.NoError(t, err)
	assert.Equal(t, int64(200), lastHead.Number)
}

func TestHeadTracker_Get(t *testing.T) {
	t.Parallel()

	start := esTesting.Head(5)

	tests := []struct {
		name    string
		initial *models.Head
		toSave  *models.Head
		want    *big.Int
	}{
		{"greater", start, esTesting.Head(6), big.NewInt(6)},
		{"less than", start, esTesting.Head(1), big.NewInt(5)},
		{"zero", start, esTesting.Head(0), big.NewInt(5)},
		{"nil", start, nil, big.NewInt(5)},
		{"nil no initial", nil, nil, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := esTesting.NewStore(t)
			config := esTesting.NewConfig(t)

			ethClient := new(mocks.Client)
			sub := new(mocks.Subscription)
			ethClient.On("ChainID", mock.Anything).Return(config.ChainID, nil)
			sub.On("Err").Return(nil)
			sub.On("Unsubscribe").Return(nil)
			chStarted := make(chan struct{})
			ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
				Run(func(mock.Arguments) { close(chStarted) }).
				Return(sub, nil)

			fnCall := ethClient.On("HeaderByNumber", mock.Anything, mock.Anything)
			fnCall.RunFn = func(args mock.Arguments) {
				num := args.Get(1).(*big.Int)
				fnCall.ReturnArguments = mock.Arguments{esTesting.Head(num.Int64()), nil}
			}

			if test.initial != nil {
				assert.Nil(t, store.InsertHead(test.initial))
			}

			ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{})
			ht.Start()
			defer ht.Stop()

			if test.toSave != nil {
				err := ht.Save(test.toSave)
				assert.NoError(t, err)
			}

			assert.Equal(t, test.want, ht.HighestSeenHead().ToInt())
		})
	}
}

func TestHeadTracker_Start_NewHeads(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)

	ethClient := new(mocks.Client)
	ethClient.On("ChainID", mock.Anything).Return(config.ChainID, nil)
	sub := new(mocks.Subscription)
	sub.On("Err").Return(nil)
	sub.On("Unsubscribe").Return(nil)
	chStarted := make(chan struct{})
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { close(chStarted) }).
		Return(sub, nil)

	ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{})

	assert.NoError(t, ht.Start())
	<-chStarted

	ht.Stop()
	<-ht.ExportedDone()
	ethClient.AssertExpectations(t)
}

func TestHeadTracker_CallsHeadTrackableCallbacks(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)

	sub := new(mocks.Subscription)
	ethClient := new(mocks.Client)

	chchHeaders := make(chan chan<- *models.Head, 1)
	ethClient.On("ChainID", mock.Anything).Return(config.ChainID, nil)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			chchHeaders <- args.Get(1).(chan<- *models.Head)
		}).
		Return(sub, nil)
	ethClient.On("HeaderByNumber", mock.Anything, mock.Anything).Return(esTesting.Head(1), nil)

	sub.On("Unsubscribe").Return()
	sub.On("Err").Return(nil)

	checker := &esTesting.MockHeadTrackable{}
	ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{checker}, esTesting.NeverSleeper{})

	assert.Nil(t, ht.Start())
	g.Eventually(func() int32 { return checker.ConnectedCount() }).Should(gomega.Equal(int32(1)))
	assert.Equal(t, int32(0), checker.DisconnectedCount())
	assert.Equal(t, int32(0), checker.OnNewLongestChainCount())

	headers := <-chchHeaders
	headers <- &models.Head{Number: 1}
	g.Eventually(func() int32 { return checker.OnNewLongestChainCount() }).Should(gomega.Equal(int32(1)))
	assert.Equal(t, int32(1), checker.ConnectedCount())
	assert.Equal(t, int32(0), checker.DisconnectedCount())

	require.NoError(t, ht.Stop())
	assert.Equal(t, int32(1), checker.DisconnectedCount())
	assert.Equal(t, int32(1), checker.ConnectedCount())
	assert.Equal(t, int32(1), checker.OnNewLongestChainCount())
}

func TestHeadTracker_ReconnectOnError(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)

	ethClient := new(mocks.Client)
	sub := new(mocks.Subscription)
	ethClient.On("ChainID", mock.Anything).Maybe().Return(config.ChainID, nil)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(nil, errors.New("cannot reconnect"))
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)
	chErr := make(chan error)
	sub.On("Unsubscribe").Return()
	sub.On("Err").Return((<-chan error)(chErr))

	checker := &esTesting.MockHeadTrackable{}
	ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{checker}, esTesting.NeverSleeper{})

	// connect
	assert.Nil(t, ht.Start())
	g.Eventually(func() int32 { return checker.ConnectedCount() }).Should(gomega.Equal(int32(1)))
	assert.Equal(t, int32(0), checker.DisconnectedCount())
	assert.Equal(t, int32(0), checker.OnNewLongestChainCount())

	// trigger reconnect loop
	chErr <- errors.New("Test error to force reconnect")
	g.Eventually(func() int32 { return checker.ConnectedCount() }).Should(gomega.Equal(int32(2)))
	g.Consistently(func() int32 { return checker.ConnectedCount() }).Should(gomega.Equal(int32(2)))
	assert.Equal(t, int32(1), checker.DisconnectedCount())
	assert.Equal(t, int32(0), checker.OnNewLongestChainCount())

	// stop
	assert.NoError(t, ht.Stop())
}

func TestHeadTracker_StartConnectsFromLastSavedHeader(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)

	sub := new(mocks.Subscription)
	ethClient := new(mocks.Client)

	chchHeaders := make(chan chan<- *models.Head, 1)
	ethClient.On("ChainID", mock.Anything).Return(config.ChainID, nil)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { chchHeaders <- args.Get(1).(chan<- *models.Head) }).
		Return(sub, nil)

	latestHeadByNumber := make(map[int64]*models.Head)
	fnCall := ethClient.On("HeaderByNumber", mock.Anything, mock.Anything)
	fnCall.RunFn = func(args mock.Arguments) {
		num := args.Get(1).(*big.Int)
		head, exists := latestHeadByNumber[num.Int64()]
		if !exists {
			head = esTesting.Head(num.Int64())
			latestHeadByNumber[num.Int64()] = head
		}
		fnCall.ReturnArguments = mock.Arguments{head, nil}
	}

	sub.On("Unsubscribe").Return()
	sub.On("Err").Return(nil)

	lastSavedBN := big.NewInt(1)
	currentBN := big.NewInt(2)
	var connectedValue atomic.Value

	checker := &esTesting.MockHeadTrackable{ConnectedCallback: func(bn *models.Head) {
		connectedValue.Store(bn.ToInt())
	}}
	ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{checker}, esTesting.NeverSleeper{})

	require.NoError(t, ht.Save(models.NewHead(lastSavedBN, esTesting.NewHash(), esTesting.NewHash(), 0)))

	assert.Nil(t, ht.Start())
	headers := <-chchHeaders
	headers <- &models.Head{Number: currentBN.Int64()}
	g.Eventually(func() int32 { return checker.ConnectedCount() }).Should(gomega.Equal(int32(1)))

	connectedBN := connectedValue.Load().(*big.Int)
	assert.Equal(t, lastSavedBN, connectedBN)

	g.Eventually(func() int32 { return checker.OnNewLongestChainCount() }).Should(gomega.Equal(int32(1)))

	assert.NoError(t, ht.Stop())

	// Check that it saved the head
	h, err := store.LastHead()
	require.NoError(t, err)
	assert.Equal(t, h.Number, currentBN.Int64())
}

func TestHeadTracker_SwitchesToLongestChain(t *testing.T) {
	t.Parallel()

	store := esTesting.NewStore(t)
	config := esTesting.NewConfig(t)

	// Need to set the buffer to something large since we inject a lot of heads at once and otherwise they will be dropped
	config.HeadTrackerMaxBufferSize = 42

	sub := new(mocks.Subscription)
	ethClient := new(mocks.Client)

	checker := new(mocks.HeadTrackable)
	ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{checker}, esTesting.NeverSleeper{})

	chchHeaders := make(chan chan<- *models.Head, 1)
	ethClient.On("ChainID", mock.Anything).Return(config.ChainID, nil)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { chchHeaders <- args.Get(1).(chan<- *models.Head) }).
		Return(sub, nil)

	sub.On("Unsubscribe").Return()
	sub.On("Err").Return(nil)

	checker.On("Connect", mock.MatchedBy(func(h *models.Head) bool {
		return h == nil
	})).Return(nil).Once()
	checker.On("Disconnect").Return(nil).Once()

	assert.Nil(t, ht.Start())

	lastHead := make(chan struct{})
	blockHeaders := []*models.Head{}

	// First block comes in
	blockHeaders = append(blockHeaders, &models.Head{Number: 1, Hash: esTesting.NewHash(), ParentHash: esTesting.NewHash(), Timestamp: time.Unix(1, 0)})
	// Blocks 2 and 3 are out of order
	head2 := &models.Head{Number: 2, Hash: esTesting.NewHash(), ParentHash: blockHeaders[0].Hash, Timestamp: time.Unix(2, 0)}
	head3 := &models.Head{Number: 3, Hash: esTesting.NewHash(), ParentHash: head2.Hash, Timestamp: time.Unix(3, 0)}
	blockHeaders = append(blockHeaders, head3)
	blockHeaders = append(blockHeaders, head2)
	// Block 4 comes in
	blockHeaders = append(blockHeaders, &models.Head{Number: 4, Hash: esTesting.NewHash(), ParentHash: blockHeaders[1].Hash, Timestamp: time.Unix(4, 0)})
	// Another block at level 4 comes in, that will be uncled
	blockHeaders = append(blockHeaders, &models.Head{Number: 4, Hash: esTesting.NewHash(), ParentHash: blockHeaders[1].Hash, Timestamp: time.Unix(5, 0)})
	// Reorg happened forking from block 2
	blockHeaders = append(blockHeaders, &models.Head{Number: 2, Hash: esTesting.NewHash(), ParentHash: blockHeaders[0].Hash, Timestamp: time.Unix(6, 0)})
	blockHeaders = append(blockHeaders, &models.Head{Number: 3, Hash: esTesting.NewHash(), ParentHash: blockHeaders[5].Hash, Timestamp: time.Unix(7, 0)})
	blockHeaders = append(blockHeaders, &models.Head{Number: 4, Hash: esTesting.NewHash(), ParentHash: blockHeaders[6].Hash, Timestamp: time.Unix(8, 0)})
	// Now the new chain is longer
	blockHeaders = append(blockHeaders, &models.Head{Number: 5, Hash: esTesting.NewHash(), ParentHash: blockHeaders[7].Hash, Timestamp: time.Unix(9, 0)})

	checker.On("OnNewLongestChain", mock.Anything, mock.MatchedBy(func(h *models.Head) bool {
		return h.Number == 1 && h.Hash == blockHeaders[0].Hash
	})).Return().Once()
	checker.On("OnNewLongestChain", mock.Anything, mock.MatchedBy(func(h *models.Head) bool {
		return h.Number == 3 && h.Hash == blockHeaders[1].Hash
	})).Return().Once()
	checker.On("OnNewLongestChain", mock.Anything, mock.MatchedBy(func(h *models.Head) bool {
		if h.Number == 4 && h.Hash == blockHeaders[3].Hash {
			// Check that the block came with its parents
			require.NotNil(t, h.Parent)
			require.Equal(t, h.Parent.Hash, blockHeaders[1].Hash)
			require.NotNil(t, h.Parent.Parent.Hash)
			require.Equal(t, h.Parent.Parent.Hash, blockHeaders[2].Hash)
			require.NotNil(t, h.Parent.Parent.Parent)
			require.NotNil(t, h.Parent.Parent.Parent.Hash, blockHeaders[0].Hash)
			return true
		}
		return false
	})).Return().Once()
	checker.On("OnNewLongestChain", mock.Anything, mock.MatchedBy(func(h *models.Head) bool {
		if h.Number == 5 && h.Hash == blockHeaders[8].Hash {
			// This is the new longest chain, check that it came with its parents
			require.NotNil(t, h.Parent)
			require.Equal(t, h.Parent.Hash, blockHeaders[7].Hash)
			require.NotNil(t, h.Parent.Parent.Hash)
			require.Equal(t, h.Parent.Parent.Hash, blockHeaders[6].Hash)
			require.NotNil(t, h.Parent.Parent.Parent)
			require.NotNil(t, h.Parent.Parent.Parent.Hash, blockHeaders[5].Hash)
			require.NotNil(t, h.Parent.Parent.Parent.Parent)
			require.NotNil(t, h.Parent.Parent.Parent.Parent.Hash, blockHeaders[0].Hash)

			return true
		}
		return false
	})).Return().Once().Run(func(_ mock.Arguments) {
		close(lastHead)
	})

	headers := <-chchHeaders

	// This grotesque construction is the only way to do dynamic return values using
	// the mock package.  We need dynamic returns because we're simulating reorgs.
	latestHeadByNumber := make(map[int64]*models.Head)
	latestHeadByNumberMu := new(sync.Mutex)

	fnCall := ethClient.On("HeaderByNumber", mock.Anything, mock.Anything)
	fnCall.RunFn = func(args mock.Arguments) {
		latestHeadByNumberMu.Lock()
		defer latestHeadByNumberMu.Unlock()
		num := args.Get(1).(*big.Int)
		head, exists := latestHeadByNumber[num.Int64()]
		if !exists {
			head = esTesting.Head(num.Int64())
			latestHeadByNumber[num.Int64()] = head
		}
		fnCall.ReturnArguments = mock.Arguments{head, nil}
	}
	for _, h := range blockHeaders {
		latestHeadByNumberMu.Lock()
		latestHeadByNumber[h.Number] = h
		latestHeadByNumberMu.Unlock()
		headers <- h
	}

	gomega.NewWithT(t).Eventually(lastHead).Should(gomega.BeClosed())
	require.NoError(t, ht.Stop())
	assert.Equal(t, int64(5), ht.HighestSeenHead().Number)

	for _, h := range blockHeaders {
		c, err := store.Chain(h.Hash, 1)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Equal(t, c.ParentHash, h.ParentHash)
		assert.Equal(t, c.Timestamp.Unix(), h.Timestamp.UTC().Unix())
		assert.Equal(t, c.Number, h.Number)
	}

	checker.AssertExpectations(t)
}

func TestHeadTracker_GetChainWithBackfill(t *testing.T) {
	t.Parallel()

	// Heads are arranged as follows:
	// headN indicates an unpersisted ethereum header
	// hN indicates a persisted head record
	//
	// (1)->(H0)
	//
	//       (14Orphaned)-+
	//                    +->(13)->(12)->(11)->(H10)->(9)->(H8)
	// (15)->(14)---------+

	now := uint64(time.Now().UTC().Unix())

	gethHead0 := &gethTypes.Header{
		Number:     big.NewInt(0),
		ParentHash: gethCommon.BigToHash(big.NewInt(0)),
		Time:       now,
	}
	head0 := models.NewHead(gethHead0.Number, esTesting.NewHash(), gethHead0.ParentHash, gethHead0.Time)

	h1 := esTesting.Head(1)
	h1.ParentHash = head0.Hash

	gethHead8 := &gethTypes.Header{
		Number:     big.NewInt(8),
		ParentHash: esTesting.NewHash(),
		Time:       now,
	}
	head8 := models.NewHead(gethHead8.Number, esTesting.NewHash(), gethHead8.ParentHash, gethHead8.Time)

	h9 := esTesting.Head(9)
	h9.ParentHash = head8.Hash

	gethHead10 := &gethTypes.Header{
		Number:     big.NewInt(10),
		ParentHash: h9.Hash,
		Time:       now,
	}
	head10 := models.NewHead(gethHead10.Number, esTesting.NewHash(), gethHead10.ParentHash, gethHead10.Time)

	h11 := esTesting.Head(11)
	h11.ParentHash = head10.Hash

	h12 := esTesting.Head(12)
	h12.ParentHash = h11.Hash

	h13 := esTesting.Head(13)
	h13.ParentHash = h12.Hash

	h14Orphaned := esTesting.Head(14)
	h14Orphaned.ParentHash = h13.Hash

	h14 := esTesting.Head(14)
	h14.ParentHash = h13.Hash

	h15 := esTesting.Head(15)
	h15.ParentHash = h14.Hash

	heads := []*models.Head{
		h9,
		h11,
		h12,
		h13,
		h14Orphaned,
		h14,
		h15,
	}

	ctx := context.Background()

	t.Run("returns chain if all the heads are in database", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)

		for _, h := range heads {
			require.NoError(t, store.InsertHead(h))
		}

		sub := new(mocks.Subscription)
		ethClient := new(mocks.Client)
		ethClient.On("ChainID", mock.Anything).Return(config.ChainID, nil)
		ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)
		sub.On("Err").Return(nil)

		ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{}, esTesting.NeverSleeper{})

		h, err := ht.GetChainWithBackfill(ctx, h12, 2)
		require.NoError(t, err)

		assert.Equal(t, int64(12), h.Number)
		require.NotNil(t, h.Parent)
		assert.Equal(t, int64(11), h.Parent.Number)
		require.Nil(t, h.Parent.Parent)
	})

	t.Run("fetches a missing head", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)

		for _, h := range heads {
			require.NoError(t, store.InsertHead(h))
		}

		ethClient := new(mocks.Client)

		ethClient.On("HeaderByNumber", mock.Anything, big.NewInt(10)).
			Return(head10, nil)

		ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{}, esTesting.NeverSleeper{})

		h, err := ht.GetChainWithBackfill(ctx, h12, 3)
		require.NoError(t, err)

		assert.Equal(t, int64(12), h.Number)
		require.NotNil(t, h.Parent)
		assert.Equal(t, int64(11), h.Parent.Number)
		require.NotNil(t, h.Parent)
		assert.Equal(t, int64(10), h.Parent.Parent.Number)
		require.Nil(t, h.Parent.Parent.Parent)

		writtenHead, err := store.HeadByHash(head10.Hash)
		require.NoError(t, err)
		assert.Equal(t, int64(10), writtenHead.Number)

		ethClient.AssertExpectations(t)
	})

	t.Run("fetches only heads that are missing", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)

		for _, h := range heads {
			require.NoError(t, store.InsertHead(h))
		}

		ethClient := new(mocks.Client)

		ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{}, esTesting.NeverSleeper{})

		ethClient.On("HeaderByNumber", mock.Anything, big.NewInt(10)).
			Return(head10, nil)
		ethClient.On("HeaderByNumber", mock.Anything, big.NewInt(8)).
			Return(head8, nil)

		// Needs to be 8 because there are 8 heads in chain (15,14,13,12,11,10,9,8)
		h, err := ht.GetChainWithBackfill(ctx, h15, 8)
		require.NoError(t, err)

		require.Equal(t, int64(8), h.ChainLength())
		earliestInChain := h.EarliestInChain()
		assert.Equal(t, head8.Number, earliestInChain.Number)
		assert.Equal(t, head8.Hash, earliestInChain.Hash)

		ethClient.AssertExpectations(t)
	})

	t.Run("returns error if first head is not in database", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)

		ethClient := new(mocks.Client)
		sub := new(mocks.Subscription)
		ethClient.On("ChainID", mock.Anything).Return(config.ChainID)
		ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)

		ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{}, esTesting.NeverSleeper{})

		h16 := esTesting.Head(16)
		h16.ParentHash = h15.Hash

		_, err := ht.GetChainWithBackfill(ctx, h16, 3)
		require.Contains(t, err.Error(), esStore.ErrNotFound.Error())
	})

	t.Run("does not backfill if chain length is already greater than or equal to depth", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)

		for _, h := range heads {
			require.NoError(t, store.InsertHead(h))
		}

		ethClient := new(mocks.Client)
		sub := new(mocks.Subscription)
		ethClient.On("ChainID", mock.Anything).Return(config.ChainID)
		ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)

		ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{}, esTesting.NeverSleeper{})

		h, err := ht.GetChainWithBackfill(ctx, h15, 3)
		require.NoError(t, err)
		require.Equal(t, int64(3), h.ChainLength())

		h, err = ht.GetChainWithBackfill(ctx, h15, 5)
		require.NoError(t, err)
		require.Equal(t, int64(5), h.ChainLength())
	})

	t.Run("only backfills to height 0 if chain length would otherwise cause it to try and fetch a negative head", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)

		ethClient := new(mocks.Client)
		ethClient.On("HeaderByNumber", mock.Anything, big.NewInt(0)).
			Return(head0, nil)

		ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{}, esTesting.NeverSleeper{})

		require.NoError(t, store.InsertHead(h1))

		h, err := ht.GetChainWithBackfill(ctx, h1, 400)
		require.NoError(t, err)
		require.Equal(t, int64(2), h.ChainLength())
		require.Equal(t, int64(0), h.EarliestInChain().Number)

		ethClient.AssertExpectations(t)
	})

	t.Run("abandons backfill and returns whatever we have if the eth node returns not found", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)

		for _, h := range heads {
			require.NoError(t, store.InsertHead(h))
		}

		ethClient := new(mocks.Client)
		ethClient.On("HeaderByNumber", mock.Anything, big.NewInt(10)).
			Return(head10, nil).
			Once()
		ethClient.On("HeaderByNumber", mock.Anything, big.NewInt(8)).
			Return(nil, ethereum.NotFound).
			Once()

		ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{}, esTesting.NeverSleeper{})

		h, err := ht.GetChainWithBackfill(ctx, h12, 400)
		require.NoError(t, err)

		// Should contain 12, 11, 10, 9
		assert.Equal(t, 4, int(h.ChainLength()))
		assert.Equal(t, int64(9), h.EarliestInChain().Number)

		ethClient.AssertExpectations(t)
	})

	t.Run("abandons backfill and returns whatever we have if the context time budget is exceeded", func(t *testing.T) {
		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)

		for _, h := range heads {
			require.NoError(t, store.InsertHead(h))
		}

		ethClient := new(mocks.Client)
		ethClient.On("HeaderByNumber", mock.Anything, big.NewInt(10)).
			Return(head10, nil)
		ethClient.On("HeaderByNumber", mock.Anything, big.NewInt(8)).
			Return(nil, context.DeadlineExceeded)

		ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{}, esTesting.NeverSleeper{})

		h, err := ht.GetChainWithBackfill(ctx, h12, 400)
		require.NoError(t, err)

		// Should contain 12, 11, 10, 9
		assert.Equal(t, 4, int(h.ChainLength()))
		assert.Equal(t, int64(9), h.EarliestInChain().Number)

		ethClient.AssertExpectations(t)
	})
}

type blockingCallback struct {
	called chan *models.Head
	resume chan bool
}

func (c *blockingCallback) Connect(bn *models.Head) error {
	return nil
}

func (c *blockingCallback) Disconnect() {
}

// OnNewLongestChain increases the OnNewLongestChainCount count by one
func (c *blockingCallback) OnNewLongestChain(ctx context.Context, h *models.Head) {
	c.called <- h
	<-c.resume
}

func TestHeadTracker_RingBuffer(t *testing.T) {
	t.Run("drops excess heads if we can't process them fast enough", func(t *testing.T) {
		t.Parallel()
		bufferSize := 3

		store := esTesting.NewStore(t)
		config := esTesting.NewConfig(t)

		config.HeadTrackerMaxBufferSize = bufferSize
		config.HeadTrackerHistoryDepth = 50

		sub := new(mocks.Subscription)
		ethClient := new(mocks.Client)

		chchHeaders := make(chan chan<- *models.Head, 1)
		ethClient.On("ChainID", mock.Anything).Return(config.ChainID, nil)
		ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				chchHeaders <- args.Get(1).(chan<- *models.Head)
			}).
			Return(sub, nil)
		// We don't care about this since we're not testing backfilling, just return anything
		ethClient.On("HeaderByNumber", mock.Anything, mock.Anything).Return(esTesting.Head(42), nil)

		sub.On("Unsubscribe").Return()
		sub.On("Err").Return(nil)

		called := make(chan *models.Head)
		resume := make(chan bool)
		cb := &blockingCallback{
			called: called,
			resume: resume,
		}
		ht := subscription.NewHeadTracker(ethClient, store, config, []subscription.HeadTrackable{cb}, esTesting.NeverSleeper{})
		require.NoError(t, ht.Start())
		headers := <-chchHeaders

		// Fill up the buffer first
		for i := 0; i < bufferSize; i++ {
			headers <- &models.Head{Number: int64(i), Hash: esTesting.NewHash()}
		}
		// Now we have heads 0, 1, 2 in buffer. Wait for callback to block on head 0
		h := <-cb.called
		require.Equal(t, int64(0), h.Number)

		// Head 0 has been pulled off. Callback is blocking on head 0.
		// Buffer: 1, 2
		headers <- &models.Head{Number: 3, Hash: esTesting.NewHash()}
		// Buffer: 1, 2, 3
		headers <- &models.Head{Number: 4, Hash: esTesting.NewHash()}
		// Buffer: 2, 3, 4 (dropped head 1)

		// Resume the headtracker callback
		cb.resume <- true

		// Next head to be pulled off ought to be 2
		h = <-cb.called
		require.Equal(t, int64(2), h.Number)
		cb.resume <- true

		// 3, 4
		h = <-cb.called
		require.Equal(t, int64(3), h.Number)
		cb.resume <- true
		h = <-cb.called
		require.Equal(t, int64(4), h.Number)
		cb.resume <- true

		// Headers channel now empty
		require.Len(t, headers, 0)
	})
}
