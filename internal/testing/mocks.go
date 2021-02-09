package testing

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/celer-network/eth-services/store/models"
)

// MockHeadTrackable allows you to mock HeadTrackable
type MockHeadTrackable struct {
	connectedCount    int32
	ConnectedCallback func(bn *models.Head)
	disconnectedCount int32
	onNewHeadCount    int32
}

// Connect increases the connected count by one
func (m *MockHeadTrackable) Connect(bn *models.Head) error {
	atomic.AddInt32(&m.connectedCount, 1)
	if m.ConnectedCallback != nil {
		m.ConnectedCallback(bn)
	}
	return nil
}

// ConnectedCount returns the count of connections made, safely.
func (m *MockHeadTrackable) ConnectedCount() int32 {
	return atomic.LoadInt32(&m.connectedCount)
}

// Disconnect increases the disconnected count by one
func (m *MockHeadTrackable) Disconnect() { atomic.AddInt32(&m.disconnectedCount, 1) }

// DisconnectedCount returns the count of disconnections made, safely.
func (m *MockHeadTrackable) DisconnectedCount() int32 {
	return atomic.LoadInt32(&m.disconnectedCount)
}

// OnNewLongestChain increases the OnNewLongestChainCount count by one
func (m *MockHeadTrackable) OnNewLongestChain(context.Context, *models.Head) {
	atomic.AddInt32(&m.onNewHeadCount, 1)
}

// OnNewLongestChainCount returns the count of new heads, safely.
func (m *MockHeadTrackable) OnNewLongestChainCount() int32 {
	return atomic.LoadInt32(&m.onNewHeadCount)
}

// NeverSleeper is a struct that never sleeps
type NeverSleeper struct{}

// Reset resets the never sleeper
func (ns NeverSleeper) Reset() {}

// Sleep puts the never sleeper to sleep
func (ns NeverSleeper) Sleep() {}

// After returns a duration
func (ns NeverSleeper) After() time.Duration { return 0 * time.Microsecond }

// Duration returns a duration
func (ns NeverSleeper) Duration() time.Duration { return 0 * time.Microsecond }
