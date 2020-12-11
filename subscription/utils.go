package subscription

import (
	"time"

	"github.com/jpillora/backoff"
	"github.com/tevino/abool"
)

// Sleeper interface is used for tasks that need to be done on some
// interval, excluding Cron, like reconnecting.
type Sleeper interface {
	Reset()
	Sleep()
	After() time.Duration
	Duration() time.Duration
}

// BackoffSleeper is a sleeper that backs off on subsequent attempts.
type BackoffSleeper struct {
	backoff.Backoff
	beenRun *abool.AtomicBool
}

// NewBackoffSleeper returns a BackoffSleeper that is configured to
// sleep for 0 seconds initially, then backs off from 1 second minimum
// to 10 seconds maximum.
func NewBackoffSleeper() *BackoffSleeper {
	return &BackoffSleeper{
		Backoff: backoff.Backoff{
			Min: 1 * time.Second,
			Max: 10 * time.Second,
		},
		beenRun: abool.New(),
	}
}

// Sleep waits for the given duration, incrementing the back off.
func (bs *BackoffSleeper) Sleep() {
	if bs.beenRun.SetToIf(false, true) {
		return
	}
	time.Sleep(bs.Backoff.Duration())
}

// After returns the duration for the next stop, and increments the backoff.
func (bs *BackoffSleeper) After() time.Duration {
	if bs.beenRun.SetToIf(false, true) {
		return 0
	}
	return bs.Backoff.Duration()
}

// Duration returns the current duration value.
func (bs *BackoffSleeper) Duration() time.Duration {
	if !bs.beenRun.IsSet() {
		return 0
	}
	return bs.ForAttempt(bs.Attempt())
}

// Reset resets the backoff intervals.
func (bs *BackoffSleeper) Reset() {
	bs.beenRun.UnSet()
	bs.Backoff.Reset()
}
