package txmanager

import (
	"math/big"
	mathRand "math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
)

func max(a, b *big.Int) *big.Int {
	if a.Cmp(b) >= 0 {
		return a
	}
	return b
}

type StartStopOnce struct {
	state StartStopOnceState
	sync.RWMutex
}

type StartStopOnceState int

const (
	StartStopOnce_Unstarted StartStopOnceState = iota
	StartStopOnce_Started
	StartStopOnce_Stopped
)

func (once *StartStopOnce) StartOnce(name string, fn func() error) error {
	once.Lock()
	defer once.Unlock()

	if once.state != StartStopOnce_Unstarted {
		return errors.Errorf("%v has already started once", name)
	}
	once.state = StartStopOnce_Started

	return fn()
}

func (once *StartStopOnce) StopOnce(name string, fn func() error) error {
	once.Lock()
	defer once.Unlock()

	if once.state != StartStopOnce_Started {
		return errors.Errorf("%v has already stopped once", name)
	}
	once.state = StartStopOnce_Stopped

	return fn()
}

func (once *StartStopOnce) OkayToStart() (ok bool) {
	once.Lock()
	defer once.Unlock()

	if once.state != StartStopOnce_Unstarted {
		return false
	}
	once.state = StartStopOnce_Started
	return true
}

func (once *StartStopOnce) OkayToStop() (ok bool) {
	once.Lock()
	defer once.Unlock()

	if once.state != StartStopOnce_Started {
		return false
	}
	once.state = StartStopOnce_Stopped
	return true
}

func (once *StartStopOnce) State() StartStopOnceState {
	once.RLock()
	defer once.RUnlock()
	return once.state
}

// withJitter adds +/- 10% to a duration
func withJitter(d time.Duration) time.Duration {
	jitter := mathRand.Intn(int(d) / 5)
	jitter = jitter - (jitter / 2)
	return time.Duration(int(d) + jitter)
}

// WrapIfError decorates an error with the given message.  It is intended to
// be used with `defer` statements, like so:
//
// func SomeFunction() (err error) {
//     defer WrapIfError(&err, "error in SomeFunction:")
//
//     ...
// }
func WrapIfError(err *error, msg string) {
	if *err != nil {
		*err = errors.Wrap(*err, msg)
	}
}
