// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"context"
	"reflect"
	"sync"
	"time"
)

// WatchSet is a collection of watch channels. The zero value is not usable.
// Use NewWatchSet to create a WatchSet.
type WatchSet map[<-chan struct{}]struct{}

// maxWatchCoordinators is the fixed upper bound on the number of coordinator
// goroutines that a single WatchCtx/Watch call may spawn for a watch set with
// more than aFew channels. The number of goroutines is therefore independent
// of the number of channels being watched.
const maxWatchCoordinators = 8

// NewWatchSet constructs a new watch set.
func NewWatchSet() WatchSet {
	return make(map[<-chan struct{}]struct{})
}

// Add appends a watchCh to the WatchSet if non-nil.
func (w WatchSet) Add(watchCh <-chan struct{}) {
	if w == nil {
		return
	}

	if _, ok := w[watchCh]; !ok {
		w[watchCh] = struct{}{}
	}
}

// AddWithLimit appends a watchCh to the WatchSet if non-nil, and if the given
// softLimit hasn't been exceeded. Otherwise, it will watch the given alternate
// channel. It's expected that the altCh will be the same on many calls to this
// function, so you will exceed the soft limit a little bit if you hit this, but
// not by much.
//
// This is useful if you want to track individual items up to some limit, after
// which you watch a higher-level channel (usually a channel from start of
// an iterator higher up in the radix tree) that will watch a superset of items.
func (w WatchSet) AddWithLimit(softLimit int, watchCh <-chan struct{}, altCh <-chan struct{}) {
	// This is safe for a nil WatchSet so we don't need to check that here.
	if len(w) < softLimit {
		w.Add(watchCh)
	} else {
		w.Add(altCh)
	}
}

// Watch blocks until one of the channels in the watch set is closed, or
// timeoutCh sends a value.
// Returns true if timeoutCh is what caused Watch to unblock.
//
// Empty watch sets and watch sets with at most aFew channels block in the
// calling goroutine without spawning any helper goroutines. Larger watch sets
// use a bounded pool of coordinator goroutines (see maxWatchCoordinators) plus
// a single goroutine bridging timeoutCh into a context.
func (w WatchSet) Watch(timeoutCh <-chan time.Time) bool {
	if w == nil {
		return false
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(w) <= aFew {
		chunk := make([]<-chan struct{}, aFew)
		idx := 0
		for watchCh := range w {
			chunk[idx] = watchCh
			idx++
		}
		// Go's select chooses uniformly among ready cases, so a timeout that
		// is ready at the same instant a watch channel closes may win the
		// select. Re-check the set so a channel trigger has deterministic
		// precedence over the timeout.
		if watchFewTimer(ctx, chunk, timeoutCh) == context.Canceled && !w.anyTriggered() {
			return true
		}
		return false
	}

	return w.watchManyTimer(ctx, cancel, timeoutCh)
}

// watchManyTimer backs Watch for watch sets larger than aFew. The timeout is
// represented as a cancellable context; a channel trigger gets deterministic
// precedence over a timeout that fires at the same time, mirroring WatchCtx.
func (w WatchSet) watchManyTimer(ctx context.Context, cancel context.CancelFunc, timeoutCh <-chan time.Time) bool {
	go func() {
		select {
		case <-timeoutCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	err := w.WatchCtx(ctx)
	return err == context.Canceled
}

// WatchCtx blocks until one of the channels in the watch set is closed, or
// ctx is done (cancelled or exceeds the deadline). WatchCtx returns an error
// if the ctx causes it to unblock, otherwise returns nil.
//
// WatchCtx should be preferred over Watch.
//
// Empty watch sets and watch sets with at most aFew channels block in the
// calling goroutine without spawning any helper goroutines. Larger watch sets
// fan the channels out to a fixed pool of at most maxWatchCoordinators
// coordinator goroutines, all of which are guaranteed to exit when WatchCtx
// returns.
//
// If a channel in the watch set triggers at the same time that ctx is
// cancelled, the channel trigger wins deterministically and WatchCtx returns
// nil.
func (w WatchSet) WatchCtx(ctx context.Context) error {
	if w == nil {
		return nil
	}

	if len(w) <= aFew {
		idx := 0
		chunk := make([]<-chan struct{}, aFew)
		for watchCh := range w {
			chunk[idx] = watchCh
			idx++
		}
		if err := watchFew(ctx, chunk); err != nil && !w.anyTriggered() {
			return err
		}
		return nil
	}

	return w.watchMany(ctx)
}

// anyTriggered reports whether at least one channel in the watch set has
// already been closed. It gives a channel trigger deterministic precedence
// over a context cancellation that races with it.
func (w WatchSet) anyTriggered() bool {
	for watchCh := range w {
		select {
		case <-watchCh:
			return true
		default:
		}
	}
	return false
}

// watchMany is used if there are many watchers. It partitions the channels
// across a fixed pool of coordinator goroutines, each of which multiplexes
// its whole partition through a single reflect.Select. Its goroutine
// footprint is therefore bounded regardless of the number of watched
// channels, while every watched channel remains observable at all times.
func (w WatchSet) watchMany(ctx context.Context) error {
	coordinators := maxWatchCoordinators
	if n := len(w); n < coordinators {
		coordinators = n
	}

	// Give every coordinator its own partition so a coordinator blocked in
	// its select can still observe any channel assigned to it, rather than
	// only the head of a shared work queue. Each partition gets a trailing
	// case that unblocks the coordinator when the pool is torn down.
	partitions := make([][]reflect.SelectCase, coordinators)
	for i := range partitions {
		partitions[i] = make([]reflect.SelectCase, 0, len(w)/coordinators+2)
	}
	idx := 0
	for watchCh := range w {
		partitions[idx%coordinators] = append(partitions[idx%coordinators],
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(watchCh)})
		idx++
	}

	watcherCtx, cancel := context.WithCancel(ctx)
	ctxDoneCase := reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(watcherCtx.Done()),
	}

	triggerCh := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(coordinators)
	for i := 0; i < coordinators; i++ {
		// The context case is always the last case: any chosen index below
		// the final index is a triggered watch channel.
		cases := append(partitions[i], ctxDoneCase)
		go func() {
			defer wg.Done()
			chosen, _, _ := reflect.Select(cases)
			if chosen < len(cases)-1 {
				select {
				case triggerCh <- struct{}{}:
				default:
				}
			}
		}()
	}

	// Reap every coordinator before returning so no watcher outlives this call.
	defer func() {
		cancel()
		wg.Wait()
	}()

	// Wait for a channel to trigger or for the context to end. A channel
	// trigger has deterministic precedence: if ctx ends at the same time a
	// watch channel fires, anyTriggered makes WatchCtx return nil.
	select {
	case <-triggerCh:
		return nil
	case <-ctx.Done():
		if w.anyTriggered() {
			return nil
		}
		return ctx.Err()
	}
}

// WatchCh returns a channel that is used to wait for any channel of the watch set to trigger
// or for the context to be cancelled. WatchCh creates a new goroutine each call, so
// callers may need to cache the returned channel to avoid creating extra goroutines.
func (w WatchSet) WatchCh(ctx context.Context) <-chan error {
	// Create the outgoing channel
	triggerCh := make(chan error, 1)

	// Create a goroutine to collect the error from WatchCtx
	go func() {
		triggerCh <- w.WatchCtx(ctx)
	}()

	return triggerCh
}
