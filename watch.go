// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"context"
	"reflect"
	"time"
)

// WatchSet is a collection of watch channels. The zero value is not usable.
// Use NewWatchSet to create a WatchSet.
type WatchSet map[<-chan struct{}]struct{}

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
func (w WatchSet) Watch(timeoutCh <-chan time.Time) bool {
	if w == nil {
		return false
	}

	// Create a context that gets cancelled when the timeout is triggered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Avoid spawning a goroutine at all if there is no timeout channel.
	if timeoutCh != nil {
		go func() {
			select {
			case <-timeoutCh:
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	return w.WatchCtx(ctx) == context.Canceled
}

// WatchCtx blocks until one of the channels in the watch set is closed, or
// ctx is done (cancelled or exceeds the deadline). WatchCtx returns an error
// if the ctx causes it to unblock, otherwise returns nil.
//
// A nil ctx is treated as context.Background(), so WatchCtx blocks until one
// of the channels triggers.
//
// If the context is done at the same time as one of the channels triggers,
// the trigger is preferred and nil is returned, so a change that has already
// happened is never reported as a cancellation.
//
// WatchCtx should be preferred over Watch.
func (w WatchSet) WatchCtx(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var err error
	if n := len(w); n <= aFew {
		idx := 0
		chunk := make([]<-chan struct{}, aFew)
		for watchCh := range w {
			chunk[idx] = watchCh
			idx++
		}
		err = watchFew(ctx, chunk)
	} else {
		err = w.watchMany(ctx)
	}
	if err == nil {
		return nil
	}

	// The context won the race, but a watch channel may have triggered at
	// the same time. Prefer reporting the trigger so that a change that has
	// already happened is not lost.
	for watchCh := range w {
		select {
		case <-watchCh:
			return nil
		default:
		}
	}
	return err
}

// watchMany is used if there are many watchers. It performs a single
// reflect.Select over all the channels in the calling goroutine instead of
// spawning a goroutine per chunk of channels, so the number of goroutines a
// wait holds is bounded (zero extra) no matter how large the watch set is.
// All references to the channels and the context are released on return.
func (w WatchSet) watchMany(ctx context.Context) error {
	cases := make([]reflect.SelectCase, 0, len(w)+1)
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	for watchCh := range w {
		// A nil channel is never ready, matching select semantics.
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(watchCh),
		})
	}

	chosen, _, _ := reflect.Select(cases)
	if chosen == 0 {
		return ctx.Err()
	}
	return nil
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
