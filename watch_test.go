// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"bytes"
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// testWatch makes a bunch of watch channels based on the given size and fires
// the one at the given fire index to make sure it's detected (or a timeout
// occurs if the fire index isn't hit). useCtx parameterizes whether the context
// based watch is used or timer based.
func testWatch(size, fire int, useCtx bool) error {
	shouldTimeout := true
	ws := NewWatchSet()
	for i := 0; i < size; i++ {
		watchCh := make(chan struct{})
		ws.Add(watchCh)
		if fire == i {
			close(watchCh)
			shouldTimeout = false
		}
	}

	var timeoutCh chan time.Time
	var ctx context.Context
	var cancelFn context.CancelFunc
	if useCtx {
		ctx, cancelFn = context.WithCancel(context.Background())
		defer cancelFn()
	} else {
		timeoutCh = make(chan time.Time)
	}

	doneCh := make(chan bool, 1)
	go func() {
		if useCtx {
			doneCh <- ws.WatchCtx(ctx) != nil
		} else {
			doneCh <- ws.Watch(timeoutCh)
		}
	}()

	if shouldTimeout {
		select {
		case <-doneCh:
			return fmt.Errorf("should not trigger")
		default:
		}

		if useCtx {
			cancelFn()
		} else {
			close(timeoutCh)
		}
		select {
		case didTimeout := <-doneCh:
			if !didTimeout {
				return fmt.Errorf("should have timed out")
			}
		case <-time.After(10 * time.Second):
			return fmt.Errorf("should have timed out")
		}
	} else {
		select {
		case didTimeout := <-doneCh:
			if didTimeout {
				return fmt.Errorf("should not have timed out")
			}
		case <-time.After(10 * time.Second):
			return fmt.Errorf("should have triggered")
		}
		if useCtx {
			cancelFn()
		} else {
			close(timeoutCh)
		}
	}
	return nil
}

func TestWatch(t *testing.T) {
	testFactory := func(useCtx bool) func(t *testing.T) {
		return func(t *testing.T) {
			// Sweep through a bunch of chunks to hit the various cases of dividing
			// the work into watchFew calls.
			for size := 0; size < 3*aFew; size++ {
				// Fire each possible channel slot.
				for fire := 0; fire < size; fire++ {
					if err := testWatch(size, fire, useCtx); err != nil {
						t.Fatalf("err %d %d: %v", size, fire, err)
					}
				}

				// Run a timeout case as well.
				fire := -1
				if err := testWatch(size, fire, useCtx); err != nil {
					t.Fatalf("err %d %d: %v", size, fire, err)
				}
			}
		}
	}

	t.Run("Timer", testFactory(false))
	t.Run("Context", testFactory(true))
}

func testWatchCh(size, fire int) error {
	shouldTimeout := true
	ws := NewWatchSet()
	for i := 0; i < size; i++ {
		watchCh := make(chan struct{})
		ws.Add(watchCh)
		if fire == i {
			close(watchCh)
			shouldTimeout = false
		}
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	doneCh := make(chan bool, 1)
	go func() {
		err := <-ws.WatchCh(ctx)
		doneCh <- err != nil
	}()

	if shouldTimeout {
		select {
		case <-doneCh:
			return fmt.Errorf("should not trigger")
		default:
		}

		cancelFn()
		select {
		case didTimeout := <-doneCh:
			if !didTimeout {
				return fmt.Errorf("should have timed out")
			}
		case <-time.After(10 * time.Second):
			return fmt.Errorf("should have timed out")
		}
	} else {
		select {
		case didTimeout := <-doneCh:
			if didTimeout {
				return fmt.Errorf("should not have timed out")
			}
		case <-time.After(10 * time.Second):
			return fmt.Errorf("should have triggered")
		}
		cancelFn()
	}
	return nil
}

func TestWatchChan(t *testing.T) {

	// Sweep through a bunch of chunks to hit the various cases of dividing
	// the work into watchFew calls.
	for size := 0; size < 3*aFew; size++ {
		// Fire each possible channel slot.
		for fire := 0; fire < size; fire++ {
			if err := testWatchCh(size, fire); err != nil {
				t.Fatalf("err %d %d: %v", size, fire, err)
			}
		}

		// Run a timeout case as well.
		fire := -1
		if err := testWatchCh(size, fire); err != nil {
			t.Fatalf("err %d %d: %v", size, fire, err)
		}
	}
}

func TestWatch_AddWithLimit(t *testing.T) {
	// Make sure nil doesn't crash.
	{
		var ws WatchSet
		ch := make(chan struct{})
		ws.AddWithLimit(10, ch, ch)
	}

	// Run a case where we trigger a channel that should be in
	// there.
	{
		ws := NewWatchSet()
		inCh := make(chan struct{})
		altCh := make(chan struct{})
		ws.AddWithLimit(1, inCh, altCh)

		nopeCh := make(chan struct{})
		ws.AddWithLimit(1, nopeCh, altCh)

		close(inCh)
		didTimeout := ws.Watch(time.After(1 * time.Second))
		if didTimeout {
			t.Fatalf("bad")
		}
	}

	// Run a case where we trigger the alt channel that should have
	// been added.
	{
		ws := NewWatchSet()
		inCh := make(chan struct{})
		altCh := make(chan struct{})
		ws.AddWithLimit(1, inCh, altCh)

		nopeCh := make(chan struct{})
		ws.AddWithLimit(1, nopeCh, altCh)

		close(altCh)
		didTimeout := ws.Watch(time.After(1 * time.Second))
		if didTimeout {
			t.Fatalf("bad")
		}
	}

	// Run a case where we trigger the nope channel that should not have
	// been added.
	{
		ws := NewWatchSet()
		inCh := make(chan struct{})
		altCh := make(chan struct{})
		ws.AddWithLimit(1, inCh, altCh)

		nopeCh := make(chan struct{})
		ws.AddWithLimit(1, nopeCh, altCh)

		close(nopeCh)
		didTimeout := ws.Watch(time.After(1 * time.Second))
		if !didTimeout {
			t.Fatalf("bad")
		}
	}
}

func TestWatchCtxLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We add a large number of channels to a WatchSet then
	// call WatchCtx. If one of those channels fires, we
	// expect to see all the goroutines spawned by WatchCtx
	// cleaned up.
	pprof.Do(ctx, pprof.Labels("foo", "bar"), func(ctx context.Context) {
		ws := NewWatchSet()
		fireCh := make(chan struct{})
		ws.Add(fireCh)
		for i := 0; i < 10000; i++ {
			watchCh := make(chan struct{})
			ws.Add(watchCh)
		}
		result := make(chan error)
		go func() {
			result <- ws.WatchCtx(ctx)
		}()

		// Watch channels signal by being closed; a bare send on an unbuffered
		// watch channel is not observable to the watch set.
		close(fireCh)

		if err := <-result; err != nil {
			t.Fatalf("expected no err got: %v", err)
		}
	})

	numRetries := 3
	var gced bool
	for i := 0; i < numRetries; i++ {
		var pb bytes.Buffer
		profiler := pprof.Lookup("goroutine")
		if profiler == nil {
			t.Fatal("unable to find profile")
		}
		err := profiler.WriteTo(&pb, 1)
		if err != nil {
			t.Fatalf("unable to read profile: %v", err)
		}
		// If the debug profile dump contains the string "foo",
		// it means one of the goroutines spawned in pprof.Do above
		// still appears in the capture.
		if !strings.Contains(pb.String(), "foo") {
			gced = true
			break
		} else {
			t.Log("retrying")
			time.Sleep(1 * time.Second)
		}
	}
	if !gced {
		t.Errorf("goroutines were not garbage collected after %d retries", numRetries)
	}
}

// countLabeledGoroutines returns the number of live goroutines carrying the
// given pprof label value. The goroutine profile aggregates goroutines that
// share a stack into a single record whose header gives the aggregated count
// ("n @ 0x..."), so the record headers must be parsed rather than counting
// label lines. The count is retried briefly because an exiting goroutine may
// still appear in a profile taken immediately after it unblocked.
func countLabeledGoroutines(t *testing.T, labelValue string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var count int
	for {
		var pb bytes.Buffer
		profiler := pprof.Lookup("goroutine")
		if profiler == nil {
			t.Fatal("unable to find profile")
		}
		if err := profiler.WriteTo(&pb, 1); err != nil {
			t.Fatalf("unable to read profile: %v", err)
		}

		count = 0
		recordCount := 0
		for _, line := range strings.Split(pb.String(), "\n") {
			if n, err := fmt.Sscanf(line, "%d @ ", &recordCount); n == 1 && err == nil {
				continue
			}
			if recordCount > 0 && strings.Contains(line, labelValue) {
				count += recordCount
				recordCount = 0
			}
		}

		if count == 0 || time.Now().After(deadline) {
			return count
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForNoLabeledGoroutines fails the test if goroutines carrying labelValue
// are still live after the retry budget is exhausted.
func waitForNoLabeledGoroutines(t *testing.T, labelValue string) {
	t.Helper()
	if n := countLabeledGoroutines(t, labelValue); n != 0 {
		t.Fatalf("found %d leaked goroutines labeled with %q", n, labelValue)
	}
}

// TestWatchManyCoordinatorBound verifies that watching even a very large watch
// set only spawns a fixed, small number of coordinator goroutines instead of
// one goroutine per group of channels.
func TestWatchManyCoordinatorBound(t *testing.T) {
	const labelValue = "watch-bounded-pool-label"

	pprof.Do(context.Background(), pprof.Labels("watch-test", labelValue), func(parent context.Context) {
		ws := NewWatchSet()
		fireCh := make(chan struct{})
		ws.Add(fireCh)
		for i := 0; i < 10000; i++ {
			ws.Add(make(chan struct{}))
		}

		result := make(chan error, 1)
		go func() {
			result <- ws.WatchCtx(parent)
		}()

		// Give the coordinator pool time to spin up, then assert the live
		// goroutine count is bounded by the fixed pool size. The label also
		// covers this test goroutine and the single WatchCtx caller, hence the
		// two extra goroutines on top of the coordinator pool.
		deadline := time.Now().Add(3 * time.Second)
		var n int
		for time.Now().Before(deadline) {
			n = countLabeledGoroutines(t, labelValue)
			if n > maxWatchCoordinators+2 {
				t.Fatalf("expected at most %d watch goroutines, found %d",
					maxWatchCoordinators+2, n)
			}
			if n >= maxWatchCoordinators {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if n < maxWatchCoordinators {
			t.Fatalf("expected the pool to reach %d goroutines within spin-up deadline, only saw %d",
				maxWatchCoordinators, n)
		}

		// Firing one channel must release every other waiter.
		close(fireCh)
		if err := <-result; err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}
	})

	waitForNoLabeledGoroutines(t, labelValue)
}

// TestWatchManyCancelReleasesAll verifies that cancelling the context while a
// large watch set is blocking tears down every coordinator goroutine.
func TestWatchManyCancelReleasesAll(t *testing.T) {
	const labelValue = "watch-cancel-releases-label"

	pprof.Do(context.Background(), pprof.Labels("watch-test", labelValue), func(parent context.Context) {
		ctx, cancel := context.WithCancel(parent)
		ws := NewWatchSet()
		for i := 0; i < 10000; i++ {
			ws.Add(make(chan struct{}))
		}

		result := make(chan error, 1)
		go func() {
			result <- ws.WatchCtx(ctx)
		}()

		// Wait until the full pool is live.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if countLabeledGoroutines(t, labelValue) >= maxWatchCoordinators {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if n := countLabeledGoroutines(t, labelValue); n < maxWatchCoordinators {
			t.Fatalf("pool never reached %d goroutines, last count %d",
				maxWatchCoordinators, n)
		}

		cancel()
		if err := <-result; err != context.Canceled {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	})

	waitForNoLabeledGoroutines(t, labelValue)
}

// TestWatchSmallPathsNoGoroutines verifies the empty watch set and the small
// watch set paths block in the caller's goroutine and spawn no helper
// goroutines of their own, for both WatchCtx and Watch.
func TestWatchSmallPathsNoGoroutines(t *testing.T) {
	const labelValue = "watch-no-extra-goroutine-label"

	// assertNoHelperGoroutines runs block in a goroutine carrying the test
	// pprof label, waits for it to settle into its blocking call, asserts
	// that exactly one labeled goroutine exists (the blocking caller itself;
	// pprof.Do attaches the label in that same goroutine and spawns nothing),
	// meaning the watch implementation created no helper goroutines, then
	// unblocks it and waits for it to exit.
	assertNoHelperGoroutines := func(block func(), unblock func()) {
		t.Helper()
		entered := make(chan struct{})
		done := make(chan struct{})
		go func() {
			pprof.Do(context.Background(), pprof.Labels("watch-test", labelValue),
				func(_ context.Context) {
					close(entered)
					block()
				})
			close(done)
		}()
		<-entered
		// Let the blocking call settle before counting goroutines.
		time.Sleep(50 * time.Millisecond)
		if n := countLabeledGoroutines(t, labelValue); n != 1 {
			t.Fatalf("expected only the blocking caller goroutine, found %d", n)
		}
		unblock()
		<-done
	}

	small := NewWatchSet()
	for i := 0; i < aFew; i++ {
		small.Add(make(chan struct{}))
	}

	// Empty WatchSet via WatchCtx.
	{
		ws := NewWatchSet()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		assertNoHelperGoroutines(
			func() {
				if err := ws.WatchCtx(ctx); err != context.Canceled {
					t.Errorf("expected context.Canceled, got: %v", err)
				}
				close(done)
			},
			cancel,
		)
		<-done
	}

	// Small WatchSet (at the aFew threshold) via WatchCtx.
	{
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		assertNoHelperGoroutines(
			func() {
				if err := small.WatchCtx(ctx); err != context.Canceled {
					t.Errorf("expected context.Canceled, got: %v", err)
				}
				close(done)
			},
			cancel,
		)
		<-done
	}

	// Small WatchSet via Watch with an un-fired timeout channel.
	{
		timeoutCh := make(chan time.Time)
		done := make(chan struct{})
		assertNoHelperGoroutines(
			func() {
				if timedOut := small.Watch(timeoutCh); !timedOut {
					t.Error("expected timeout")
				}
				close(done)
			},
			func() { close(timeoutCh) },
		)
		<-done
	}

	// Empty WatchSet via Watch with an un-fired timeout channel.
	{
		ws := NewWatchSet()
		timeoutCh := make(chan time.Time)
		done := make(chan struct{})
		assertNoHelperGoroutines(
			func() {
				if timedOut := ws.Watch(timeoutCh); !timedOut {
					t.Error("expected timeout")
				}
				close(done)
			},
			func() { close(timeoutCh) },
		)
		<-done
	}

	waitForNoLabeledGoroutines(t, labelValue)
}

// TestWatchCtxTriggerPrecedence verifies the deterministic resolution when a
// watch channel and context cancellation are both already terminal before
// WatchCtx is called: the channel trigger must always win and WatchCtx must
// return nil, on both the small and many-channel paths.
func TestWatchCtxTriggerPrecedence(t *testing.T) {
	sizes := []int{0, 1, aFew - 1, aFew, aFew + 1, 2 * aFew, 10 * aFew}
	iterations := 25

	for _, size := range sizes {
		for i := 0; i < iterations; i++ {
			ws := NewWatchSet()
			fireCh := make(chan struct{})
			// Size zero means the fired channel is the only member.
			ws.Add(fireCh)
			for j := 0; j < size; j++ {
				ws.Add(make(chan struct{}))
			}

			ctx, cancel := context.WithCancel(context.Background())
			close(fireCh)
			cancel()

			if err := ws.WatchCtx(ctx); err != nil {
				t.Fatalf("size=%d iter=%d: expected channel trigger to win, got err: %v",
					size, i, err)
			}
		}
	}
}

// TestWatchTriggerPrecedenceTimer verifies the deterministic precedence
// through the timer-based Watch API when a watch channel and the timeout are
// both already terminal: the closed watch channel is directly observable in
// the set, so it wins and Watch reports no timeout. This cannot hold for an
// empty set, where only the timeout can unblock the call.
func TestWatchTriggerPrecedenceTimer(t *testing.T) {
	sizes := []int{1, 2, aFew - 1, aFew, aFew + 1, 2 * aFew}
	for _, size := range sizes {
		ws := NewWatchSet()
		fireCh := make(chan struct{})
		ws.Add(fireCh)
		for j := 0; j < size; j++ {
			ws.Add(make(chan struct{}))
		}

		timeoutCh := make(chan time.Time, 1)
		timeoutCh <- time.Now()
		close(fireCh)

		if timedOut := ws.Watch(timeoutCh); timedOut {
			t.Fatalf("size=%d: expected watch trigger to beat timeout", size)
		}
	}

	// An empty set with a pending timeout must still report the timeout.
	timeoutCh := make(chan time.Time, 1)
	timeoutCh <- time.Now()
	if timedOut := NewWatchSet().Watch(timeoutCh); !timedOut {
		t.Fatal("empty watch set should report the timeout")
	}
}

// TestWatchCtxConcurrentCancelAndTrigger hammers WatchCtx with context
// cancellation and channel triggers racing at the same time, asserting every
// call returns exactly one well-defined result (nil for a trigger,
// context.Canceled for cancellation) and leaves no goroutines behind.
func TestWatchCtxConcurrentCancelAndTrigger(t *testing.T) {
	const labelValue = "watch-race-label"

	pprof.Do(context.Background(), pprof.Labels("watch-test", labelValue), func(_ context.Context) {
		const iterations = 200
		for i := 0; i < iterations; i++ {
			ws := NewWatchSet()
			fireCh := make(chan struct{})
			ws.Add(fireCh)
			for j := 0; j < 5*aFew; j++ {
				ws.Add(make(chan struct{}))
			}

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				result <- ws.WatchCtx(ctx)
			}()

			// Fire both termination paths concurrently.
			go close(fireCh)
			go cancel()

			err := <-result
			if err != nil && err != context.Canceled {
				t.Fatalf("iter=%d: unexpected error: %v", i, err)
			}
			// Ensure the context is released regardless of which path won.
			cancel()
		}
	})

	waitForNoLabeledGoroutines(t, labelValue)
}

// TestWatchCtxHighConcurrencyCancellation creates and cancels a large number
// of blocking watches over large watch sets in rapid succession, the workload
// most likely to strand coordinator goroutines under the old per-group
// design. No goroutines may survive once all calls have returned.
func TestWatchCtxHighConcurrencyCancellation(t *testing.T) {
	const labelValue = "watch-high-concurrency-label"

	pprof.Do(context.Background(), pprof.Labels("watch-test", labelValue), func(_ context.Context) {
		const iterations = 200
		errs := make(chan error, iterations)

		for i := 0; i < iterations; i++ {
			ws := NewWatchSet()
			for j := 0; j < 10*aFew; j++ {
				ws.Add(make(chan struct{}))
			}
			ctx, cancel := context.WithCancel(context.Background())
			go func(cancel context.CancelFunc) {
				errs <- ws.WatchCtx(ctx)
				cancel()
			}(cancel)

			// Randomize the cancellation point relative to pool startup.
			if i&1 == 0 {
				cancel()
			} else {
				go func() {
					time.Sleep(time.Duration(i%5) * time.Millisecond)
					cancel()
				}()
			}
		}

		for i := 0; i < iterations; i++ {
			if err := <-errs; err != context.Canceled {
				t.Fatalf("iter=%d: expected context.Canceled, got: %v", i, err)
			}
		}
	})

	waitForNoLabeledGoroutines(t, labelValue)
}

// TestWatchChConcurrentCancelAndTrigger exercises the public WatchCh wrapper
// under the same racing cancellation/trigger workload and checks its returned
// channel always delivers exactly one result.
func TestWatchChConcurrentCancelAndTrigger(t *testing.T) {
	const iterations = 100
	for i := 0; i < iterations; i++ {
		ws := NewWatchSet()
		fireCh := make(chan struct{})
		ws.Add(fireCh)
		for j := 0; j < 3*aFew; j++ {
			ws.Add(make(chan struct{}))
		}

		ctx, cancel := context.WithCancel(context.Background())
		ch := ws.WatchCh(ctx)

		go close(fireCh)
		go cancel()

		err, ok := <-ch
		if !ok {
			t.Fatalf("iter=%d: result channel closed without a value", i)
		}
		if err != nil && err != context.Canceled {
			t.Fatalf("iter=%d: unexpected error: %v", i, err)
		}
		cancel()

		// The buffered channel must not deliver a second result.
		select {
		case extra, ok := <-ch:
			if ok {
				t.Fatalf("iter=%d: unexpected second result: %v", i, extra)
			}
		default:
		}
	}
}

func BenchmarkWatch(b *testing.B) {
	ws := NewWatchSet()
	for i := 0; i < 1024; i++ {
		watchCh := make(chan struct{})
		ws.Add(watchCh)
	}

	timeoutCh := make(chan time.Time)
	close(timeoutCh)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ws.Watch(timeoutCh)
	}
}

// BenchmarkWatchCtxCancelMany measures the full lifecycle of a blocking
// WatchCtx over a large watch set: bounded coordinator pool setup, the
// blocking reflect.Select, and teardown when the context is cancelled.
func BenchmarkWatchCtxCancelMany(b *testing.B) {
	ws := NewWatchSet()
	for i := 0; i < 1024; i++ {
		ws.Add(make(chan struct{}))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- ws.WatchCtx(ctx) }()
		cancel()
		if err := <-done; err != context.Canceled {
			b.Fatalf("expected context.Canceled, got: %v", err)
		}
	}
}

// BenchmarkWatchCtxTriggerMany measures the trigger path of a blocking
// WatchCtx over a large watch set.
func BenchmarkWatchCtxTriggerMany(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ws := NewWatchSet()
		fireCh := make(chan struct{})
		ws.Add(fireCh)
		for j := 0; j < 1023; j++ {
			ws.Add(make(chan struct{}))
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- ws.WatchCtx(ctx) }()
		close(fireCh)
		if err := <-done; err != nil {
			b.Fatalf("expected no err, got: %v", err)
		}
		cancel()
	}
}
