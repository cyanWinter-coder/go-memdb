// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// mkObj builds a TestObject that satisfies every index of testValidSchema.
func mkObj(id string) *TestObject {
	return &TestObject{ID: id, Foo: "x", Qux: []string{"q"}}
}

// commitObj is a small helper that commits a single insert of obj with
// change tracking enabled and returns the generation it published.
func commitObj(t *testing.T, db *MemDB, obj *TestObject) uint64 {
	t.Helper()
	txn := db.Txn(true)
	txn.TrackChanges()
	if err := txn.Insert("main", obj); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	txn.Commit()
	return db.Generation()
}

func TestAtomicCommitView_GenerationAndChanges(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if gen := db.Generation(); gen != 0 {
		t.Fatalf("expected generation 0, got %d", gen)
	}

	// Commit with multiple mutations to the same key: the change log must
	// collapse them to the final state, in commit order.
	txn := db.Txn(true)
	txn.TrackChanges()
	obj1 := mkObj("1")
	obj1b := mkObj("1")
	obj1b.Foo = "b"
	obj2 := mkObj("2")
	if err := txn.Insert("main", obj1); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("main", obj1b); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("main", obj2); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	if gen := db.Generation(); gen != 1 {
		t.Fatalf("expected generation 1, got %d", gen)
	}
	changes := db.CommittedChanges()
	if len(changes) != 2 {
		t.Fatalf("expected 2 collapsed changes, got %d", len(changes))
	}
	// Commit order preserved: key "1" first, then "2".
	if got := changes[0].After.(*TestObject); got.ID != "1" || got.Foo != "b" {
		t.Fatalf("bad final state for key 1: %+v", got)
	}
	if got := changes[1].After.(*TestObject); got.ID != "2" {
		t.Fatalf("bad change order: %+v", got)
	}

	// The same generation's data must be readable.
	rtxn := db.Txn(false)
	got, err := rtxn.First("main", "id", "1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got.(*TestObject).Foo != "b" {
		t.Fatalf("data not readable at committed generation: %+v", got)
	}
}

func TestAtomicCommitView_WatchSeesCommittedData(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Establish a watch on a missing key.
	txn := db.Txn(false)
	watchCh, _, err := txn.FirstWatch("main", "id", "1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// An aborted transaction must never notify.
	wtxn := db.Txn(true)
	aborted := mkObj("1")
	aborted.Foo = "aborted"
	if err := wtxn.Insert("main", aborted); err != nil {
		t.Fatalf("err: %v", err)
	}
	wtxn.Abort()
	select {
	case <-watchCh:
		t.Fatalf("watch fired for aborted transaction")
	case <-time.After(50 * time.Millisecond):
	}
	if gen := db.Generation(); gen != 0 {
		t.Fatalf("abort bumped generation to %d", gen)
	}

	// Commit: when the watch fires, the data must already be readable and
	// the generation must match the committed change log.
	obj := mkObj("1")
	obj.Foo = "committed"
	gen := commitObj(t, db, obj)

	select {
	case <-watchCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("watch did not fire after commit")
	}
	if db.Generation() != gen {
		t.Fatalf("generation moved after watch fired: %d != %d", db.Generation(), gen)
	}
	rtxn := db.Txn(false)
	got, err := rtxn.First("main", "id", "1")
	if err != nil || got == nil {
		t.Fatalf("committed data not readable when watch fired: %v", err)
	}
	if got.(*TestObject).Foo != "committed" {
		t.Fatalf("stale data visible after watch: %+v", got)
	}
	changes := db.CommittedChanges()
	if len(changes) != 1 || changes[0].After.(*TestObject) != obj {
		t.Fatalf("change log out of sync with generation: %+v", changes)
	}
}

func TestSubscribe_OrderedEvents(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	sub, base, err := db.Subscribe(16)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer sub.Unsubscribe()
	if base != 0 {
		t.Fatalf("expected base generation 0, got %d", base)
	}

	const n = 10
	for i := 0; i < n; i++ {
		commitObj(t, db, mkObj(fmt.Sprintf("%d", i)))
	}

	// Events arrive in commit order, one per generation, and each event's
	// data is already readable when received.
	for i := 0; i < n; i++ {
		select {
		case ev := <-sub.C:
			want := uint64(i + 1)
			if ev.Generation != want {
				t.Fatalf("event out of order: got generation %d, want %d", ev.Generation, want)
			}
			if len(ev.Changes) != 1 {
				t.Fatalf("expected 1 change, got %d", len(ev.Changes))
			}
			obj := ev.Changes[0].After.(*TestObject)
			rtxn := db.Txn(false)
			got, err := rtxn.First("main", "id", obj.ID)
			if err != nil || got == nil {
				t.Fatalf("data for generation %d not readable at delivery", ev.Generation)
			}
			rtxn.Abort()
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}
}

func TestSubscribe_CommitInterleaving(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Many subscribers join while commits are in flight. Every subscriber
	// must see a contiguous event stream starting exactly at base+1: no
	// missed commits, no duplicates, no events at or below base.
	const (
		commits     = 200
		subscribers = 8
	)

	var wg sync.WaitGroup
	errCh := make(chan error, subscribers*commits)

	stop := make(chan struct{})
	for s := 0; s < subscribers; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				sub, base, err := db.Subscribe(commits + 1)
				if err != nil {
					errCh <- fmt.Errorf("subscribe: %w", err)
					return
				}
				want := base + 1
			consume:
				for {
					select {
					case <-stop:
						sub.Unsubscribe()
						return
					case ev, ok := <-sub.C:
						if !ok {
							// Terminated (e.g. lag); start over.
							break consume
						}
						if ev.Generation != want {
							errCh <- fmt.Errorf("sub %d: gap/dup: got gen %d, want %d (base %d)",
								s, ev.Generation, want, base)
							sub.Unsubscribe()
							return
						}
						want++
					}
				}
				sub.Unsubscribe()
			}
		}(s)
	}

	// Interleave commits with the subscriptions.
	for i := 0; i < commits; i++ {
		commitObj(t, db, mkObj(fmt.Sprintf("obj-%d", i)))
	}
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("err: %v", err)
	}
	if gen := db.Generation(); gen != commits {
		t.Fatalf("expected generation %d, got %d", commits, gen)
	}
}

func TestSubscribe_SlowConsumerDoesNotBlockCommits(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// A subscriber that never drains its channel.
	slow, _, err := db.Subscribe(1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer slow.Unsubscribe()

	// A healthy subscriber that keeps up.
	fast, _, err := db.Subscribe(64)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer fast.Unsubscribe()

	const n = 20
	start := time.Now()
	for i := 0; i < n; i++ {
		commitObj(t, db, mkObj(fmt.Sprintf("%d", i)))
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("commits blocked on slow consumer: %v", elapsed)
	}

	// The slow subscription is terminated with ErrLagged; the commit log
	// and generation are unaffected.
	select {
	case <-slow.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("slow subscription never terminated")
	}
	if slow.Err() != ErrLagged {
		t.Fatalf("expected ErrLagged, got %v", slow.Err())
	}
	if gen := db.Generation(); gen != n {
		t.Fatalf("slow consumer rolled back commits: generation %d", gen)
	}

	// The healthy subscriber received every event in order.
	for i := 0; i < n; i++ {
		select {
		case ev := <-fast.C:
			if ev.Generation != uint64(i+1) {
				t.Fatalf("fast subscriber saw gap: %d", ev.Generation)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("fast subscriber missing event %d", i)
		}
	}
}

func TestSubscribe_ConcurrentCloseAndUnsubscribe(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	const subs = 32
	var wg sync.WaitGroup

	// Hammer the registry: subscribers joining, leaving and committing
	// while the database is closed from underneath them.
	for i := 0; i < subs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sub, _, err := db.Subscribe(4)
			if err != nil {
				return // closed already
			}
			// Drain until the channel closes.
			var drain sync.WaitGroup
			drain.Add(1)
			go func() {
				defer drain.Done()
				for range sub.C {
				}
			}()
			if i%2 == 0 {
				sub.Unsubscribe()
			}
			// Odd subscribers rely on Close to terminate them.
			drain.Wait()
			sub.Unsubscribe() // idempotent
			<-sub.Done()
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			txn := db.Txn(true)
			txn.TrackChanges()
			if err := txn.Insert("main", mkObj(fmt.Sprintf("%d", i))); err != nil {
				t.Errorf("err: %v", err)
				return
			}
			txn.Commit()
		}
	}()

	// Close partway through; must not block or panic.
	time.Sleep(time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	wg.Wait()

	// No subscriber channels may remain registered.
	db.subs.mu.Lock()
	remaining := len(db.subs.subs)
	db.subs.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d subscriber channels leaked after close", remaining)
	}

	// Subscribe after close fails.
	if _, _, err := db.Subscribe(1); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}

	// Commits still work after close; the generation keeps advancing and
	// readers stay consistent — only notification is gone.
	gen := db.Generation()
	commitObj(t, db, mkObj("post-close"))
	if db.Generation() != gen+1 {
		t.Fatalf("commit after close did not advance generation")
	}
	rtxn := db.Txn(false)
	got, err := rtxn.First("main", "id", "post-close")
	if err != nil || got == nil {
		t.Fatalf("data not readable after close: %v", err)
	}
	rtxn.Abort()
}

func TestSubscribe_SnapshotBaseGeneration(t *testing.T) {
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	commitObj(t, db, mkObj("before"))

	// Subscribe, then apply events onto a snapshot taken at the base
	// generation; the result must match the live database.
	sub, base, err := db.Subscribe(8)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer sub.Unsubscribe()
	if base != 1 {
		t.Fatalf("expected base 1, got %d", base)
	}

	commitObj(t, db, mkObj("after"))

	select {
	case ev := <-sub.C:
		if ev.Generation != base+1 {
			t.Fatalf("first event is not base+1: %d (base %d)", ev.Generation, base)
		}
		if len(ev.Changes) != 1 || ev.Changes[0].After.(*TestObject).ID != "after" {
			t.Fatalf("unexpected event payload: %+v", ev.Changes)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no event received")
	}
}
