package memdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func rrSchema() *DBSchema {
	return &DBSchema{Tables: map[string]*TableSchema{
		"thing": {
			Name: "thing",
			Indexes: map[string]*IndexSchema{
				"id": {Name: "id", Unique: true, Indexer: &StringFieldIndex{Field: "ID"}},
			},
		},
	}}
}

type rrThing struct {
	ID    string
	Value string
}

func rrCommit(t *testing.T, db *MemDB, id, value string) Position {
	t.Helper()
	txn := db.Txn(true)
	if err := txn.Insert("thing", &rrThing{ID: id, Value: value}); err != nil {
		t.Fatal(err)
	}
	txn.Commit()
	return db.Position()
}

func rrWait[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
		var zero T
		return zero
	}
}

func TestRR_ChangesBeforeAndKinds(t *testing.T) {
	db, err := NewMemDB(rrSchema())
	if err != nil {
		t.Fatal(err)
	}

	rrCommit(t, db, "a", "one")
	rrCommit(t, db, "a", "two")

	txn := db.Txn(true)
	old := &rrThing{ID: "a", Value: "two"}
	if err := txn.Delete("thing", old); err != nil {
		t.Fatal(err)
	}
	txn.Commit()

	events, err := db.ChangesSince(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events", len(events))
	}
	created := events[0].Changes[0]
	if !created.Created() || created.Before != nil || created.After.(*rrThing).Value != "one" {
		t.Fatalf("bad created: %#v", created)
	}
	updated := events[1].Changes[0]
	if !updated.Updated() || updated.Before.(*rrThing).Value != "one" || updated.After.(*rrThing).Value != "two" {
		t.Fatalf("bad updated before: %#v", updated)
	}
	deleted := events[2].Changes[0]
	if !deleted.Deleted() || deleted.Before.(*rrThing).Value != "two" || deleted.After != nil {
		t.Fatalf("bad deleted before: %#v", deleted)
	}
}

func TestRR_ReplayThenLiveContiguous(t *testing.T) {
	db, err := NewMemDB(rrSchema())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		rrCommit(t, db, fmt.Sprintf("k%d", i), "v")
	}

	sub, err := db.SubscribeFrom(2, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	for want := Position(3); want <= 5; want++ {
		ev := rrWait(t, sub.C, "replay")
		if ev.Position != want {
			t.Fatalf("replay got %d want %d", ev.Position, want)
		}
	}

	rrCommit(t, db, "k5", "v")
	ev := rrWait(t, sub.C, "live")
	if ev.Position != 6 {
		t.Fatalf("live got %d want 6", ev.Position)
	}
}

func TestRR_RetentionGapReportsRange(t *testing.T) {
	db, err := NewMemDB(rrSchema(), WithRetentionWindow(3))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		rrCommit(t, db, fmt.Sprintf("k%d", i), "v")
	}
	if old := db.OldestPosition(); old != 8 {
		t.Fatalf("oldest=%d want 8", old)
	}

	_, err = db.SubscribeFrom(2, 1)
	var gap *PositionError
	if !errors.As(err, &gap) {
		t.Fatalf("expected PositionError, got %T %v", err, err)
	}
	if !errors.Is(err, ErrPositionUnavailable) {
		t.Fatalf("not ErrPositionUnavailable: %v", err)
	}
	if gap.FirstDiscarded != 3 || gap.LastDiscarded != 7 {
		t.Fatalf("bad discarded %d..%d", gap.FirstDiscarded, gap.LastDiscarded)
	}
	if gap.OldestAvailable != 8 || gap.Latest != 10 {
		t.Fatalf("bad boundaries %+v", gap)
	}
	r, ok := db.DiscardedRangeAfter(2)
	if ok || r.Start != 3 || r.End != 7 {
		t.Fatalf("bad helper range %+v ok=%v", r, ok)
	}

	// Oldest-1 is recoverable and starts at oldest.
	events, err := db.ChangesSince(7)
	if err != nil || len(events) != 3 {
		t.Fatalf("edge replay err=%v n=%d", err, len(events))
	}
}

func TestRR_SlowIsolationAndReplay(t *testing.T) {
	db, err := NewMemDB(rrSchema(), WithRetentionWindow(100))
	if err != nil {
		t.Fatal(err)
	}
	slow, err := db.SubscribeFrom(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := db.SubscribeFrom(0, 128)
	if err != nil {
		t.Fatal(err)
	}

	const n = 30
	start := time.Now()
	for i := 0; i < n; i++ {
		rrCommit(t, db, fmt.Sprintf("k%d", i), "v")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("commit blocked")
	}
	rrWait(t, slow.Done(), "slow done")
	if slow.Err() != ErrLagged {
		t.Fatalf("slow err=%v", slow.Err())
	}

	var last Position
	for i := 0; i < n; i++ {
		ev := rrWait(t, fast.C, "fast")
		last = ev.Position
	}
	if last != n {
		t.Fatalf("fast last=%d", last)
	}

	// Simulate restart from the last processed position.
	recovered, err := db.SubscribeFrom(0, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Unsubscribe()
	for i := 0; i < n; i++ {
		ev := rrWait(t, recovered.C, "recovery")
		if ev.Position != Position(i+1) {
			t.Fatalf("recovery %d want %d", ev.Position, i+1)
		}
	}
}

func TestRR_TwentySubscribersAndUnsubscribe(t *testing.T) {
	db, err := NewMemDB(rrSchema(), WithRetentionWindow(1000))
	if err != nil {
		t.Fatal(err)
	}
	const nodes = 20
	var wg sync.WaitGroup
	for i := 0; i < nodes; i++ {
		sub, err := db.SubscribeFrom(0, 256)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(sub *Subscription) {
			defer wg.Done()
			var n int
			for range sub.C {
				n++
				if n == 50 {
					sub.Unsubscribe()
					return
				}
			}
		}(sub)
	}
	for i := 0; i < 100; i++ {
		rrCommit(t, db, fmt.Sprintf("k%d", i), "v")
	}
	wg.Wait()

	db.subs.mu.Lock()
	remaining := len(db.subs.subs)
	db.subs.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("leaked %d", remaining)
	}
}

func TestRR_ThousandsReplay(t *testing.T) {
	db, err := NewMemDB(rrSchema(), WithRetentionWindow(20_000))
	if err != nil {
		t.Fatal(err)
	}
	const n = 5000
	for i := 1; i <= n; i++ {
		rrCommit(t, db, fmt.Sprintf("k%05d", i), "v")
	}
	events, err := db.ChangesSince(1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != n-1000 {
		t.Fatalf("got %d want %d", len(events), n-1000)
	}
	for i, ev := range events {
		if want := Position(1001 + i); ev.Position != want {
			t.Fatalf("at %d got %d want %d", i, ev.Position, want)
		}
	}
}

func TestRR_RollbackNoEventAndChangesCompatible(t *testing.T) {
	db, err := NewMemDB(rrSchema())
	if err != nil {
		t.Fatal(err)
	}
	txn := db.Txn(true)
	if err := txn.Insert("thing", &rrThing{ID: "x"}); err != nil {
		t.Fatal(err)
	}
	txn.Abort()
	if got := txn.Changes(); got != nil {
		t.Fatalf("aborted Changes=%v", got)
	}
	if db.Position() != 0 {
		t.Fatalf("abort advanced position %d", db.Position())
	}
	events, err := db.ChangesSince(0)
	if err != nil || len(events) != 0 {
		t.Fatalf("abort published events=%v err=%v", events, err)
	}

	// Existing opt-in API remains nil unless TrackChanges is called.
	txn = db.Txn(true)
	_ = txn.Insert("thing", &rrThing{ID: "y"})
	if got := txn.Changes(); got != nil {
		t.Fatalf("Changes should remain opt-in, got %v", got)
	}
	txn.Commit()
	if got := db.CommittedChanges(); len(got) != 1 {
		t.Fatalf("commit event should still capture changes: %+v", got)
	}

	txn = db.Txn(true)
	txn.TrackChanges()
	_ = txn.Insert("thing", &rrThing{ID: "z"})
	if got := txn.Changes(); len(got) != 1 {
		t.Fatalf("tracked Changes=%v", got)
	}
	txn.Commit()
}

func TestRR_SlowSubscriberGapDetails(t *testing.T) {
	db, err := NewMemDB(rrSchema(), WithRetentionWindow(2))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := db.SubscribeFrom(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		rrCommit(t, db, fmt.Sprintf("k%d", i), "v")
	}
	rrWait(t, sub.Done(), "lagged done")
	if !errors.Is(sub.Err(), ErrLagged) {
		t.Fatalf("err=%v", sub.Err())
	}
	gap := sub.Gap()
	if gap == nil || gap.FirstDiscarded != 1 || gap.LastDiscarded != 8 {
		t.Fatalf("bad gap details: %+v", gap)
	}
}

func TestRR_ContextCancelUnsubscribes(t *testing.T) {
	db, err := NewMemDB(rrSchema())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sub, err := db.SubscribeFrom(ctx, uint64(0), 4)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-sub.Done():
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not close subscription")
	}
}
