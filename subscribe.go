// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"fmt"
	"sync"
)

// ErrClosed is returned by Subscribe when the database has been closed.
var ErrClosed = fmt.Errorf("memdb: database closed")

// ErrLagged is reported through Subscriber.Err when events were dropped
// because the consumer did not keep up. The missed generations can be
// recovered by reading a fresh snapshot and re-subscribing.
var ErrLagged = fmt.Errorf("memdb: subscriber lagged, events dropped")

// CommitEvent describes one committed generation. Events are delivered to
// subscribers in commit order, one per committed write transaction.
type CommitEvent struct {
	// Generation identifies the commit. It matches the value of
	// MemDB.Generation() once the commit is published, and the data of
	// this generation is already readable when the event is received.
	Generation uint64

	// Changes is the change log entry of this commit: the de-duplicated
	// set of mutations in the order they were applied, with multiple
	// mutations to the same object collapsed to its final state. It is
	// nil if the committing transaction did not enable change tracking.
	Changes Changes
}

// Subscriber receives commit notifications. Each commit produces exactly
// one event carrying the full change log of that commit's generation.
//
// Delivery is non-blocking for the database: if the event channel is full
// because the consumer is too slow, the subscription is terminated with
// ErrLagged and the commit proceeds unaffected. A lagged subscriber must
// re-read from a fresh snapshot and re-subscribe.
type Subscriber struct {
	db *MemDB

	// C delivers commit events in commit order. It is closed when the
	// subscription is terminated (Unsubscribe, lag, or database close).
	C <-chan CommitEvent

	ch     chan CommitEvent
	closed chan struct{} // closed when C is closed and the sub is deregistered

	errMu sync.Mutex
	err   error
	once  sync.Once
}

// Err returns the reason the subscription terminated, or nil if it is
// still active. After Err reports ErrLagged the channel is closed and no
// further events arrive.
func (s *Subscriber) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// Done returns a channel that is closed once the subscription is fully
// torn down and deregistered from the database.
func (s *Subscriber) Done() <-chan struct{} {
	return s.closed
}

// Unsubscribe removes the subscription. It never blocks, is safe to call
// multiple times and is safe to call concurrently with commits and Close.
// After Unsubscribe returns, the database holds no reference to the
// subscription's channel.
func (s *Subscriber) Unsubscribe() {
	s.db.subs.finish(s, nil)
}

// Subscribe registers a subscriber that receives one CommitEvent per
// committed write transaction. The returned base generation is the
// generation the subscription starts from: the caller should read its
// initial state from a snapshot taken at or after base, and will then
// receive an event for every commit after base. Registration is atomic
// with respect to commits — a subscriber either receives the complete
// event stream after base, or observes a lag error; it never silently
// misses a commit.
//
// buf is the capacity of the delivery channel. A slow consumer only
// affects its own subscription (see ErrLagged); it can never block, delay
// or roll back a commit.
func (db *MemDB) Subscribe(buf int) (*Subscriber, uint64, error) {
	if buf < 1 {
		buf = 1
	}
	sub := &Subscriber{
		db:     db,
		ch:     make(chan CommitEvent, buf),
		closed: make(chan struct{}),
	}
	sub.C = sub.ch
	base, err := db.subs.add(sub)
	if err != nil {
		return nil, 0, err
	}
	return sub, base, nil
}

// subscriberRegistry tracks the live subscribers of a database. All
// mutations are serialized by mu; event delivery is non-blocking so a
// commit broadcasting to the registry can never be delayed by a slow
// consumer.
//
// The registry lock also orders subscription against publication: the
// committing transaction holds mu while it swaps the committed view and
// broadcasts the event, so a subscriber that registers concurrently
// either sees the pre-commit base generation (and receives the commit's
// event) or the post-commit one (and starts from the new snapshot) —
// never something in between.
type subscriberRegistry struct {
	mu     sync.Mutex
	subs   map[*Subscriber]struct{}
	closed bool
}

func newSubscriberRegistry() *subscriberRegistry {
	return &subscriberRegistry{subs: make(map[*Subscriber]struct{})}
}

// add registers sub and returns the base generation it starts from.
func (r *subscriberRegistry) add(sub *Subscriber) (uint64, error) {
	db := sub.db
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, ErrClosed
	}
	r.subs[sub] = struct{}{}
	return db.getView().generation, nil
}

// finish terminates the subscription exactly once: it deregisters the
// subscriber, records the terminal error (if any) and closes the
// subscriber's channels. It never blocks.
func (r *subscriberRegistry) finish(sub *Subscriber, err error) {
	sub.once.Do(func() {
		r.mu.Lock()
		delete(r.subs, sub)
		r.mu.Unlock()

		if err != nil {
			sub.errMu.Lock()
			sub.err = err
			sub.errMu.Unlock()
		}
		close(sub.ch)
		close(sub.closed)
	})
}

// publish swaps the committed view and broadcasts the event to every
// registered subscriber, atomically with respect to Subscribe. Called by
// the committing transaction while holding the writer lock. Delivery to
// each subscriber is non-blocking: a subscriber whose buffer is full is
// terminated with ErrLagged instead of delaying the commit.
func (r *subscriberRegistry) publish(db *MemDB, view *commitView, ev CommitEvent) {
	r.mu.Lock()
	if !r.closed {
		db.setView(view)
		var lagged []*Subscriber
		for sub := range r.subs {
			select {
			case sub.ch <- ev:
			default:
				// Slow consumer: drop the subscription rather than
				// block the commit. The consumer finds ErrLagged via
				// Err and the closed channel.
				delete(r.subs, sub)
				lagged = append(lagged, sub)
			}
		}
		r.mu.Unlock()
		for _, sub := range lagged {
			r.finish(sub, ErrLagged)
		}
		return
	}
	r.mu.Unlock()

	// The database is closed: still publish the view so the data and the
	// generation stay consistent for readers, just skip notification.
	db.setView(view)
}

// close terminates all subscribers (without an error) and rejects future
// subscriptions. Non-blocking and safe to call multiple times.
func (r *subscriberRegistry) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	subs := make([]*Subscriber, 0, len(r.subs))
	for sub := range r.subs {
		subs = append(subs, sub)
	}
	r.mu.Unlock()

	for _, sub := range subs {
		r.finish(sub, nil)
	}
}
