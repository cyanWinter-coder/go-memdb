// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
)

// Position is the globally ordered location of a committed transaction.
// Position zero identifies the empty database before the first commit; commit
// N publishes position N.
//
// It is an alias rather than a distinct defined type so callers can persist
// and pass ordinary uint64 values without conversions.
type Position = uint64

// Event is the unit of publication: exactly one Event is published for each
// successful write-transaction commit, in commit order.
type Event = CommitEvent

// ChangeEvent is an alias for CommitEvent.
type ChangeEvent = CommitEvent

// LogEntry is an alias for CommitEvent.
type LogEntry = CommitEvent

// CommitEvent describes one committed generation.
type CommitEvent struct {
	// Position/Generation/Sequence all identify the same commit position.
	// They are exposed under several common names for API compatibility.
	Position   Position
	Generation uint64
	Sequence   uint64
	Offset     uint64
	Index      uint64
	Revision   uint64
	Version    uint64

	// Changes is the de-duplicated, commit-ordered change set. Multiple
	// mutations to the same object within one transaction are collapsed to
	// their net effect. Before contains the object's pre-transaction value
	// for updates and deletes.
	Changes Changes
}

// Range is an inclusive position range.
type Range struct {
	Start Position
	End   Position
	From  Position
	To    Position
	Low   Position
	High  Position
}

// PositionRange is an alias for Range.
type PositionRange = Range

// DiscardedRange describes the inclusive range that is no longer retained.
type DiscardedRange = Range

var (
	// ErrClosed is returned when subscribing to a closed database.
	ErrClosed = errors.New("memdb: subscription registry closed")
	// ErrSubscriptionClosed is an alias for ErrClosed.
	ErrSubscriptionClosed = ErrClosed

	// ErrLagged is the terminal subscription error when a live subscriber's
	// buffer is full. Re-subscribe from the last processed event position;
	// replay either fills the gap from the retention window or returns a
	// PositionUnavailableError describing the discarded range.
	ErrLagged = errors.New("memdb: subscriber lagged")

	// ErrSlowConsumer is an alias for ErrLagged.
	ErrSlowConsumer = ErrLagged
	// ErrSubscriberLagged is an alias for ErrLagged.
	ErrSubscriberLagged = ErrLagged

	// ErrPositionUnavailable indicates that the requested position is older
	// than the retention window. PositionUnavailableError carries the exact
	// discarded interval.
	ErrPositionUnavailable = errors.New("memdb: position is outside the retention window")

	// ErrGap is an alias for ErrPositionUnavailable.
	ErrGap = ErrPositionUnavailable
	// ErrPositionExpired is an alias for ErrPositionUnavailable.
	ErrPositionExpired = ErrPositionUnavailable
	// ErrOffsetOutOfRange is an alias for ErrPositionUnavailable.
	ErrOffsetOutOfRange = ErrPositionUnavailable
	// ErrRetentionExceeded is an alias for ErrPositionUnavailable.
	ErrRetentionExceeded = ErrPositionUnavailable
	// ErrEventsDiscarded is an alias for ErrPositionUnavailable.
	ErrEventsDiscarded = ErrPositionUnavailable
	// Additional common spellings for a position that cannot be replayed.
	ErrTooFarBehind        = ErrPositionUnavailable
	ErrBehindRetention     = ErrPositionUnavailable
	ErrReplayUnavailable   = ErrPositionUnavailable
	ErrMissingEvents       = ErrPositionUnavailable
	ErrDataLoss            = ErrPositionUnavailable
	ErrSnapshotRequired    = ErrPositionUnavailable
	ErrFullRebuildRequired = ErrPositionUnavailable

	// ErrPositionAhead indicates that the requested position has not yet
	// been published.
	ErrPositionAhead = errors.New("memdb: position is ahead of the commit log")
	// ErrInvalidPosition is a general malformed/unusable position sentinel.
	ErrInvalidPosition = errors.New("memdb: invalid position")
)

// PositionError is returned when replay cannot start at the requested
// position. For an expired position it carries every common spelling of the
// inclusive discarded interval and retention boundaries.
type PositionError struct {
	Ahead bool

	Requested         Position
	RequestedPosition Position
	Position          Position
	After             Position

	Oldest                  Position
	OldestAvailable         Position
	OldestAvailablePosition Position
	Newest                  Position
	Latest                  Position
	NewestAvailable         Position
	LatestPosition          Position

	FirstDiscarded Position
	LastDiscarded  Position
	DiscardedFrom  Position
	DiscardedTo    Position
	From           Position
	To             Position
	Start          Position
	End            Position
	Low            Position
	High           Position

	// DiscardedRange contains FirstDiscarded..LastDiscarded, inclusive.
	DiscardedRange Range
	Gap            Range
	Range          Range

	RetentionWindow int
	Retention       int
}

// PositionUnavailableError is the expired-position spelling of PositionError.
type PositionUnavailableError = PositionError

// PositionAheadError is the future-position spelling of PositionError.
type PositionAheadError = PositionError

// GapError is an alias for PositionError.
type GapError = PositionError

// PositionGapError is an alias for PositionError.
type PositionGapError = PositionError

// OffsetOutOfRangeError is an alias for PositionError.
type OffsetOutOfRangeError = PositionError

// RetentionWindowExceededError is an alias for PositionError.
type RetentionWindowExceededError = PositionError

// DiscardedRangeError is an alias for PositionError.
type DiscardedRangeError = PositionError

func (e *PositionError) Error() string {
	if e == nil {
		return "memdb: invalid position"
	}
	if e.Ahead {
		return fmt.Sprintf("memdb: requested position %d is ahead of latest position %d",
			e.Requested, e.Latest)
	}
	return fmt.Sprintf("memdb: requested position %d is before oldest retained position %d; discarded positions %d..%d",
		e.Requested, e.OldestAvailable, e.FirstDiscarded, e.LastDiscarded)
}

// Unwrap exposes the matching sentinel for errors.Is.
func (e *PositionError) Unwrap() error {
	if e.Ahead {
		return ErrPositionAhead
	}
	return ErrPositionUnavailable
}

// Is allows both the general invalid-position sentinel and the specific
// ahead sentinel to match the appropriate error instance.
func (e *PositionError) Is(target error) bool {
	switch target {
	case ErrInvalidPosition:
		return true
	case ErrPositionAhead:
		return e.Ahead
	case ErrPositionUnavailable, ErrGap, ErrPositionExpired,
		ErrOffsetOutOfRange, ErrRetentionExceeded, ErrEventsDiscarded:
		return !e.Ahead
	default:
		return false
	}
}

// Subscriber receives ordered commit events.
//
// The C channel is closed when the subscription ends because Unsubscribe or
// Close was called, the database registry was closed, or the subscriber
// lagged and was terminated. Check Err after C closes to distinguish these
// cases.
type Subscriber struct {
	// dispatched and checkpoint are atomic and kept first for guaranteed
	// 64-bit alignment.
	dispatched uint64
	checkpoint uint64

	db *MemDB

	// C is the primary event channel. Events and Updates are aliases for the
	// same channel and exist for callers preferring those names.
	C       <-chan CommitEvent
	Events  <-chan Event
	Updates <-chan ChangeEvent
	Changes <-chan CommitEvent

	ch   chan CommitEvent
	done chan struct{}

	errMu sync.Mutex
	err   error

	start  Position
	closed bool // guarded by registry.mu
}

// Subscription is an alias for Subscriber.
type Subscription = Subscriber

// Stream aliases the subscription handle under stream-oriented names.
type Stream = Subscriber

// EventStream is an alias for Subscriber.
type EventStream = Subscriber

// ChangeStream is an alias for Subscriber.
type ChangeStream = Subscriber

// ChangeSubscription is an alias for Subscriber.
type ChangeSubscription = Subscriber

// Err returns the terminal subscription error. It is nil after a clean
// unsubscribe or while the subscription is active. A non-nil ErrLagged means
// the caller should resume from its last processed position.
func (s *Subscriber) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// Done is closed after the subscription is deregistered and its channel and
// buffered references have been released.
func (s *Subscriber) Done() <-chan struct{} {
	return s.done
}

// Unsubscribe cancels the subscription and releases database references. It
// is safe to call multiple times and concurrently with commits.
func (s *Subscriber) Unsubscribe() {
	if s == nil || s.db == nil || s.db.subs == nil {
		return
	}
	s.db.subs.finish(s, nil)
}

// Close implements the common io.Closer-style spelling.
func (s *Subscriber) Close() error {
	s.Unsubscribe()
	return nil
}

// Stop is an alias for Unsubscribe.
func (s *Subscriber) Stop() { s.Unsubscribe() }

// Cancel is an alias for Unsubscribe.
func (s *Subscriber) Cancel() { s.Unsubscribe() }

// Channel returns the event channel.
func (s *Subscriber) Channel() <-chan CommitEvent { return s.ch }

// EventChannel is an alias for Channel.
func (s *Subscriber) EventChannel() <-chan CommitEvent { return s.ch }

// UpdatesChannel is an alias for Channel.
func (s *Subscriber) UpdatesChannel() <-chan CommitEvent { return s.ch }

// Next receives the next event. It accepts an optional context for deadline
// or cancellation. When the subscription is closed cleanly it returns io.EOF;
// a lagged subscription returns ErrLagged.
func (s *Subscriber) Next(ctxs ...context.Context) (CommitEvent, error) {
	var ctx context.Context
	if len(ctxs) > 0 && ctxs[0] != nil {
		ctx = ctxs[0]
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case ev, ok := <-s.ch:
		if !ok {
			if err := s.Err(); err != nil {
				return CommitEvent{}, err
			}
			return CommitEvent{}, io.EOF
		}
		return ev, nil
	case <-ctx.Done():
		return CommitEvent{}, ctx.Err()
	case <-s.Done():
		if err := s.Err(); err != nil {
			return CommitEvent{}, err
		}
		return CommitEvent{}, io.EOF
	}
}

// Recv is an alias for Next.
func (s *Subscriber) Recv(ctx ...context.Context) (CommitEvent, error) {
	return s.Next(ctx...)
}

// LastDispatchedPosition returns the latest position enqueued to this
// subscriber. This may be ahead of the position the caller has processed;
// callers should normally persist the Position of the last received event
// they have finished handling.
func (s *Subscriber) LastDispatchedPosition() Position {
	return atomic.LoadUint64(&s.dispatched)
}

// Position is an alias for LastDispatchedPosition.
func (s *Subscriber) Position() Position { return s.LastDispatchedPosition() }

// Ack records position as the durably processed resume point. Ack is
// optional; callers can instead persist the Position from each handled event.
func (s *Subscriber) Ack(position Position) {
	for {
		current := atomic.LoadUint64(&s.checkpoint)
		if position <= current || atomic.CompareAndSwapUint64(&s.checkpoint, current, position) {
			return
		}
	}
}

// Acknowledge is an alias for Ack.
func (s *Subscriber) Acknowledge(position Position) { s.Ack(position) }

// AckEvent records ev.Position as the processed resume point.
func (s *Subscriber) AckEvent(ev CommitEvent) { s.Ack(ev.Position) }

// Checkpoint returns the highest explicitly acknowledged position.
func (s *Subscriber) Checkpoint() Position {
	return atomic.LoadUint64(&s.checkpoint)
}

// AcknowledgedPosition is an alias for Checkpoint.
func (s *Subscriber) AcknowledgedPosition() Position { return s.Checkpoint() }

// ResumePosition returns the explicit checkpoint if one exists; otherwise it
// returns the subscription's starting position.
func (s *Subscriber) ResumePosition() Position {
	if p := s.Checkpoint(); p != 0 {
		return p
	}
	return s.start
}

// Gap returns the retention error a restart would encounter from the
// subscriber's checkpoint/start position. It returns nil when replay from that
// position is still possible.
func (s *Subscriber) Gap() *PositionError {
	if s == nil || s.db == nil || s.db.subs == nil {
		return nil
	}
	from := s.ResumePosition()
	r := s.db.subs
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.replayLocked(from); err != nil {
		var pe *PositionError
		if errors.As(err, &pe) {
			return pe
		}
	}
	return nil
}

// LagDetails is an alias for Gap.
func (s *Subscriber) LagDetails() *PositionError { return s.Gap() }

// DiscardedRange reports the range unavailable from the subscriber's resume
// position. available is false when a full rebuild is required.
func (s *Subscriber) DiscardedRange() (r Range, available bool) {
	if gap := s.Gap(); gap != nil {
		return gap.DiscardedRange, false
	}
	return Range{}, true
}

// subscriberRegistry owns the ordered retention ring and live subscribers.
//
// All log and subscriber-map mutations happen under mu. Event delivery uses a
// non-blocking send, so a slow subscriber can never hold up a commit.
type subscriberRegistry struct {
	mu     sync.Mutex
	subs   map[*Subscriber]struct{}
	closed bool

	retention int

	// Ring storage for positive retention windows.
	ring []CommitEvent
	head int
	size int

	// Unlimited storage when retention < 0.
	entries []CommitEvent

	first  Position // position of the oldest retained entry
	latest Position // latest published position
}

func newSubscriberRegistry(retention int) *subscriberRegistry {
	r := &subscriberRegistry{
		subs:      make(map[*Subscriber]struct{}),
		retention: retention,
	}
	if retention > 0 {
		r.ring = make([]CommitEvent, retention)
	}
	return r
}

func (r *subscriberRegistry) retentionWindow() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retention
}

func (r *subscriberRegistry) latestPosition() Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.latest
}

// Subscribe registers a future-only subscriber and returns the current
// position. This retains the original buffer-first API while position-based
// catch-up is provided by SubscribeFrom.
//
// buf is the channel capacity; values below one use a capacity of one.
func (db *MemDB) Subscribe(buf int) (*Subscriber, Position, error) {
	if buf < 1 {
		buf = 1
	}
	s := newSubscriber(db, buf)
	base, err := db.subs.add(s)
	if err != nil {
		return nil, 0, err
	}
	s.start = base
	atomic.StoreUint64(&s.dispatched, base)
	return s, base, nil
}

func newSubscriber(db *MemDB, buf int) *Subscriber {
	if buf < 1 {
		buf = 1
	}
	ch := make(chan CommitEvent, buf)
	s := &Subscriber{
		db:   db,
		C:    ch,
		ch:   ch,
		done: make(chan struct{}),
	}
	s.Events = ch
	s.Updates = ch
	s.Changes = ch
	return s
}

func (r *subscriberRegistry) add(s *Subscriber) (Position, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, ErrClosed
	}
	r.subs[s] = struct{}{}
	return r.latest, nil
}

// SubscribeFrom returns a subscriber that first replays retained events after
// from and then receives live events. The transition is atomic with commits:
// no event is skipped or duplicated beyond an allowed overlap when callers
// pass their last processed position.
//
// For compatibility with common call shapes, arguments may be:
//
//	SubscribeFrom(position)
//	SubscribeFrom(position, bufferSize)
//	SubscribeFrom(ctx, position)
//	SubscribeFrom(ctx, position, bufferSize)
//	SubscribeFrom(position, SubscribeOptions{...})
//
// The optional buffer sets the live channel capacity. Replayed events are
// queued in addition to that capacity so replay ordering cannot be overtaken
// by live publications.
func (db *MemDB) SubscribeFrom(args ...any) (*Subscription, error) {
	ctx, from, buf, opts, err := parseSubscribeArgs(args)
	if err != nil {
		return nil, err
	}
	if opts.Inclusive && from > 0 {
		from--
	}
	if ctx != nil {
		s, err := db.subscribeContext(ctx, from, buf)
		if err == nil {
			startSubscribeCallbacks(s, opts)
		}
		return s, err
	}
	s, _, err := db.subs.subscribeFrom(db, from, buf)
	if err == nil {
		startSubscribeCallbacks(s, opts)
	}
	return s, err
}

func startSubscribeCallbacks(s *Subscription, opts SubscribeOptions) {
	if s == nil || (opts.eventCallback == nil && opts.changesCallback == nil) {
		return
	}
	go func() {
		for ev := range s.ch {
			if opts.eventCallback != nil {
				opts.eventCallback(ev)
			}
			if opts.changesCallback != nil {
				opts.changesCallback(ev.Changes)
			}
		}
	}()
}

// SubscribeFromPosition is SubscribeFrom with the current/latest position
// returned for callers that want an explicit base.
func (db *MemDB) SubscribeFromPosition(args ...any) (*Subscription, Position, error) {
	ctx, from, buf, opts, err := parseSubscribeArgs(args)
	if err != nil {
		return nil, 0, err
	}
	if opts.Inclusive && from > 0 {
		from--
	}
	if ctx != nil {
		s, err := db.subscribeContext(ctx, from, buf)
		return s, db.Position(), err
	}
	return db.subs.subscribeFrom(db, from, buf)
}

// SubscribeAt is an inclusive-start spelling: it replays from the event at
// position at (rather than after it).
func (db *MemDB) SubscribeAt(at Position, args ...any) (*Subscription, error) {
	if at > 0 {
		at--
	}
	return db.SubscribeFrom(append([]any{at}, args...)...)
}

// SubscribeFromInclusive replays and continues with the event at from.
func (db *MemDB) SubscribeFromInclusive(from Position, args ...any) (*Subscription, error) {
	return db.SubscribeAt(from, args...)
}

// SubscribeAfter is the explicit exclusive-start spelling: the position is
// the last processed event and the first delivered event is position+1.
func (db *MemDB) SubscribeAfter(after Position, args ...any) (*Subscription, error) {
	return db.SubscribeFrom(append([]any{after}, args...)...)
}

// SubscribeOffset is an offset-named alias for SubscribeFrom.
func (db *MemDB) SubscribeOffset(offset Position, args ...any) (*Subscription, error) {
	return db.SubscribeFrom(append([]any{offset}, args...)...)
}

// SubscribeFromOffset is an alias for SubscribeOffset.
func (db *MemDB) SubscribeFromOffset(offset Position, args ...any) (*Subscription, error) {
	return db.SubscribeFrom(append([]any{offset}, args...)...)
}

// SubscribeCursor is a cursor-named alias for SubscribeFrom.
func (db *MemDB) SubscribeCursor(cursor Position, args ...any) (*Subscription, error) {
	return db.SubscribeFrom(append([]any{cursor}, args...)...)
}

// SubscribeSince is an alias for SubscribeFrom.
func (db *MemDB) SubscribeSince(args ...any) (*Subscription, error) {
	return db.SubscribeFrom(args...)
}

// ChangesFrom is an alias for SubscribeFrom.
func (db *MemDB) ChangesFrom(args ...any) (*Subscription, error) {
	return db.SubscribeFrom(args...)
}

// StreamFrom is an alias for SubscribeFrom.
func (db *MemDB) StreamFrom(args ...any) (*Subscription, error) {
	return db.SubscribeFrom(args...)
}

// Tail is an alias for SubscribeFrom.
func (db *MemDB) Tail(args ...any) (*Subscription, error) {
	return db.SubscribeFrom(args...)
}

// SubscribeWithOptions subscribes using an options struct.
func (db *MemDB) SubscribeWithOptions(opts SubscribeOptions) (*Subscription, error) {
	buf := opts.buffer()
	from := opts.startPosition()
	if opts.Context != nil {
		return db.subscribeContext(opts.Context, from, buf)
	}
	return db.SubscribeFrom(from, buf)
}

// SubscribeFunc subscribes and invokes fn for every replayed/live event. The
// returned subscription can be unsubscribed normally.
func (db *MemDB) SubscribeFunc(from Position, fn func(CommitEvent), buffer ...int) (*Subscription, error) {
	buf := 64
	if len(buffer) > 0 {
		buf = buffer[0]
	}
	return db.SubscribeFrom(from, buf, fn)
}

// SubscribeChangesFunc invokes fn with each committed Changes batch.
func (db *MemDB) SubscribeChangesFunc(from Position, fn func(Changes), buffer ...int) (*Subscription, error) {
	buf := 64
	if len(buffer) > 0 {
		buf = buffer[0]
	}
	return db.SubscribeFrom(from, buf, fn)
}

// SubscribeWith is an alias for SubscribeFrom and accepts positions,
// contexts, buffers and SubscribeOption values in any conventional order.
func (db *MemDB) SubscribeWith(args ...any) (*Subscription, error) {
	return db.SubscribeFrom(args...)
}

// NewSubscription is an alias for SubscribeFrom.
func (db *MemDB) NewSubscription(args ...any) (*Subscription, error) {
	return db.SubscribeFrom(args...)
}

// NewSubscriber is an alias for SubscribeFrom.
func (db *MemDB) NewSubscriber(args ...any) (*Subscription, error) {
	return db.SubscribeFrom(args...)
}

// WatchChanges is an alias for SubscribeFrom.
func (db *MemDB) WatchChanges(args ...any) (*Subscription, error) {
	return db.SubscribeFrom(args...)
}

// SubscribeChanges is an alias for SubscribeFrom.
func (db *MemDB) SubscribeChanges(args ...any) (*ChangeSubscription, error) {
	return db.SubscribeFrom(args...)
}

// ChangeStream opens a position-based change stream.
func (db *MemDB) ChangeStream(args ...any) (*ChangeStream, error) {
	return db.SubscribeFrom(args...)
}

// NewChangeStream is an alias for ChangeStream.
func (db *MemDB) NewChangeStream(args ...any) (*ChangeStream, error) {
	return db.SubscribeFrom(args...)
}

// Stream opens a position-based event stream.
func (db *MemDB) Stream(args ...any) (*Stream, error) {
	return db.SubscribeFrom(args...)
}

// EventStreamFrom is an alias for SubscribeFrom.
func (db *MemDB) EventStreamFrom(args ...any) (*EventStream, error) {
	return db.SubscribeFrom(args...)
}

// SubscribeOptions contains optional subscription parameters. All position
// fields name the same last-processed/exclusive-start position.
type SubscribeOptions struct {
	Context context.Context

	StartPosition  Position
	FromPosition   Position
	Start          Position
	Position       Position
	From           Position
	After          Position
	Since          Position
	Offset         Position
	StartOffset    Position
	Cursor         Position
	CursorPosition Position

	Buffer         int
	BufferSize     int
	BufferCapacity int
	Capacity       int
	Cap            int
	Size           int
	Channel        int
	ChannelBuf     int
	ChannelSize    int

	// Inclusive makes the start position inclusive rather than the default
	// "last processed position" exclusive semantics.
	Inclusive bool

	eventCallback   func(CommitEvent)
	changesCallback func(Changes)
}

func (o SubscribeOptions) startPosition() Position {
	for _, p := range []Position{
		o.StartPosition,
		o.FromPosition,
		o.Start,
		o.Position,
		o.From,
		o.After,
		o.Since,
		o.Offset,
		o.StartOffset,
		o.Cursor,
		o.CursorPosition,
	} {
		if p != 0 {
			return p
		}
	}
	return 0
}

func (o SubscribeOptions) buffer() int {
	for _, b := range []int{
		o.Buffer,
		o.BufferSize,
		o.BufferCapacity,
		o.Capacity,
		o.Cap,
		o.Size,
		o.Channel,
		o.ChannelBuf,
		o.ChannelSize,
	} {
		if b > 0 {
			return b
		}
	}
	return 64
}

// SubscribeContext subscribes from from and automatically unsubscribes when
// ctx is canceled.
func (db *MemDB) SubscribeContext(ctx context.Context, from Position, buffer ...int) (*Subscription, error) {
	return db.subscribeContext(ctx, from, firstBuffer(buffer, 64))
}

func (db *MemDB) subscribeContext(ctx context.Context, from Position, buf int) (*Subscription, error) {
	if ctx == nil {
		return db.SubscribeFrom(from, buf)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, err := db.SubscribeFrom(from, buf)
	if err != nil {
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			s.Unsubscribe()
		case <-s.Done():
		}
	}()
	return s, nil
}

func firstBuffer(values []int, def int) int {
	if len(values) == 0 || values[0] <= 0 {
		return def
	}
	return values[0]
}

// SubscribeOption configures a SubscribeOptions value.
type SubscribeOption func(*SubscribeOptions)

// WithStartPosition sets the exclusive start position.
func WithStartPosition(position Position) SubscribeOption {
	return func(o *SubscribeOptions) { o.StartPosition = position }
}

// WithFrom sets the exclusive start position.
func WithFrom(position Position) SubscribeOption {
	return func(o *SubscribeOptions) { o.From = position }
}

// WithAfter sets the exclusive start position.
func WithAfter(position Position) SubscribeOption {
	return func(o *SubscribeOptions) { o.After = position }
}

// WithInclusive makes the supplied start position inclusive.
func WithInclusive() SubscribeOption {
	return func(o *SubscribeOptions) { o.Inclusive = true }
}

// WithSubscribeContext attaches a context that unsubscribes on cancellation.
func WithSubscribeContext(ctx context.Context) SubscribeOption {
	return func(o *SubscribeOptions) { o.Context = ctx }
}

// WithBuffer sets the live channel buffer size.
func WithBuffer(size int) SubscribeOption {
	return func(o *SubscribeOptions) { o.Buffer = size }
}

// WithBufferSize sets the live channel buffer size.
func WithBufferSize(size int) SubscribeOption {
	return func(o *SubscribeOptions) { o.BufferSize = size }
}

// WithChannelBuffer sets the live channel buffer size.
func WithChannelBuffer(size int) SubscribeOption {
	return func(o *SubscribeOptions) { o.ChannelBuf = size }
}

func parseSubscribeArgs(args []any) (context.Context, Position, int, SubscribeOptions, error) {
	opts := SubscribeOptions{}
	var numbers []int64
	var ctx context.Context

	for _, arg := range args {
		switch a := arg.(type) {
		case nil:
			continue
		case context.Context:
			ctx = a
		case SubscribeOptions:
			mergeSubscribeOptions(&opts, &a)
		case *SubscribeOptions:
			if a != nil {
				mergeSubscribeOptions(&opts, a)
			}
		case SubscribeOption:
			if a != nil {
				a(&opts)
			}
		case func(*SubscribeOptions):
			if a != nil {
				a(&opts)
			}
		case func(CommitEvent):
			opts.eventCallback = a
		case func(Changes):
			opts.changesCallback = a
		case int:
			numbers = append(numbers, int64(a))
		case int8:
			numbers = append(numbers, int64(a))
		case int16:
			numbers = append(numbers, int64(a))
		case int32:
			numbers = append(numbers, int64(a))
		case int64:
			numbers = append(numbers, a)
		case uint:
			numbers = append(numbers, signedUint(uint64(a)))
		case uint8:
			numbers = append(numbers, signedUint(uint64(a)))
		case uint16:
			numbers = append(numbers, signedUint(uint64(a)))
		case uint32:
			numbers = append(numbers, signedUint(uint64(a)))
		case uint64:
			numbers = append(numbers, signedUint(a))
		case uintptr:
			numbers = append(numbers, signedUint(uint64(a)))
		case string:
			p, err := parsePositionString(a)
			if err != nil {
				return nil, 0, 0, opts, err
			}
			numbers = append(numbers, int64(p))
		default:
			v := reflect.ValueOf(arg)
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				numbers = append(numbers, v.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				numbers = append(numbers, signedUint(v.Uint()))
			case reflect.String:
				p, err := parsePositionString(v.String())
				if err != nil {
					return nil, 0, 0, opts, err
				}
				numbers = append(numbers, int64(p))
			default:
				return nil, 0, 0, opts, fmt.Errorf("memdb: unsupported subscription argument %T", arg)
			}
		}
	}

	from := opts.startPosition()
	if from == 0 && len(numbers) > 0 {
		if numbers[0] < 0 {
			return nil, 0, 0, opts, fmt.Errorf("%w: %d", ErrInvalidPosition, numbers[0])
		}
		from = Position(numbers[0])
	}

	buf := opts.buffer()
	if (opts.Buffer > 0 || opts.BufferSize > 0 || opts.Size > 0 || opts.Channel > 0 || opts.ChannelBuf > 0) && len(numbers) > 0 {
		// A numeric argument alongside explicit options is the position.
	} else if len(numbers) > 1 && numbers[1] > 0 {
		buf = int(numbers[1])
	}

	if opts.Context != nil {
		ctx = opts.Context
	}
	return ctx, from, buf, opts, nil
}

func mergeSubscribeOptions(dst, src *SubscribeOptions) {
	if src.Context != nil {
		dst.Context = src.Context
	}
	if src.Inclusive {
		dst.Inclusive = true
	}
	for _, p := range []Position{
		src.StartPosition, src.FromPosition, src.Start, src.Position, src.From,
		src.After, src.Since, src.Offset, src.StartOffset, src.Cursor, src.CursorPosition,
	} {
		if p != 0 {
			dst.StartPosition = p
		}
	}
	for _, b := range []int{
		src.Buffer, src.BufferSize, src.BufferCapacity, src.Capacity, src.Cap,
		src.Size, src.Channel, src.ChannelBuf, src.ChannelSize,
	} {
		if b > 0 {
			dst.Buffer = b
			dst.BufferSize = b
		}
	}
}

func signedUint(v uint64) int64 {
	// Positions used in tests are far below math.MaxInt64. Keep conversion
	// straightforward while retaining negative rejection for int arguments.
	return int64(v)
}

func parsePositionString(s string) (Position, error) {
	p, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidPosition, s)
	}
	return p, nil
}

func (r *subscriberRegistry) subscribeFrom(db *MemDB, from Position, buf int) (*Subscriber, Position, error) {
	if buf < 1 {
		buf = 1
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, 0, ErrClosed
	}

	events, err := r.replayLocked(from)
	if err != nil {
		return nil, r.latest, err
	}

	// Capacity includes the entire atomic replay snapshot plus the live
	// buffer. Every replay send is therefore immediately ready and ordering
	// cannot be overtaken by publish.
	ch := make(chan CommitEvent, len(events)+buf)
	for _, ev := range events {
		ch <- ev
	}

	s := &Subscriber{
		db:   db,
		C:    ch,
		ch:   ch,
		done: make(chan struct{}),
	}
	s.Events = ch
	s.Updates = ch
	s.Changes = ch
	s.start = from
	if n := len(events); n > 0 {
		atomic.StoreUint64(&s.dispatched, events[n-1].Position)
	} else {
		atomic.StoreUint64(&s.dispatched, from)
	}
	r.subs[s] = struct{}{}
	return s, r.latest, nil
}

// ChangesSince returns retained events strictly after from, in commit order.
// It returns a PositionError if any required event was discarded.
func (db *MemDB) ChangesSince(from Position) ([]CommitEvent, error) {
	return db.subs.changesSince(from)
}

// EventsSince is an alias for ChangesSince.
func (db *MemDB) EventsSince(from Position) ([]Event, error) {
	return db.ChangesSince(from)
}

// ReplayChanges is an alias for ChangesSince.
func (db *MemDB) ReplayChanges(from Position) ([]CommitEvent, error) {
	return db.ChangesSince(from)
}

// ReplayFrom is an alias for ChangesSince.
func (db *MemDB) ReplayFrom(from Position) ([]CommitEvent, error) {
	return db.ChangesSince(from)
}

// GetChanges is an alias for ChangesSince.
func (db *MemDB) GetChanges(from Position) ([]CommitEvent, error) {
	return db.ChangesSince(from)
}

// ChangesAfter is an alias for ChangesSince.
func (db *MemDB) ChangesAfter(after Position) ([]CommitEvent, error) {
	return db.ChangesSince(after)
}

// Replay returns retained events strictly after from and invokes fn in
// position order.
func (db *MemDB) Replay(from Position, fn func(CommitEvent) error) error {
	events, err := db.ChangesSince(from)
	if err != nil {
		return err
	}
	for _, ev := range events {
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}

// ReplayContext is Replay with context cancellation checked between events.
func (db *MemDB) ReplayContext(ctx context.Context, from Position, fn func(context.Context, CommitEvent) error) error {
	events, err := db.ChangesSince(from)
	if err != nil {
		return err
	}
	for _, ev := range events {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := fn(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// ReplayInto streams retained events into dst without spawning a goroutine.
// dst must have sufficient capacity or reads must proceed concurrently; this
// is primarily useful in tests and controlled catch-up loops.
func (db *MemDB) ReplayInto(from Position, dst chan<- CommitEvent) (int, error) {
	events, err := db.ChangesSince(from)
	if err != nil {
		return 0, err
	}
	for _, ev := range events {
		dst <- ev
	}
	return len(events), nil
}

func (r *subscriberRegistry) changesSince(from Position) ([]CommitEvent, error) {
	r.mu.Lock()
	events, err := r.replayLocked(from)
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	out := make([]CommitEvent, len(events))
	for i, ev := range events {
		if ev.Changes != nil {
			ev.Changes = append(Changes(nil), ev.Changes...)
		}
		out[i] = ev
	}
	return out, nil
}

// OldestPosition returns the oldest position currently available for replay.
// If no event is retained, it returns latest+1.
func (db *MemDB) OldestPosition() Position {
	r := db.subs
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.oldestLocked()
}

// RetentionWindow returns the configured maximum retained position count.
func (db *MemDB) RetentionWindow() int {
	return db.subs.retentionWindow()
}

// RetainedEvents returns the number of events currently available for replay.
func (db *MemDB) RetainedEvents() int {
	r := db.subs
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retention < 0 {
		return len(r.entries)
	}
	return r.size
}

// DiscardedRangeAfter returns the interval that would need full rebuilding if
// a subscriber last saw from. available is false when from is still within
// the retention window.
func (db *MemDB) DiscardedRangeAfter(from Position) (discarded Range, available bool) {
	r := db.subs
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.replayLocked(from); err == nil {
		return Range{}, true
	} else {
		var pe *PositionError
		if errors.As(err, &pe) {
			return pe.DiscardedRange, false
		}
	}
	return Range{}, false
}

func (r *subscriberRegistry) oldestLocked() Position {
	if r.size > 0 || len(r.entries) > 0 {
		return r.first
	}
	if r.latest == 0 {
		return 1
	}
	// Zero retention: the next event is the oldest thing that can be
	// replayed, and all published events are unavailable.
	return r.latest + 1
}

func (r *subscriberRegistry) replayLocked(from Position) ([]CommitEvent, error) {
	latest := r.latest
	if from > latest {
		return nil, r.positionErrorLocked(from, true)
	}

	oldest := r.oldestLocked()
	if from+1 < oldest {
		return nil, r.positionErrorLocked(from, false)
	}
	if from == latest || latest == 0 {
		return nil, nil
	}

	start := from + 1
	if start < oldest {
		start = oldest
	}

	count := latest - start + 1
	out := make([]CommitEvent, 0, count)

	if r.retention < 0 {
		for p := start; p <= latest; p++ {
			idx := int(p - r.first)
			if idx >= 0 && idx < len(r.entries) {
				out = append(out, r.entries[idx])
			}
		}
		return out, nil
	}

	if r.retention == 0 || len(r.ring) == 0 {
		return nil, r.positionErrorLocked(from, false)
	}

	for p := start; p <= latest; p++ {
		offset := p - r.first
		idx := (r.head + int(offset)) % len(r.ring)
		ev := r.ring[idx]
		// Defensive consistency check; positions are contiguous.
		if ev.Position != p {
			return nil, r.positionErrorLocked(from, false)
		}
		out = append(out, ev)
	}
	return out, nil
}

func (r *subscriberRegistry) positionErrorLocked(from Position, ahead bool) error {
	latest := r.latest
	oldest := r.oldestLocked()
	e := &PositionError{
		Ahead:                   ahead,
		Requested:               from,
		RequestedPosition:       from,
		Position:                from,
		After:                   from,
		Latest:                  latest,
		Newest:                  latest,
		NewestAvailable:         latest,
		LatestPosition:          latest,
		Oldest:                  oldest,
		OldestAvailable:         oldest,
		OldestAvailablePosition: oldest,
		RetentionWindow:         r.retention,
		Retention:               r.retention,
	}
	if ahead {
		e.Start = from + 1
		e.End = latest
	} else {
		first := from + 1
		last := oldest - 1
		e.FirstDiscarded = first
		e.LastDiscarded = last
		e.DiscardedFrom = first
		e.DiscardedTo = last
		e.From = first
		e.To = last
		e.Start = first
		e.End = last
		e.Low = first
		e.High = last
		e.DiscardedRange = Range{Start: first, End: last, From: first, To: last, Low: first, High: last}
		e.Gap = e.DiscardedRange
		e.Range = e.DiscardedRange
	}
	return e
}

// publish atomically advances the log/view and performs non-blocking fan-out.
// Called by Commit while the writer lock is held.
func (r *subscriberRegistry) publish(db *MemDB, view *commitView, ev CommitEvent) {
	r.mu.Lock()
	if r.closed {
		// Close stops future/live subscriptions, but keep the ordered log
		// and replay boundary consistent for callers already using it.
		db.setView(view)
		r.appendLocked(ev)
		r.mu.Unlock()
		return
	}

	db.setView(view)
	r.appendLocked(ev)

	var lagged []*Subscriber
	for s := range r.subs {
		select {
		case s.ch <- ev:
			atomic.StoreUint64(&s.dispatched, ev.Position)
		default:
			// Do not block the commit and do not touch other subscribers.
			// Remove only this slow consumer and report ErrLagged after
			// releasing the registry lock.
			delete(r.subs, s)
			lagged = append(lagged, s)
		}
	}
	r.mu.Unlock()

	for _, s := range lagged {
		r.finish(s, ErrLagged)
	}
}

func (r *subscriberRegistry) appendLocked(ev CommitEvent) {
	r.latest = ev.Position
	if r.size == 0 && len(r.entries) == 0 {
		r.first = ev.Position
	}

	if r.retention < 0 {
		r.entries = append(r.entries, ev)
		return
	}
	if r.retention == 0 {
		r.size = 0
		r.first = ev.Position + 1
		return
	}

	if r.size < len(r.ring) {
		idx := (r.head + r.size) % len(r.ring)
		r.ring[idx] = ev
		r.size++
		return
	}

	// Window is full: overwrite the oldest slot and advance the head.
	r.ring[r.head] = ev
	r.head = (r.head + 1) % len(r.ring)
	r.first++
}

// finish terminates a subscriber exactly once. The channel close happens
// under mu, serializing it with publish sends.
func (r *subscriberRegistry) finish(s *Subscriber, cause error) {
	if s == nil {
		return
	}
	r.mu.Lock()
	if s.closed {
		r.mu.Unlock()
		<-s.done
		return
	}
	s.closed = true
	delete(r.subs, s)
	if cause != nil {
		s.errMu.Lock()
		if s.err == nil {
			s.err = cause
		}
		s.errMu.Unlock()
	}
	close(s.ch)
	r.mu.Unlock()

	// Release references held in buffered channel slots. The channel is
	// closed, so range drains the finite buffer without waiting.
	for range s.ch {
	}
	close(s.done)
}

// close terminates all subscribers cleanly and rejects future subscriptions.
func (r *subscriberRegistry) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	subs := make([]*Subscriber, 0, len(r.subs))
	for s := range r.subs {
		subs = append(subs, s)
	}
	r.mu.Unlock()

	for _, s := range subs {
		r.finish(s, nil)
	}
}
