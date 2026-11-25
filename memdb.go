// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/hashicorp/go-immutable-radix"
)

// commitView is the atomically published result of one committed
// transaction. Tying the root and generation together ensures that an event
// at a position can never be observed without its corresponding data root.
type commitView struct {
	generation uint64
	root       *iradix.Tree
	changes    Changes
}

// MemDB is an in-memory database providing Atomicity, Consistency, and
// Isolation from ACID. MemDB doesn't provide Durability since it is an
// in-memory database.
//
// MemDB provides a table abstraction to store objects (rows) with multiple
// indexes based on inserted values. The database makes use of immutable radix
// trees to provide transactions and MVCC.
//
// Objects inserted into MemDB are not copied. It is **extremely important**
// that objects are not modified in-place after they are inserted since they
// are stored directly in MemDB. It remains unsafe to modify inserted objects
// even after they've been deleted from MemDB since there may still be older
// snapshots of the DB being read from other goroutines.
type MemDB struct {
	schema *DBSchema
	// root retains the historical atomic radix-root field alongside view.
	root    unsafe.Pointer // *iradix.Tree underneath
	view    unsafe.Pointer // *commitView underneath
	primary bool

	// There can only be a single writer at once
	writer sync.Mutex

	// subs owns the ordered commit log, retention window and live
	// subscriptions. It is always present on a primary database.
	subs *subscriberRegistry
}

// Option configures optional MemDB behavior.
type Option func(*options)

type options struct {
	retentionWindow int
}

// DefaultRetentionWindow is the number of committed positions retained for
// replay when no explicit retention window is supplied. It is intentionally
// larger than the expected few-thousand-event disconnect lag.
const DefaultRetentionWindow = 10_000

// Common aliases for DefaultRetentionWindow.
const (
	DefaultRetention       = DefaultRetentionWindow
	DefaultEventBufferSize = 64
)

func defaultOptions() options {
	return options{retentionWindow: DefaultRetentionWindow}
}

// WithRetentionWindow sets the maximum number of committed positions retained
// for replay. A value of zero disables replay retention; a negative value
// retains the complete log for the lifetime of the database.
func WithRetentionWindow(positions int) Option {
	return func(o *options) {
		o.retentionWindow = positions
	}
}

// WithRetention is an alias for WithRetentionWindow.
func WithRetention(positions int) Option {
	return WithRetentionWindow(positions)
}

// WithMaxReplayEntries is an alias for WithRetentionWindow.
func WithMaxReplayEntries(positions int) Option {
	return WithRetentionWindow(positions)
}

// WithMaxEvents is an alias for WithRetentionWindow.
func WithMaxEvents(events int) Option {
	return WithRetentionWindow(events)
}

// WithRetentionSize is an alias for WithRetentionWindow.
func WithRetentionSize(positions int) Option {
	return WithRetentionWindow(positions)
}

// WithWindowSize is an alias for WithRetentionWindow.
func WithWindowSize(positions int) Option {
	return WithRetentionWindow(positions)
}

// WithLogSize is an alias for WithRetentionWindow.
func WithLogSize(positions int) Option {
	return WithRetentionWindow(positions)
}

// WithCapacity is an alias for WithRetentionWindow.
func WithCapacity(positions int) Option {
	return WithRetentionWindow(positions)
}

// WithEventRetention is an alias for WithRetentionWindow.
func WithEventRetention(positions int) Option {
	return WithRetentionWindow(positions)
}

// WithMaxChanges is an alias for WithRetentionWindow. Despite the name, the
// window is indexed by commit position; each event carries its full Changes.
func WithMaxChanges(positions int) Option {
	return WithRetentionWindow(positions)
}

// NewMemDB creates a new MemDB with the given schema.
//
// Optional arguments may be Option functions, an Options/Config value, or a
// bare integer retention window. The strongly typed functional form is
// preferred:
//
//	db, err := memdb.NewMemDB(schema, memdb.WithRetentionWindow(10_000))
func NewMemDB(schema *DBSchema, args ...any) (*MemDB, error) {
	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	cfg := defaultOptions()
	for _, arg := range args {
		switch opt := arg.(type) {
		case nil:
			continue
		case Option:
			if opt != nil {
				opt(&cfg)
			}
		case func(*options):
			if opt != nil {
				opt(&cfg)
			}
		case func(*Config):
			if opt != nil {
				publicCfg := Config{}
				opt(&publicCfg)
				applyConfig(&cfg, publicCfg)
			}
		case Config:
			applyConfig(&cfg, opt)
		case *Config:
			if opt != nil {
				applyConfig(&cfg, *opt)
			}
		case int:
			cfg.retentionWindow = opt
		case int64:
			cfg.retentionWindow = int(opt)
		default:
			return nil, fmt.Errorf("memdb: unsupported option %T", arg)
		}
	}

	initialRoot := iradix.New()
	db := &MemDB{
		schema:  schema,
		root:    unsafe.Pointer(initialRoot),
		view:    unsafe.Pointer(&commitView{root: initialRoot}),
		primary: true,
		subs:    newSubscriberRegistry(cfg.retentionWindow),
	}
	if err := db.initialize(); err != nil {
		return nil, err
	}

	return db, nil
}

func applyConfig(dst *options, cfg Config) {
	for _, n := range []int{
		cfg.RetentionWindow,
		cfg.Retention,
		cfg.RetentionSize,
		cfg.MaxReplayEntries,
		cfg.MaxEvents,
		cfg.MaxPositions,
		cfg.Window,
	} {
		if n != 0 {
			dst.retentionWindow = n
			return
		}
	}
}

// NewMemDBWithRetention creates a MemDB with an explicit replay retention
// window. See WithRetentionWindow for the meaning of positions.
func NewMemDBWithRetention(schema *DBSchema, positions int) (*MemDB, error) {
	return NewMemDB(schema, WithRetentionWindow(positions))
}

// NewMemDBWithRetentionWindow is an alias for NewMemDBWithRetention.
func NewMemDBWithRetentionWindow(schema *DBSchema, positions int) (*MemDB, error) {
	return NewMemDBWithRetention(schema, positions)
}

// NewMemDBWithWindow is an alias for NewMemDBWithRetention.
func NewMemDBWithWindow(schema *DBSchema, positions int) (*MemDB, error) {
	return NewMemDBWithRetention(schema, positions)
}

// NewMemDBWithSize is an alias for NewMemDBWithRetention.
func NewMemDBWithSize(schema *DBSchema, positions int) (*MemDB, error) {
	return NewMemDBWithRetention(schema, positions)
}

// NewMemDBWithOptions creates a MemDB from a Config value. Zero-valued
// retention fields use DefaultRetentionWindow; use NewMemDB with
// WithRetentionWindow(0) to explicitly disable replay retention.
func NewMemDBWithOptions(schema *DBSchema, cfg Config) (*MemDB, error) {
	retention := defaultOptions()
	applyConfig(&retention, cfg)
	return NewMemDB(schema, WithRetentionWindow(retention.retentionWindow))
}

// Config is the configuration accepted by NewMemDBWithOptions.
type Config struct {
	RetentionWindow  int
	Retention        int
	RetentionSize    int
	MaxReplayEntries int
	MaxEvents        int
	MaxPositions     int
	Window           int
}

// Options is an alias for Config.
type Options = Config

// MemDBConfig and DBConfig are common config-name aliases.
type MemDBConfig = Config
type DBConfig = Config

// NewMemDBWithConfig is an alias for NewMemDBWithOptions.
func NewMemDBWithConfig(schema *DBSchema, cfg Config) (*MemDB, error) {
	return NewMemDBWithOptions(schema, cfg)
}

// DBSchema returns schema in use for introspection.
//
// The method is intended for *read-only* debugging use cases,
// returned schema should *never be modified in-place*.
func (db *MemDB) DBSchema() *DBSchema {
	return db.schema
}

// getView is used to do an atomic load of the committed view.
func (db *MemDB) getView() *commitView {
	return (*commitView)(atomic.LoadPointer(&db.view))
}

// setView is used to atomically publish a committed view.
func (db *MemDB) setView(view *commitView) {
	atomic.StorePointer(&db.view, unsafe.Pointer(view))
}

func (db *MemDB) setRoot(root *iradix.Tree) {
	atomic.StorePointer(&db.root, unsafe.Pointer(root))
}

// getRoot is used to do an atomic load of the root pointer.
func (db *MemDB) getRoot() *iradix.Tree {
	return db.getView().root
}

// Generation returns the position/generation of the latest commit. It is 0
// before any write transaction has committed and increases by one for every
// successful write-transaction commit.
func (db *MemDB) Generation() uint64 {
	return db.getView().generation
}

// Position is an alias for Generation: it is the globally ordered commit
// position used for replay and subscription resumption.
func (db *MemDB) Position() Position {
	return db.getView().generation
}

// LatestPosition is an alias for Position.
func (db *MemDB) LatestPosition() Position { return db.Position() }

// CurrentPosition is an alias for Position.
func (db *MemDB) CurrentPosition() Position { return db.Position() }

// CommittedPosition is an alias for Position.
func (db *MemDB) CommittedPosition() Position { return db.Position() }

// Offset is a Kafka-style alias for Position.
func (db *MemDB) Offset() uint64 { return db.Position() }

// Cursor is an alias for Position.
func (db *MemDB) Cursor() Position { return db.Position() }

// Sequence is an alias for Position.
func (db *MemDB) Sequence() Position { return db.Position() }

// Revision is an alias for Position.
func (db *MemDB) Revision() Position { return db.Position() }

// Version is an alias for Position.
func (db *MemDB) Version() Position { return db.Position() }

// CommittedChanges returns the change set of the latest committed
// transaction. The result is nil for the initial empty generation.
func (db *MemDB) CommittedChanges() Changes {
	return db.getView().changes
}

// Txn is used to start a new transaction in either read or write mode.
// There can only be a single concurrent writer, but any number of readers.
func (db *MemDB) Txn(write bool) *Txn {
	if write {
		db.writer.Lock()
	}
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: db.getRoot().Txn(),
	}
	return txn
}

// Snapshot is used to capture a point-in-time snapshot  of the database that
// will not be affected by any write operations to the existing DB.
//
// If MemDB is storing reference-based values (pointers, maps, slices, etc.),
// the Snapshot will not deep copy those values. Therefore, it is still unsafe
// to modify any inserted values in either DB.
func (db *MemDB) Snapshot() *MemDB {
	view := db.getView()
	retention := DefaultRetentionWindow
	if db.subs != nil {
		retention = db.subs.retentionWindow()
	}
	clone := &MemDB{
		schema:  db.schema,
		root:    unsafe.Pointer(view.root),
		view:    unsafe.Pointer(view),
		primary: false,
		subs:    newSubscriberRegistry(retention),
	}
	// A snapshot is pinned to view.generation and does not participate in the
	// primary database's subscription registry.
	clone.subs.latest = view.generation
	return clone
}

// Close shuts down subscription delivery and releases all live subscription
// buffers. It is idempotent. Reads, writes and position-based replay remain
// usable; only active subscriptions are terminated.
func (db *MemDB) Close() error {
	if db.subs != nil {
		db.subs.close()
	}
	return nil
}

// initialize is used to setup the DB for use after creation. This should
// be called only once after allocating a MemDB.
func (db *MemDB) initialize() error {
	root := db.getRoot()
	for tName, tableSchema := range db.schema.Tables {
		for iName := range tableSchema.Indexes {
			index := iradix.New()
			path := indexPath(tName, iName)
			root, _, _ = root.Insert(path, index)
		}
	}
	atomic.StorePointer(&db.root, unsafe.Pointer(root))
	db.setView(&commitView{generation: db.Generation(), root: root})
	return nil
}

// indexPath returns the path from the root to the given table index
func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}
