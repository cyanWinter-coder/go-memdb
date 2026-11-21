// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/hashicorp/go-immutable-radix"
)

// commitView is the atomically published result of a single commit. The
// pointer to it is swapped in one atomic store, so a reader that loads the
// view always observes the data root, the generation number and the change
// log of exactly one commit — never a mix of generations.
type commitView struct {
	// generation is a monotonically increasing identifier of the commit.
	// It is 0 for a freshly initialized (empty) database.
	generation uint64

	// root is the root of the immutable radix trees holding all table
	// indexes (the data) as of this generation.
	root *iradix.Tree

	// changes is the de-duplicated, commit-ordered change log entry for
	// this generation. It is nil for generations that did not track
	// changes and for the initial empty generation.
	changes Changes
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
	schema  *DBSchema
	view    unsafe.Pointer // *commitView underneath
	primary bool

	// There can only be a single writer at once
	writer sync.Mutex

	// subs tracks the live commit subscribers. Registration, removal and
	// broadcast are serialized by its mutex; broadcasting never blocks a
	// commit because delivery to each subscriber is non-blocking.
	subs *subscriberRegistry
}

// NewMemDB creates a new MemDB with the given schema.
func NewMemDB(schema *DBSchema) (*MemDB, error) {
	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Create the MemDB
	db := &MemDB{
		schema:  schema,
		view:    unsafe.Pointer(&commitView{root: iradix.New()}),
		primary: true,
		subs:    newSubscriberRegistry(),
	}
	if err := db.initialize(); err != nil {
		return nil, err
	}

	return db, nil
}

// DBSchema returns schema in use for introspection.
//
// The method is intended for *read-only* debugging use cases,
// returned schema should *never be modified in-place*.
func (db *MemDB) DBSchema() *DBSchema {
	return db.schema
}

// getView is used to do an atomic load of the committed view. The returned
// view ties the data root, the generation and the change log of a single
// commit together.
func (db *MemDB) getView() *commitView {
	return (*commitView)(atomic.LoadPointer(&db.view))
}

// setView atomically publishes a new committed view. Committing
// transactions must go through subscriberRegistry.publish so that the
// view swap and the subscriber broadcast stay atomic with respect to
// concurrent Subscribe calls.
func (db *MemDB) setView(view *commitView) {
	atomic.StorePointer(&db.view, unsafe.Pointer(view))
}

// getRoot is used to do an atomic load of the root pointer
func (db *MemDB) getRoot() *iradix.Tree {
	return db.getView().root
}

// Generation returns the generation of the latest commit. It is 0 before
// any write transaction has committed. Every successful commit publishes
// its data, change log and watch notifications under a new, strictly
// increasing generation.
func (db *MemDB) Generation() uint64 {
	return db.getView().generation
}

// CommittedChanges returns the change log entry of the latest commit, or
// nil if the latest commit did not track changes. The returned slice is
// exactly the set of mutations that transitioned the database from
// Generation()-1 to Generation(), de-duplicated per object and in
// commit order.
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
	clone := &MemDB{
		schema:  db.schema,
		view:    unsafe.Pointer(db.getView()),
		primary: false,
		subs:    newSubscriberRegistry(),
	}
	return clone
}

// Close shuts down the database's notification machinery: every registered
// subscriber channel is closed and future Subscribe calls fail. It is safe
// to call Close concurrently with commits, subscriptions and
// unsubscriptions; it never blocks on slow subscribers. In-flight and future
// transactions are unaffected — reads and writes still work — but no further
// commit notifications are delivered.
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
	db.view = unsafe.Pointer(&commitView{root: root})
	return nil
}

// indexPath returns the path from the root to the given table index
func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}
