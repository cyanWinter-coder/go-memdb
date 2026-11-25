// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testUniqueSchema returns a schema with a unique secondary index (foo),
// a non-unique multi index (qux) and a required single index (boo) to
// exercise atomicity and uniqueness behaviors.
func testUniqueSchema() *DBSchema {
	return &DBSchema{
		Tables: map[string]*TableSchema{
			"main": &TableSchema{
				Name: "main",
				Indexes: map[string]*IndexSchema{
					"id": &IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
					"foo": &IndexSchema{
						Name:    "foo",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "Foo"},
					},
					"qux": &IndexSchema{
						Name:    "qux",
						Indexer: &StringSliceFieldIndex{Field: "Qux"},
					},
					"boo": &IndexSchema{
						Name:    "boo",
						Indexer: &StringFieldIndex{Field: "Boo"},
					},
				},
			},
		},
	}
}

func testUniqueDB(t *testing.T) *MemDB {
	db, err := NewMemDB(testUniqueSchema())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return db
}

func indexLen(t *testing.T, txn *Txn, table, index string) int {
	t.Helper()
	return txn.readableIndex(table, index).CommitOnly().Len()
}

// TestTxn_Insert_UniqueIndexConflict checks that inserting an object that
// violates a unique index returns an identifiable error carrying the table,
// index and key, and that the failed insert leaves no trace behind while the
// transaction remains usable.
func TestTxn_Insert_UniqueIndexConflict(t *testing.T) {
	db := testUniqueDB(t)

	obj1 := &TestObject{ID: "1", Foo: "a", Qux: []string{"q1"}, Boo: String("b1")}
	obj2 := &TestObject{ID: "2", Foo: "a", Qux: []string{"q2"}, Boo: String("b2")}

	txn := db.Txn(true)
	if err := txn.Insert("main", obj1); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	txn = db.Txn(true)
	txn.TrackChanges()
	err := txn.Insert("main", obj2)
	if err == nil {
		t.Fatalf("expected unique index conflict")
	}

	// The error must be identifiable and carry the conflict details.
	var uniqErr *UniqueIndexError
	if !errors.As(err, &uniqErr) {
		t.Fatalf("expected a UniqueIndexError, got: %v", err)
	}
	if uniqErr.Table != "main" || uniqErr.Index != "foo" {
		t.Fatalf("bad conflict location: %#v", uniqErr)
	}
	if string(uniqErr.Key) != "a\x00" {
		t.Fatalf("bad conflict key: %q", uniqErr.Key)
	}
	if uniqErr.Value != obj1 {
		t.Fatalf("bad conflict value: %#v", uniqErr.Value)
	}

	// The failed insert must not have written anything: the object must not
	// be reachable from any index, and no change may be recorded.
	if raw, _ := txn.First("main", "id", "2"); raw != nil {
		t.Fatalf("failed insert leaked into id index: %#v", raw)
	}
	if raw, _ := txn.First("main", "qux", "q2"); raw != nil {
		t.Fatalf("failed insert leaked into qux index: %#v", raw)
	}
	if raw, _ := txn.First("main", "boo", "b2"); raw != nil {
		t.Fatalf("failed insert leaked into boo index: %#v", raw)
	}
	if n := len(txn.Changes()); n != 0 {
		t.Fatalf("failed insert recorded %d changes", n)
	}

	// The pre-existing object must still own the conflicting key.
	if raw, _ := txn.First("main", "foo", "a"); raw != obj1 {
		t.Fatalf("conflicting key no longer owned by original object: %#v", raw)
	}

	// The transaction must remain usable.
	obj3 := &TestObject{ID: "3", Foo: "c", Qux: []string{"q3"}, Boo: String("b3")}
	if err := txn.Insert("main", obj3); err != nil {
		t.Fatalf("transaction unusable after conflict: %v", err)
	}
	txn.Commit()

	txn = db.Txn(false)
	defer txn.Abort()
	if raw, _ := txn.First("main", "id", "1"); raw != obj1 {
		t.Fatalf("obj1 missing: %#v", raw)
	}
	if raw, _ := txn.First("main", "id", "2"); raw != nil {
		t.Fatalf("obj2 should not exist: %#v", raw)
	}
	if raw, _ := txn.First("main", "id", "3"); raw != obj3 {
		t.Fatalf("obj3 missing: %#v", raw)
	}
	if n := indexLen(t, txn, "main", "id"); n != 2 {
		t.Fatalf("bad id index size: %d", n)
	}
	if n := indexLen(t, txn, "main", "foo"); n != 2 {
		t.Fatalf("bad foo index size: %d", n)
	}
}

// TestTxn_Insert_UpdateUniqueIndex checks that updating an object in place
// keeps working when its unique values are unchanged, and that stealing
// another object's unique key is rejected without corrupting anything.
func TestTxn_Insert_UpdateUniqueIndex(t *testing.T) {
	db := testUniqueDB(t)

	obj1 := &TestObject{ID: "1", Foo: "a", Qux: []string{"q1"}, Boo: String("b1")}
	obj2 := &TestObject{ID: "2", Foo: "b", Qux: []string{"q2"}, Boo: String("b2")}

	txn := db.Txn(true)
	if err := txn.Insert("main", obj1); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("main", obj2); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Re-inserting an object with its own unique values is an update, not
	// a conflict.
	txn = db.Txn(true)
	obj1Copy := &TestObject{ID: "1", Foo: "a", Qux: []string{"q1"}, Boo: String("b1")}
	if err := txn.Insert("main", obj1Copy); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Moving an object onto another object's unique key must fail and leave
	// both objects fully intact.
	txn = db.Txn(true)
	obj2Conflict := &TestObject{ID: "2", Foo: "a", Qux: []string{"q2"}, Boo: String("b2")}
	err := txn.Insert("main", obj2Conflict)
	var uniqErr *UniqueIndexError
	if !errors.As(err, &uniqErr) {
		t.Fatalf("expected a UniqueIndexError, got: %v", err)
	}
	if uniqErr.Index != "foo" {
		t.Fatalf("bad conflict index: %#v", uniqErr)
	}
	txn.Abort()

	txn = db.Txn(false)
	defer txn.Abort()
	if raw, _ := txn.First("main", "foo", "a"); raw != obj1Copy {
		t.Fatalf("obj1 lost its key: %#v", raw)
	}
	if raw, _ := txn.First("main", "foo", "b"); raw != obj2 {
		t.Fatalf("obj2 lost its key: %#v", raw)
	}
	if raw, _ := txn.First("main", "qux", "q2"); raw != obj2 {
		t.Fatalf("obj2 lost its qux entry: %#v", raw)
	}
	if n := indexLen(t, txn, "main", "id"); n != 2 {
		t.Fatalf("bad id index size: %d", n)
	}
	if n := indexLen(t, txn, "main", "foo"); n != 2 {
		t.Fatalf("bad foo index size: %d", n)
	}
}

// TestTxn_Insert_AtomicOnIndexError checks that when building an index value
// fails midway through an insert, none of the object's index entries are
// left behind and the transaction remains usable.
func TestTxn_Insert_AtomicOnIndexError(t *testing.T) {
	db := testUniqueDB(t)

	txn := db.Txn(true)
	txn.TrackChanges()

	// boo is a required index and Boo is nil: the insert must fail. The
	// (random) schema iteration order means other indexes may be processed
	// first, which is exactly what used to leave partial writes behind.
	bad := &TestObject{ID: "bad", Foo: "x", Qux: []string{"q"}}
	if err := txn.Insert("main", bad); err == nil {
		t.Fatalf("expected missing index error")
	}

	if raw, _ := txn.First("main", "id", "bad"); raw != nil {
		t.Fatalf("failed insert leaked into id index: %#v", raw)
	}
	if raw, _ := txn.First("main", "foo", "x"); raw != nil {
		t.Fatalf("failed insert leaked into foo index: %#v", raw)
	}
	if raw, _ := txn.First("main", "qux", "q"); raw != nil {
		t.Fatalf("failed insert leaked into qux index: %#v", raw)
	}
	if n := len(txn.Changes()); n != 0 {
		t.Fatalf("failed insert recorded %d changes", n)
	}

	// The transaction must remain usable.
	good := &TestObject{ID: "good", Foo: "y", Qux: []string{"q"}, Boo: String("b")}
	if err := txn.Insert("main", good); err != nil {
		t.Fatalf("transaction unusable after failed insert: %v", err)
	}
	txn.Commit()

	txn = db.Txn(false)
	defer txn.Abort()
	if raw, _ := txn.First("main", "id", "bad"); raw != nil {
		t.Fatalf("bad object should not exist: %#v", raw)
	}
	if raw, _ := txn.First("main", "id", "good"); raw != good {
		t.Fatalf("good object missing: %#v", raw)
	}
	if n := indexLen(t, txn, "main", "id"); n != 1 {
		t.Fatalf("bad id index size: %d", n)
	}
	if n := indexLen(t, txn, "main", "qux"); n != 1 {
		t.Fatalf("bad qux index size: %d", n)
	}
}

// TestTxn_Abort_DiscardsWrites checks that after Abort none of the
// transaction's objects, index entries or recorded changes are visible.
func TestTxn_Abort_DiscardsWrites(t *testing.T) {
	db := testUniqueDB(t)

	txn := db.Txn(true)
	txn.TrackChanges()
	obj := &TestObject{ID: "1", Foo: "a", Qux: []string{"q1"}, Boo: String("b1")}
	if err := txn.Insert("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}
	if n := len(txn.Changes()); n != 1 {
		t.Fatalf("expected 1 change before abort, got %d", n)
	}
	txn.Abort()

	// Changes are discarded by the abort.
	if n := len(txn.Changes()); n != 0 {
		t.Fatalf("expected no changes after abort, got %d", n)
	}

	// Nothing may be readable from a fresh transaction.
	rtxn := db.Txn(false)
	defer rtxn.Abort()
	if raw, _ := rtxn.First("main", "id", "1"); raw != nil {
		t.Fatalf("aborted insert visible in id index: %#v", raw)
	}
	if raw, _ := rtxn.First("main", "foo", "a"); raw != nil {
		t.Fatalf("aborted insert visible in foo index: %#v", raw)
	}
	if raw, _ := rtxn.First("main", "qux", "q1"); raw != nil {
		t.Fatalf("aborted insert visible in qux index: %#v", raw)
	}
	if raw, _ := rtxn.First("main", "boo", "b1"); raw != nil {
		t.Fatalf("aborted insert visible in boo index: %#v", raw)
	}
	for _, index := range []string{"id", "foo", "qux", "boo"} {
		if n := indexLen(t, rtxn, "main", index); n != 0 {
			t.Fatalf("index %q not empty after abort: %d entries", index, n)
		}
	}
}

// TestTxn_DeleteInsert_SameKey checks that deleting and re-inserting the
// same primary key within one transaction keeps only the last version and
// leaves every index consistent with the table contents.
func TestTxn_DeleteInsert_SameKey(t *testing.T) {
	db := testUniqueDB(t)

	obj1 := &TestObject{ID: "1", Foo: "a", Qux: []string{"q1", "q2"}, Boo: String("b1")}
	txn := db.Txn(true)
	if err := txn.Insert("main", obj1); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	// Delete then re-insert the same key with different index values in a
	// single transaction.
	obj2 := &TestObject{ID: "1", Foo: "b", Qux: []string{"q3"}, Boo: String("b2")}
	txn = db.Txn(true)
	if err := txn.Delete("main", obj1); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("main", obj2); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	txn = db.Txn(false)
	defer txn.Abort()

	// Only the last version survives.
	if raw, _ := txn.First("main", "id", "1"); raw != obj2 {
		t.Fatalf("expected final version of object: %#v", raw)
	}
	if raw, _ := txn.First("main", "foo", "a"); raw != nil {
		t.Fatalf("stale foo entry for old version: %#v", raw)
	}
	if raw, _ := txn.First("main", "foo", "b"); raw != obj2 {
		t.Fatalf("missing foo entry for new version: %#v", raw)
	}
	for _, q := range []string{"q1", "q2"} {
		if raw, _ := txn.First("main", "qux", q); raw != nil {
			t.Fatalf("stale qux entry %q: %#v", q, raw)
		}
	}
	if raw, _ := txn.First("main", "qux", "q3"); raw != obj2 {
		t.Fatalf("missing qux entry for new version: %#v", raw)
	}

	// Index entry counts must match the object count.
	if n := indexLen(t, txn, "main", "id"); n != 1 {
		t.Fatalf("bad id index size: %d", n)
	}
	if n := indexLen(t, txn, "main", "foo"); n != 1 {
		t.Fatalf("bad foo index size: %d", n)
	}
	if n := indexLen(t, txn, "main", "qux"); n != 1 {
		t.Fatalf("bad qux index size: %d", n)
	}
	if n := indexLen(t, txn, "main", "boo"); n != 1 {
		t.Fatalf("bad boo index size: %d", n)
	}
}

// TestTxn_Commit_AtomicRootPublication checks that readers only ever see
// the complete state from before or after a commit, never a mixture.
func TestTxn_Commit_AtomicRootPublication(t *testing.T) {
	db := testDB(t)

	wtxn := db.Txn(true)
	obj := testObj()
	if err := wtxn.Insert("main", obj); err != nil {
		t.Fatalf("err: %v", err)
	}

	// A read transaction started before the commit sees the old root.
	before := db.Txn(false)
	defer before.Abort()

	wtxn.Commit()

	// A read transaction started after the commit sees the new root.
	after := db.Txn(false)
	defer after.Abort()

	if raw, _ := before.First("main", "id", obj.ID); raw != nil {
		t.Fatalf("pre-commit reader observed uncommitted object: %#v", raw)
	}
	if raw, _ := after.First("main", "id", obj.ID); raw == nil {
		t.Fatalf("post-commit reader missed committed object")
	}
	if raw, _ := after.First("main", "foo", obj.Foo); raw == nil {
		t.Fatalf("post-commit reader missed committed index entry")
	}
}

// TestTxn_Commit_ReaderConsistency hammers the DB with single-object write
// transactions while readers verify that every snapshot they observe has
// consistent entry counts across all indexes (no torn commits).
func TestTxn_Commit_ReaderConsistency(t *testing.T) {
	db := testDB(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			txn := db.Txn(true)
			obj := &TestObject{
				ID:  fmt.Sprintf("obj-%d", i),
				Foo: "foo",
				Qux: []string{"q"},
			}
			if err := txn.Insert("main", obj); err != nil {
				t.Errorf("err: %v", err)
				return
			}
			txn.Commit()
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				txn := db.Txn(false)
				idLen := txn.readableIndex("main", "id").CommitOnly().Len()
				fooLen := txn.readableIndex("main", "foo").CommitOnly().Len()
				quxLen := txn.readableIndex("main", "qux").CommitOnly().Len()
				txn.Abort()
				if idLen != fooLen || idLen != quxLen {
					t.Errorf("torn read: id=%d foo=%d qux=%d", idLen, fooLen, quxLen)
					return
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}
