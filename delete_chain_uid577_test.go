// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"errors"
	"fmt"
	"testing"
)

type uid577FlakyIndexer struct {
	*StringFieldIndex
	failFromObject bool
}

func (u *uid577FlakyIndexer) FromObject(obj interface{}) (bool, []byte, error) {
	if u.failFromObject {
		return false, nil, errors.New("uid577 index failure")
	}
	return u.StringFieldIndex.FromObject(obj)
}

func uid577AtomicSchema() *DBSchema {
	return &DBSchema{Tables: map[string]*TableSchema{"main": {
		Name: "main",
		Indexes: map[string]*IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &StringFieldIndex{Field: "ID"},
			},
			"foo": {
				Name:    "foo",
				Indexer: &StringFieldIndex{Field: "Foo"},
			},
			"flaky": {
				Name: "flaky",
				Indexer: &uid577FlakyIndexer{
					StringFieldIndex: &StringFieldIndex{Field: "Baz"},
				},
			},
		},
	}}}
}

func TestUID577DeletePrefixRemovesAllMultiIndexValues(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	obj := &TestObject{
		ID:  "multi-value",
		Foo: "foo",
		Qux: []string{"uid577-match", "uid577-other-value"},
	}
	if err := txn.Insert("main", obj); err != nil {
		t.Fatal(err)
	}

	deleted, err := txn.DeletePrefix("main", "qux_prefix", "uid577-match")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}

	for _, value := range obj.Qux {
		raw, err := txn.First("main", "qux", value)
		if err != nil {
			t.Fatal(err)
		}
		if raw != nil {
			t.Fatalf("secondary index %q still contains %#v", value, raw)
		}
	}
	txn.Commit()

	readTxn := db.Txn(false)
	for _, value := range obj.Qux {
		raw, err := readTxn.First("main", "qux", value)
		if err != nil {
			t.Fatal(err)
		}
		if raw != nil {
			t.Fatalf("committed secondary index %q still contains %#v", value, raw)
		}
	}
}

func TestUID577DeletePrefixCountsDistinctObjectsAndIsIdempotent(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	// One object produces two keys matching the same prefix.
	obj := &TestObject{
		ID:  "duplicate-matches",
		Foo: "foo",
		Qux: []string{"uid577-a", "uid577-b"},
	}
	if err := txn.Insert("main", obj); err != nil {
		t.Fatal(err)
	}

	deleted, err := txn.DeletePrefix("main", "qux_prefix", "uid577-")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("first DeletePrefix count = %d, want 1", deleted)
	}

	deleted, err = txn.DeletePrefix("main", "qux_prefix", "uid577-")
	if err != nil {
		t.Fatalf("second DeletePrefix returned error: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("second DeletePrefix count = %d, want 0", deleted)
	}
	txn.Commit()

	writeTxn := db.Txn(true)
	deleted, err = writeTxn.DeletePrefix("main", "qux_prefix", "uid577-")
	if err != nil {
		t.Fatalf("post-commit DeletePrefix returned error: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("post-commit DeletePrefix count = %d, want 0", deleted)
	}
	writeTxn.Commit()
}

func TestUID577DeleteIndexFailureIsAtomic(t *testing.T) {
	schema := uid577AtomicSchema()
	db, err := NewMemDB(schema)
	if err != nil {
		t.Fatal(err)
	}

	txn := db.Txn(true)
	obj := &TestObject{ID: "1", Foo: "foo", Baz: "baz", Qux: []string{"unused"}}
	if err := txn.Insert("main", obj); err != nil {
		t.Fatal(err)
	}

	flaky := schema.Tables["main"].Indexes["flaky"].Indexer.(*uid577FlakyIndexer)
	flaky.failFromObject = true
	if err := txn.Delete("main", obj); err == nil {
		t.Fatal("expected index failure")
	}

	if raw, _ := txn.First("main", "id", obj.ID); raw == nil {
		t.Fatal("primary index lost object after failed delete")
	}
	if raw, _ := txn.First("main", "foo", obj.Foo); raw == nil {
		t.Fatal("foo index lost object after failed delete")
	}
	if raw, _ := txn.First("main", "flaky", obj.Baz); raw == nil {
		t.Fatal("flaky index lost object after failed delete")
	}

	flaky.failFromObject = false
	other := &TestObject{ID: "2", Foo: "two", Baz: "baz-two", Qux: []string{"q2"}}
	if err := txn.Insert("main", other); err != nil {
		t.Fatal(err)
	}
	txn.Commit()

	readTxn := db.Txn(false)
	if raw, _ := readTxn.First("main", "id", obj.ID); raw == nil {
		t.Fatal("failed delete was not rolled back by returning without mutation")
	}
	if raw, _ := readTxn.First("main", "id", other.ID); raw == nil {
		t.Fatal("transaction did not commit operations after index failure")
	}
}

func TestUID577DeleteNotFoundIsRecognizableAndTxnContinues(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	missing := &TestObject{ID: "uid577-missing"}
	err := txn.Delete("main", missing)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("errors.Is(err, ErrNotFound) = false for %v", err)
	}
	if err != ErrNotFound {
		t.Fatalf("err = %v, want exact ErrNotFound for compatibility", err)
	}

	other := &TestObject{ID: "uid577-present", Foo: "present", Qux: []string{"present"}}
	if err := txn.Insert("main", other); err != nil {
		t.Fatal(err)
	}
	txn.Commit()

	readTxn := db.Txn(false)
	raw, err := readTxn.First("main", "id", other.ID)
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		t.Fatal("transaction did not commit after ErrNotFound")
	}
}

func TestUID577DeleteChangeMarksDeletedOriginal(t *testing.T) {
	db := testDB(t)
	obj := &TestObject{ID: "uid577-change", Foo: "foo", Qux: []string{"q"}}

	writeTxn := db.Txn(true)
	if err := writeTxn.Insert("main", obj); err != nil {
		t.Fatal(err)
	}
	writeTxn.Commit()

	txn := db.Txn(true)
	txn.TrackChanges()
	if err := txn.Delete("main", obj); err != nil {
		t.Fatal(err)
	}

	changes := txn.Changes()
	if len(changes) != 1 {
		t.Fatalf("len(changes) = %d, want 1: %#v", len(changes), changes)
	}
	change := changes[0]
	if !change.Deleted() {
		t.Fatalf("change is not marked deleted: %#v", change)
	}
	if change.Table != "main" || change.Before == nil || change.After != nil {
		t.Fatalf("unexpected delete change: %#v", change)
	}
	if before, ok := change.Before.(*TestObject); !ok || before.ID != obj.ID {
		t.Fatalf("Before does not contain original object: %#v", change.Before)
	}
	txn.Commit()
}

func TestUID577DeleteAllUsesBoundedBatches(t *testing.T) {
	db := testDB(t)
	txn := db.Txn(true)

	n := deleteBatchSize*2 + 17
	for i := 0; i < n; i++ {
		obj := &TestObject{
			ID:  fmt.Sprintf("uid577-%06d", i),
			Foo: "uid577-batch",
			Qux: []string{"uid577-batch"},
		}
		if err := txn.Insert("main", obj); err != nil {
			t.Fatal(err)
		}
	}

	deleted, err := txn.DeleteAll("main", "foo", "uid577-batch")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != n {
		t.Fatalf("DeleteAll count = %d, want %d", deleted, n)
	}

	deleted, err = txn.DeleteAll("main", "foo", "uid577-batch")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Fatalf("repeat DeleteAll count = %d, want 0", deleted)
	}
	txn.Commit()
}
