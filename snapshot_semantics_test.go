// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"errors"
	"fmt"
	"sort"
	"testing"
)

func uid576SnapshotObject(i int, baz string) *TestObject {
	return &TestObject{
		ID:  fmt.Sprintf("obj-%06d", i),
		Foo: "snapshot-scan",
		Baz: baz,
		Qux: []string{"snapshot-scan"},
	}
}

func newUID576SnapshotScanDB(t *testing.T, n int) *MemDB {
	t.Helper()

	db := testDB(t)
	txn := db.Txn(true)
	for i := 0; i < n; i++ {
		if err := txn.Insert("main", uid576SnapshotObject(i, "old")); err != nil {
			txn.Abort()
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	txn.Commit()
	return db
}

func drainUID576ObjectIDs(t *testing.T, iter ResultIterator) ([]string, map[string]string) {
	t.Helper()

	var ids []string
	bazByID := make(map[string]string)
	for raw := iter.Next(); raw != nil; raw = iter.Next() {
		obj, ok := raw.(*TestObject)
		if !ok {
			t.Fatalf("unexpected value type %T", raw)
		}
		ids = append(ids, obj.ID)
		bazByID[obj.ID] = obj.Baz
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator failed: %v", err)
	}
	return ids, bazByID
}

func TestUID576SnapshotIteratorsStableAcrossCommits(t *testing.T) {
	const n = 500

	db := newUID576SnapshotScanDB(t, n)
	txn := db.Txn(false)

	forward, err := txn.Get("main", "id")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	prefix, err := txn.Get("main", "id_prefix", "obj-")
	if err != nil {
		t.Fatalf("Get prefix: %v", err)
	}
	lower, err := txn.LowerBound("main", "id", "obj-000000")
	if err != nil {
		t.Fatalf("LowerBound: %v", err)
	}
	reverse, err := txn.ReverseLowerBound("main", "id", "obj-999999")
	if err != nil {
		t.Fatalf("ReverseLowerBound: %v", err)
	}
	getReverse, err := txn.GetReverse("main", "id")
	if err != nil {
		t.Fatalf("GetReverse: %v", err)
	}

	// Begin every scan before publishing any of the new versions.
	iters := []ResultIterator{forward, prefix, lower, reverse, getReverse}
	firstBaz := make(map[ResultIterator]string, len(iters))
	for _, iter := range iters {
		raw := iter.Next()
		if raw == nil {
			t.Fatal("iterator should contain at least one row before concurrent commits")
		}
		obj, ok := raw.(*TestObject)
		if !ok {
			t.Fatalf("unexpected first value type %T", raw)
		}
		firstBaz[iter] = obj.Baz
	}

	// Commit both updates to existing rows and inserts that a non-snapshot
	// scan could mistake for missing/duplicate rows.
	writer := db.Txn(true)
	for i := 0; i < n; i++ {
		if err := writer.Insert("main", uid576SnapshotObject(i, "new")); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		obj := uid576SnapshotObject(n+i, "new")
		obj.ID = fmt.Sprintf("added-%06d", i)
		if err := writer.Insert("main", obj); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	writer.Commit()

	expectedIDs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		expectedIDs = append(expectedIDs, fmt.Sprintf("obj-%06d", i))
	}
	ascending := append([]string(nil), expectedIDs...)
	sort.Strings(ascending)
	descending := append([]string(nil), ascending...)
	sort.Sort(sort.Reverse(sort.StringSlice(descending)))

	cases := []struct {
		name      string
		iter      ResultIterator
		firstID   string
		remaining []string
		fullOrder []string
		reverse   bool
	}{
		{name: "forward", iter: forward, firstID: ascending[0], remaining: ascending[1:], fullOrder: ascending},
		{name: "prefix", iter: prefix, firstID: ascending[0], remaining: ascending[1:], fullOrder: ascending},
		{name: "lower bound", iter: lower, firstID: ascending[0], remaining: ascending[1:], fullOrder: ascending},
		{name: "reverse lower bound", iter: reverse, firstID: descending[0], remaining: descending[1:], fullOrder: descending, reverse: true},
		{name: "get reverse", iter: getReverse, firstID: descending[0], remaining: descending[1:], fullOrder: descending, reverse: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ids, bazByID := drainUID576ObjectIDs(t, tc.iter)
			bazByID[tc.firstID] = firstBaz[tc.iter]

			allIDs := append([]string{tc.firstID}, ids...)
			if len(allIDs) != n {
				t.Fatalf("expected %d rows, got %d (%v)", n, len(allIDs), allIDs)
			}

			seen := make(map[string]struct{}, n)
			for _, id := range allIDs {
				if _, dup := seen[id]; dup {
					t.Fatalf("duplicate row %q", id)
				}
				seen[id] = struct{}{}
				if baz := bazByID[id]; baz != "old" {
					t.Fatalf("row %q mixed snapshot/new value %q", id, baz)
				}
			}

			if tc.reverse {
				if sort.StringsAreSorted(allIDs) {
					t.Fatalf("reverse iterator returned ascending order: %v", allIDs)
				}
			} else if !sort.StringsAreSorted(allIDs) {
				t.Fatalf("forward iterator order changed: %v", allIDs)
			}

			if len(ids) != len(tc.remaining) {
				t.Fatalf("remaining length = %d, want %d", len(ids), len(tc.remaining))
			}
			for i, want := range tc.fullOrder {
				if allIDs[i] != want {
					t.Fatalf("order mismatch at %d: got %q want %q (all=%v)", i, allIDs[i], want, allIDs)
				}
			}
		})
	}
}

func TestUID576IteratorUseAfterFinishReturnsError(t *testing.T) {
	for _, finish := range []struct {
		name string
		run  func(*Txn)
		want error
	}{
		{name: "commit", run: (*Txn).Commit, want: ErrTxnCommitted},
		{name: "abort", run: (*Txn).Abort, want: ErrTxnAborted},
	} {
		t.Run(finish.name, func(t *testing.T) {
			for _, create := range []struct {
				name string
				run  func(*Txn) (ResultIterator, error)
			}{
				{name: "Get", run: func(tx *Txn) (ResultIterator, error) { return tx.Get("main", "id") }},
				{name: "GetReverse", run: func(tx *Txn) (ResultIterator, error) { return tx.GetReverse("main", "id") }},
				{name: "LowerBound", run: func(tx *Txn) (ResultIterator, error) {
					return tx.LowerBound("main", "id", "closed-000000")
				}},
				{name: "ReverseLowerBound", run: func(tx *Txn) (ResultIterator, error) {
					return tx.ReverseLowerBound("main", "id", "obj-999999")
				}},
			} {
				t.Run(create.name, func(t *testing.T) {
					db := newUID576SnapshotScanDB(t, 3)
					txn := db.Txn(false)

					iter, err := create.run(txn)
					if err != nil {
						t.Fatalf("create iterator: %v", err)
					}
					if raw := iter.Next(); raw == nil {
						t.Fatal("expected an initial row")
					}

					finish.run(txn)

					if raw := iter.Next(); raw != nil {
						t.Fatalf("Next after txn finish = %#v, want nil", raw)
					}
					if err := iter.Err(); !errors.Is(err, ErrTxnClosed) {
						t.Fatalf("Err after finish = %v, want wrapping %v", err, ErrTxnClosed)
					} else if !errors.Is(err, finish.want) {
						t.Fatalf("Err after %s = %v, want %v", finish.name, err, finish.want)
					}

					// The terminal result is stable and must not start reading
					// radix nodes again.
					if raw := iter.Next(); raw != nil {
						t.Fatalf("second Next after finish = %#v, want nil", raw)
					}
					if err := iter.Err(); !errors.Is(err, finish.want) {
						t.Fatalf("stable Err = %v, want %v", err, finish.want)
					}

					if _, err := create.run(txn); !errors.Is(err, ErrTxnClosed) {
						t.Fatalf("new iterator after finish err = %v, want ErrTxnClosed", err)
					}
					if _, err := txn.First("main", "id", "obj-000000"); !errors.Is(err, ErrTxnClosed) {
						t.Fatalf("First after finish err = %v, want ErrTxnClosed", err)
					}
				})
			}
		})
	}
}

func TestUID576FilterIteratorPropagatesTxnClosedError(t *testing.T) {
	db := newUID576SnapshotScanDB(t, 1)
	txn := db.Txn(false)
	base, err := txn.Get("main", "id")
	if err != nil {
		t.Fatal(err)
	}
	iter := NewFilterIterator(base, func(interface{}) bool { return true })

	txn.Abort()
	if raw := iter.Next(); raw != nil {
		t.Fatalf("Next after abort = %#v, want nil", raw)
	}
	if err := iter.Err(); !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("FilterIterator.Err = %v, want %v", err, ErrTxnAborted)
	}
}
