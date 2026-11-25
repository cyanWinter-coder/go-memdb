## Unreleased

### Improvements

### Changes

### Fixed

* txn: `Insert` is now atomic: all index entries are computed and validated before any of them is applied, so a failure part way through (indexer error, missing required index value or unique index conflict) no longer leaves a partially indexed object behind and the transaction remains usable.
* txn: `Insert` now enforces the uniqueness constraint of unique indexes instead of silently overwriting the conflicting entry. A conflict is reported as a `*UniqueIndexError` carrying the table, index, key and the object currently holding the key.

### Security
