## Unreleased

### Improvements

* Atomic commit view: every commit now publishes its data, indexes, change log and watch notifications under a single monotonically increasing generation (`MemDB.Generation`, `MemDB.CommittedChanges`). Watch channels fire only after the committed data is readable.
* Add `MemDB.Subscribe` for ordered per-commit `CommitEvent` delivery with non-blocking broadcast (slow consumers are terminated with `ErrLagged` instead of delaying commits), and `MemDB.Close` to terminate all subscriptions without leaking channels.

### Changes

### Fixed

### Security
