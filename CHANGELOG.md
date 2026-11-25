## Unreleased

### Improvements

### Changes

### Fixed

* watch: `WatchSet.WatchCtx` no longer spawns a goroutine per 32 watched channels; large watch sets now wait with a single `reflect.Select` in the calling goroutine, so the number of goroutines held by a wait is bounded regardless of set size. `WatchCtx` also prefers reporting a triggered channel over a simultaneous context cancellation, and treats a nil context as `context.Background()`.

### Security
