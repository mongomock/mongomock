# Known Limitations

## Not implemented (raise NotImplementedError)

- Change streams (`watch()`)
- `find_raw_batches()`, `aggregate_raw_batches()`
- Sessions and transactions (basic ClientSession exists but limited)
- `$out` with aggregation (basic support only)
- Full `$merge` stage support
- Collation on most operations
- `hint` parameter on delete operations
- `$query`/`$orderby` legacy format

## Behavioral differences

- `aggregate()` returns `list` instead of `CommandCursor`
- TTL indexes are simulated via `simulate_ttl()` — not automatic
- No network overhead — all in-memory
- `SERVER_VERSION` defaults to `7.0.34` (configurable via env `MONGODB`)
- `ObjectId` generation is not guaranteed globally unique (mock)
- Write concerns are accepted but always acknowledged

## Partial implementations

- `$lookup` pipeline syntax works but limited sub-pipeline support
- `$setWindowFields` — basic window functions, not all boundaries
- `$fill` — linear/method not implemented
- `$graphLookup` — depth-first only, no `maxDepth` validation
- Geospatial: 2dsphere index simulated, only GeoJSON geometries
- `$convert` — most conversions work, some edge cases differ

## Not planned

- Real network/replica set behavior
- Sharding
- Actual persistent storage
