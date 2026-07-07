# MDB 7+ Compatibility Gap

**Purpose**: Track MongoDB 7+ compatibility gaps vs mongomock-ng
**Type**: compat-gap
**Covers**: missing stages, missing window operators, not-tracked features, known bugs
**Refresh**: after every new feature impl or code review finding
**Last-updated**: 2026-07-07

## Legend
- ❌ not implemented
- ⚠️ partial (stub/raises NotImplementedError)
- ✅ implemented

## Aggregation Pipeline Stages

| Stage | Status | Notes |
|-------|--------|-------|
| `$setWindowFields` | ⚠️ | 12/19 operators impl; missing `$top`, `$topN`, `$bottom`, `$bottomN`, `$derivative`, `$integral`, `$expMovingAvg`, `$covariancePop`, `$covarianceSamp` |
| `$fill` | ✅ | locf/linear/value/partitionByFields/sortBy |
| `$merge` | ✅ | Supports `into`, `on`, `whenMatched` except `pipeline`, and `whenNotMatched` |
| `$redact` | ✅ | `$$KEEP` / `$$PRUNE` / `$$DESCEND` |
| `$replaceWith` | ✅ | Alias of `$replaceRoot` expression form |
| `$sortByCount` | ✅ | |
| `$stdDevPop` | ✅ | Group/project accumulator (7.5.4). `$setWindowFields` variant still missing |
| `$stdDevSamp` | ✅ | Group/project accumulator (7.5.4). `$setWindowFields` variant still missing |
| `$convert` | ✅ | All 10 types + onError/onNull |
| `$reduce` | ✅ | |
| `$unset` | ✅ | Nested paths |
| `$unionWith` | ✅ | MDB 4.4+. String or {coll, pipeline} form |
| `$documents` | ❌ | MDB 5.0+ |
| `$search` | ❌ | Atlas Search (expected absent) |
| `$vectorSearch` | ❌ | Atlas Search (expected absent) |

## `$setWindowFields` Details

### Implemented operators (12/19)
`$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`, `$count`, `$documentNumber`, `$rank`, `$denseRank`, `$shift`

### Missing operators (9)
`$stdDevPop`, `$stdDevSamp` — MDB 5.0+; available as group/project accumulators but NOT in window context
`$top`, `$topN`, `$bottom`, `$bottomN` — MDB 5.2+ window operators
`$derivative`, `$integral`, `$expMovingAvg` — MDB 5.0+ window ops (stub only)
`$covariancePop`, `$covarianceSamp` — MDB 5.0+ window ops (stub only)

### Window clause
| Format | Status |
|--------|--------|
| `documents` | ✅ [unbounded, current, <int>] range bounds |
| `range` | ❌ MDB 5.0+ range-based windows |
| `unit` | ❌ MDB 5.0+ time-unit windows |

### `$shift` gaps
- `unit` param not supported (MDB 7.0+ allows time-based offset)

## Date Expression Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$dateAdd` | ✅ | `timezone` not implemented |
| `$dateSubtract` | ✅ | Mirrors `$dateAdd`; `timezone` not implemented |
| `$dateDiff` | ⚠️ | `week`, `timezone`, `startOfWeek` not implemented |
| `$dateTrunc` | ⚠️ | Core unit truncation implemented; `binSize`, `timezone`, `startOfWeek` not implemented |
| `$dateFromString` | ⚠️ | ISO 8601 parsing implemented; `format` and `timezone` not implemented |

## Array Expression Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$concatArrays` | ✅ | Supports array expressions and nested array literals with parsed field references |
| `$indexOfArray` | ✅ | Supports optional `start` / `end`; returns `null` for missing or `null` arrays |

## Geospatial Query Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$geoIntersects` | ✅ | GeoJSON `$geometry`; all GeoJSON doc types |
| `$geoWithin` | ✅ | GeoJSON `$geometry`; all GeoJSON doc types |
| `$near` | ✅ | Planar distance; GeoJSON + legacy array syntax; doc Point only |
| `$nearSphere` | ✅ | Spherical (haversine) distance; doc Point only |
| `$geoNear` (agg) | ✅ | `_handle_geonear_stage` in `aggregate.py:2821` |
| `distanceMultiplier` | ✅ | `$geoNear` option |
| `includeLocs` | ✅ | `$geoNear` option |
| Custom CRS | ❌ | `crs` field in `$geometry` ignored |
| Doc non-Point geometry | ✅ | `geo_intersects`/`geo_within` support all GeoJSON types |
| 2dsphere index sim | ✅ | Required for `$near`/`$nearSphere`/`$geoNear`; raises `OperationFailure` if missing |

**Module**: `mongomock_ng/geospatial.py` (435 lines)
**Tests**: `tests/test__geospatial.py` (77 tests, all pass)
**Docs**: `docs/geospatial.md`

## Known Bugs
- `aggregate.py:1757` — "setWindowsFields" typo (fixed)
- `collection.py:_bit_updater` — `$bit` on existing non-int field silently coerces instead of raising error. Should validate `doc_value` is int before bitwise op.
- `collection.py:_bit_updater` — `$bit` on nonexistent field treats as `0` (matches MongoDB ✅)
- `aggregate.py:745` — bitwise aggregation ops (`$bitAnd`/`$bitOr`/`$bitXor`) accept `bool` subtypes (`isinstance(True, int)` is `True` in Python). MongoDB would reject bool. Should add `type(x) is int` check.
- `store.py:162` — `doc.get(ttl_field_name)` does not resolve nested dotted field names (e.g., `data.timestamp`). Fixed in 7.5.12 by replacing with `helpers.get_value_by_dot`.

## Not Tracked (explicitly absent)
- changeStreams
- timeseries collections
- clustered indexes
- unique multikey indexes on array fields — fixed in 7.5.12 by expanding array values into per-element key entries in `_ensure_uniques` and `create_index`
- Queryable Encryption (QE)
- Atlas Search (`$search`, `$vectorSearch`)
- MongoDB 7.0+ new query operators not yet encountered
