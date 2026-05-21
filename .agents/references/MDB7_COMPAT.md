# MDB 7+ Compatibility Gap

**Purpose**: Track MongoDB 7+ compatibility gaps vs mongomock-ng
**Type**: compat-gap
**Covers**: missing stages, missing window operators, not-tracked features, known bugs
**Refresh**: after every new feature impl or code review finding
**Last-updated**: 2026-05-21

## Legend
- ❌ not implemented
- ⚠️ partial (stub/raises NotImplementedError)
- ✅ implemented

## Aggregation Pipeline Stages

| Stage | Status | Notes |
|-------|--------|-------|
| `$setWindowFields` | ⚠️ | 12/19 operators impl; missing `$top`, `$topN`, `$bottom`, `$bottomN`, `$derivative`, `$integral`, `$expMovingAvg`, `$covariancePop`, `$covarianceSamp` |
| `$fill` | ✅ | locf/linear/value/partitionByFields/sortBy |
| `$merge` | ⚠️ | Supports `into`, `on`, `whenMatched` except `pipeline`, and `whenNotMatched` |
| `$redact` | ✅ | `$$KEEP` / `$$PRUNE` / `$$DESCEND` |
| `$replaceWith` | ✅ | Alias of `$replaceRoot` expression form |
| `$sortByCount` | ✅ | |
| `$convert` | ✅ | All 10 types + onError/onNull |
| `$reduce` | ✅ | |
| `$unset` | ✅ | Nested paths |
| `$unionWith` | ❌ | MDB 4.4+ |
| `$documents` | ❌ | MDB 5.0+ |
| `$search` | ❌ | Atlas Search (expected absent) |
| `$vectorSearch` | ❌ | Atlas Search (expected absent) |

## `$setWindowFields` Details

### Implemented operators (12/19)
`$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`, `$count`, `$documentNumber`, `$rank`, `$denseRank`, `$shift`

### Missing operators (7)
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

## Not Tracked (explicitly absent)
- changeStreams
- timeseries collections
- clustered indexes
- Queryable Encryption (QE)
- Atlas Search (`$search`, `$vectorSearch`)
- MongoDB 7.0+ new query operators not yet encountered

## Test Status
- Tested against MongoDB 7.0.34 per CHANGELOG
- No formal compatibility matrix in repo

## Known Bugs
- `aggregate.py:1757` — "setWindowsFields" typo (fixed)
