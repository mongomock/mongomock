# Completed Features — Post PR #195

3 features completed after PR #195 consolidation (commit `3d2342b`).

---

## 1. `$convert` — `onError`/`onNull` + missing `to` types

### What
`mongomock_ng/aggregate.py:_handle_convert` (~line 1053)

### Added
- `_handle_type_convertion_to_double` — float conversion. null→null, bool→0.0/1.0, int→float, string→parse float or error, Decimal128→float
- `_handle_type_convertion_to_bool` — MongoDB truth: 0/0.0/false→False, non-zero/string/object/array→True, null→null
- `_handle_type_convertion_to_date` — int/float→epoch ms→datetime, string→ISO parse, ObjectId→generation_time
- `onError` — wraps handler in try/except, returns onError value on failure instead of raising
- `onNull` — if input is None, returns onNull instead of None

### `_CONVERT_TO_HANDLERS` now complete
All 10 types: `double/string/objectId/bool/date/int/long/decimal` + numeric aliases `1/2/7/8/9/16/18/19`.

### Tests
`test__mongomock.py:test_aggregate_convert` — covers all types + onError + onNull.

---

## 2. `$setWindowFields` — 12 operators + `window`

### What
`mongomock_ng/aggregate.py:_accumulate_set_window_fields` (~line 1376)

### Helpers
- `_get_window_bounds(window_spec, total_len)` — returns `[(start,end)]` per doc index. Supports `documents: ["unbounded"|"current"|<int>, "unbounded"|"current"|<int>]`. Default = entire partition.
- `_sort_keys_equal(doc1, doc2, sort_by)` — compares sort key values for rank.

### Operators
| Operator | Behavior |
|---|---|
| `$sum` | Sum non-null values in window |
| `$avg` | Average of non-null values in window |
| `$min` | Min of non-null values in window |
| `$max` | Max of non-null values in window |
| `$first` | First value in window (even if null) |
| `$last` | Last value in window (even if null) |
| `$push` | All values in window, ordered |
| `$addToSet` | Unique values in window, insertion order |
| `$count` | Count of docs in window |
| `$documentNumber` | 1-indexed position in partition |
| `$rank` | Sort-key rank with gaps (requires sortBy) |
| `$denseRank` | Sort-key rank without gaps (requires sortBy) |
| `$shift` | Existing impl (pre-PR195) |

Not implemented: `$stdDevPop/$stdDevSamp/$covariancePop/$covarianceSamp/$derivative/$integral/$expMovingAvg` — raise NotImplementedError.

### Tests (test__collection_api.py)
7 new methods: sum_avg, min_max_first_last, push_add_to_set, count, document_number, rank_dense_rank, window_bounds.

---

## 3. `$fill` — method/sortBy/partitionByFields

### What
`mongomock_ng/aggregate.py:_handle_fill` (~line 1704)

### Behavior
- No method + value: same as original (fill null/missing with value)
- `method: "locf"`: last non-null carries forward per partition. First doc null → null (or `value` fallback).
- `method: "linear"`: linear interpolation between surrounding non-null values. Numeric only. Boundaries → null (or `value` fallback).
- `sortBy`: sort before fill (delegates to `_handle_sort_stage`)
- `partitionByFields`: groupby before fill, each partition independent
- Multiple output fields: each field may have its own method/value

### Tests (test__collection_api.py)
5 new methods: locf, linear, partition_by_fields, multiple_fields, value_and_missing.
