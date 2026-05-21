Complete the 3 partially-implemented aggregation features from PR #195.

## Context

PR #195 consolidated 10 aggregation operators from mongomock into
mongomock-ng (commit `3d2342b`). Three issues remain partial.

Files:
- `mongomock_ng/aggregate.py` — operator handlers + pipeline stages
- `.agents/references/aggregation-prs/REVIEW_PR195.md` — code review
- `tests/test__collection_api.py`
- `tests/test__mongomock.py`

---

## 1. #167 — `$convert` — missing `to` types + `onError`/`onNull`

### What exists
`_handle_convert` in `aggregate.py:1053-1073` dispatches via
`_CONVERT_TO_HANDLERS` (line 1097). Supported types: `"string"`/2,
`"int"`/16, `"long"`/18, `"decimal"`/19.

### What's missing
- `"double"`/1, `"objectId"`/7, `"bool"`/8, `"date"`/9 →
  `NotImplementedError` via `_raise_convert_not_implemented`
- `onError`/`onNull` → `NotImplementedError` (line 1062-1067)

### Requirements
- `to: "double"` / `to: 1`: convert input to float/Python float.
  Follow MongoDB semantics (null→null, missing→null, bool→0/1.0,
  int→float, string→parse float, invalid string→error).
- `to: "bool"` / `to: 8`: convert to bool using MongoDB truth rules.
- `to: "date"` / `to: 9`: convert to datetime.
- `to: "objectId"` / `to: 7`: convert to ObjectId.
- `onError`: if conversion fails, return this value instead of raising.
- `onNull`: if input is null/missing, return this value instead of null.

Each `to` type gets a handler in `_CONVERT_TO_HANDLERS` (replace the
`_raise_convert_not_implemented` placeholders).

### Tests
- `test_aggregate_convert` in both test files: add cases for each new
  `to` type.
- Error cases: invalid string for double/date/objectId, onError
  returning fallback, onNull returning fallback.

---

## 2. #176 — `$setWindowFields` — more window operators + `window`

### What exists
- `set_window_fields_operators` list (line 71-92) declares 20 operators.
- `_accumulate_set_window_fields` handles only `$shift` (line 1348-1360).
- `_handle_set_window_fields_stage` handles `partitionBy` and `sortBy`.

### What's missing
- All other operators (`$sum`, `$avg`, `$min`, `$max`, `$first`,
  `$last`, `$push`, `$addToSet`, `$count`, `$stdDevPop`,
  `$stdDevSamp`, `$covariancePop`, `$covarianceSamp`, `$derivative`,
  `$integral`, `$expMovingAvg`, `$denseRank`, `$documentNumber`,
  `$rank`) → `NotImplementedError`
- `window` field → `NotImplementedError` (line 1344-1348)

### Requirements
- `$sum`, `$avg`, `$min`, `$max`: reduce over partition window.
- `$first`, `$last`: first/last value in window.
- `$push`, `$addToSet`: accumulate values.
- `$count`: count of documents in window.
- `$documentNumber`, `$rank`, `$denseRank`: position-based.
- `window`: support `documents: [<start>, <end>]` and `range` bounds.
  Default is `{ documents: ["unbounded", "unbounded"] }` (entire
  partition).

### Tests
- Add tests for each new operator.
- Edge cases: empty partition, single doc partition, null values.
- `window` bounds: unbounded to current, current to unbounded, sliding.

---

## 3. #160 — `$fill` — `method`, `sortBy`, `partitionByFields`

### What exists
`_handle_fill` in `aggregate.py:1704-1711` — bare bones: reads first
`output` key, fills missing field with `value`.

### What's missing
- `method`: `"linear"` (interpolation) or `"locf"` (last observation
  carried forward). Default behavior when no method specified is to use
  `value`.
- `sortBy`: sort documents before filling.
- `partitionByFields`: fill within each partition independently.

### Requirements
- `method: "locf"`: for each partition sorted by `sortBy`, carry last
  non-null value forward. First doc with null → remains null.
- `method: "linear"`: linear interpolation between surrounding non-null
  values. Only for numeric fields.
- `partitionByFields`: group by these fields before filling.
- Multiple output fields: each field may have its own `value`/`method`.
- `value` as fallback when method can't determine (e.g. first doc in
  `locf`).

### Tests
- `method: "locf"` with null gaps.
- `method: "linear"` with numeric interpolation.
- `partitionByFields` groups.
- `sortBy` ordering.
- Multiple output fields simultaneously.
- Edge: all-null field, single doc, mixed types.
