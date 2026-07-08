# QueryProfiler

## Overview

QueryProfiler is a diagnostic mode that captures every query executed through
mongomock-ng during a test run, normalizes filter predicates (replacing values
with `?`), groups identical query shapes together, and analyzes whether each
query pattern is covered by the collection's indexes.

The output is a JSON report that answers:

- Which queries ran, how often, and in which tests?
- Which index (if any) covers each query pattern?
- Are there **uncovered** queries that would trigger a collection scan in real
  MongoDB?

The profiler is **zero-config** — set one environment variable and run your
existing test suite as-is.

## Quickstart

```bash
MONGOMOCK_PROFILER=1 pytest
```

After the suite completes, a `mongomock-profiler-report.json` file appears in
the working directory. No code changes, no imports, no fixtures.

```json
{
  "metadata": {
    "total_queries": 142,
    "total_patterns": 18,
    "filter_coverage_summary": {
      "full": 12,
      "partial": 4,
      "none": 2
    },
    "sort_coverage_summary": {
      "full": 3,
      "none": 15
    }
  },
  "collections": { ... }
}
```

## API Reference

### `get_profiler() -> QueryProfiler`

Singleton accessor. Returns the global `QueryProfiler` instance, creating it on
first call.

```python
from mongomock_ng.profiler import get_profiler

profiler = get_profiler()
```

### `QueryProfiler.start(output_path: str = 'mongomock-profiler-report.json')`

Enable recording and clear any prior records. Optionally set the default export
path.

```python
profiler.start('/tmp/my-report.json')
```

### `QueryProfiler.stop()`

Disable recording. Already-captured records are preserved.

```python
profiler.stop()
```

### `QueryProfiler.reset()`

Clear all captured records and index snapshots.

```python
profiler.reset()
```

### `QueryProfiler.record(filter, collection, operation, indexes=None, sort=None)`

Add a query record. Called automatically by the internal hooks — you should
not need to call this manually.

| Parameter    | Type          | Description                                          |
|--------------|---------------|------------------------------------------------------|
| `filter`     | `dict`        | Original query filter document                       |
| `collection` | `str`         | Fully qualified collection name (e.g. `mydb.users`) |
| `operation`  | `str`         | `find`, `count`, `update`, `delete`, or `aggregate`  |
| `indexes`    | `dict \| None` | Snapshot of the collection's index configurations    |
| `sort`       | `list \| None` | Sort specification from the query (thread-local)     |

### `QueryProfiler.export_json(path: str | None = None)`

Serialize the full report to JSON and write it to disk. Uses the path from
`start()`, or the argument, or falls back to `mongomock-profiler-report.json`.

```python
profiler.export_json('custom-report.json')
```

### `QueryProfiler.report() -> dict`

Return the report as a Python dictionary (same structure as the JSON output).

```python
report = profiler.report()
print(report['metadata']['total_queries'])
```

### `normalize_predicate(filter: Any) -> Any`

Replace all literal values in a filter with `?`, preserving the operator
structure. Useful standalone for test assertions or custom reporting.

```python
from mongomock_ng.profiler import normalize_predicate

normalize_predicate({"status": "active", "age": {"$gt": 18}})
# → {"status": {"$eq": "?"}, "age": {"$gt": "?"}}
```

## How It Works

### Collection Hooks

The primary hook lives in `Collection._iter_documents()`, the single entry
point through which `find()`, `find_one()`, `count_documents()`,
`update_many()`, `delete_many()`, and similar operations all pass. Each call
triggers `get_profiler().record(...)` with the current filter, operation,
index snapshot, and sort context.

Operation type is set via thread-local helpers before the operation-specific
method is entered:

| Helper                       | Set by                     |
|------------------------------|----------------------------|
| `_profiler_set_operation()`  | `find`, `update`, `delete`, `count` paths |
| `_profiler_set_sort()`       | `find()` before cursor creation           |

### Aggregation Hook

The `$match` pipeline stage handler in `aggregate.py` calls
`get_profiler().record(...)` with the stage spec and `operation='aggregate'`.
This captures every aggregation pipeline that contains a `$match` stage.

### Index Snapshots

Indexes are captured **on first query** per collection. The
`dict(self._list_all_indexes())` call returns the full index configuration at
the time of the query. Because indexes in tests are typically created in setup
and unchanged during the test, a single snapshot per collection suffices.

### Sort Context

Sort is tracked via `threading.local()` because `sort()` is called on the
cursor (after `find()`) while the hook fires inside `_iter_documents`. The
thread-local `_profiler_set_sort(sort)` / `_profiler_get_sort()` bridge this
gap.

### Auto-export on Exit

When `MONGOMOCK_PROFILER=1` is set, the module's top-level code calls
`get_profiler().start()` and registers `auto_export()` via `atexit`. If any
records were captured, the JSON report is written when the process exits.

## Predicate Normalization

The profiler normalizes query filters so that queries with the same **shape**
but different values cluster together. This is essential for coverage analysis:
`{"status": "active"}` and `{"status": "inactive"}` are the same pattern.

### Algorithm (`normalize_predicate`)

1. **Dict values** → recurse into each key-value pair
2. **List items** → recurse into each element
3. **Scalars** → replace with `?`

The result preserves the operator structure:

| Filter                                      | Normalized                                   |
|---------------------------------------------|----------------------------------------------|
| `{"status": "active"}`                      | `{"status": {"$eq": "?"}}`                   |
| `{"age": {"$gt": 18}}`                      | `{"age": {"$gt": "?"}}`                      |
| `{"tags": ["a", "b"]}`                      | `{"tags": ["?", "?"]}`                       |
| `{"$or": [{"x": 1}, {"y": 2}]}`             | `{"$or": [{"x": {"$eq": "?"}}, {"y": ...}]}` |

Note that top-level bare keys become `{"$eq": "?"}` (not just `"?"`) because
MongoDB treats `{"field": value}` as `{"field": {"$eq": value}}`.

### Recursive Ops

`$and`, `$or`, `$nor`, and `$not` are recursed into — their sub-expressions
are normalized identically.

## Coverage Analysis

### Filter Coverage

For each normalized predicate, the profiler extracts the set of queried fields
and compares them against index keys:

| Classification | Condition                                        |
|----------------|--------------------------------------------------|
| `full`         | Every queried field is covered by at least one index |
| `partial`      | Some queried fields are indexed, some are not     |
| `none`         | No queried field appears in any index             |

When `q_fields` is empty (e.g. `{}` filter), coverage is reported as `none`.

### Sort Coverage

| Classification | Condition                                          |
|----------------|----------------------------------------------------|
| `full`         | An index key list starts with the sort fields (direction can be inverted) |
| `partial`      | A single-field sort matches a field in a compound index (not as first key — first-key prefix match is `full`) |
| `none`         | No index matches the sort                          |

Direction tolerance: `[("status", 1)]` is covered by an index with key
`[("status", -1)]` (asc ↔ desc swap counts as `full`).

### Partial Indexes

Partial indexes (`partialFilterExpression`) are matched against the
**original** filter values (not the normalized predicate). If the query filter
does not satisfy the partial expression, the index is **not** considered as
covering that query, even if the field names align.

## JSON Report Format

```json
{
  "metadata": {
    "total_queries": 5,
    "total_patterns": 2,
    "filter_coverage_summary": {
      "full": 1,
      "partial": 0,
      "none": 1
    },
    "sort_coverage_summary": {
      "full": 1,
      "none": 1
    }
  },
  "collections": {
    "mydb.users": {
      "indexes": [
        {
          "name": "status_1",
          "key": [["status", 1]]
        },
        {
          "name": "age_-1_name_1",
          "key": [["age", -1], ["name", 1]]
        }
      ],
      "patterns": [
        {
          "predicate": {
            "status": {"$eq": "?"}
          },
          "operations": ["find"],
          "count": 3,
          "tests": [
            "test_users.test_find_active",
            "test_users.test_find_inactive"
          ],
          "filter_coverage": "full",
          "sort_coverage": "none",
          "covered_fields": ["status"],
          "uncovered_fields": [],
          "covering_index": "status_1",
          "sort": null
        },
        {
          "predicate": {
            "email": {"$eq": "?"}
          },
          "operations": ["find", "update"],
          "count": 2,
          "tests": [
            "test_users.test_find_by_email"
          ],
          "filter_coverage": "none",
          "sort_coverage": "none",
          "covered_fields": [],
          "uncovered_fields": ["email"],
          "covering_index": null,
          "sort": null
        }
      ]
    }
  }
}
```

### Top-level keys

| Key            | Description                                            |
|----------------|--------------------------------------------------------|
| `metadata`     | Totals across all collections                          |
| `collections`  | Per-collection map, keyed by fully qualified name      |

### Per-pattern keys

| Key                | Description                                               |
|--------------------|-----------------------------------------------------------|
| `predicate`        | Normalized filter (values replaced with `?`)              |
| `operations`       | Operation types that produced this pattern                |
| `count`            | Number of queries matching this pattern                   |
| `tests`            | Test identifiers that triggered this pattern              |
| `filter_coverage`  | `full`, `partial`, or `none`                              |
| `sort_coverage`    | `full` or `none`                                          |
| `covered_fields`   | Fields in the predicate that appear in an index           |
| `uncovered_fields` | Fields in the predicate that appear in **no** index       |
| `covering_index`   | Name of the first index that covers the predicate, or `null` |
| `sort`             | Sort specification from the query (or `null`)             |

## Usage Examples

### Via Environment Variable (recommended)

```bash
MONGOMOCK_PROFILER=1 pytest tests/ -x
```

Zero code changes. Report written on exit.

To customize the output path:

```bash
MONGOMOCK_PROFILER=1 python -c "
import os
os.environ['MONGOMOCK_PROFILER'] = '1'
from mongomock_ng.profiler import get_profiler
get_profiler().start('custom-report.json')
"
pytest
```

### Via conftest.py (teardown check)

```python
# conftest.py
import pytest
from mongomock_ng.profiler import get_profiler


@pytest.fixture(autouse=True)
def profiler_teardown(request):
    profiler = get_profiler()
    if not profiler.enabled:
        profiler.start()
    yield
    # At the end of the session, export and fail if uncovered queries exist
    if request.session.testscollector:
        report = profiler.report()
        uncovered = sum(
            1 for coll in report['collections'].values()
            for p in coll['patterns']
            if p['filter_coverage'] == 'none'
        )
        profiler.export_json()
        if uncovered:
            pytest.exit(f"Found {uncovered} uncovered query pattern(s). See profiler report.")
```

### Programmatic (within a single test)

```python
from mongomock_ng.profiler import get_profiler

def test_query_patterns():
    profiler = get_profiler()
    profiler.start()
    profiler.reset()

    collection.insert_many([
        {"_id": 1, "status": "active", "age": 25},
        {"_id": 2, "status": "inactive", "age": 30},
    ])
    collection.create_index([("status", 1)])

    list(collection.find({"status": "active"}))
    list(collection.find({"status": "inactive"}))
    list(collection.find({"age": {"$gt": 20}}))

    report = profiler.report()
    patterns = report['collections']['mydb.test']['patterns']

    assert len(patterns) == 2  # two normalized shapes
    assert patterns[0]['count'] == 2  # status queries cluster
    assert patterns[0]['filter_coverage'] == 'full'
    assert patterns[1]['filter_coverage'] == 'none'  # age not indexed
```

## See Also

- [MongoDB Query Plans & Indexes](https://www.mongodb.com/docs/manual/core/query-plans/)
- [MongoDB explain() Output](https://www.mongodb.com/docs/manual/reference/command/explain/)
