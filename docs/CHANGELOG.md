# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [7.9.3] - 2026-07-23
### Added
- CI: split test jobs into mock-only and MongoDB, with separate codecov uploads (flags: `total`, `mock-only`, `mongodb`)
- Tests: `$pullAll` nested paths, `$addToSet` nested paths, `$pull` dict filters
- Tests: `$elemMatch` no-match and empty-array projection edge cases
- Tests: `CodecOptions`, `WriteConcern`, `ReadConcern` direct unit tests
- Tests: comparison tests for `$fill`, `$redact`, `$out`, `$sortByCount`, `$unwind` edges, `$regexMatch`, `$facet`, `$unionWith`

### Changed
- Version bump 7.9.2 → 7.9.3


## [7.9.2] - 2026-07-23
### Removed
- pymongo <4 compatibility guards from source code (collection.py, codec_options.py)
- ~74 `@skipIf(not helpers.HAVE_PYMONGO)` decorators from tests (pymongo no longer optional)
- pymongo 4+ skip decorators and version-gated test methods (dead code)
- Simplified always-true version guards (check_keys, try/except fallbacks)
- Bumped minimum pymongo fallback version to 4.11


## [7.9.1] - 2026-07-21
### Added
- MkDocs documentation site with Material theme (docs, dark mode, search, code copy)
- GitHub Pages auto-deploy via GitHub Actions on push to develop
- `make clean-coverage` target to remove `.coverage` and `*,cover` files

### Changed
- Documentation migrated from flat markdown files to structured MkDocs site
- New pages: `docs/index.md`, `docs/installation.md`, `docs/contributing.md`

## [7.8.2] - 2026-07-20
### Added
- `find()` positional projection `$`: returns first matching array element (#114)
- `$size` projection operator: filters documents by array length in find() (#78)
- Aggregation dot notation: verified `$project` and `$addFields` nested field extraction (#118)


## [7.8.1] - 2026-07-20
### Fixed
- `bulk_write`: `upserted_ids` keys now use real operation index instead of sequential count (#60)
- `DuplicateKeyError`: now includes `details` dict with `keyPattern` and `keyValue` matching pymongo format (#88)


## [7.9.0]
### Fixed
- `$group`: missing properties in subdocument no longer nullify the entire object (#66)
- `$group`: falsey `_id` values (`0`, `""`, `false`) handled correctly (#103)
- `$toLong`/`$toInt`: datetime conversion to epoch milliseconds (#49)
- `$add`: datetime + timedelta arithmetic support (#108)
- `$count`: accumulator alias for `$sum:1` in `$group` stage (#42)
- `$map` + `$sum` nesting: array flattening in `$sum`, expression parse fix (#76)
- `$multiply`/`$add`: Decimal128 support in arithmetic operators (#36)
- `find()` projection: computed field references (`$dotted.path`) resolved correctly (#31)
- `$project`: nested object inclusion specs return actual values, not literal 1s (#835)
- `$slice` projection-only: no longer drops other fields from result (#81)
- `$slice` projection: deep copy prevents database mutation (#30)


## [7.8.0]
### Added
- `QueryProfiler` — query capture and index coverage analyzer (docs/profiler.md)
  - Activated via `MONGOMOCK_PROFILER=1` env var with `atexit` auto-export
  - Hooks in `Collection._iter_documents`, `_update_documents`, `_delete`, and `aggregate $match`
  - Predicate normalization (values → `?`) for query pattern grouping
  - Filter coverage analysis: `full`/`partial`/`none` per query pattern
  - Sort coverage analysis against index keys
  - Partial index support (matches original filter values against `partialFilterExpression`)
  - JSON report with per-collection patterns, indexes, and coverage summary
- Documentation: AGGREGATION.md, API.md, LIMITATIONS.md
- Examples: basic CRUD, aggregation, filtering, TTL, validation

## [7.7.0] - 2026-07-06
### Added
- `ClientSession` and transaction support via `MongoClient.start_session()`
- `SessionOptions` and `TransactionOptions` classes
- Transaction flow: `start_transaction()`, `commit_transaction()`, `abort_transaction()`
- `with_transaction(callback)` convenience method
- Session parameter support in all CRUD operations (insert, update, delete, find, find_one_and_*, bulk_write, aggregate, etc.)
- Transaction isolation: writes within a transaction are buffered and only applied on commit, discarded on abort
- Independent client instances have isolated storage

## [7.6.0] - 2026-05-28
### Added
- Geospatial query operators: `$geoIntersects` and `$geoWithin` for Point-in-Polygon and Polygon intersection queries
- Geospatial distance operators: `$near` and `$nearSphere` for proximity queries with distance filtering
- `$geoNear` aggregation stage for geospatial aggregation pipelines
- Pure Python GeoJSON support: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection
- Point-in-polygon ray casting algorithm with hole (inner ring) support
- Haversine distance calculations for spherical geometry
- Euclidean distance calculations for planar geometry
- GeoJSON coordinate validation (longitude [-180, 180], latitude [-90, 90])
- Distance-based sorting on `$near` queries (implicit unless explicit `.sort()` provided)
- Support for `$maxDistance` and `$minDistance` constraints on proximity queries

## [7.5.14] - 2026-05-28
### Added
- Document validation on collections via `validator` option on `create_collection` and `collMod` command (mongomock#704)
- `Collection.options()` method returns collection options including validator
- Documents validated on insert/update against JSON Schema-like validator expressions
- `validationLevel` support: `strict` (default) and `moderate`
- `validationAction` support: `error` (default) and `warn` (no-op)
- `bypass_document_validation` flag on insert/update operations as per MongoDB API

## [7.5.13] - 2026-05-26
### Added
- `$where` operator support with Python-based evaluation (simple `this.field` expressions)
- RuntimeWarning when using `$where` to alert about security implications

### Changed
- Optimized document cloning by replacing `copy.deepcopy` with custom `_clone_document` helper
- Significant insert/update performance improvement for bulk operations
## [7.5.12] - 2026-05-26
### Added
- Unique index constraints now enforce per-element uniqueness on array fields (multikey behavior)
- `create_indexes()` forwards `partialFilterExpression` to `create_index()`

### Fixed
- `create_index()` with `unique=True` and `partialFilterExpression` skips non-matching documents during duplicate pre-check
- TTL index expiration now resolves nested dotted field names (e.g., `data.timestamp`) correctly


## [7.5.11] - 2026-05-25
### Fixed
- `$rename` operator now supports dot notation for nested field renaming
- `$addToSet` preserves boolean type distinction from numeric 1/0
- `$addToSet` with `$each` modifier handles array values correctly
- Query engine accepts `MutableMapping` objects (not just `dict`)
- `UpdateOne`/`UpdateMany` validation raises proper errors for list criteria


## [7.5.10] - 2026-05-25
### Added
- Python 3.14 and PyPy 3.10 support in test matrix and classifiers
- `Database.list_collections()` returns `CommandCursor` with collection metadata
- `Database.command()` now supports `ismaster`/`isMaster` admin command
- `Cursor.explain()` returns mock query execution plan
- `MongoClient` constructor accepts `_store` parameter

### Fixed
- `CommandCursor.alive` now tracks exhaustion; returns `False` after iteration complete
- `list_database_names()` returns active databases including defaults
- `list_collection_names()` filter behavior aligned with PyMongo
- `database.command('ismaster')` omits replica set keys for standalone mock
- `Cursor.explain()` caches `_compute_results` to avoid double computation


## [7.5.9] - 2026-05-24
### Fixed
- Aggregation missing vs null distinction: `get_value_by_dot` returns `NOTHING` sentinel instead of raising `KeyError` for missing fields (mongomock#770)
- `$group`, `$cond`, `$switch`, `$type` operators now distinguish missing fields from `None` values
- `$ifNull` now correctly handles `NOTHING` (missing) as nullish
- `$arrayElemAt` returns `NOTHING` for out-of-bounds indices instead of raising `KeyError`
- `_parse_or_nothing` method removed; replaced by `parse()` which returns `NOTHING` directly
- `ignore_missing_keys` parameter removed from `_Parser` and `_parse_expression`

## [7.5.8] - 2026-05-24
### Added
- `Database.__iter__` returns `self`, `Database.__next__` raises `TypeError`, matching PyMongo behavior (closes #64)
- 48+ edge-case tests for `$[]` positional all and `arrayFilters` operators

### Fixed
- `_lookup_array_filter` now handles dot-notation filter keys (e.g., `{'e.x': {'$gte': 1}}`)
- `_lookup_array_filter` now handles `$and`/`$or` logical operators in array filter specs
- `_array_filter_applies` now strips `filter_id` prefix from `$and`/`$or` subfilters
- `_update_document_fields_positional` no longer overwrites the `subdocument` parameter with `current_doc`, preventing incorrect subdocument carry-over between fields

## [7.5.7] - 2026-05-24
### Fixed
- `Database.__bool__` raises `NotImplementedError`, matching PyMongo behavior (closes #64)
- `MongoClient.drop_database` uses `is not None` instead of truth check on Database object (closes #64)

## [7.5.5] - 2026-05-23
### Fixed
- `$in` operator now correctly handles NaN values in query matching — NaN matches NaN (closes #105)
- `$in` operator no longer coerces across BSON types (e.g., `1` no longer matches `True`)
- Aggregation `$in` expression operator uses NaN-aware comparison (closes #105)
- `$[]` all-positional array update operator implemented for `$set`/`$unset`/`$inc`/`$pop`/`$bit`/`$max`/`$min` (closes #128)
- `arrayFilters` parameter now supported for `update_one`/`update_many` with `$[<id>]` pattern (closes #126)
- `$project` stage no longer falsely rejects `{_id: 1}` in exclusion projections (closes #122)
- `$concatArrays` now raises `OperationFailure` on empty argument list (closes #107)
- `$exp`/`$pow`/`$mod` aggregation operators wrap `OverflowError` as `OperationFailure` (closes #134)
- `$toDouble` conversion wraps `OverflowError` for huge integers (closes #134)
- `pandas.NaT` values in documents no longer crash comparison/sort — treated as datetime type (closes #135)
- Test `DBRef` stub made immutable — `__setattr__` raises `AttributeError`, matching real `bson.DBRef` (closes #83)
- `bool(collection)` now raises `NotImplementedError`, matching PyMongo behavior
- `Database` and `Collection` no longer rely on truthiness (`or`) for optional parameter defaults, ensuring compatibility with objects that forbid `bool()`
- `Database.write_concern` property added, matching PyMongo Database API

## [7.5.4] - 2026-05-23
### Added
- `$bit` update operator — bitwise AND/OR/XOR on document fields (mongomock#891)
- `$bitAnd`, `$bitOr`, `$bitXor`, `$bitNot` aggregation expression operators for bitwise operations
- `$stdDevPop` and `$stdDevSamp` group/project accumulator operators — closes #71

### Changed
- **Upstream compatibility**: tests now import `mongomock_ng as mongomock` via alias + `sys.modules` shim in conftest, enabling test reuse with upstream `mongomock` repo. `MongoClient.__repr__` outputs `mongomock.MongoClient(...)` to match.


## [7.5.3] - 2026-05-23
### Added
- `$unionWith` aggregation pipeline stage with string and `{coll, pipeline}` syntax — closes #87
- `$trim`, `$ltrim`, `$rtrim` string expression operators — closes #112
- `$toDate` type conversion expression operator (standalone, previously only via `$convert` `{to: 9}`) — closes #85;
- `$getField` field expression operator with string shorthand and `{field, input}` syntax — closes #25

### Security
- `$function` custom aggregation expression now raises `NotImplementedError` with security advisory instead of silently accepting


## [7.5.2] - 2026-05-21
### Added
- `$indexOfArray` aggregation operator and `$concatArrays` array-literal parsing improvements — replicates mongomock#739 and #931; closes #188, #148, #107

### Fixed
- `$addToSet` group accumulation preserves falsey values while still ignoring missing fields — closes #133


## [7.5.1] - 2026-05-21
### Added
- `$replaceWith` aggregation stage and update-pipeline stage aliasing `$replaceRoot` semantics — closes #96, #37
- `$merge` aggregation stage with `into`, `on`, `whenMatched` (`replace`/`merge`/`keepExisting`/`fail`) and `whenNotMatched` (`insert`/`discard`/`fail`) support; `whenMatched: pipeline` remains unimplemented — closes #89
- `$dateAdd`, `$dateSubtract`, `$dateDiff`, `$dateTrunc`, and `$dateFromString` aggregation date operators — replicates mongomock#815, #817, #772, #902; closes #181, #179, #185, #156, #110, #24

### Changed
- Refactor `$replaceRoot` to share root-replacement expression handling with `$replaceWith`


## [7.4.1] - 2026-05-21
### Changed
- Drop Python 3.14 from test matrix and classifiers — not yet stable, incompatible with runtime dependencies
- Add pymongo 4.12.0 and 4.14.0 to hatch-test matrix for broader compatibility coverage

### Fixed
- TTL document expiry no longer crashes on Python 3.13 — normalize naive datetimes to UTC-aware before subtraction in `_value_meets_expiry` (store.py)
- Replace deprecated `datetime.utcfromtimestamp` with `datetime.fromtimestamp(..., tz=timezone.utc)` in aggregation `$convert` to date handlers (aggregate.py)
- Replace deprecated `datetime.utcnow()` with `datetime.now(timezone.utc).replace(tzinfo=None)` in helpers and test suite
- Fix hatch-test matrix so environment `hatch-test.*-4.11.0` correctly installs `pymongo==4.11.0` (missing `if` clause)
- `find()` projection no longer mutates original document data (mongomock#692 — closes #191)
- `$in` operator correctly handles empty-list values (mongomock#795 — closes #184)
- `$slice` aggregation operator evaluates arguments as expressions (mongomock#819 — closes #178)
- `$addToSet` with `$each` correctly deduplicates repeated values across runs (mongomock#847 — closes #170)
- `$redact` aggregation stage now supported (mongomock#860 — closes #169)
- `Cursor.collation` matches PyMongo 4.x method signature (mongomock#895 — closes #158)
- `$in` operator matches whole arrays in document values (mongomock#919 — closes #154)
- `BulkOperationBuilder.add_update` and `_update` support `sort` parameter (mongomock#933 — closes #147)
- NaN comparison handling in filter `$eq` operator (mongomock#936 — closes #145)
- Replace deprecated `datetime.utcnow` with `datetime.now(UTC)` on Python 3.11+ (mongomock#944 — closes #144)

## [7.4.0] - 2026-05-21
### Added
- `$lookup`: DBRef `.$id` support in `localField` — resolves `refs.$id` through DBRef arrays (mongomock#878 — closes #164)
- `$lookup`: DBRef `$id` filtering support in filter matching (mongomock#884 — closes #162)
- DBRef dotted-field traversal in `helpers.get_value_by_dot` for `$lookup` join conditions

## [7.3.0] - 2026-05-20
### Added
- `$setIntersection`, `$setDifference`, `$setIsSubset`, `$anyElementTrue`, `$allElementsTrue` set operators in aggregation (mongomock#737 — closes #189, mongomock#840 — closes #172, mongomock#842 — closes #171)
- `helpers.to_hashable` — convert values to hashable representations for set operations

## [7.1.1] - 2026-05-20
### Fixed
- `$setWindowFields` error message typo `$setWindowsFields` → `$setWindowFields`

### Added
- `.agents/references/MDB7_COMPAT.md` — MongoDB 7+ compatibility gap tracker

## [7.2.1] - 2026-05-20
### Added
- Repository reference files (`CHANGELOG.md`, `README.md`, `CLAUDE.md`, etc.) migrated into `.agents/references/` for persistent agent context

## [7.2.0] - 2026-05-20
### Added
- `$convert`: support `onError`/`onNull` + `to: double`/`bool`/`date`/`objectId` (mongomock#864 — closes #167)
- `$setWindowFields`: `$sum`/`$avg`/`$min`/`$max`/`$first`/`$last`/`$push`/`$addToSet`/`$count`/`$documentNumber`/`$rank`/`$denseRank` + `window` bounds (mongomock#821 — closes #176)
- `$fill`: `method` (locf/linear), `sortBy`, `partitionByFields` support (mongomock#892 — closes #160)

### Changed
- Version now defined statically in `mongomock_ng/__version__.py` (single source of truth), replacing dynamic `hatch-vcs` — closes #197
- Tag creation automated via `tag-on-merge` workflow on PR merge to `develop`, replacing manual `make release`

## [7.1.0] - 2026-05-20
### Added
- `CLAUDE.md` with token-efficiency techniques for opencode (opencode instructions)
- `.github/copilot-instructions.md` moved to correct location

### Fixed
- `$type` now returns `"double"` for `float` values (mongomock#929)
- `$setWindowFields` no longer produces duplicate documents with multiple `output` fields (mongomock#821)

### Added
- `$sortByCount` aggregation stage (mongomock#896 — closes #157)
- `$fill` aggregation stage (mongomock#892 — closes #160)
- `$convert` aggregation operator with type dispatch (mongomock#864 — closes #167)
- `$reduce` array aggregation operator (mongomock#820 — closes #177)
- `$setWindowFields` aggregation stage with `$shift` operator (mongomock#821 — closes #176)
- `$unset` aggregation stage with nested field support (mongomock#925 — closes #151)
- `$type` aggregation operator with date support (mongomock#929 — closes #150)
- `$round` aggregation operator with `<place>` parameter (mongomock#930 — closes #149)
- `$toObjectId` type conversion operator (mongomock#935 — closes #146)
- `$timezone` expression support in date operators (mongomock#822 — closes #175)

## [7.0.2] - 2026-05-19
### Added
- `comment` parameter support for `find()`, `find_one()`, `count_documents()`,
  `estimated_document_count()`, `distinct()`, `bulk_write()`, `insert_one()`,
  `delete_one()`, `update_one()`, `find_one_and_update()`, `find_one_and_replace()`
  (mongomock#921, mongomock#922, mongomock#915, mongomock#831)
- `hint` parameter support for `find()`, `find_one()`, `count_documents()`, `distinct()`
  (mongomock#922)
- `let` parameter support for `delete_one()` (mongomock#831)
- Accepts `comment` and `hint` via `_IGNORED_FEATURES` in `not_implemented.py`

### Changed
- Updated `find()` signature to accept `comment` and `hint` before `**kwargs`

## [7.0.0] - 2026-05-18
### Changed
- **Rebranded from `mongomock` to `mongomock_ng`** — all imports must use the new package name
- Versioned to against MongoDB 7.0.34 server behavior
- Migrated build system to Hatch with PEP 621
- Replaced `tox` with Hatch matrix testing (Python 3.10–3.13 × PyMongo 3/4/4.11/7.0/latest/none)
- Updated CI to GitHub Actions with lint, type-check, test matrix, and code coverage
- Switched code formatting to Ruff
- Updated `hatch.toml` test matrix for PyMongo 4.11 and 7.0 compatibility
- Updated `Makefile` with docker and release helpers

### Removed
- Legacy build files: `setup.py`, `setup.cfg`, `tox.ini`, `.travis.yml`

### Fixed
- Compatibility with PyMongo 4.11 (BSON validation error messages, BulkOperationBuilder)
- Codec options not forwarded to update methods
- mypy type errors

### Notes
- This is a major release. Consumers should run their project's test-suite against
  `mongomock-ng` 7.0.0 and review any deprecation warnings.

## [4.4.0] - tbd
### Added
- Add support for Python 3.13

### Changed
- Remove legacy syntax constructs using `pyupgrade --py39-plus`

### Removed
- Remove support for deprecated Python version 3.8


## [4.3.0] - 2024-11-16
### Added
- Support for aggregation pipelines in updates [@maximkir-fl](https://github.com/maximkir-fl)

### Changed
- Remove legacy syntax constructs using `pyupgrade --py38-plus`

### Fixed
- The Mongo Python driver did refactor the `gridfs` implementation, so that the patched code had to
  be adapted.

## [4.2.0] - 2024-09-11
### Changed
- Switch to [hatch](https://hatch.pypa.io) as build system.
- Switch to [PEP 621](https://peps.python.org/pep-0621/) compliant project setup.
- Updated the license to [ISC](https://en.wikipedia.org/wiki/ISC_license).

### Removed
- The [setuptools](https://setuptools.pypa.io) specific files e.g. `setup.cfg` and `setup.py` have
  been removed in the scope of the switch to `hatch`.
- Remove support for deprecated Python versions (everything prior to 3.8)


[7.8.1]: https://github.com/engFelipeMonteiro/mongomock-ng/compare/7.8.0...7.8.1
[7.0.0]: https://github.com/engFelipeMonteiro/mongomock-ng/compare/4.3.0...7.0.0
[4.4.0]: https://github.com/engFelipeMonteiro/mongomock-ng/compare/4.3.0...4.4.0
[4.3.0]: https://github.com/engFelipeMonteiro/mongomock-ng/compare/4.2.0...4.3.0
[4.2.0]: https://github.com/engFelipeMonteiro/mongomock-ng/compare/4.1.3...4.2.0
