# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [7.5.10] - 2026-05-25
### Added
- `Database.list_collections()` returns `CommandCursor` with collection metadata (closes #729)
- `Database.command()` now supports `ismaster`/`isMaster` admin command (closes #764)
- `Cursor.explain()` returns mock query execution plan (closes #843)
- `MongoClient` constructor accepts `_store` parameter (closes #841)

### Fixed
- `CommandCursor.alive` now tracks exhaustion; returns `False` after iteration complete (closes #901)
- `list_database_names()` returns active databases including defaults (closes #802)
- `list_collection_names()` filter behavior aligned with PyMongo (closes #872)

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


[7.0.0]: https://github.com/engFelipeMonteiro/mongomock-ng/compare/4.3.0...7.0.0
[4.4.0]: https://github.com/engFelipeMonteiro/mongomock-ng/compare/4.3.0...4.4.0
[4.3.0]: https://github.com/engFelipeMonteiro/mongomock-ng/compare/4.2.0...4.3.0
[4.2.0]: https://github.com/engFelipeMonteiro/mongomock-ng/compare/4.1.3...4.2.0
