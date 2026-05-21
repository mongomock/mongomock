# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
