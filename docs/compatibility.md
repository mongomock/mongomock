---
description: Python, PyPy, and pymongo version compatibility matrix for mongomock-ng
tags:
  - compatibility
  - python
  - pypy
  - pymongo
  - versions
---

# Compatibility Matrix

mongomock-ng supports multiple Python implementations, pymongo versions, and MongoDB server versions.

## Supported Versions

| Component | Supported | Notes |
|-----------|-----------|-------|
| **Python** | 3.10, 3.11, 3.12, 3.13, 3.14 | CPython |
| **PyPy** | 3.10, 3.11 | CPython-compatible |
| **pymongo** | >= 4.11 | Optional dependency |
| **MongoDB** | 7.0+ | Server version emulated |

## Test Matrix

The CI test matrix covers:

| Implementation | Version | pymongo 4.11 | pymongo 4.12 | pymongo 4.14 | pymongo latest |
|----------------|---------|:-----------:|:------------:|:------------:|:--------------:|
| CPython | 3.10 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| CPython | 3.11 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| CPython | 3.12 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| CPython | 3.13 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| CPython | 3.14 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| PyPy | 3.10 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| PyPy | 3.11 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

!!! note "PyPy + pandas"
    pandas does not support PyPy. If you use pandas in your tests, install mongomock-ng without the `pandas` extra on PyPy:

    ```bash
    pip install mongomock-ng  # without pandas
    ```

    Tests that use pandas are automatically skipped on PyPy.

## Installation Extras

```bash
# Basic (no pymongo — mock only)
pip install mongomock-ng

# With pymongo support
pip install mongomock-ng[pymongo]

# With pandas support (CPython only)
pip install mongomock-ng[pandas]

# Everything
pip install mongomock-ng[all]
```

## MongoDB Server Version

mongomock-ng emulates MongoDB server behavior. The default server version is `7.0.34`.

```bash
# Override server version
MONGODB=7.0.34 python -m pytest tests/
```

## Version Constraints

| Package | Constraint | Reason |
|---------|-----------|--------|
| Python | >= 3.10 | Type hints (`X \| Y`), `match`/`case` |
| pymongo | >= 4.11 | API alignment, codec options |
| packaging | any | Version parsing |
| pytz | any | Timezone support |
| sentinels | any | NOTHING sentinel |
| pandas | any | Optional, not on PyPy |
