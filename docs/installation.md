# Getting Started

## Installation

```bash
pip install mongomock-ng
```

### With pymongo support

```bash
pip install mongomock-ng[pymongo]
```

### With pandas support (CPython only)

```bash
pip install mongomock-ng[pandas]
```

### All extras

```bash
pip install mongomock-ng[all]
```

## Compatibility

| Component | Supported |
|-----------|-----------|
| Python | 3.10, 3.11, 3.12, 3.13, 3.14 |
| PyPy | 3.10, 3.11 |
| pymongo | >= 4.11 |
| MongoDB | 7.0+ (emulated) |

See [Compatibility Matrix](compatibility.md) for full test coverage details.

!!! note "PyPy + pandas"
    pandas does not support PyPy. Install without the `pandas` extra on PyPy.
    Tests using pandas are automatically skipped.

## Basic Usage

```python
import mongomock_ng as mongomock

client = mongomock.MongoClient()
db = client['test_db']
collection = db['test_collection']

# Insert
collection.insert_one({'name': 'Alice', 'age': 30})
collection.insert_many([
    {'name': 'Bob', 'age': 25},
    {'name': 'Charlie', 'age': 35},
])

# Find
doc = collection.find_one({'name': 'Alice'})
docs = list(collection.find({'age': {'$gte': 30}}))

# Update
collection.update_one({'name': 'Alice'}, {'$set': {'age': 31}})
collection.update_many({}, {'$inc': {'age': 1}})

# Delete
collection.delete_one({'name': 'Bob'})
collection.delete_many({'age': {'$lt': 25}})

# Aggregate
pipeline = [
    {'$match': {'age': {'$gte': 25}}},
    {'$group': {'_id': None, 'avg_age': {'$avg': '$age'}}},
]
result = list(collection.aggregate(pipeline))
```

## Running Tests

### With hatch (recommended)

```bash
git clone git@github.com:engFelipeMonteiro/mongomock-ng.git
pipx install hatch
cd mongomock-ng
hatch test
```

### With Docker/Podman

```bash
git clone git@github.com:engFelipeMonteiro/mongomock-ng.git
cd mongomock-ng
docker compose build
docker compose run --rm mongomock_ng
```

### Specific test environment

```bash
docker compose run --rm mongomock_ng hatch test -py=3.11 -i pymongo=4
```

### Single test

```bash
docker compose run --rm mongomock_ng hatch test -py=3.12 -i pymongo=4 tests/test__mongomock.py::MongoClientCollectionTest::test__insert
```

## Upgrading from pymongo

Mongomock-ng adapts its API to match your installed pymongo version.
If your tests pass with mongomock-ng, they will work with real MongoDB.

1. Upgrade to mongomock-ng v7+: API adapts to pymongo version automatically
2. Upgrade to pymongo v4+: test failures indicate real production issues

## Versioning

- mongomock-ng targets MongoDB 7.0.34 server behavior
- `SERVER_VERSION` defaults to `7.0.34` (configurable via `MONGODB` env var)
- Requires Python >= 3.10, pymongo >= 4.11

## Code Formatting

All code is formatted with [ruff](https://docs.astral.sh/ruff/formatter/):

```bash
hatch fmt
```

## utcnow Helper

```python
import mongomock_ng
now_reference = mongomock_ng.utcnow()
```

Provides a consistent way to mock "now" in tests.
