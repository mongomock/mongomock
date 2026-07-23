# mongomock-ng

[![PyPI version](https://img.shields.io/pypi/v/mongomock-ng.svg?style=flat-square)](https://pypi.python.org/pypi/mongomock-ng)
[![CI](https://img.shields.io/github/actions/workflow/status/engFelipeMonteiro/mongomock-ng/lint-and-test.yml?branch=develop&style=flat-square)](https://github.com/engFelipeMonteiro/mongomock-ng/actions?query=workflow%3Alint-and-test)
[![License](https://img.shields.io/pypi/l/mongomock-ng.svg?style=flat-square)](https://pypi.python.org/pypi/mongomock-ng)
[![Codecov](https://img.shields.io/codecov/c/github/engFelipeMonteiro/mongomock-ng.svg?style=flat-square)](https://codecov.io/gh/engFelipeMonteiro/mongomock-ng)

In-memory MongoDB mock for Python testing. Drop-in replacement for pymongo in tests — no MongoDB server required.

## Quick Start

```bash
pip install mongomock-ng
```

```python
import mongomock_ng as mongomock

client = mongomock.MongoClient()
db = client['test_db']
collection = db['test_collection']
collection.insert_one({'name': 'test', 'value': 42})
print(collection.find_one())
```

## Why mongomock-ng?

Testing code that interacts with MongoDB typically requires either a real instance
(high maintenance, CI overhead) or hand-crafted mocks (fragile, violates DRY).
mongomock-ng provides a third option: an in-memory mock that behaves like real MongoDB.

```python
def increase_votes(collection):
    collection.update_many({}, {'$inc': {'votes': 1}})

def test_increase_votes():
    collection = mongomock.MongoClient().db.collection
    collection.insert_many([{'votes': 1}, {'votes': 2}])
    increase_votes(collection)
    for doc in collection.find():
        assert doc['votes'] >= 2
```

## Features

- **CRUD operations**: insert, find, update, delete — full pymongo API
- **Query operators**: `$gt`, `$in`, `$regex`, `$exists`, and more
- **Aggregation pipeline**: `$match`, `$group`, `$lookup`, `$project`, 30+ stages
- **Geospatial queries**: `$geoIntersects`, `$geoWithin`, `$near`, `$geoNear`
- **Sessions & transactions**: `ClientSession` with snapshot isolation
- **Index support**: unique, sparse, TTL, compound, 2dsphere
- **Query profiler**: capture queries, analyze index coverage
- **Document validation**: `$jsonSchema` validator support

## Examples

Example scripts are in the [`examples/`](https://github.com/engFelipeMonteiro/mongomock-ng/tree/develop/examples) directory:

- [`basic_crud.py`](https://github.com/engFelipeMonteiro/mongomock-ng/blob/develop/examples/basic_crud.py) — Basic CRUD operations
- [`filtering.py`](https://github.com/engFelipeMonteiro/mongomock-ng/blob/develop/examples/filtering.py) — Query operators, projections, sorting
- [`aggregation.py`](https://github.com/engFelipeMonteiro/mongomock-ng/blob/develop/examples/aggregation.py) — Aggregation pipeline
- [`validation.py`](https://github.com/engFelipeMonteiro/mongomock-ng/blob/develop/examples/validation.py) — Schema validation
- [`ttl.py`](https://github.com/engFelipeMonteiro/mongomock-ng/blob/develop/examples/ttl.py) — TTL index simulation

## Patching pymongo

If your code creates the connection itself, use `mongomock_ng.patch`:

```python
@mongomock_ng.patch(servers=(('server.example.com', 27017),))
def test_increase_votes_endpoint():
    client = pymongo.MongoClient('server.example.com')
    client.db.collection.insert_many([{'votes': 1}])
    call_endpoint('/votes')
    # verify client.db.collection
```

## Acknowledgements

Originally developed by [Rotem Yaari](https://github.com/vmalloc/),
then by [Martin Domke](https://github.com/mdomke), then [Pascal Corpet](https://github.com/pcorpet).
Currently maintained by [Pascal Corpet](https://github.com/pcorpet), [Felipe Monteiro Jácome](https://github.com/engFelipeMonteiro).
Fork maintained by [Felipe Monteiro Jácome](https://github.com/engFelipeMonteiro).
