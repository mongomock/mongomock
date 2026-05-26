# Mongomock-ng Unit Tests Against Real MongoDB

## Executive Summary

This document provides a comprehensive reference for understanding how mongomock-ng validates its implementation against a real MongoDB instance. The project uses an elegant comparison testing strategy to ensure compatibility with PyMongo.

- **Total Tests**: 1,700+
- **Tests Against Real MongoDB**: 345+ (in classes inheriting from `_CollectionComparisonTest`)
- **Mock-Only Tests**: 1,000+
- **MongoDB Version in CI**: 7.0.34
- **PyMongo Versions Tested**: 3.x, 4.x, 4.11.0, 7.0, latest

---

## Test Architecture: MultiCollection Pattern

### Base Class: `_CollectionComparisonTest`

**Location**: [`tests/test__mongomock.py`](../../../tests/test__mongomock.py) (lines 268-330)

```python
@skipIf(not helpers.HAVE_PYMONGO, 'pymongo not installed')
@skipIf(os.getenv('NO_LOCAL_MONGO'), 'No local Mongo server running')
class _CollectionComparisonTest(TestCase):
    """Compares a fake collection with the real MongoDB collection implementation
    
    This is done via cross-comparison of the results.
    """
    
    def setUp(self):
        super().setUp()
        self.fake_conn = mongomock_ng.MongoClient()
        self.mongo_conn = self._connect_to_local_mongodb()
        self.db_name = 'mongomock___testing_db'
        self.collection_name = 'mongomock___testing_collection'
        
        self.cmp = MultiCollection({
            'fake': self.fake_collection,
            'real': self.mongo_collection,
        })
```

#### How It Works

1. **Dual Client Setup**: Creates two MongoDB clients:
   - `mongomock_ng.MongoClient()` - In-memory mock
   - `pymongo.MongoClient()` - Real MongoDB instance

2. **MultiCollection Wrapper**: Wraps both collections to enable synchronized operation execution

3. **Parallel Execution**: Executes identical operations on both clients simultaneously

4. **Result Comparison**: Uses `MultiCollection.compare()` to automatically assert results are identical

5. **Skip Conditions**:
   - If pymongo is not installed
   - If `NO_LOCAL_MONGO` environment variable is set
   - Retries up to 60 times with 0.5s intervals (30s total timeout)

### MultiCollection Tool

**Location**: [`tests/multicollection.py`](../../../tests/multicollection.py)

This utility class is the backbone of comparison testing:

```python
class MultiCollection:
    def __init__(self, conns):
        self.conns = conns.copy()  # {'fake': ..., 'real': ...}
        self.do = Foreach(self.conns, compare=False)
        self.compare = Foreach(self.conns, compare=True)
        self.compare_ignore_order = Foreach(self.conns, compare=True, ignore_order=True)
        self.compare_exceptions = Foreach(self.conns, compare=_COMPARE_EXCEPTIONS)
```

#### Usage Patterns

| Pattern | Behavior | Use Case |
|---------|----------|----------|
| `self.cmp.do.insert_one({})` | Execute on both, no comparison | Setup operations |
| `self.cmp.compare.find()` | Execute on both, compare results | Main assertions |
| `self.cmp.compare_ignore_order.aggregate()` | Compare results ignoring order | Aggregation pipelines |
| `self.cmp.compare_exceptions.update()` | Compare exception types | Error handling validation |

---

## Test Classes Against Real MongoDB

All classes listed below inherit from `_CollectionComparisonTest` and automatically validate their implementations against real MongoDB:

### In `tests/test__mongomock.py`

| Class | Purpose | Method Count |
|-------|---------|--------------|
| **EqualityCollectionTest** | Database and collection equality/hashing | 4 tests |
| **MongoClientCollectionTest** | Collection CRUD operations | 200+ tests |
| **GroupTest** | Aggregation with `$group` stage | 15+ tests |
| **MongoClientAggregateTest** | Full aggregation pipeline | 100+ tests |
| **MongoClientGraphLookupTest** | `$graphLookup` aggregation operator | 20+ tests |
| **MongoClientSortSkipLimitTest** | Sort, skip, limit operations | 10+ tests |
| **MongoClientTest** | General client operations | 5+ tests |
| **DatabaseTest** | Database operations | 3+ tests |

**Total Test Methods**: 345+ in comparison base classes

### GridFS Testing

**Location**: [`tests/test__gridfs.py`](../../../tests/test__gridfs.py)

```python
@skipUnless(helpers.HAVE_PYMONGO, 'pymongo not installed')
@skipUnless(_HAVE_GRIDFS and hasattr(gridfs.__builtins__, 'copy'), 'gridfs not installed')
@skipIf(os.getenv('NO_LOCAL_MONGO'), 'No local Mongo server running')
class GridFsTest(TestCase):
```

- Validates GridFS file storage and retrieval
- Tests both small files (50 bytes) and large files (500KB+)
- Compares `real_gridfs` vs `fake_gridfs` results
- Validates chunk distribution and deletion

### Bulk Operations Testing

**Location**: [`tests/test__bulk_operations.py`](../../../tests/test__bulk_operations.py)

Dual-mode test classes:

```python
class BulkOperationsTest(TestCase):
    test_with_pymongo = False  # Runs with mongomock-ng
    
    def setUp(self):
        if self.test_with_pymongo:
            self.client = pymongo.MongoClient(
                host=os.environ.get('TEST_MONGO_HOST', 'localhost')
            )
        else:
            self.client = mongomock_ng.MongoClient()

class BulkOperationsWithPymongoTest(BulkOperationsTest):
    test_with_pymongo = True  # Runs with real MongoDB
```

- `BulkOperationsTest`: Tests against mongomock-ng (no DB required)
- `BulkOperationsWithPymongoTest`: Same tests against real MongoDB
- `CollectionComparisonTest`: Direct comparison of bulk operations

---

## Environment Configuration

### Environment Variables

| Variable | Purpose | Default | Location |
|----------|---------|---------|----------|
| `NO_LOCAL_MONGO` | Disable tests against real MongoDB | (not set) | Skip decorators in comparison tests |
| `TEST_MONGO_HOST` | MongoDB connection string | `localhost` | Used by `_connect_to_local_mongodb()` |
| `EXECJS_RUNTIME` | JavaScript runtime for MapReduce | `node` | `hatch.toml` configuration |

### Running Tests

#### With Real MongoDB

```bash
# All tests with MongoDB comparison
hatch test

# Specific test file
pytest tests/test__mongomock.py

# With docker-compose (includes MongoDB service)
docker compose run --rm mongomock_ng hatch test

# Custom MongoDB host
TEST_MONGO_HOST=my-server:27017 pytest tests/
```

#### Without Real MongoDB (Mongomock-ng Only)

```bash
# Skip all tests requiring real MongoDB
NO_LOCAL_MONGO=1 pytest tests/

# Still runs 1,000+ mock-only tests
```

---

## CI/CD Pipeline

### GitHub Actions Workflow

**Location**: [`.github/workflows/lint-and-test.yml`](../.github/workflows/lint-and-test.yml)

#### Test Matrix

```yaml
matrix:
  python: ['3.10', '3.11', '3.12', '3.13', '3.14', 'pypy3.10']
  pymongo: ['4.11.0', 'latest']
```

- **Total Matrix Combinations**: 36+ (6 Python versions × 6 PyMongo versions)
- **MongoDB Service**: Supercargo/mongodb-github-action (v1.10.0)
- **MongoDB Version**: 5.0.5

#### Pipeline Stages

1. **Lint & Format Check**
   - `hatch fmt --check` - Code formatting validation
   - Type checking with mypy
   
2. **Test Execution**
   - Retries MongoDB connection up to 30 seconds
   - Runs full test suite: ~1,700 tests
   - Collects coverage metrics

3. **Coverage Upload**
   - Uploads to Codecov
   - Tracks coverage trends across releases

4. **Publish (on release)**
   - Builds distribution packages
   - Publishes to PyPI

---

## Key Test Features

### Comparison Strategy

Tests use the following logic to validate mongomock-ng:

```
FOR EACH test_case IN test_class:
    1. Execute operation on mongomock-ng
    2. Execute SAME operation on real MongoDB (with same arguments)
    3. Compare results:
       - Value equality
       - Type matching
       - Exception type matching (for error cases)
    4. Assert results are identical
    5. Clean up MongoDB test database
```

### Error Handling

The framework handles MongoDB connection failures gracefully:

```python
def _connect_to_local_mongodb(self, num_retries=60):
    """Performs retries on connection refused errors (for travis-ci builds)"""
    for retry in range(num_retries):
        if retry > 0:
            time.sleep(0.5)  # 0.5s delay between retries
        try:
            return PymongoClient(
                host=os.environ.get('TEST_MONGO_HOST', 'localhost'),
                maxPoolSize=1
            )
        except pymongo.errors.ConnectionFailure as e:
            if retry == num_retries - 1:
                raise  # Re-raise on final attempt
```

### PyMongo Version Compatibility

Tests are conditioned on PyMongo version using `packaging.version`:

```python
from packaging import version

@skipIf(version.parse('4.0') <= helpers.PYMONGO_VERSION, 'pymongo v4 or above')
class BulkOperationsTest(TestCase):
    """Only runs with PyMongo < 4.0"""
```

---

## .github/references Directory

**Purpose**: Stores reference outputs and expected test data for real MongoDB validation

**Use Cases**:
- Baseline outputs for batch operations
- Expected error messages from specific MongoDB versions
- Performance benchmarks
- MongoDB version-specific behavior snapshots

**Status**: Ready to receive reference files

**Naming Convention** (Recommended):
```
mongodb_<version>_<feature>_<platform>.json
```

Examples:
- `mongodb_5.0_aggregation_results.json`
- `mongodb_8.0_bulk_operations_output.json`
- `mongodb_5.0_gridfs_chunks_structure.json`

---

## Important Files Reference

| File | Purpose |
|------|---------|
| [`mongomock_ng/helpers.py`](../../../mongomock_ng/helpers.py) | Utility functions, version detection |
| [`tests/multicollection.py`](../../../tests/multicollection.py) | MultiCollection comparison tool |
| [`tests/utils.py`](../../../tests/utils.py) | Test utilities and helpers |
| [`tests/diff.py`](../../../tests/diff.py) | Diff tool for comparison output |
| `docker-compose.yml` | MongoDB + test environment setup |
| `hatch.toml` | Test environment configuration |
| `pyproject.toml` | PyMongo dependency matrix |

---

## Best Practices

### When Writing New Tests

1. **For core MongoDB operations**: Inherit from `_CollectionComparisonTest`
   ```python
   class MyNewFeatureTest(_CollectionComparisonTest):
       def test__my_feature(self):
           self.cmp.compare.insert_one({'test': 'value'})
           self.cmp.compare.find_one()
   ```

2. **For pure mongomock-ng features**: Use standard `TestCase`
   ```python
   class Mongomock-ngSpecificTest(TestCase):
        def test__mongomock_ng_only_feature(self):
           # No need for real MongoDB
   ```

3. **Always use meaningful operations**: Let `MultiCollection` handle both clients
   ```python
   # ✓ Good: MultiCollection handles both
   self.cmp.compare.update_many({}, {'$inc': {'count': 1}})
   
   # ✗ Avoid: Manual client switching
   self.fake_collection.update_many({}, ...)
   self.mongo_collection.update_many({}, ...)
   ```

### Debugging Failed Comparisons

When comparison tests fail:

1. Check the diff output - shows value differences
2. Verify MongoDB is running: `TEST_MONGO_HOST=localhost:27017 pytest -v`
3. Check PyMongo version: `python -c "import pymongo; print(pymongo.__version__)"`
4. Review exception types separately: `self.cmp.compare_exceptions.problematic_operation()`

---

## Version History

- **v4.x**: Compatibility with PyMongo 4.x
- **v3.x**: Original comparison testing framework
- **Current**: Supports PyMongo 3.x, 4.x, 7.0+ with flexible test matrix

---

## Related Documentation

- [Contributing Guide](../../../README.rst)
- [MongoDB Compatibility Notes](../../../Missing_Features.rst)
- [PyMongo Migration Guide](https://pymongo.readthedocs.io/en/stable/migrate-to-pymongo4.html)
- [Mongomock-ng GitHub Issues](https://github.com/engFelipeMonteiro/mongomock-ng/issues)

---

**Last Updated**: May 2026
**Status**: Actively Maintained
**Maintainers**: See [README.rst](../../../README.rst)
