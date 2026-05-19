# Mongomock Real MongoDB Testing - Quick Reference Index

## Test Files and Classes Hierarchy

### ✅ Files WITH Real MongoDB Validation Tests

#### 1. `tests/test__mongomock.py` - PRIMARY TEST FILE
**Status**: ✅ Validates against real MongoDB
**MongoDB Required**: Yes
**Skip Condition**: `NO_LOCAL_MONGO` env var

**Base Class**: `_CollectionComparisonTest` (lines 268-330)
- Connects to real MongoDB via `_connect_to_local_mongodb()`
- Uses `MultiCollection` for dual testing
- Skips if: pymongo not installed OR `NO_LOCAL_MONGO` set

**Test Classes** (all inherit from `_CollectionComparisonTest`):

| Line | Class | Tests | Purpose |
|------|-------|-------|---------|
| 322 | `EqualityCollectionTest` | 4 | Database/collection equality |
| 348 | `MongoClientCollectionTest` | 200+ | Collection CRUD operations |
| 2290 | `CollectionMapReduceTest` | ~20 | MapReduce aggregation |
| 2467 | `GroupTest` | 15+ | `$group` stage |
| 2517 | `MongoClientAggregateTest` | 100+ | Aggregation pipelines |
| 4821 | `MongoClientGraphLookupTest` | 20+ | `$graphLookup` |
| 5061 | `MongoClientSortSkipLimitTest` | 10+ | Sort/skip/limit |
| 5253 | `MongoClientTest` | 5+ | Client operations |
| 5271 | `DatabaseTest` | 3+ | Database operations |

**Total**: 345+ tests

---

#### 2. `tests/test__gridfs.py` - GridFS VALIDATION
**Status**: ✅ Validates against real MongoDB
**MongoDB Required**: Yes
**Skip Conditions**:
- `NO_LOCAL_MONGO` env var
- gridfs not installed
- pymongo not installed

**Main Class**: `GridFsTest(TestCase)` (line 35)
- Compares `real_gridfs` vs `fake_gridfs`
- Tests file storage, retrieval, deletion
- Tests chunk distribution

**Key Tests**:
- `test__put_get_small()` - 50 byte files
- `test__put_get_big()` - 500KB files
- `test__delete_exists_small/big()` - Deletion validation

---

#### 3. `tests/test__bulk_operations.py` - BULK OPERATIONS
**Status**: ✅ Partial validation (3 classes)
**MongoDB Required**: For comparison tests only
**Skip Conditions**: `NO_LOCAL_MONGO`, PyMongo v4+

**Test Classes**:

| Line | Class | MongoDB | Purpose |
|------|-------|---------|---------|
| 23 | `BulkOperationsTest` | ❌ No | Tests with mongomock_ng |
| 196 | `BulkOperationsWithPymongoTest` | ✅ Yes | Same tests with real DB |
| 204 | `CollectionComparisonTest` | ✅ Yes | Direct comparison |

**Key Logic**: `test_with_pymongo` flag determines if tests run against:
- `False` (default): mongomock_ng only
- `True`: real MongoDB

---

### ❌ Files WITHOUT Real MongoDB Tests

These only test mongomock_ng in isolation (no real MongoDB required):

| File | Classes | Purpose | Notes |
|------|---------|---------|-------|
| `test__client_api.py` | `ClientAPITest` | Client initialization, configuration | Mock only |
| `test__collection_api.py` | `CollectionAPITest` | Collection API surface | Mock only |
| `test__database_api.py` | `DatabaseAPITest` | Database API surface | Mock only |
| `test__diff.py` | `DiffTest` | Diff utility testing | Utility tests |
| `test__helpers.py` | 4 classes | Helper functions | Utility tests |
| `test__not_implemented.py` | 1 class | NotImplementedError cases | Error handling |
| `test__patch.py` | `PatchTest` | mongomock_ng.patch() decorator | Mocking framework |
| `test__readme_doctest.py` | `ReadMeDocTest` | README code examples | Documentation |
| `test__thread.py` | 2 classes | Thread safety | Internal behavior |

---

## Environment Variables and Configuration

### For Running Tests

```bash
# Run ALL tests (with MongoDB comparison)
hatch test

# Run only tests that don't need MongoDB
NO_LOCAL_MONGO=1 pytest tests/

# Run specific comparison tests with custom MongoDB host
TEST_MONGO_HOST=my-mongo-server:27017 pytest tests/test__mongomock.py -v

# Skip tests if MongoDB is unavailable
# (automatic: tests marked with @skipIf(os.getenv('NO_LOCAL_MONGO')))
```

### Environment Variable Reference

| Var | Default | Used By | Effect |
|-----|---------|---------|--------|
| `NO_LOCAL_MONGO` | (not set) | Skip decorators | Skips all comparison tests |
| `TEST_MONGO_HOST` | `localhost` | `_connect_to_local_mongodb()` | MongoDB connection string |
| `EXECJS_RUNTIME` | `node` | MapReduce tests | JavaScript runtime for operations |

---

## MongoDB Connection Flow

### Connection Setup in Comparison Tests

```
Test setUp():
  1. Create mongomock_ng.MongoClient()  ← In-memory, instant
  2. Call _connect_to_local_mongodb()
       ├─ Retry loop: 60 attempts (30 sec total)
       ├─ Delay: 0.5s between attempts
       └─ Return: PymongoClient if successful
  3. Create MultiCollection wrapper
  4. Run test operations on BOTH clients
  5. Compare results automatically
  6. tearDown(): Close MongoDB connection
```

### Retry Logic

```python
def _connect_to_local_mongodb(self, num_retries=60):
    for retry in range(num_retries):
        if retry > 0:
            time.sleep(0.5)
        try:
            return PymongoClient(host=os.environ.get('TEST_MONGO_HOST', 'localhost'))
        except pymongo.errors.ConnectionFailure as e:
            if retry == num_retries - 1:
                raise
            if 'connection refused' not in e.message.lower():
                raise
```

**Timeout**: 30 seconds total (60 retries × 0.5s)

---

## CI/CD Test Matrix

### `.github/workflows/lint-and-test.yml`

**Python Versions**: 3.10, 3.11, 3.12, 3.13, PyPy3 (6 total)

**PyMongo Versions**: 3.x, 4.x, 4.11.0, 7.0, latest, none (6 total)

**Total Matrix**: 36 combinations

**MongoDB**: 7.0.34 (via mongodb-github-action)

### Matrix Notable Cases

- `pymongo=none`: Tests run with mongomock_ng only (no real DB comparison)
- `pypy3`: Python implementation differences tested
- `pymongo=3` vs `pymongo=4+`: API compatibility validation

---

## Test Architecture Patterns

### Pattern 1: Comparison Testing

```python
# Location: tests/test__mongomock.py
class MongoClientCollectionTest(_CollectionComparisonTest):
    def test__insert_one(self):
        self.cmp.do.drop()  # Clean both
        result = self.cmp.compare.insert_one({'name': 'Alice'})  # Compare results
        self.assertEqual(result['fake'].inserted_id, result['real'].inserted_id)
```

**Execution**:
1. `self.cmp.do.drop()` executes on both, no comparison
2. `self.cmp.compare.insert_one()` executes on both AND compares
3. Results dict: `{'fake': ..., 'real': ...}`

### Pattern 2: Dual Mode Testing

```python
# Location: tests/test__bulk_operations.py
class BulkOperationsTest(TestCase):
    test_with_pymongo = False
    
    def setUp(self):
        if self.test_with_pymongo:
            self.client = pymongo.MongoClient(
                host=os.environ.get('TEST_MONGO_HOST', 'localhost')
            )
        else:
            self.client = mongomock_ng.MongoClient()

class BulkOperationsWithPymongoTest(BulkOperationsTest):
    test_with_pymongo = True  # Inherit all tests, run against MongoDB
```

**Same test methods run twice**:
- `BulkOperationsTest`: With mongomock_ng
- `BulkOperationsWithPymongoTest`: With real MongoDB

### Pattern 3: GridFS Dual Validation

```python
# Location: tests/test__gridfs.py
class GridFsTest(TestCase):
    def setUp(self):
        self.real_gridfs = gridfs.GridFS(self.mongo_conn[self.db_name])
        self.fake_gridfs = gridfs.GridFS(self.fake_conn[self.db_name])
    
    def test__put_get(self):
        fid = self.fake_gridfs.put(GenFile(50))
        rid = self.real_gridfs.put(GenFile(50))
        assert self.fake_gridfs.get(fid).read() == self.real_gridfs.get(rid).read()
```

---

## Skip Conditions (Decorator Priority)

Tests are skipped in this order:

1. **pymongo not installed**: `@skipIf(not helpers.HAVE_PYMONGO, ...)`
2. **MongoDB unavailable**: `@skipIf(os.getenv('NO_LOCAL_MONGO'), ...)`
3. **PyMongo version mismatch**: `@skipIf(version.parse('4.0') <= helpers.PYMONGO_VERSION, ...)`
4. **PyPy + PyMongo < 3**: `@skipIf(_USING_PYPY and ..., ...)`
5. **GridFS not available**: `@skipUnless(_HAVE_GRIDFS, ...)`

**Result**: 
- With all dependencies → 1,700+ tests run
- Without MongoDB → 1,000+ mock-only tests
- Without PyMongo → 500+ basic tests

---

## Utility Modules

### `tests/multicollection.py`
- **Purpose**: Wrap multiple collections for synchronized testing
- **Key Classes**: `MultiCollection`, `Foreach`, `ForeachMethod`
- **Usage**: `self.cmp.compare.find()` executes on both clients, compares results

### `tests/diff.py`
- **Purpose**: Generate diffs for comparison failures
- **Key Function**: `_assert_no_diff(results, ignore_order=False, sort_by=None)`
- **Usage**: Automatically called by `MultiCollection` for assertions

### `tests/utils.py`
- **Purpose**: Test utilities, DBRef stub
- **Classes**: `DBRef` (when pymongo not available)
- **Usage**: Fallback when pymongo module is missing

---

## .github/references Directory Usage

### Current Status
- **Location**: `.github/references/`
- **Contents**: Reference documentation for real MongoDB validation
- **Purpose**: Store Copilot references in English for future use

### Files to Create (Recommended)

1. **REAL_MONGODB_VALIDATION.md** ✅ (Created)
   - Comprehensive validation strategy documentation

2. **TEST_CLASS_HIERARCHY.md**
   - Visual class inheritance tree
   - Test count per class

3. **ENVIRONMENT_VARIABLES.md**
   - Complete env var reference
   - Usage examples

4. **CI_TEST_MATRIX.md**
   - GitHub Actions matrix configuration
   - Expected test count per combination

5. **MONGODB_VERSION_SUPPORT.md**
   - Supported MongoDB versions
   - Feature availability per version

---

## Key Statistics

| Metric | Value |
|--------|-------|
| Total Test Files | 12 |
| Total Test Classes | 35+ |
| Total Test Methods | 1,700+ |
| Classes Using Real MongoDB | 12+ |
| Test Methods Using Real MongoDB | 345+ |
| Mock-Only Test Methods | 1,000+ |
| CI Test Matrix Combinations | 36 |
| PyMongo Versions Tested | 6 (3, 4, 4.11, 7, latest, none) |
| Python Versions Tested | 6 (3.10-3.13, PyPy3) |

---

## Quick Troubleshooting

### Tests Skip Because MongoDB Not Found

```bash
# Verify MongoDB is running
mongo --version
mongod --version

# Or start MongoDB with Docker
docker run -d -p 27017:27017 mongo:5.0

# Then run tests
pytest tests/test__mongomock.py -v
```

### Tests Fail with Connection Refused

```bash
# Check if MongoDB is listening
curl http://localhost:27017/  # Should hang (MongoDB protocol)

# Or use Python
python -c "import pymongo; print(pymongo.MongoClient().server_info())"
```

### Specific PyMongo Version Issues

```bash
# Check current PyMongo version
python -c "import pymongo; print(pymongo.__version__)"

# Install specific version
pip install pymongo==4.0

# Run tests with version specification
hatch test -i pymongo=4.0
```

### Run Only Comparison Tests

```bash
# Run tests that validate against MongoDB
pytest tests/test__mongomock.py::MongoClientCollectionTest -v

# Run with verbose output showing both clients
pytest tests/test__mongomock.py -vv --tb=short
```

---

**Document Version**: 1.0
**Last Updated**: May 2026
**Purpose**: Quick reference for Copilot understanding mongomock_ng's real MongoDB validation strategy
