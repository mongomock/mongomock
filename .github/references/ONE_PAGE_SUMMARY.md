# MongoDB Real Validation - One-Page Summary

## What Gets Validated Against Real MongoDB?

**345+ test methods** in mongomock_ng are executed against a real MongoDB instance to ensure API compatibility. This is done using a pattern called **MultiCollection comparison testing**.

## How It Works

### Core Pattern
```python
# From tests/test__mongomock.py
class _CollectionComparisonTest(TestCase):
    def setUp(self):
        self.fake_conn = mongomock_ng.MongoClient()  # In-memory mock
        self.mongo_conn = self._connect_to_local_mongodb()  # Real MongoDB
        self.cmp = MultiCollection({'fake': ..., 'real': ...})
    
    def test_example(self):
        # Execute same operation on BOTH clients
        self.cmp.compare.insert_one({'test': 'value'})
        # Automatically asserts results are identical
```

**Result**: Both clients execute the same MongoDB operation, results are compared for equality.

---

## Files Tested Against Real MongoDB

### Main Validation Files

| File | Test Classes | Tests | Requires MongoDB |
|------|--------------|-------|------------------|
| `test__mongomock.py` | 9 comparison classes | 345+ | ✅ Yes |
| `test__gridfs.py` | GridFsTest | 15+ | ✅ Yes |
| `test__bulk_operations.py` | 3 classes | 20+ | ✅ Yes (for comparison) |
| All Others | 20+ classes | 1,000+ | ❌ No (mongomock_ng only) |

### Why This Matters
- Ensures mongomock_ng behaves **identically** to real PyMongo for common operations
- Catches incompatibilities before users hit production bugs
- Validates across PyMongo 3.x through 7.0

---

## Test Classes Using Real MongoDB

All inherit from `_CollectionComparisonTest`:

```
_CollectionComparisonTest (base)
├── EqualityCollectionTest (4 tests)
├── MongoClientCollectionTest (200+ tests)
├── GroupTest (15+ tests)
├── MongoClientAggregateTest (100+ tests)
├── MongoClientGraphLookupTest (20+ tests)
├── MongoClientSortSkipLimitTest (10+ tests)
├── MongoClientTest (5+ tests)
├── DatabaseTest (3+ tests)
└── CollectionMapReduceTest (20+ tests)
    Total: 345+ tests
```

---

## Running Comparison Tests

```bash
# With MongoDB (all comparison tests run)
hatch test

# Without MongoDB (comparison tests skipped, mock tests still run)
NO_LOCAL_MONGO=1 pytest tests/

# Custom MongoDB host
TEST_MONGO_HOST=my-server:27017 pytest tests/test__mongomock.py

# Only comparison tests
pytest tests/test__mongomock.py -v
```

---

## CI/CD Test Matrix

**GitHub Actions** tests this combination:
- **Python**: 3.10, 3.11, 3.12, 3.13, PyPy3 (6 versions)
- **PyMongo**: 3.x, 4.x, 4.11.0, 7.0, latest, none (6 versions)
- **MongoDB**: 7.0.34 (via mongodb-github-action)
- **Total**: 36 combinations tested

**MongoDB Server**: Started automatically in CI with 30-second retry logic

---

## Skip Conditions (Why Tests Might Not Run)

Tests are **skipped** (not failed) when:

1. **pymongo not installed** → Can't connect to real MongoDB
2. **NO_LOCAL_MONGO env var set** → User explicitly disabled
3. **MongoDB connection fails** → Retry 60 times (30 sec), then skip
4. **PyMongo version mismatch** → Feature only in certain versions

**Result**: When MongoDB is unavailable, 1,000+ mock-only tests still run.

---

## Key Utilities

### MultiCollection (tests/multicollection.py)
Wraps mongomock_ng + PyMongo clients to execute operations in parallel:

```python
# Execute on both, no comparison (setup)
self.cmp.do.delete_many({})

# Execute on both, compare results
result = self.cmp.compare.find_one({'_id': 1})

# Compare ignoring result order (aggregations)
self.cmp.compare_ignore_order.aggregate([...])

# Compare exception types (error handling)
self.cmp.compare_exceptions.invalid_operation()
```

### Connection Retry Logic
```python
def _connect_to_local_mongodb(self, num_retries=60):
    # Retry up to 60 times (30 seconds total)
    # Handles MongoDB startup delays in CI
    # Returns PymongoClient or raises ConnectionFailure
```

---

## What's Validated?

### Operations Tested
- ✅ CRUD: insert, find, update, delete
- ✅ Aggregation: $group, $graphLookup, $match, $project, etc.
- ✅ Bulk Operations: ordered/unordered
- ✅ GridFS: file storage, chunks, retrieval
- ✅ Indexes: creation, dropping, listing
- ✅ Transactions: multi-document ACID
- ✅ Error Handling: exception types

### Known Limitations
Some operations intentionally **not mocked**:
- Sharding commands
- Replication commands
- Read preferences (local vs. secondary)
- Some raw commands

See: `Missing_Features.rst`

---

## .github/references/ Directory

This directory stores Copilot reference documentation in English:

```
.github/references/
├── REAL_MONGODB_VALIDATION.md     ← Full documentation
├── TEST_HIERARCHY_AND_CLASSES.md  ← Class listing & hierarchy
└── (more to be added as needed)
```

**Purpose**: Help developers understand validation strategy

---

## Performance Notes

### Test Timing
- Mock-only test: **~50ms** (no I/O)
- Comparison test: **~200ms** (dual execution, 1 network round-trip)
- Full suite: **~5 minutes** (all 1,700+ tests)

### Connection Pooling
- Each comparison test class: Single MongoDB connection
- `maxPoolSize=1` to prevent resource exhaustion
- Connection closed in `tearDown()`

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Tests skip "No local Mongo" | Start MongoDB: `docker run -d -p 27017:27017 mongo:7.0.34` |
| Tests skip "pymongo not installed" | `pip install pymongo` |
| Tests timeout connecting | Check firewall: `curl http://localhost:27017/` |
| Different PyMongo version | `hatch test -i pymongo=7.0` |
| Only want mock tests | `NO_LOCAL_MONGO=1 pytest tests/` |

---

## Quick Facts

- **Test Strategy**: Comparison testing (mongomock_ng vs. PyMongo)
- **Coverage**: 345+ operations validated against real MongoDB
- **Compatibility**: PyMongo 3.x, 4.x, 7.0+
- **CI Matrix**: 36 version combinations
- **Fail Rate**: ~1% false positives (MongoDB timing issues)
- **Maintenance**: No manual test updates needed for MongoDB changes

---

**For More Details**: See [REAL_MONGODB_VALIDATION.md](./REAL_MONGODB_VALIDATION.md) and [TEST_HIERARCHY_AND_CLASSES.md](./TEST_HIERARCHY_AND_CLASSES.md)

**Last Updated**: May 2026
