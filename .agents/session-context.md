# Geospatial PR — Session 2+ Context

## 📊 Current Status
- **Branch:** `feat/geospatial-support`
- **Tests passing:** 91/91 ✅
- **Coverage:** geospatial.py 89%, filtering.py 45%, aggregate.py 13%, collection.py 22%
- **Files modified:** 4 (aggregate.py, filtering.py, geospatial.py, test__geospatial.py)

## ✅ Completed in Session 1
1. **Code review (caveman-review):** 12 findings documented
2. **Tests added:** 91 tests (+29 new)
3. **Bug fixes via cavecrew-builder:**
   - ✅ geospatial.py:L197 — Guard division by zero
   - ✅ filtering.py:L75-77, L90-92 — Narrow exception catches
   - ✅ aggregate.py:L2841 — Move import to top
   - ✅ aggregate.py:L2854 — Narrow exception catch

## 🔧 Remaining High-Priority Work

### 1. geospatial.py:L246-263 — Extend geo_intersects/geo_within for non-Point docs
**Status:** IN PROGRESS (cavecrew-builder delegated)
**What:** Currently only support Point doc geometry, need to add LineString/Polygon support
**How:** 
- Point doc intersects with query geo ✅ (done)
- LineString doc: check if query geometry intersects the line
- Polygon doc: check if query point/geometry is in/on polygon
**Test:** `pytest tests/test__geospatial.py::GeoIntersectsTest -v`

### 2. geospatial.py:L129 — Use epsilon comparison for float ring closure
**Status:** PENDING
**Location:** `_validate_polygon_coords()` function
**Current:** `if ring[0] != ring[-1]:` (exact float comparison)
**Fix:** Use `_points_equal()` helper which has epsilon check: `abs(a[0]-b[0])<1e-12 and abs(a[1]-b[1])<1e-12`
**Impact:** Prevent false polygon-not-closed errors due to floating point precision
**File:** mongomock_ng/geospatial.py:L123-132
**Test:** Create test `test_polygon_ring_closure_floating_point_tolerance` in test__geospatial.py

### 3. collection.py:L1433 — Clarify distance aggregation logic
**Status:** PENDING
**Location:** Collection.find() method with $near queries
**Context:** When multiple $near operators exist, uses `min(distances)` to sort
**Issue:** Logic unclear if multiple fields have $near — which one is primary?
**Recommended fix:** Add comment explaining behavior or implement explicit priority
**Current code location:** mongomock_ng/collection.py:L1419-1450
**Test:** Add test `test_near_multiple_fields_priority` 

## 📋 Medium-Priority Work

### Add aggregate.py $geoNear integration tests
- Current coverage: 13% (mostly untested edge cases)
- Target: 60%+ coverage
- Focus areas:
  - geoNear with nested $lookup
  - geoNear with $match stages
  - geoNear error handling for malformed queries
  - geoNear with $limit > num
  - geoNear sorting on non-Point docs

### Improve collection.py near sorting test coverage
- Current coverage: 22%
- Add tests for:
  - Multiple near fields with same distance
  - Near with array/nested field paths
  - Near with computed/projected fields

## 🧪 Test Framework Summary
Tests located in: `tests/test__geospatial.py`
- 91 total tests organized in 10 test classes
- All tests passing with 89% geospatial.py coverage
- Test commands:
  ```bash
  # All tests
  .env/bin/python -m pytest tests/test__geospatial.py -v
  
  # With coverage
  .env/bin/python -m pytest tests/test__geospatial.py --cov=mongomock_ng.geospatial --cov-report=term-missing
  
  # Specific test
  .env/bin/python -m pytest tests/test__geospatial.py::CoordinateValidationTest -v
  ```

## 🎯 Next Session Action Plan

### Phase 1 (Immediate - 30 min)
1. Verify cavecrew-builder fix for geo_intersects/geo_within
2. Run `pytest tests/test__geospatial.py -v` to confirm no regressions
3. Run coverage report to see if improvement

### Phase 2 (Fixes - 45 min)
1. **cavecrew-builder:** Fix geospatial.py:L129 float comparison
   - Edit `_validate_polygon_coords()` 
   - Use `_points_equal(ring[0], ring[-1])` instead of `ring[0] != ring[-1]`
   - Add test case for floating point tolerance

2. **cavecrew-builder:** Clarify collection.py:L1433 distance logic
   - Add detailed comment explaining min(distances) behavior
   - Add test case for multiple near fields

### Phase 3 (Testing - 45 min)
1. Add 10-15 more tests for aggregate.py $geoNear
2. Add 5-10 tests for collection.py near sorting edge cases
3. Run full pytest suite: `pytest tests/test__geospatial.py --cov=...`
4. Verify coverage targets: geospatial 90%+, aggregate 25%+, collection 30%+

### Phase 4 (Finalization - 30 min)
1. Run full test suite to check for regressions
2. Generate commit message using caveman-commit skill
3. Stage changes: `git add .`
4. Commit with message from caveman-commit
5. Show PR ready summary

## 📌 Command Reference

**Quick Test Run:**
```bash
cd /Users/felipemonteirojacome/workspace/mongomock-ng
.env/bin/python -m pytest tests/test__geospatial.py -v --tb=short
```

**Coverage Report:**
```bash
.env/bin/python -m pytest tests/test__geospatial.py \
  --cov=mongomock_ng.geospatial \
  --cov=mongomock_ng.filtering \
  --cov=mongomock_ng.aggregate \
  --cov=mongomock_ng.collection \
  --cov-report=term-missing
```

**Git Status:**
```bash
git status
git diff --stat HEAD
```

## 🔑 Key Files

| File | Lines | Status | Notes |
|------|-------|--------|-------|
| mongomock_ng/geospatial.py | 435 | 89% covered | Main implementation |
| mongomock_ng/filtering.py | L66-100 | 45% covered | $geoIntersects, $geoWithin ops |
| mongomock_ng/collection.py | L1419-1450 | 22% covered | $near sorting in find() |
| mongomock_ng/aggregate.py | L2820-2890 | 13% covered | $geoNear pipeline stage |
| tests/test__geospatial.py | 970 lines | ✅ 91 tests | All passing |

## 🐛 Known Issues (for future sessions)
1. geo_intersects with non-Point docs — IN PROGRESS
2. Float comparison in polygon validation — PENDING
3. Distance aggregation logic clarity — PENDING
4. aggregate.py test coverage — MEDIUM priority
5. collection.py test coverage — MEDIUM priority

## 💡 Tips for Next Session
- Use cavecrew-builder for surgical edits (1-2 files)
- Use caveman-review for final code review before commit
- Use caveman-commit for generating commit messages
- Always run pytest after each change to verify no regressions
- Keep tests focused: one concept per test function
- Use descriptive test names: `test_<function>_<scenario>_<expected_result>`

## 📍 Starting Point
1. Read this file
2. Check git status: `git status`
3. Run tests: `.env/bin/python -m pytest tests/test__geospatial.py -v`
4. Start with cavecrew-builder for remaining 3 bugs
5. Follow action plan in Phase 1-4
