```markdown
# Mypy: Problems and Solutions - Mongomock Reference

## Problem 1: "Already Defined" in Try/Except Imports

### Description
When you use a try/except pattern to import from an optional module (e.g., pymongo):

```python
try:
    from pymongo.results import InsertOneResult  # Line 1: Defines InsertOneResult
except ImportError:
    class InsertOneResult(_WriteResult):          # Line 2: Redefines InsertOneResult
        pass
```

**Mypy error:**
```
error: Name "InsertOneResult" already defined (possibly by an import) [no-redef]
```

### Cause
Mypy sees **two definitions of the same name** in the same file, violating its rule against redefining symbols. It does not understand the runtime "fallback" semantics.

### Solution: Intermediate Classes with Reassignment ✅

Instead of redefining the name directly, use intermediate classes:

```python
try:
    from pymongo.results import InsertOneResult
except ImportError:
    class _FallbackInsertOneResult(_WriteResult):  # Unique name
        pass
    
    InsertOneResult = _FallbackInsertOneResult      # Reassignment (not redefinition)
```

**Why it works:**
- Mypy sees only **one definition** of `InsertOneResult` (the reassignment on line 4)
- Python chooses which class to use at runtime
- No `# type: ignore` needed

### Implementation in Mongomock

**Fixed files:**
1. `mongomock/results.py` - 5 refactored classes
2. `mongomock/__init__.py` - 10 refactored error classes
3. `mongomock/collection.py` - 2 refactored classes

**Example result:**
- Before: 36 errors including 17× "already defined"
- After: 19 errors (all "already defined" resolved)

---

## Problem 2: Namedtuple with Incorrect Name

### Description
```python
_KwargOption = collections.namedtuple('KwargOption', [...])
```

**Error:**
```
error: First argument to namedtuple() should be "_KwargOption", not "KwargOption" [name-match]
```

### Cause
Mypy expects the first argument of `namedtuple()` (the name) to **match the variable name**.

### Solution ✅

```python
_KwargOption = collections.namedtuple('_KwargOption', [...])  # Name = '_KwargOption'
```

**Fixed files:**
- `mongomock/collection.py` - line 80
- `tests/test__database_api.py` - line 292

---

## Problem 3: Missing Type Stubs (Pytz)

### Description
```python
import pytz
```

**Error:**
```
mongomock/aggregate.py:17: error: Library stubs not installed for "pytz" [import-untyped]
Hint: "python3 -m pip install types-pytz"
```

### Cause
Pytz does not have type hints. Mypy needs `types-pytz` for full type checking.

### Solution ✅

```bash
pip install types-pytz
# or let mypy install automatically:
mypy --install-types
```

---

## Problem 4: Missing Type Arguments (Generics)

### Description
```python
from collections.abc import Iterable

def func(items: Iterable):  # ❌ Missing type argument
    pass
```

**Error:**
```
error: "Iterable" expects 1 type argument, but 0 given [type-arg]
```

### Solution ✅

```python
from collections.abc import Iterable
from typing import Any

def func(items: Iterable[Any]):  # ✅ Specify type
    pass
```

**Example in mongomock:**
- `mongomock/helpers.py:98` - `Iterable` without type argument

---

## Problem 5: Type Incompatibility in Assignment

### Description
```python
SUPPORTED_TYPES: tuple[type[float], type[bool], ...] = (
    float, bool, ...  # Types different from expected
)
```

**Error:**
```
error: Incompatible types in assignment (expression has type tuple[...], 
variable has type tuple[...]) [assignment]
```

### Cause
The tuple does not have exactly the expected types. Frequent in `codec_options.py` with `PYMONGO_VERSION`.

### Solutions:
1. **Fix the actual types** in the tuple
2. **Use `TypeAlias`** with more flexible types:
   ```python
   from typing import TypeAlias
   SUPPORTED_TYPES: TypeAlias = tuple[type, ...]
   ```
3. **Use `# type: ignore[assignment]`** if necessary

---

## Problem 6: "Cannot assign to a type"

### Description
```python
import re
Pattern = re.compile("").pattern.__class__  # ❌ Assigns type to a variable

# Later used:
def func(p: Pattern):
    pass
```

**Error:**
```
error: Cannot assign to a type [misc]
```

### Cause
Mypy interprets `Pattern` as a type, not a variable. Types cannot be assigned dynamically.

### Solution ✅

```python
from typing import Pattern as PatternType
# or
from re import Pattern

# Use the type directly:
def func(p: PatternType[str]):
    pass
```

---

## Checklist: Correct Mypy Configuration in Pre-Commit

- [x] Exclude `__init__.pyi` if there is a conflict (add to `.pre-commit-config.yaml`)
- [x] Ensure try/except classes use the reassignment pattern
- [x] Namedtuple names must match the variable name
- [ ] Install missing type stubs (`types-pytz`, etc.)
- [ ] Add type hints to generics (`Iterable[T]`, `Pattern[str]`)
- [ ] Annotate variables with complex types

---

## `.pre-commit-config.yaml` Configuration

```yaml
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.13.0
    hooks:
    -   id: mypy
        exclude: ^mongomock/__init__.pyi$  # Avoids duplicate module conflict
```

---

## Summary of Improvements

| Problem | Affected Files | Solution | Status |
|---------|----------------|----------|--------|
| "Already defined" in try/except | 3 files, 17 errors | Intermediate classes | ✅ Resolved |
| Namedtuple name mismatch | 2 files | Fix name | ✅ Resolved |
| Missing type stubs (pytz) | 1 file | Install types-pytz | ⏳ Pending |
| Missing type arguments | 1 file | Add generics | ⏳ Pending |
| Type incompatibility | 2 files | Fix types or aliasing | ⏳ Pending |
| "Cannot assign to a type" | 2 files | Use correct imports | ⏳ Pending |

**Overall status:** 19 mypy errors (reduced from 36 after refactorings)
```