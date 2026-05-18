# Copilot Instructions for mongomock-ng-fork

## Project Overview
This is a mongomock-ng library (fork of mongomock), which provides a lightweight in-memory MongoDB mock for testing Python applications. The codebase simulates MongoDB's query language (filters, projections, aggregations) and operations on collections. All writen code, docs and tests is in english, even if the original codebase, tests, issues or promts is in other languages.

## Code Style
- Follow PEP 8, with a maximum line length of 88 characters (Black-compatible).
- Use type hints for all new function signatures.
- Prefer `def` over `lambda` for readability, except in trivial helpers.
- Docstrings must use Google style, describing parameters, return values, and exceptions.
- All public methods should have a docstring; private helpers may have brief comments.

## Testing
- The test suite lives in `tests/` and uses `pytest`.
- Tests should be isolated and not depend on external services.
- Mocking of PyMongo internals is done via monkey-patching or the `MongoClient` mock.
- When fixing a bug, always include a test that reproduces the issue.
- Run `pytest` before proposing any changes.

## Architecture Notes
- The core mock is a `Database` object that holds `Collection` mocks.
- Operations (insert, find, update, aggregate) are implemented by interpreting the MongoDB query language as closely as possible.
- Be aware of `pymongo`'s version compatibility; support pymongo >= 4.0, but ideally >= 4.3.
- When adding a new operation or operator, mimic the actual MongoDB behavior as documented in MongoDB Manual.
- Exception handling must match real `pymongo.errors` (raise `CollectionInvalid`, `DuplicateKeyError`, etc.) where appropriate.

## Refactoring Instructions
- Prefer small, focused commits that don't mix formatting and logic changes.
- Use `ruff` for linting and `black` for formatting; do not disable formatter rules without a comment.
- When adding Python 3.10+ features (e.g., `match`/`case`), keep backward compatibility with Python 3.9 if possible, but prioritize clean code.
- For any external dependency update, ensure it's reflected in `setup.cfg`/`pyproject.toml` and that tests pass on the new version.

## Copilot Behavior
- When asked to "add support for MongoDB operator X", generate:
  1. The implementation in the appropriate handler (e.g., mongomock-ng/collection.py).
  2. A unit test in `tests/test__collection.py` or a new test file.
  3. Update any relevant documentation strings.
- If a user asks for a bug fix, first output a failing test, then the fix.
- For documentation requests, provide concise Markdown snippets suitable for the README or a Sphinx docs directory.
- When generating code, prefer readability over cleverness.
- Avoid generating long scripts that change the public API unless explicitly requested.

## Reference Material in `references/`

To minimize token consumption and avoid re-explaining concepts, the repository
maintains a `references/` folder with persistent documentation. The Copilot
must treat the contents of this folder as **source of truth** and actively use
and update it.
The references MUST be writen in English, even if the codebase, tests or proompt are in other languages.

### Structure
The folder is organized into the following files (create if missing):

- **`references/api-coverage.md`** – List of supported MongoDB operations,
  operators, and expressions. Mark clearly what is fully implemented, partially
  implemented, and what is planned. Use tables for quick overview.
- **`references/decisions.md`** – Architecture Decision Records (ADR) that
  explain *why* certain choices were made (e.g., “we parse `$regex` using
  Python’s `re` module instead of delegating to PyMongo because ...”).
- **`references/compatibility.md`** – Compatibility matrix: Python versions,
  PyMongo versions, and any known quirks. Also list which MongoDB server
  features are deliberately not supported.
- **`references/patterns.md`** – Common code patterns used in the project
  (e.g., how to register a new operator handler, how tests are structured).
- **`references/glossary.md`** – Term mapping between MongoDB concepts and
  internal class/function names (e.g., “MongoDB 'collection scan' → our
  `_filter_documents` method”).

### Usage rules for Copilot
1. **Before implementing a new feature or bugfix**, quickly scan the relevant
   reference file(s) to understand existing support and conventions.
2. **After any significant change**, propose an update to the affected
   reference file (add new operators to `api-coverage.md`, document a new
   pattern, update compatibility notes, etc.). Output the proposed file diff.
3. When the user asks a question about “what is supported”, **prefer answering
   from the reference files** instead of scanning the entire codebase.
4. If a reference file is missing, suggest creating it with an initial
   scaffold.
5. Keep the files **concise and tabular** when possible – this further saves
   tokens when they are read back in future sessions.