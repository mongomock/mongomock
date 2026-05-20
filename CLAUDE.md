# mongomock-ng — CLAUDE.md

## Identity
In-memory MongoDB mock for Python testing. Fork of mongomock. Simulates MongoDB query language (filters, projections, aggregation) on Collection mocks.

## Code Style
- PEP 8, line length 88, Black-compatible. Ruff for linting.
- Type hints on all new signatures. Google-style docstrings for public methods.
- All code/docs/tests in English, even if source material is in another language.
- Prefer `def` over `lambda`. Readability > cleverness.
- Python >=3.10 required (PEP 604 `X | Y` syntax OK, `match`/`case` OK).

## Testing
- `pytest` in `tests/`. Isolated, no external services.
- Bug fix → include reproducer test first.
- Run `pytest` before proposing changes.

## Architecture
- Core: `Database` → `Collection` mocks. Operations parse MongoDB query language.
- Mimic real MongoDB behavior (docs.mongodb.com). Match `pymongo.errors` exception types.
- Support pymongo >=4.0, ideally >=4.3.
- New operator → handler in appropriate file + test + update `CHANGELOG.md`.

## Token Efficiency
Use caveman/cavecrew skills to save context:
- `/caveman` — ultra-compressed communication mode
- `cavecrew-investigator` — locate code/symbols (returns ~60% smaller output than vanilla Explore)
- `cavecrew-builder` — surgical 1-2 file edits (returns compressed confirmation)
- `cavecrew-reviewer` — diff/branch review in one-line-per-finding format
- Trigger: "delegate to subagent", "spawn investigator", "use cavecrew"

## Reference Files (Source of Truth)
`.agents/references/` — persistent docs for API coverage, decisions, compatibility, patterns.
Scan before implementing features. Update after significant changes.

## Conventional Commits
Subject ≤50 chars. Type: feat/fix/refactor/style/docs/chore.
Body only when "why" isn't obvious from subject.
