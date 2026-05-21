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

## Reference Files (Persistent LLM Memory)
`.agents/references/` — memory store across sessions. Prevents context loss, ensures precision.

### Protocol
1. **Always scan** `.agents/references/README.md` first — it tells you which files to read for your task
2. **Read relevant refs** before implementing features, reviewing code, or diagnosing issues
3. **Update refs** after any significant change:
   - New feature → update status in `MDB7_COMPAT.md` + `completed-features.md`
   - New gap/bug found → add to `MDB7_COMPAT.md:Known Bugs` or `Not Tracked`
   - New mongomock PR replicated → update `aggregation-prs/README.md` index
4. **Metadata headers** at top of each ref file tell you: Purpose, Type, Covers, Refresh cadence

### File index
| File | When to read |
|------|-------------|
| `README.md` | First — entry point, decides what else to read |
| `MDB7_COMPAT.md` | New feature, release prep, gap discovery |
| `aggregation-prs/README.md` | Replicating mongomock PRs |
| `aggregation-prs/completed-features.md` | Need impl details for `$convert`/`$setWindowFields`/`$fill` |

## Conventional Commits
Subject ≤50 chars. Type: feat/fix/refactor/style/docs/chore.
Body only when "why" isn't obvious from subject.

## Personal enviroment
- local python bin at .env/bin/python
