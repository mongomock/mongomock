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
Use caveman/cavecrew skills in always-on mode to save context.

### Always-on defaults
- `caveman` active by default in `full` mode for all normal responses.
- Keep caveman mode persistent until user explicitly says `stop caveman` or `normal mode`.
- Temporarily drop caveman compression when clarity matters: security warnings, irreversible actions, or multi-step instructions where terse phrasing could confuse order/meaning.
- Resume caveman mode after the clear section.

### Delegation defaults
- Prefer `cavecrew` subagents over vanilla subagents when delegation helps and compressed output is enough.
- Use `cavecrew-investigator` for code location, symbol usage, callers, and test discovery.
- Use `cavecrew-builder` for surgical edits limited to 1-2 files when target files are already known.
- Use `cavecrew-reviewer` for diff/branch/file review when findings-first output is desired.
- Use vanilla exploration/review only when longer prose, architecture discussion, or broad cross-file reasoning is more important than token savings.

### Skill auto-use rules
- Use `caveman-commit` by default when generating commit messages.
- Use `caveman-review` by default when user asks for code review or PR review.
- Use `caveman-compress` when user asks to compress memory/docs files such as `CLAUDE.md`.
- Use `caveman-help` and `caveman-stats` on explicit request.

## Current Task: Geospatial PR — Session 2+
**Branch:** `feat/geospatial-support`  
**Status:** 91 tests (+29 new), 89% coverage on geospatial.py, 4 bugs fixed  
**Next:** Extend geo_intersects for non-Point docs, float comparison, distance logic  
**Details:** `.agents/session-context.md`

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
