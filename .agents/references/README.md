# References Index

LLM entry point for project knowledge. Scan this first to decide which refs to read.

## When to Read References

| Scenario | Read |
|----------|------|
| Implement new stage/operator | `MDB7_COMPAT.md` → `aggregation-prs/README.md` |
| Review aggregation code | `aggregation-prs/completed-features.md` |
| Check existing impl details | `aggregation-prs/completed-features.md` |
| Code review PR | `aggregation-prs/REVIEW_PR195.md` (patterns) |
| Before release/tag | `MDB7_COMPAT.md` (gaps worth fixing?) + `pr-prompts/16-close-resolved-issues.md` |
| Diagnose test failure | `aggregation-prs/completed-features.md` |
| Any unknown feature | `MDB7_COMPAT.md:Not Tracked` |
| Close stale issues | `pr-prompts/16-close-resolved-issues.md` |

## Adding New Findings

Discovered a gap vs MongoDB? Add it to `MDB7_COMPAT.md`.
Discovered a bug/behavior difference? Add to `MDB7_COMPAT.md:Known Bugs`.
Implemented a new feature? Update references (status, impl notes, test locations).

---

## Reference Files

### `MDB7_COMPAT.md`
- **Purpose**: Track MongoDB 7+ compatibility gaps vs mongomock-ng
- **Covers**: Missing stages, operators, window clause formats, known untracked features
- **Refresh**: After every new feature impl or code review finding

### `aggregation-prs/README.md`
- **Purpose**: Index of 10 PRs replicated from original mongomock
- **Covers**: Which PRs applied, what features, file impact, complexity
- **Refresh**: After replicating new mongomock PRs

### `aggregation-prs/completed-features.md`
- **Purpose**: Implementation details for `$convert`/`$setWindowFields`/`$fill`
- **Covers**: Handler locations, operator list, test locations, edge cases
- **Refresh**: After changing any of these 3 features

### `aggregation-prs/REVIEW_PR195.md`
- **Purpose**: Code review findings from PR #195 consolidation
- **Covers**: Per-operator review notes, test coverage comments
- **Refresh**: Not needed (historical)

### `aggregation-prs/COMPLETE_PARTIAL_PROMPT.md`
- **Purpose**: Prompt used for partial feature completion
- **Covers**: Remaining work for `$convert`/`$setWindowFields`/`$fill`
- **Refresh**: Not needed (historical)
