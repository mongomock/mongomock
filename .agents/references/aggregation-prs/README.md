# Aggregation PR Consolidation — Reference

## Context
Replicated from mongomock/mongomock. Target: mongomock-ng compliance with MongoDB 7+/PyMongo latest.

## Current State
- `mongomock_ng/aggregate.py` — main aggregation code (~1750 lines)
- `mongomock_ng/collection.py` — `_apply_update_pipeline` (line 910) already implemented
- `_PIPELINE_HANDLERS` dict at aggregate.py:1694-1725
- Version: 7.0.3

## Selected PRs (10 total)

| # | Source PR | Issue | Status | Feature | File impact | Complexity |
|---|---|---|---|---|---|---|
| 1 | mongomock#820 | #177 | ✅ | `$reduce` array op | aggregate.py _handle_array_operator | medium |
| 2 | mongomock#821 | #176 | 🟡 | `$setWindowFields` stage | aggregate.py new handler + accumulator | high |
| 3 | mongomock#822 | #175 | ✅ | `$expression` in timezone | aggregate.py _handle_date_operator (1 line) | low |
| 4 | mongomock#864 | #167 | 🟡 | `$convert` operator | aggregate.py refactor + new handler | high |
| 5 | mongomock#892 | #160 | 🟡 | `$fill` stage | aggregate.py new handler + _PIPELINE_HANDLERS | low |
| 6 | mongomock#896 | #157 | ✅ | `$sortByCount` stage | aggregate.py new handler + _PIPELINE_HANDLERS | low |
| 7 | mongomock#925 | #151 | ✅ | `$unset` stage (nested) | aggregate.py new handler + _PIPELINE_HANDLERS | medium |
| 8 | mongomock#929 | #150 | ✅ | `$type` agg operator (+date) | aggregate.py type_operators + handler | low |
| 9 | mongomock#930 | #149 | ✅ | `$round` (w/ place) | aggregate.py arithmetic operators | low |
| 10 | mongomock#935 | #146 | ✅ | `$toObjectId` operator | aggregate.py type_convertion_operators + handler | low |

## Excluded PRs + reasons
- #742 (agg in updates): already implemented
- #770 (NOTHING/KeyError refactor): cross-cutting risk, no compat gain
- #816 ($round): superseded by #930
- #838 ($type): superseded by #929
- #863 ($unset): superseded by #925
- #920 (examples): docs only

## Diff files
Individual `.diff` files for each PR are in this directory named `<pr-number>.diff`.

## Related mongomock-ng PRs
| PR | Status | Description |
|----|--------|-------------|
| #195 | 🟣 | Aggregation consolidation (base PR) |
| #196 | 🟢 | v7.1.1: typo fix + MDB7 compat ref + ref structure |
