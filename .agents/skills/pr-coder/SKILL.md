---
name: pr-coder
description: >
  Creates a branch in mongomock-ng based on an original mongomock PR, applying
  its code changes (commits, diff) to the target repo. The agent fetches the
  source PR diff, creates a branch, applies patches, and optionally opens a
  new PR linking back to the original.
  Trigger: "replicate PR", "apply PR", "cherry-pick PR", "clone PR",
  "port PR", "replicate changes", "replicate code".
---

## Overview

Mongomock PRs contain code changes that need to be ported to mongomock-ng.
This skill guides the agent to fetch the original PR's diff/patches, create
a corresponding branch in the target repo, apply the changes, and open a
new PR that references the original.

## Workflow

### 1. Select PR to replicate

Find a target issue with title `[PR] #N: ...` that has NOT been replicated
yet. Check:
- No branch exists named `pr/N-original`
- No existing PR in target repo references the original PR number

Label the source issue with `replicating` to mark it as in progress (avoid
duplicate work).

### 2. Fetch source PR diff

Get the full diff from mongomock:

```
GET /repos/mongomock/mongomock/pulls/N
```

Extract: `head.sha`, `head.ref` (source branch), `base.ref` (target branch).

Get the patch/diff in two formats:

**Unified diff** (for git apply):
```
GET /repos/mongomock/mongomock/pulls/N.diff
```

**Individual commits** (for cherry-pick approach):
```
GET /repos/mongomock/mongomock/pulls/N/commits?per_page=100
```

### 3. Create branch in mongomock-ng

Create a branch from the default branch:

```
POST /repos/engFelipeMonteiro/mongomock-ng/git/refs
{"ref": "refs/heads/pr/N-original", "sha": "<default-branch-tip-sha>"}
```

### 4. Apply changes

**Option A — git apply (simpler):**
1. Save the `.diff` content to a temp file
2. Run `git apply --3way patch.diff` on the branch checkout
3. Resolve conflicts if any (use LLM to decide resolution)
4. Commit with message referencing original PR

**Option B — individual commits (more faithful):**
1. For each commit in the source PR, fetch its patch:
   ```
   GET /repos/mongomock/mongomock/pulls/N/commits
   ```
2. Apply each commit's patch with `git apply`
3. Commit each with original message + `Co-authored-by: @original_author`
4. Preserves original authorship attribution

### 5. Push branch

```
git push origin pr/N-original
```

### 6. Open PR in target repo

Create a new PR:

```
POST /repos/engFelipeMonteiro/mongomock-ng/pulls
{
  "title": "Replicate #N: <original PR title>",
  "head": "pr/N-original",
  "base": "main",
  "body": "Original PR: mongomock/mongomock#N\n\n<original description>\n\n---\n*Replicated from mongomock/mongomock#N*"
}
```

### 7. Label and link

Add the `replicated` label to both the source issue and the new PR.

Update the source `[PR] #N` issue with a link to the new PR:
```
Comment: This PR was replicated as #target_pr_number.
```

## MongoDB version relevance

Before replicating, analyze if the PR change is relevant for MongoDB >= 7:

1. Read the PR description, diff, and linked issues
2. Check if the feature/behavior already exists in MongoDB 7+ (reference: MongoDB 7 release notes, PyMongo 7 compat)
3. If the change targets a deprecated/removed MongoDB feature, adapt to the equivalent MongoDB 7+ approach
4. If the change targets behavior already handled by mongomock-ng's current MongoDB 7 compat layer, skip or simplify

Document the MongoDB version relevance analysis in the PR body.

## Validation against real MongoDB

All replicated code must be validated against a real MongoDB instance using the
`_CollectionComparisonTest` pattern documented in
`.github/references/REAL_MONGODB_VALIDATION.md`.

### Setup

Start MongoDB via docker-compose (the user uses podman):

```
podman compose up -d mongo
```

Or use the docker-compose.yml service directly.

Set environment (optional, default is localhost:27017):
```
export TEST_MONGO_HOST=localhost:27017
```

### Run comparison tests

Run the existing comparison tests to ensure nothing is broken:

```
hatch test
```

For new features, write tests following the `_CollectionComparisonTest` pattern:

```python
class MyNewFeatureTest(_CollectionComparisonTest):
    def test__my_feature(self):
        self.cmp.do.insert_one({'value': 1})
        self.cmp.compare.aggregate([{'$match': {'value': 1}}])
```

### Skip if no real MongoDB

Tests automatically skip if `NO_LOCAL_MONGO=1` is set or pymongo is not installed.

## Test coverage

New code must have >= 80% test coverage. Use pytest-cov to measure:

```
python -m pytest --cov=mongomock_ng --cov-report=term-missing tests/
```

Focus on:
1. Lines added/modified by the PR — ensure each branch is exercised
2. Edge cases (empty inputs, None, exceptions, type mismatches)
3. MongoDB version-specific paths (if any)

If coverage is below 80%, add tests for the uncovered lines. The
`--cov-report=term-missing` output shows exactly which lines are missed.

For targeted coverage on specific files only:

```
python -m pytest --cov=mongomock_ng/path/to/file.py tests/
```

## Pre-commit

Run pre-commit before every push to catch issues early:

```
pre-commit run --all-files
```

This runs all hooks defined in `.pre-commit-config.yaml` (ruff, ruff-format,
mypy). If any hook fails, fix the issues and re-run until clean.

To install pre-commit as a git hook (one-time setup):
```
pre-commit install
```

## Code quality checks

All replicated code must pass both lint and type checks:

```
ruff check .
ruff format --check .
python -m mypy . --strict
```

If ruff reports errors:
1. Auto-fix with `ruff check --fix .`
2. If any remain, fix manually
3. Run `ruff format .` to match project style (single quotes, 100 chars)

If mypy reports errors:
1. Fix type annotations in the applied code
2. Ensure new functions/variables have proper type hints
3. Do NOT add `# type: ignore` unless the original mongomock code also has it
4. Run mypy again until clean

Run all checks in sequence before pushing. The replicated code must match the
project's existing quality standards.

## Conflict resolution

If `git apply --3way` fails:
1. LLM analyzes the conflicting hunks
2. Adapts to mongomock-ng's current codebase state
3. Applies resolved version manually (edit + commit)
4. Document changes made vs original in the PR description

## Commit message format

When applying individual commits:
```
<original message>

(cherry picked from commit <sha>)
Co-authored-by: <original author>
```

When squashing all changes:
```
Replicate mongomock/mongomock#N: <title>

Original commits:
- <sha>: <message>
- <sha>: <message>

*Replicated from mongomock/mongomock#N*
```

## Detection

Check if already replicated:
- Target repo has branch `pr/N-original`
- Target repo has PR with title containing `Replicate #N:`
- Source issue has label `replicated`

## Branch naming

`pr/<source-number>-original` for straightforward ports.
If modifications are needed, use `pr/<source-number>-adapted`.
