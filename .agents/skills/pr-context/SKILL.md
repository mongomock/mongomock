---
name: pr-context
description: >
  Enriches migrated PR issues in the target repo with full context from source
  mongomock PRs using the LLM agent. The agent fetches source PR data (comments,
  commits, files changed, linked issues), analyzes it, and posts enriched
  summaries to the corresponding target issues.
  Trigger: "enrich PRs", "migrate PRs", "sync PR data", "backfill PR info",
  "handle existing PRs", "update migrated PRs".
---

## Overview

PRs from `mongomock/mongomock` were migrated as issues (title: `[PR] #N: ...`)
to `engFelipeMonteiro/mongomock-ng`. The initial migration only copied the PR
description and a source footer. The agent backfills the full context using
the GitHub API, with LLM analysis to summarize and connect related issues.

## Workflow

### 1. Discover unenriched PRs

Fetch all open issues from the target repo and filter for `[PR] #(\d+):`:

```
GET /repos/engFelipeMonteiro/mongomock-ng/issues?state=open&per_page=100
```

Skip any that already have an "Original PR Context" comment or body section
(already enriched). The remaining need backfill.

### 2. Fetch source data from mongomock

For each source PR number `N`, fetch:

**PR details** (description, state, author, labels, created_at):
```
GET /repos/mongomock/mongomock/issues/N
GET /repos/mongomock/mongomock/pulls/N
```

**Comments** (paginate all pages):
```
GET /repos/mongomock/mongomock/issues/N/comments?per_page=100
```

**Commits** (sha, message, author):
```
GET /repos/mongomock/mongomock/pulls/N/commits?per_page=100
```

**Files changed** (filename, status, additions, deletions):
```
GET /repos/mongomock/mongomock/pulls/N/files?per_page=100
```

### 3. LLM analyzes and enriches

With all source data in hand, the agent uses the LLM to:

- **Summarize** the PR: what it does, why it matters
- **Summarize comments**: key discussions, decisions, open questions
- **Identify linked issues**: scan body + comments for `Fixes #X`, `Closes #X`,
  `Related to #X`, `mongomock/mongomock#X`
- **Cross-reference migrated issues**: for each linked `#X`, search target repo
  for title `#X:` — if found, include the link (e.g., `→ #target_num`)
- **Highlight key changes**: from files changed + commits

### 4. Post to target issue

Post the enriched content as a comment:

```
POST /repos/engFelipeMonteiro/mongomock-ng/issues/{target_num}/comments
```

Structure:

```
## Original PR Context

**State:** merged | open | closed
**Author:** @user
**Created:** date
**Labels:** label1, label2

### Summary
<LLM-generated summary of the PR>

### Commits
| SHA | Author | Message |
|-----|--------|---------|
| abc1234 | @user | Fix thing |

### Files Changed
| File | Status | +/- |
|------|--------|-----|
| path/to/file.py | modified | +5 -2 |

### Linked Issues
- Fixes mongomock#X → migrated as #target_issue
- Related to #Y

### Discussion Summary
<LLM-generated summary of key comments, decisions, and open questions>
```

### 5. Add enrichment label

Add the label `llm-enriched` to the target issue to mark it as processed:

```
POST /repos/engFelipeMonteiro/mongomock-ng/issues/{target_num}/labels
{"labels": ["llm-enriched"]}
```

If the label doesn't exist yet, create it first:

```
POST /repos/engFelipeMonteiro/mongomock-ng/labels
{"name": "llm-enriched", "color": "bfd4f2"}
```

### 6. Update issue body (optional)

If the target issue body only has the raw PR description + footer, replace it
with the enriched body using:

```
PATCH /repos/engFelipeMonteiro/mongomock-ng/issues/{target_num}
```

Include the enriched summary, structured sections, and the original footer.

## GitHub API

- Auth: `GITHUB_TOKEN` env var (Bearer token)
- Accept: `application/vnd.github+json`
- User-Agent: `mongomock-ng-migrate`
- Write rate: `time.sleep(0.5)` between POST/PATCH requests

## Enrichment detection

Skip already-enriched issues. Check two signals:
1. Issue has label `llm-enriched`
2. Issue body or any comment contains `## Original PR Context`

If either is true, skip.

## Tools

The agent has access to:
- **bash/curl** — for GitHub API calls
- **grep/glob** — for file search in repo
- **read/write/edit** — for creating/modifying files
