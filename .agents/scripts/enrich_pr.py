#!/usr/bin/env python3
"""
Enrich a migrated PR issue with full context from the source mongomock PR.
Usage: python3 enrich_pr.py <target_issue_number>
"""
import json, os, re, sys, time, urllib.request

GH_TOKEN = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
if not GH_TOKEN:
    # Try to get from gh CLI
    import subprocess
    try:
        result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=10)
        GH_TOKEN = result.stdout.strip()
    except Exception:
        pass

if not GH_TOKEN:
    print("FATAL: GITHUB_TOKEN or GH_TOKEN env var required")
    sys.exit(1)

TARGET_REPO = "engFelipeMonteiro/mongomock-ng"
SOURCE_REPO = "mongomock/mongomock"
HEADERS = {
    "Authorization": f"Bearer {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "mongomock-ng-migrate"
}

def gh_api(method, url, data=None):
    if not url.startswith("https://"):
        url = f"https://api.github.com{url}"
    req = urllib.request.Request(url, headers=HEADERS, method=method)
    if data is not None:
        req.data = json.dumps(data).encode()
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  HTTP {e.code}: {body[:200]}")
        return None

def get_target_issue(num):
    return gh_api("GET", f"/repos/{TARGET_REPO}/issues/{num}")

def post_comment(issue_num, body):
    return gh_api("POST", f"/repos/{TARGET_REPO}/issues/{issue_num}/comments",
                  {"body": body})

def add_label(issue_num, label):
    return gh_api("POST", f"/repos/{TARGET_REPO}/issues/{issue_num}/labels",
                  {"labels": [label]})

def get_source_pr(pr_num):
    """Get source PR details, comments, commits, files"""
    pr = gh_api("GET", f"/repos/{SOURCE_REPO}/pulls/{pr_num}")
    if not pr or "number" not in pr:
        print(f"  Source PR #{pr_num} not found, trying issue endpoint...")
        pr = gh_api("GET", f"/repos/{SOURCE_REPO}/issues/{pr_num}")
    comments = gh_api("GET", f"/repos/{SOURCE_REPO}/issues/{pr_num}/comments?per_page=100") or []
    commits = gh_api("GET", f"/repos/{SOURCE_REPO}/pulls/{pr_num}/commits?per_page=100") or []
    files = gh_api("GET", f"/repos/{SOURCE_REPO}/pulls/{pr_num}/files?per_page=100") or []
    return pr, comments, commits, files

def format_commits(commits):
    rows = []
    for c in commits[:20]:
        sha = c.get("sha", "")[:7]
        author = c.get("author", {}) or {}
        author_login = author.get("login", c.get("commit", {}).get("author", {}).get("name", "?"))
        msg = c.get("commit", {}).get("message", "").split("\n")[0][:80]
        rows.append(f"| {sha} | @{author_login} | {msg} |")
    return "\n".join(rows) if rows else "_(no commits)_"

def format_files(files):
    rows = []
    for f in files[:30]:
        fn = f.get("filename", "?")
        status = f.get("status", "?")
        add = f.get("additions", 0)
        dele = f.get("deletions", 0)
        rows.append(f"| `{fn}` | {status} | +{add}/-{dele} |")
    return "\n".join(rows) if rows else "_(no files)_"

def format_comments(comments):
    parts = []
    for c in comments[:30]:
        user = c.get("user", {}).get("login", "?")
        created = c.get("created_at", "?")[:10]
        body = c.get("body", "")[:300]
        parts.append(f"**@{user}** ({created}):\n{body}\n")
    return "\n\n".join(parts) if parts else "_(no comments)_"

def enrich(target_num, source_pr_num):
    print(f"\n{'='*60}")
    print(f"Enriching #{target_num} from source PR #{source_pr_num}")
    print(f"{'='*60}")

    # Check if already enriched
    issue = get_target_issue(target_num)
    if not issue:
        print(f"  FAIL: Could not fetch target issue #{target_num}")
        return False

    labels = [l["name"] for l in (issue.get("labels") or [])]
    if "llm-enriched" in labels:
        print(f"  SKIP: already has llm-enriched label")
        return True

    body = issue.get("body") or ""
    if "## Original PR Context" in body:
        print(f"  SKIP: already has Original PR Context")
        return True

    # Fetch source data
    pr, comments, commits, files = get_source_pr(source_pr_num)
    if not pr:
        print(f"  FAIL: Could not fetch source PR #{source_pr_num}")
        return False

    pr_title = pr.get("title", "?")
    pr_state = pr.get("state", "?")
    pr_author = (pr.get("user", {}) or {}).get("login", "?")
    pr_created = (pr.get("created_at") or "?")[:10]
    pr_labels = ", ".join(l["name"] for l in (pr.get("labels") or []) if l.get("name"))
    pr_body = (pr.get("body") or "_(no description)_")[:500]

    # Build linked issues
    linked = []
    for ref in re.findall(r'(Fixes|Closes|Resolves|Related to|See)\s+(#\d+|mongomock/mongomock#\d+)', body + " " + " ".join(c.get("body","") for c in comments)):
        linked.append(f"{ref[0]} {ref[1]}")
    linked = list(set(linked))

    # Build enrichment comment
    comment = f"""## Original PR Context

**State:** {pr_state}
**Author:** @{pr_author}
**Created:** {pr_created}
**Labels:** {pr_labels or "_(none)_"}

### Summary
{pr_title}

### Description
{pr_body}

### Commits
| SHA | Author | Message |
|-----|--------|---------|
{format_commits(commits)}

### Files Changed
| File | Status | +/- |
|------|--------|-----|
{format_files(files)}

### Linked Issues
{chr(10).join(f"- {l}" for l in linked) if linked else "_(none identified)_"}

### Discussion Summary
{format_comments(comments) if comments else "_(no comments on original PR)_"}

---
*Automatically enriched from mongomock/mongomock PR #{source_pr_num}*
"""

    # Post comment
    result = post_comment(target_num, comment)
    if not result:
        print(f"  FAIL: Could not post comment to #{target_num}")
        return False
    print(f"  Comment posted: {result.get('html_url', '?')}")

    # Add label
    time.sleep(0.5)
    result = add_label(target_num, "llm-enriched")
    if result:
        print(f"  Label 'llm-enriched' added")
    else:
        print(f"  WARN: Could not add label (may already exist)")

    print(f"  DONE: #{target_num} enriched")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 enrich_pr.py <target_issue_number> [source_pr_number]")
        sys.exit(1)
    target = int(sys.argv[1])
    source = int(sys.argv[2]) if len(sys.argv) > 2 else None
    # Extract source PR from target issue if not provided
    if not source:
        issue = get_target_issue(target)
        if not issue:
            print(f"FATAL: Could not fetch target issue #{target}")
            sys.exit(1)
        match = re.search(r'\[PR\] #(\d+)', issue.get("title", ""))
        if not match:
            print(f"FATAL: Could not extract source PR number from title: {issue.get('title')}")
            sys.exit(1)
        source = int(match.group(1))
    enrich(target, source)
