import argparse
import json
import os
import re
import sys
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen


SOURCE_OWNER = 'mongomock'
SOURCE_REPO = 'mongomock'
TARGET_OWNER = 'engFelipeMonteiro'
TARGET_REPO = 'mongomock-ng'
MIGRATED_PR_LABEL = 'migrated-pr'
MIGRATED_PR_COLOR = 'bfd4f2'

Headers = dict[str, str]


def get_headers() -> Headers:
    token = os.environ.get('GITHUB_TOKEN')
    if not token:
        print('::error ::GITHUB_TOKEN env var is required')
        sys.exit(1)
    return {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github+json',
        'User-Agent': 'mongomock-ng-migrate',
    }


def api_get(url: str, headers: Headers) -> Any:
    req = Request(url, headers=headers, method='GET')  # noqa: S310
    with urlopen(req) as resp:  # noqa: S310
        return json.loads(resp.read())


def api_post(url: str, headers: Headers, data: dict[str, Any]) -> Any:
    body = json.dumps(data).encode()
    req = Request(url, data=body, headers=headers, method='POST')  # noqa: S310
    req.add_header('Content-Type', 'application/json')
    with urlopen(req) as resp:  # noqa: S310
        return json.loads(resp.read())


def paginate(url: str, headers: Headers) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page = 1
    while True:
        data = api_get(f'{url}&page={page}&per_page=100', headers)
        if not data:
            break
        items.extend(data)
        page += 1
    return items


def fetch_labels(source_repo_full: str, headers: Headers) -> dict[str, str]:
    url = f'https://api.github.com/repos/{source_repo_full}/labels?per_page=100'
    labels: dict[str, str] = {}
    for label in paginate(url, headers):
        labels[label['name']] = label['color']
    return labels


def ensure_labels(
    target_repo_full: str,
    headers: Headers,
    labels: dict[str, str],
    dry_run: bool,
) -> None:
    extra_labels = {}
    if MIGRATED_PR_LABEL not in labels:
        extra_labels[MIGRATED_PR_LABEL] = MIGRATED_PR_COLOR
    labels_to_create = {**labels, **extra_labels}
    for name, color in labels_to_create.items():
        if dry_run:
            print(f'  [dry-run] label: {name}')
            continue
        try:
            api_post(
                f'https://api.github.com/repos/{target_repo_full}/labels',
                headers,
                {'name': name, 'color': color},
            )
            print(f'  label created: {name}')
        except HTTPError as e:
            if e.code == 422:
                pass
            else:
                print(f'  label error {name}: {e.code}')
        time.sleep(0.1)


def fetch_migrated_ids(target_full: str, headers: Headers) -> set[int]:
    url = f'https://api.github.com/repos/{target_full}/issues?state=all&per_page=100'
    migrated: set[int] = set()
    for issue in paginate(url, headers):
        title = issue.get('title') or ''
        match = re.match(r'(?:\[PR\]\s*)?#(\d+):', title)
        if match:
            migrated.add(int(match.group(1)))
    return migrated


def migrate_issues(
    source_full: str,
    target_full: str,
    headers: Headers,
    dry_run: bool,
    max_issues: int,
) -> None:
    source_labels = fetch_labels(source_full, headers)
    print(f'Found {len(source_labels)} labels in source repo')
    ensure_labels(target_full, headers, source_labels, dry_run)
    print('Labels synced')

    url = f'https://api.github.com/repos/{source_full}/issues?state=open&per_page=100'
    all_issues = paginate(url, headers)

    issues = [i for i in all_issues if 'pull_request' not in i]
    prs = [i for i in all_issues if 'pull_request' in i]

    print(f'Found {len(issues)} open issues and {len(prs)} open PRs')

    migrated_ids = fetch_migrated_ids(target_full, headers)
    if migrated_ids:
        print(f'Already migrated: {len(migrated_ids)} issues/PRs')

    created = 0
    skipped = 0
    errors = 0
    for item in issues + prs:
        num = item['number']
        if num in migrated_ids:
            skipped += 1
            continue

        if max_issues and created >= max_issues:
            break

        is_pr = 'pull_request' in item
        prefix = '[PR] ' if is_pr else ''
        attach = (
            f'\n\n---\n*Originally submitted as PR at {source_full}#{num}*'
            if is_pr
            else f'\n\n---\n*Originally reported at {source_full}#{num}*'
        )
        body = (item['body'] or '') + attach
        label_names = [lab['name'] for lab in item['labels']]
        if is_pr:
            label_names.append(MIGRATED_PR_LABEL)

        title = f'{prefix}#{num}: {item["title"]}'

        if dry_run:
            print(f'  [dry-run] #{num}: {title[:60]}')
            continue

        try:
            result = api_post(
                f'https://api.github.com/repos/{target_full}/issues',
                headers,
                {'title': title, 'body': body, 'labels': label_names},
            )
            print(f'  #{result["number"]}: {title[:60]}')
            created += 1
        except HTTPError as e:
            print(f'  ERROR #{num} ({e.code}): {title[:60]}')
            errors += 1
        time.sleep(0.5)

    print(f'\nDone! {created} created, {skipped} skipped, {errors} errors')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--max', type=int, default=0)
    args = parser.parse_args()

    headers = get_headers()
    source = f'{SOURCE_OWNER}/{SOURCE_REPO}'
    target = f'{TARGET_OWNER}/{TARGET_REPO}'

    print(f'Migrating open issues from {source} to {target}')
    if args.dry_run:
        print('DRY RUN — no changes will be made')
    migrate_issues(source, target, headers, args.dry_run, args.max)


if __name__ == '__main__':
    main()
