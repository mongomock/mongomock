# Contributing

## Prerequisites

- Python >= 3.10
- [hatch](https://hatch.pypa.io/) (`pipx install hatch`)
- [pre-commit](https://pre-commit.com/) (`pip install pre-commit`)

## Setup

```bash
git clone git@github.com:engFelipeMonteiro/mongomock-ng.git
cd mongomock-ng
pre-commit install
```

## Running Tests

```bash
hatch test
```

## Code Quality

```bash
# Lint
ruff check mongomock_ng/ tests/

# Auto-fix
ruff check --fix mongomock_ng/ tests/

# Format
ruff format mongomock_ng/ tests/

# Type check
python -m mypy -p mongomock_ng
```

## Pre-commit Hooks

The project uses pre-commit hooks (ruff, ruff-format, mypy). To run manually:

```bash
pre-commit run --all-files
```

## Branching Model

We use [gitflow workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow).
PRs should be issued against the `develop` branch.

## Releasing

1. Update `__version__` in `mongomock_ng/__version__.py`
2. Update `CHANGELOG.md` with the same version
3. Open a PR against `develop` — on merge, a tag `v<version>` is created automatically
4. The tag push triggers a build and publish to PyPI

## Container Engine

The Makefile uses a configurable container engine via `.container-engine`:

```bash
echo podman > .container-engine   # default
echo docker > .container-engine
```
