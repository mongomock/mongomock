default: test

detox-test:
	detox

travis-test: test

test: env
	.env/bin/pytest tests/

coverage-test: env
	.env/bin/pytest --cov=mongomock --cov-report=html tests/

env: .env/.up-to-date

.env/.up-to-date: pyproject.toml Makefile
	virtualenv .env
	.env/bin/pip install -e .
	.env/bin/pip install pytest pytest-cov PyExecJS pymongo
	touch .env/.up-to-date



.PHONY: doc

# Create a release: run tests, build, create annotated tag `vM.m.P` and push it.
# Usage: `make release VERSION=7.0.0`
release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Specify VERSION, e.g. make release VERSION=7.0.0"; exit 1; \
	fi
	@echo "Checking VERSION format: $(VERSION)"
	@echo "$(VERSION)" | grep -E '^([0-9]+)\.([0-9]+)\.([0-9]+)$$' >/dev/null || \
		( echo "ERROR: VERSION must match M.m.P (semver), e.g. 7.0.0"; exit 1 )

	@echo "Running tests..."
	@$(MAKE) test

	@echo "Building distributions..."
	@hatch build

	@echo "Artifacts in dist/:"; ls -la dist || true

	@echo "Creating annotated Git tag v$(VERSION)"
	@git tag -a v$(VERSION) -m "Release v$(VERSION)"
	@echo "Pushing tag to origin"
	@git push origin v$(VERSION)

# Publish built artifacts to PyPI using Hatch. Requires `PYPI_USERNAME` and `PYPI_PASSWORD` env vars
# Usage: `make publish-release VERSION=7.0.0`
publish-release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Specify VERSION, e.g. make publish-release VERSION=7.0.0"; exit 1; \
	fi
	@echo "Publishing release for v$(VERSION) using hatch publish"
	@HATCH_INDEX_USER="${PYPI_USERNAME}" HATCH_INDEX_AUTH="${PYPI_PASSWORD}" hatch publish

