default: test

detox-test:
	detox

travis-test: test

test: env
	.env/bin/pytest tests/

coverage-test: env
	.env/bin/pytest --cov=mongomock_ng --cov-report=html tests/

env: .env/.up-to-date

.env/.up-to-date: pyproject.toml Makefile
	virtualenv .env
	.env/bin/pip install -e .
	.env/bin/pip install pytest pytest-cov PyExecJS pymongo
	touch .env/.up-to-date

.PHONY: doc build fmt hatch-test docker-build docker-run docker-hatch-test delete-tag

# Run the Hatch formatter (README: `hatch fmt`)
fmt:
	hatch fmt

fix:
	hatch fmt

# Run tests with Hatch (README: `hatch test`)
hatch-test:
	hatch test

# Docker helpers (README: docker compose build / run)
docker-build:
	docker compose build

docker-run:
	docker compose run --rm mongomock_ng

# Run tests inside the docker service (customizable: PYTHON, PYMONGO, TEST)
# Usage: make docker-hatch-test PYTHON=3.12 PYMONGO=4 TEST="tests/..."
docker-hatch-test:
	docker compose run --rm mongomock_ng hatch test -py=${PYTHON} -i pymongo=${PYMONGO} ${TEST}


# Build distribution packages locally for testing.
build:
	hatch build

.PHONY: doc

# Delete a tag locally and from origin (undo a mistaken release).
# Usage: `make delete-tag VERSION=7.0.0`
delete-tag:
	@if [ -z "$(VERSION)" ]; then \
		echo "Specify VERSION, e.g. make delete-tag VERSION=7.0.0"; exit 1; \
	fi
	@echo "Deleting tag v$(VERSION) locally and from origin..."
	-git tag -d v$(VERSION)
	-git push origin :refs/tags/v$(VERSION)



