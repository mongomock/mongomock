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

