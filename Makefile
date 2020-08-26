clean:
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '.pytest_cache' -exec rm -fr {} +
	find . -name '.mypy_cache' -exec rm -fr {} +
	find . -name 'pip-wheel-metadata' -exec rm -fr {} +
	find . -name 'modelark.egg-info' -exec rm -fr {} +

test:
	pytest

mypy:
	mypy modelark

COVFILE ?= .coverage

coverage:
	export COVERAGE_FILE=$(COVFILE); pytest -x --cov-branch \
	--cov=modelark tests/ --cov-report term-missing -s -o \
	cache_dir=/tmp/.pytest_cache -vv

PART ?= patch

version:
	bump2version $(PART) pyproject.toml modelark/__init__.py --tag --commit
