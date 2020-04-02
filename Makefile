# --- User Parameters

# Package directory
PKG_DIR=yfinance

# Testing parameters
NPROCS=auto

# --- Internal Parameters

# Code directories
CODE_DIRS=${PKG_DIR}

# Testing parameters
PYTEST_SEARCH_PATHS=${CODE_DIRS}
PYTEST_OPTIONS=-n ${NPROCS} --cov=${PKG_DIR}
PYTEST_PYLINT_OPTIONS=

# --- Targets

# Default target
all: fast-test

# Testing
fast-test fast-check:
	make test PYTEST_OPTIONS="-x ${PYTEST_OPTIONS}"

full-test full-check:
	make test PYTEST_PYLINT_OPTIONS="--pylint --pylint-error-types=EF";

test check:
	pycodestyle setup.py
	py.test ${PYTEST_SEARCH_PATHS} ${PYTEST_OPTIONS} ${PYTEST_PYLINT_OPTIONS}

.coverage:
	-make test

coverage-report: .coverage
	coverage report -m

coverage-html: .coverage
	coverage html -d coverage

# Code quality
radon-mi:
	radon mi ${CODE_DIRS} -s --sort
radon-mi-fail:
	radon mi ${CODE_DIRS} -nB -s --sort
radon-cc:
	radon cc ${CODE_DIRS} --total-average
radon-cc-fail:
	radon cc ${CODE_DIRS} -nC --average
radon-raw:
	radon raw ${CODE_DIRS} -s

# Package distribution
dist: clean
	python setup.py sdist --formats=gztar,zip

# Maintenance
clean:
	find . -name "__pycache__" -delete  # remove compiled python directories
	find . -name "*.pyc" -exec rm -f {} \;  # compiled python
	rm -rf .cache  # pytest
	rm -rf .coverage .coverage.* coverage  # coverage
	rm -rf dist *.egg-info  # distribution


# Phony Targets
.PHONY: all clean dist \
        test fast-test full-test \
        check fast-check full-check \
        coverage-report coverage-html
