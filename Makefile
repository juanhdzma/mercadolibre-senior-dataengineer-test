.PHONY: copy all typecheck lint security deps test coverage run-local

all: typecheck lint security deps test

typecheck:
	mypy src tests apps/runner.py

lint:
	flake8 src tests apps

security:
	bandit -r src apps --severity-level low --confidence-level low

deps:
	pip-audit

test: copy
	pytest --maxfail=1 -ra \
	       --cov=src \
	       --cov-report=term-missing \
	       --cov-fail-under=80

run-local: copy
	python -m apps.runner

copy:
	cp envs/local.env .env