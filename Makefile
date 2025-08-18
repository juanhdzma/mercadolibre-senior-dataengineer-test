.PHONY: all typecheck lint security deps test coverage run-local

all: typecheck lint security deps test coverage

typecheck:
	mypy src tests apps

lint:
	flake8 src tests apps

security:
	bandit -r src apps --severity-level low --confidence-level low

deps:
	pip-audit

test:
	pytest --maxfail=1 -ra \
	       --cov=src \
	       --cov-report=term-missing \
	       --cov-fail-under=80

run-local:
	cp envs/local.env .env
	python -m apps.runner