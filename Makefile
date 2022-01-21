root=$(shell git rev-parse --show-toplevel)

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null)
SHA := $(shell git rev-parse --short HEAD 2>/dev/null)
SHELL=/bin/bash

REGISTRY?=contiamo
COMPONENT=datahub-sap-hana



all: setup requirements.txt format lint test

setup:
	poetry install

format:
	poetry run black .

test:
	poetry run pytest -v

lint:
	poetry run flake8 .
	poetry run mypy .

poetry.lock: pyproject.toml
	poetry lock --no-update
	@touch poetry.lock

requirements.txt: poetry.lock
	poetry export -f requirements.txt --without-hashes > requirements.txt

.PHONY: dist
dist:
	rm -r dist
	poetry build

.PHONY: image
image: requirements.txt
	docker build --build-arg VERSION=$(VERSION) --build-arg SHA=$(SHA) -t ${REGISTRY}/${COMPONENT}:latest .