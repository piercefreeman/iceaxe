#
# Main Makefile for project development and CI
#

# Default shell
SHELL := /bin/bash
# Fail on first error
.SHELLFLAGS := -ec

# Global variables
ICEAXE := ./
ICEAXE_NAME := iceaxe

# Ignore these directories in the local filesystem if they exist
.PHONY: lint test

# Main lint target
lint: lint-iceaxe

# Lint validation target
lint-validation: lint-validation-iceaxe

# Testing target
test: test-iceaxe

# Install all sub-project dependencies with uv
install-deps: install-deps-iceaxe

install-deps-iceaxe:
	@echo "Installing dependencies for $(ICEAXE)..."
	@(cd $(ICEAXE) && uv sync)

# Clean the current uv.lock files, useful for remote CI machines
# where we're running on a different base architecture than when
# developing locally
clean-uv-lock:
	@echo "Cleaning uv.lock files..."
	@rm -f $(ICEAXE)/uv.lock

# Standard linting - local development, with fixing enabled
lint-iceaxe:
	$(call lint-common,$(ICEAXE),$(ICEAXE_NAME))

# Lint intended for CI usage - will fail on any errors
lint-validation-iceaxe:
	$(call lint-validation-common,$(ICEAXE),$(ICEAXE_NAME))

# Tests
test-iceaxe:
	@set -e; \
	$(call test-common,$(ICEAXE),$(MOUNTAINEER_PLUGINS_NAME))

#
# Common helper functions
#

define test-common
	echo "Running tests for $(2)..."
	@(cd $(1) && uv run pytest -W error -vv $(test-args) $(2))
endef

define lint-common
	echo "Running linting for $(2)..."
	@(cd $(1) && uv run ruff format $(2))
	@(cd $(1) && uv run ruff check --fix $(2))
	echo "Running pyright for $(2)..."
	@(cd $(1) && uv run pyright $(2))
endef

define lint-validation-common
	echo "Running lint validation for $(2)..."
	@(cd $(1) && uv run ruff format --check $(2))
	@(cd $(1) && uv run ruff check $(2))
	echo "Running mypy for $(2)..."
	@(cd $(1) && uv run mypy $(2))
	echo "Running pyright for $(2)..."
	@(cd $(1) && uv run pyright $(2))
endef


# Function to wait for PostgreSQL to be ready
define wait-for-postgres
	@echo "Waiting for PostgreSQL to be ready..."
	@timeout=$(1); \
	while ! nc -z localhost $(2) >/dev/null 2>&1; do \
		timeout=$$((timeout-1)); \
		if [ $$timeout -le 0 ]; then \
			echo "Timed out waiting for PostgreSQL to start on port $(2)"; \
			exit 1; \
		fi; \
		echo "Waiting for PostgreSQL to start..."; \
		sleep 1; \
	done; \
	echo "PostgreSQL is ready on port $(2)."
endef
