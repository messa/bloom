.PHONY: help install check lint lint-fix typecheck clean

help:
	@echo "Available targets:"
	@echo "  install    - Install the package in development mode with all dependencies"
	@echo "  check      - Run all tests"
	@echo "  lint       - Check code style with ruff"
	@echo "  lint-fix   - Fix code style issues with ruff"
	@echo "  typecheck  - Run type checking with mypy"
	@echo "  clean      - Remove build artifacts and cache files"

install:
	uv venv
	uv add -e ".[dev]"

check:
	uv run pytest -v

lint:
	uv run ruff check .
	uv run ruff format --check .

lint-fix:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy bloom

clean:
	rm -rf build/ dist/ *.egg-info
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.so" -delete