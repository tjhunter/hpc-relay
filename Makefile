.PHONY: lint test format

lint:
	uv run python scripts/check_headers.py
	uv run ruff check .
	uv run pyrefly check
	uv run basedpyright

test:
	uv run pytest

format:
	uv run ruff check --fix .
	uv run ruff format .
