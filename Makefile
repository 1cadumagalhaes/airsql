.PHONY: docs-serve docs-build docs-deploy help install-docs install-dev test lint format type-check

help:
	@echo "AirSQL Development Commands"
	@echo ""
	@echo "Commands:"
	@echo "  docs-serve    Serve documentation locally with live reload"
	@echo "  docs-build    Build static documentation site"
	@echo "  docs-deploy   Deploy documentation to GitHub Pages"
	@echo "  help          Show this help message"
	@echo ""
	@echo "Development:"
	@echo "  install-dev   Install development dependencies"
	@echo "  test          Run tests"
	@echo "  lint          Run linter"
	@echo "  format        Format code"
	@echo "  type-check    Run type checking"

docs-serve:
	uv run zensical serve

docs-build:
	uv run zensical build

docs-deploy:
	@echo "Documentation will be automatically deployed via GitHub Actions"
	@echo "See .github/workflows/docs.yml"

install-docs:
	uv add zensical

install-dev:
	uv sync --dev

test:
	uv run pytest tests/

lint:
	uv run ruff check .

format:
	uv run ruff format .

type-check:
	uv run ty check