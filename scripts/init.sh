#!/usr/bin/env bash

set -euo pipefail

rm -rf .venv/

uv venv
uv sync

if [ -e "./.git/HEAD" ]; then
  echo "Installing pre-commit ..."
  uv pip install pre-commit
  pre-commit install
else
  echo "Skipping pre-commit"
fi

