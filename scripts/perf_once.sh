#!/usr/bin/env bash

env \
MICRODATA_TOOLS_ROW_COUNT='1_000_000_000' \
MICRODATA_TOOLS_TEST_PROGRESS='quiet' \
MICRODATA_TOOLS_DELETE_FILES='false' \
PYTHONUNBUFFERED=1 \
uv run pytest \
-m focus \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no \
tests/test_validation/test_validate_big_datasets.py
