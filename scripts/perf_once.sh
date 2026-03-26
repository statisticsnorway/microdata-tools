#!/usr/bin/env bash

env MICRODATA_TOOLS_WATCH_MODE='true' \
uv run scalene run \
--profile-all \
-m pytest \
-m focus \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no \
tests/test_validation/test_validate_big_datasets.py

./scripts/perf_info.py