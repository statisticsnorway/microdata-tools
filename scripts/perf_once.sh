#!/usr/bin/env bash

mkdir -p logs/

TEST_FILE='tests/test_validation/test_validate_big_datasets.py'

LOG_DATE="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="logs/log_${LOG_DATE}.txt"

echo "Logging to '${LOG_FILE}'"

echo "Test file is '${TEST_FILE}'"
echo "git describe is '$(git describe --dirty)'"

echo "Test file is '${TEST_FILE}'" > "${LOG_FILE}"
echo "git describe is '$(git describe --dirty)'" >> "${LOG_FILE}"

set -euo pipefail

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
"${TEST_FILE}" \
| tee --append --ignore-interrupts "${LOG_FILE}"

