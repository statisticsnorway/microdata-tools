#!/usr/bin/env bash

TEST_FILE='tests/test_packaging/test_package_dataset.py'

mkdir -p logs/

LOG_DATE="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="logs/log_${LOG_DATE}.txt"

echo "Logging to '${LOG_FILE}'"

echo "Test file is '${TEST_FILE}'"
echo "git describe is '$(git describe --dirty)'"

echo "Test file is '${TEST_FILE}'" > "${LOG_FILE}"
echo "git describe is '$(git describe --dirty)'" >> "${LOG_FILE}"

rm -f tests/resources/validation/validate_dataset/big_datasets/ACCUMULATED_DS/*.csv || true

FOCUS=""
if grep -q "^@pytest.mark.focus" $(find "tests" -name '*.py'); then
  echo "Running focused tests ..."
  FOCUS="-m focus"
else
  echo "Running all tests ..."
fi

set -euox pipefail

env \
MICRODATA_TOOLS_TEST_PROGRESS='quiet' \
PYTHONUNBUFFERED=1 \
uv run pytest \
$FOCUS \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no \
| tee --append --ignore-interrupts "${LOG_FILE}"
