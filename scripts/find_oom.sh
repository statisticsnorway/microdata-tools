#!/usr/bin/env bash

mkdir -p logs/

LOG_DATE="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="logs/log_${LOG_DATE}.txt"

echo "Logging to '${LOG_FILE}'"

#echo "Test file is '${TEST_FILE}'"
echo "git describe is '$(git describe --dirty)'"

#echo "Test file is '${TEST_FILE}'" > "${LOG_FILE}"
echo "git describe is '$(git describe --dirty)'" >> "${LOG_FILE}"

set -euox pipefail

export MICRODATA_TOOLS_ROW_COUNT='30_000_000'
export MICRODATA_TOOLS_TEST_PROGRESS='quiet'
export MICRODATA_TOOLS_DELETE_FILES='false'
export PYTHONUNBUFFERED=1

#which sar || { echo "command 'sar' not installed. please install it. for example: sudo apt-get install sysstat"; exit 1;}

echo "Creating CSV file ..."
uv run pytest \
-m perf_init \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no

echo "Creating parquet file ..."
uv run pytest \
-m create_parquet \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no
