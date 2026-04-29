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

TOTAL_RAM=$(cat /proc/meminfo | grep -i 'memtotal' | grep -o '[[:digit:]]*')
echo "Total RAM: $((TOTAL_RAM / 1024)) MB"

export MICRODATA_TOOLS_ROW_COUNT='75_000_000'
export MICRODATA_TOOLS_TEST_PROGRESS='quiet'
export MICRODATA_TOOLS_DELETE_FILES='false'
export PYTHONUNBUFFERED=1

which sar || { echo "command 'sar' not installed. please install it. for example: sudo apt-get install -y sysstat"; exit 1;}

echo "Creating CSV file ..."
uv run pytest \
-m perf_init \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no 2>&1 | sed -u 's/^/perf_init: /' | tee --append --ignore-interrupts "${LOG_FILE}"

echo "Creating parquet file ..."
uv run pytest \
-m create_parquet \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no 2>&1 | sed -u 's/^/create_parquet: /' | tee --append --ignore-interrupts "${LOG_FILE}"

sar -B 1 > mem_validate_parquet_old.log 2>&1 &
SAR_PID1="$!"

onEXIT () {
  set +x
  EXIT_STATUS="$?"
  kill "$SAR_PID1"
  exit "$EXIT_STATUS"
}

trap onEXIT EXIT

OOM_COUNT_1="$(dmesg | grep -i 'out of memory' | wc -l || echo '0')"
echo "Validating parquet file ..."
uv run pytest \
-m validate_parquet \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no 2>&1 | sed -u 's/^/validate_parquet: /' | tee --append --ignore-interrupts "${LOG_FILE}" \
&& printf "\e[0;32mOK validate\e[0m\n" \
|| printf "\e[0;31mFAILED to validate\e[0m\n"

OOM_COUNT_2="$(dmesg | grep -i 'out of memory' | wc -l || echo '0')"
if [[ "$OOM_COUNT_1" != "$OOM_COUNT_2" ]]; then
  printf "\e[0;31mOOM occurred\e[0m\n"
fi
