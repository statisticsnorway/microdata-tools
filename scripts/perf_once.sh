#!/usr/bin/env bash

mkdir -p logs/

LOG_DATE="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="logs/log_${LOG_DATE}.txt"

echo "Logging to '${LOG_FILE}'"
touch "${LOG_FILE}"

echo "git describe is '$(git describe --dirty)'" | tee --append --ignore-interrupts "${LOG_FILE}"

TOTAL_RAM=$(cat /proc/meminfo | grep -i 'memtotal' | grep -o '[[:digit:]]*')
echo "Total RAM: $((TOTAL_RAM / 1024)) MB" | tee --append --ignore-interrupts "${LOG_FILE}"

set -euox pipefail

export MICRODATA_TOOLS_ROW_COUNT='2_000_000_000'
export MICRODATA_TOOLS_TEST_PROGRESS='quiet'
export MICRODATA_TOOLS_DELETE_FILES='false'
export PYTHONUNBUFFERED=1

export MICRODATA_TOOLS_TEST_DISK="/Volumes/Lakris/microdata_tools"
export MICRODATA_TOOLS_WORK_DIR="/Volumes/Lakris/microdata_tools/workdir"

which sar || { echo "command 'sar' not installed. please install it. for example: sudo apt-get install -y sysstat"; exit 1;}

uv run pytest \
-m perf_init \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no \
| tee --append --ignore-interrupts "${LOG_FILE}"

sar -B 1 > mem_new.log 2>&1 &
SAR_PID1="$!"

onEXIT () {
  set +x
  EXIT_STATUS="$?"
  kill "$SAR_PID1"
  exit "$EXIT_STATUS"
}

trap onEXIT EXIT

uv run pytest \
-m perf_new \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no \
| tee --append --ignore-interrupts "${LOG_FILE}"

echo 'perf_once.sh exiting'
