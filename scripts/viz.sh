#!/usr/bin/env bash

export MICRODATA_TOOLS_TEST_PROGRESS='quiet'
export MICRODATA_TOOLS_DELETE_FILES='false'
export PYTHONUNBUFFERED=1

export MICRODATA_TOOLS_MEM_LOG_FILE="mem_new.log"

uv run pytest \
-m mem_viz \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no
