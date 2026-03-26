#!/usr/bin/env bash

env MICRODATA_TOOLS_WATCH_MODE='true' \
uv run pytest -m focus \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no
