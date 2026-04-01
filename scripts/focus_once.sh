#!/usr/bin/env bash

env \
MICRODATA_TOOLS_TEST_PROGRESS='quiet' \
uv run pytest -m focus \
--no-header \
--failed-first \
--exitfirst \
--quiet \
--capture no
