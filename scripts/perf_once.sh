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
tests/test_validation/test_validate_big_datasets.py \
2>&1 | grep --line-buffered \
-v "Scalene: profile saved to scalene-profile.json\|  To view in browser:  scalene view\|  To view in terminal: scalene view --cli" \
| perl -pe 'chomp if eof && /^$/'

# perl thing: https://stackoverflow.com/questions/4448826/removing-last-blank-line

./scripts/perf_info.py