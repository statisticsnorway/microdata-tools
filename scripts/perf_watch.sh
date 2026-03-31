#!/usr/bin/env bash

watchexec -e py,sh --on-busy-update=restart --clear=reset --stop-signal=SIGINT -- ./scripts/perf_once.sh
