#!/usr/bin/env bash

watchexec -e py,sh --on-busy-update=restart --clear=reset --stop-signal=SIGINT -- ./scripts/test_once.sh
