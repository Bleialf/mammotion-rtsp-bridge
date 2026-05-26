#!/usr/bin/env sh
set -eu

mkdir -p /app/.agora
exec python -u /app/mammotion_go2rtc_bridge.py "$@"
