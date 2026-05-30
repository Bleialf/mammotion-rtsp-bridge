#!/usr/bin/env sh
set -eu

mkdir -p /app/.agora
# This branch (vp8-direct-passthrough) ships the WHEP/WS signaling bridge as
# the default; it asks Agora for VP8 so the media flows directly into
# go2rtc/Pion without the H265-passthrough hack. The legacy ffmpeg bridge
# is still in the image as an opt-in fallback.
exec python -u "/app/${BRIDGE_SCRIPT:-mammotion_webrtc_bridge.py}" "$@"
