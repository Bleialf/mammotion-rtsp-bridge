#!/usr/bin/env sh
set -eu

mkdir -p /app/.agora
# BRIDGE_SCRIPT selects which bridge to run:
#   mammotion_go2rtc_bridge.py  (default) - ffmpeg->RTSP, the stable path
#   mammotion_webrtc_bridge.py            - experimental WebRTC/WHEP passthrough
exec python -u "/app/${BRIDGE_SCRIPT:-mammotion_go2rtc_bridge.py}" "$@"
