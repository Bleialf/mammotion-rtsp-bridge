# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow
[SemVer](https://semver.org/) (pre-1.0 — expect changes).

## [Unreleased]

## [0.1.2] - 2026-05-28

### Added
- Experimental Mammotion -> go2rtc WebRTC passthrough bridge (`mammotion_webrtc_bridge.py`) and standalone signaling server (`mammotion_webrtc/whep_server.py`) for direct Agora edge negotiation.
- go2rtc-native WebSocket signaling endpoint (`/api/ws?src=...`) as an alternative to one-shot WHEP client mode.
- Runtime signaling mode switch with `MAMMOTION_GO2RTC_SIGNALING` (`http` or `ws`).
- Periodic stream-registration reconciliation (`MAMMOTION_GO2RTC_RECONCILE_SECONDS`) to auto-recover registration after go2rtc/Frigate restarts.
- Dedicated passthrough example compose file and docs updates.

### Changed
- SDP answer generation now includes a Pion/go2rtc compatibility profile for better interop with Frigate-bundled go2rtc.

### Fixed
- Improved reconnect behavior when closing/reopening a viewer by handling trickle ICE candidates throughout the WS session lifecycle.
- Added explicit WHEP client logging for easier diagnostics of active compatibility mode.

## [0.1.1] - 2026-05-27

First public release. Experimental.

### Added
- Bridge that subscribes to a Mammotion mower's Agora video and publishes it to
  go2rtc over RTSP, transcoded to a steady 10 fps H.264 stream.
- Automatic recovery from the mower leaving the Agora channel (~50 s cycle):
  proactive 20 s keep-alive plus reactive wake-up.
- Fresh cloud login per cycle so an expired session token can't wedge the bridge.
- Watchdogs: startup-frame timeout, frame-stall, publisher-gone-too-long, and an
  ffmpeg subprocess health check with auto-restart.
- Docker healthcheck driven by a frame-ingress heartbeat file (defaults to
  `/tmp/mammotion_heartbeat`).
- Multi-arch image (amd64 + arm64) published to GHCR with `:latest`, `:stable`,
  and semver tags via GitHub Actions.
- Example configs for Frigate and standalone go2rtc, plus a documented HA
  advanced-camera-card snippet.

[Unreleased]: https://github.com/Bleialf/mammotion-rtsp-bridge/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.2
[0.1.1]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.1
