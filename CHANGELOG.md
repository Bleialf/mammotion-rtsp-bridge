# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow
[SemVer](https://semver.org/) (pre-1.0 — expect changes).

## [Unreleased]

## [0.1.7] - 2026-05-30

### Added
- Proactive PLI when the first H265 RTP packet of a new upstream session
  arrives. The mower is typically mid-GOP at that moment and the first
  packets we receive are P-frames — which carry no parameter sets, so
  RTSP `DESCRIBE` blocks waiting for VPS/SPS/PPS and go2rtc times out at
  5 s. Firing one PLI as soon as we know the publisher's SSRC pushes the
  mower onto a fresh IDR (which prepends parameter sets), so DESCRIBE
  completes in ~1 s instead of ~6 s (next natural GOP boundary).

  Cuts black-screen time after every reconnect — token expiry, no-RTP
  watchdog fire, network blip, etc. Most visible at startup and after
  any of v0.1.6's cheap-recovery escalations.

## [0.1.6] - 2026-05-30

### Added
- Wire pymammotion's `refresh_fpv` (`MulSetEncode(encode=True)`) in as the
  camera-specific keep-alive. Three integration points:
  - `wake_publisher` now sends `refresh_fpv` after the
    `device_agora_join_channel_with_position`, so every upstream reconnect
    explicitly re-enables the encoder.
  - The periodic keep-alive loop calls `refresh_fpv` instead of the generic
    `send_todev_ble_sync sync_type=2` (which only kept the cloud session
    warm and didn't touch the encoder). Falls back to the old command if
    pymammotion is too old to expose `refresh_fpv`.
  - The no-RTP watchdog now tries `refresh_fpv` first as a cheap recovery
    (`MAMMOTION_CHEAP_RECOVERY_WAIT_SECONDS`, default 10 s) and only
    escalates to full teardown + reconnect if RTP doesn't resume. This
    eliminates most of the reconnect churn that was burning the MQTT budget.

### Why
`send_todev_ble_sync sync_type=2` is a generic Aliyun MQTT heartbeat, not
a camera command. The mower's multimedia SoC was going idle even while we
were sending those pings. `refresh_fpv` targets the actual encoder.
Surfaced by reading Mikey's recent pymammotion additions
([`pymammotion/mammotion/commands/messages/video.py:48`](pymammotion/mammotion/commands/messages/video.py#L48)).

## [0.1.5] - 2026-05-30

### Fixed
- Drop the early-SDP fallback in `agora_edge` that fired on
  `on_add_video_stream` timeout. The fallback built an answer SDP without the
  publisher's actual SSRC/PT, which silently broke codec routing in the
  aiortc receiver (`codecs_by_pt` lookup misses → RTP tap drops every
  packet → watchdog fires → reconnect → same timeout → infinite loop).
  Raising on timeout instead lets the supervisor back off and retry cleanly.
- Re-tag `:stable` to the actual newest code. After the v0.1.2/3/4 history
  rewrite, parallel CI builds for all three retagged versions raced for the
  `:stable` floating tag and v0.1.2 (WHEP bridge code) won, leaving `:stable`
  serving the old WHEP signaling bridge. v0.1.5 is unambiguously the newest
  tag so CI publishes the aiortc relay code as `:stable`.

## [0.1.4] - 2026-05-30

### Added
- No-RTP watchdog (`MAMMOTION_NO_RTP_WATCHDOG_SECONDS`, default 30 s). If the
  upstream ICE/DTLS session stays "connected" but no H265 RTP packet arrives
  for the configured window, the bridge tears down the upstream and lets the
  supervisor reconnect — which refetches credentials and re-triggers the
  mower to publish. Fixes the silent-stall mode where the publisher would
  back off after a few minutes and the bridge would happily sit on a dead
  session until the hourly token expiry forced a reconnect.

### Changed
- `MAMMOTION_KEEPALIVE_SECONDS` default raised from 10 s to 300 s. The old
  cadence sent ~8640 MQTT messages per day; pymammotion's Aliyun MQTT
  transport self-imposes a ~300/24 h cap, so the bridge would hit the limit
  ~50 min in and then silently stop nudging the mower. 5 min cadence keeps
  the bridge well under the cap and the no-RTP watchdog handles the case
  where the publisher times out anyway.

## [0.1.3] - 2026-05-29

### Changed
- **Replaced the architecture.** The bridge is now an aiortc-based passive
  WebRTC subscriber that taps Agora's H265 RTP packets and republishes them
  byte-for-byte from an embedded RTSP server. No ffmpeg, no transcoding, no
  WHEP/WS signaling step — Frigate's go2rtc consumes the bridge with a plain
  `rtsp://` source. See the rewritten README for the rationale.
- Default (and only) entrypoint is `mammotion_webrtc_bridge.py`. Image no
  longer needs `BRIDGE_SCRIPT` to select the relay.

### Removed
- Legacy ffmpeg transcode bridge (`mammotion_go2rtc_bridge.py`) — the new
  passthrough path supersedes it.
- WebRTC signaling passthrough mode (WHEP/WS) — Pion's interop with Agora's
  edge is unreliable (`p2p_lost: Timeout` ~10 s into every session), so this
  path is effectively unusable for Mammotion and has been removed.
- `ffmpeg` apt package, the `docker-entrypoint.sh` wrapper, and the
  `MAMMOTION_GO2RTC_SIGNALING` / `MAMMOTION_WHEP_*` env vars.
- Stale `DESIGN-webrtc-passthrough.md` design notes.

### Added
- aiortc H265 patch (`mammotion_webrtc/h265_patch.py`) — registers H265 with
  aiortc as opaque RTP so the SDP/codec-routing/decoder-spawn paths accept it
  without a native H265 decoder dependency.
- Minimal asyncio RTSP server (`mammotion_webrtc/rtsp_server.py`) with
  TCP-interleaved transport, parameter-set-aware SDP, and per-session queue
  back-pressure.
- Single combined `docker-compose.example.yml` modelled on a real working
  Frigate + bridge setup.

## [0.1.2] - 2026-05-28

### Added
- Experimental Mammotion -> go2rtc WebRTC passthrough bridge (`mammotion_webrtc_bridge.py`) and standalone signaling server (`mammotion_webrtc/whep_server.py`) for direct Agora edge negotiation.
- go2rtc-native WebSocket signaling endpoint (`/api/ws?src=...`) as an alternative to one-shot WHEP client mode.
- Runtime signaling mode switch with `MAMMOTION_GO2RTC_SIGNALING` (`http` or `ws`).
- Periodic stream-registration reconciliation (`MAMMOTION_GO2RTC_RECONCILE_SECONDS`) to auto-recover registration after go2rtc/Frigate restarts.
- Dedicated passthrough example compose file and docs updates.

### Changed
- SDP answer generation now includes a Pion/go2rtc compatibility profile for better interop with Frigate-bundled go2rtc.
- WebRTC passthrough now enforces go2rtc-native WS signaling; WHEP mode is treated as unsupported for Frigate/go2rtc.

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

[Unreleased]: https://github.com/Bleialf/mammotion-rtsp-bridge/compare/v0.1.7...HEAD
[0.1.7]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.7
[0.1.6]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.6
[0.1.5]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.5
[0.1.4]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.4
[0.1.3]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.3
[0.1.2]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.2
[0.1.1]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.1
