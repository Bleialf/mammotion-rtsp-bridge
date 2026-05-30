# Mammotion -> Frigate / go2rtc RTSP bridge

Single-container bridge that gets a Mammotion mower's camera into Frigate (or
any RTSP consumer) by joining the mower's Agora WebRTC channel as a passive
subscriber and re-exposing the H265 stream over plain RTSP.

```
Mammotion cloud -> Agora SFU -> bridge (aiortc subscriber)
                                       |
                                       v
                              RTSP server :8554
                                       |
                                       v
                          Frigate / go2rtc / VLC / ffplay
```

No ffmpeg, no transcoding. The Agora RTP packets are forwarded byte-for-byte;
the bridge only rewrites the 12-byte RTP header (SSRC, sequence number,
payload type) and leaves the H265 payload untouched.

> Unofficial. Not endorsed by Mammotion. Use at your own risk.

> Use a separate Mammotion account shared from your main account. Mammotion
> is effectively single-session, so using one account for both the app and the
> bridge will cause one to log the other out.

## Why this architecture

Earlier versions tried two other approaches; both lost to upstream realities:

- **ffmpeg transcode (0.1.1).** Worked but added CPU load, GOP-rewrite
  artefacts, and a per-frame jitter problem. Replaced.
- **WebRTC signaling passthrough (0.1.2).** The bridge brokered SDP only and
  go2rtc was meant to be the WebRTC peer to Agora directly. go2rtc's Pion
  stack does not interoperate cleanly with Agora's edge — `p2p_lost: Timeout`
  ~10 s into every session — so the path was effectively dead on arrival for
  this stream. Removed.

The current path runs aiortc as the WebRTC peer (which talks to Agora's edge
without issue), monkey-patches it to accept H265 as opaque bytes
([mammotion_webrtc/h265_patch.py](mammotion_webrtc/h265_patch.py)), taps the
inbound RTP, and republishes it as a minimal RTSP server. Frigate's bundled
go2rtc then consumes it like any normal IP camera.

## Quick start

1. Copy [docker-compose.example.yml](docker-compose.example.yml) into your
   Frigate stack (or paste just the `mammotion-bridge` service into your
   existing compose so it shares Frigate's network).
2. Fill in `MAMMOTION_EMAIL`, `MAMMOTION_PASSWORD`, and (optionally)
   `MAMMOTION_DEVICE_NAME`.
3. Start:

```bash
docker compose pull mammotion-bridge
docker compose up -d mammotion-bridge
```

4. The bridge logs in to the Mammotion cloud, joins the Agora channel,
   starts its RTSP server, and registers itself with Frigate's go2rtc.
5. Open `http://<frigate-host>:1984/stream.html?src=mammotion` to view.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `MAMMOTION_EMAIL` | — | Mammotion cloud account email (**required**) |
| `MAMMOTION_PASSWORD` | — | Mammotion cloud password (**required**) |
| `MAMMOTION_DEVICE_NAME` | `first` | Device name in the app, or `first` to auto-pick |
| `GO2RTC_API_URL` | `http://frigate:1984` | go2rtc REST API base for stream auto-registration |
| `MAMMOTION_STREAM_NAME` | `mammotion` | Name go2rtc registers the stream under |
| `MAMMOTION_RTSP_HOST` | container hostname | Hostname go2rtc uses to dial back to the bridge |
| `MAMMOTION_RTSP_PORT` | `8554` | RTSP listen port inside the container |
| `MAMMOTION_RTSP_BIND` | `0.0.0.0` | RTSP bind address |
| `MAMMOTION_GO2RTC_RECONCILE_SECONDS` | `20` | Periodic go2rtc registration self-heal interval |
| `MAMMOTION_KEEPALIVE_SECONDS` | `300` | MQTT keepalive cadence to the mower (stay under pymammotion's 300/24h cap) |
| `MAMMOTION_NO_RTP_WATCHDOG_SECONDS` | `5` | Tear down + reconnect the Agora session if no H265 RTP arrives for this long |
| `MAMMOTION_CHEAP_RECOVERY_WAIT_SECONDS` | `3` | On stall, wait this long after a `refresh_fpv` MQTT nudge before escalating to a full teardown |
| `MAMMOTION_RECONNECT_BACKOFF_SECONDS` | `8` | Retry delay on cloud/session failure |
| `MAMMOTION_LOG_LEVEL` | `INFO` | `DEBUG` exposes RTSP method exchange and signaling traces |

## Viewer compatibility

The bridge serves a plain RTP-over-RTSP H265 stream. Consumers tested:

- **Frigate / go2rtc (RTSP source)** — works (this is the primary target).
- **ffplay / VLC** — works (`rtsp://<host>:8554/<stream>`).
- **go2rtc `stream.html?mode=webrtc`** — works smoothly when the browser
  supports H265 over WebRTC (Safari/Chrome on Apple Silicon do).
- **go2rtc `stream.html` (MSE default)** — choppy. Mammotion encodes at
  ~10 fps and Chrome's MSE low-delay renderer stalls on low-framerate H265.
  Use `&mode=webrtc` or play the RTSP URL directly.

## Reliability

- The Agora subscription is self-healing with bounded backoff. If the upstream
  WebRTC connection fails, the bridge tears it down and reconnects, refetching
  cloud credentials so expired tokens are handled implicitly.
- A no-RTP watchdog (default 30 s) detects the case where the ICE/DTLS session
  stays nominally healthy but the mower's publisher has gone silent. When that
  happens the bridge tears down the upstream and lets the supervisor reconnect
  — which fetches fresh credentials and re-triggers the mower to publish.
- MQTT keepalive cadence is conservative (default 5 min) because pymammotion's
  Aliyun MQTT transport caps the bridge at ~300 sends/24 h. The publisher is
  kept alive primarily by remaining subscribed as an Agora audience client,
  not by aggressive MQTT pings.
- go2rtc registration is reconciled periodically, so the stream comes back on
  its own after a Frigate restart.
- A fresh Mammotion login is performed per cycle so a stale cloud token can't
  wedge the bridge for hours.

## Limitations

- **H265 only.** Mammotion's camera publishes H265; this bridge does not
  transcode. Consumers that need H264 should put a transcoder downstream (or
  use the Frigate `ffmpeg`-based hwaccel preset to record/restream as H264).
- **No RTCP Sender Reports.** go2rtc gets RTP timestamps without a wall-clock
  anchor. This is fine for Frigate (which timestamps on arrival) but can
  cause MSE-mode browser playback to drift over long sessions.
- **Single device per container.** Run one container per mower.
- **Local-only is not possible.** All Mammotion video is brokered through
  Agora's cloud; there is no documented LAN-direct path today.

## Integration idea for Mammotion-HA

The Agora-edge subscriber and H265 RTP tap could be folded into Mammotion-HA
as an opt-in "expose camera as RTSP" feature, so users get a Frigate-ready
camera without a side container. Happy to help wire it up — see the issue
thread for context.

## Releases

| Tag | Meaning |
| --- | --- |
| `:stable` | Latest tagged release |
| `:latest` | `main` branch HEAD |
| `:x.y.z` | Pinned release |
| `:sha-<short>` | Pinned commit |

See [CHANGELOG.md](CHANGELOG.md).

## Credits

Built on [pymammotion](https://github.com/mikey0000/PyMammotion) for cloud
auth and Agora token fetch. The H265 patch + relay design borrows ideas from
the PetKit HA integration's Agora WebRTC client (MIT,
© 2024-2026 @Jezza34000).
