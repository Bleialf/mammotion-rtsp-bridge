# Mammotion -> go2rtc/Frigate WebRTC Bridge

WS-only WebRTC passthrough from Mammotion (Agora) into go2rtc/Frigate.

This project now treats the old ffmpeg/transcode route as legacy. The supported
path is signaling-only WebRTC restreaming through go2rtc-native WS signaling.

```
Mammotion cloud -> Agora WebRTC -> bridge (signaling only) -> go2rtc -> Frigate
```

## Status

- Production path: WS signaling (`webrtc:ws://.../api/ws?src=...`)
- WHEP path: unsupported/unreliable for Frigate/go2rtc in this project
- ffmpeg path: legacy, no longer the recommended architecture

> Unofficial and unsupported by Mammotion. Use at your own risk.

> Use a separate Mammotion account shared from your main account. Mammotion is
effectively single-session, so one account for app + bridge causes logouts.

## What it does

1. Logs into Mammotion cloud with pymammotion.
2. Fetches Agora stream subscription credentials.
3. Exposes go2rtc-compatible WS signaling on `/api/ws?src=<stream>`.
4. Registers a go2rtc stream source pointing to that WS endpoint.
5. Maintains publisher keepalive and reconnect behavior.
6. Periodically re-ensures stream registration after Frigate/go2rtc restarts.

## Quick start

1. Use [docker-compose.webrtc.yml](docker-compose.webrtc.yml) as your base.
2. Fill in Mammotion credentials.
3. Ensure this bridge can resolve/reach Frigate as `frigate:1984`.
4. Start or recreate the service.

```bash
docker compose -f docker-compose.webrtc.yml pull
docker compose -f docker-compose.webrtc.yml up -d --force-recreate
```

5. Validate in logs:

- bridge startup shows `go2rtc signaling=ws`
- on stream open, you see `go2rtc WS session established`

## Frigate/go2rtc wiring

The bridge auto-registers the stream in go2rtc using `GO2RTC_API_URL` and
`MAMMOTION_STREAM_NAME`, so manual go2rtc source editing is usually not needed.

Open in go2rtc UI with:

`http://<frigate-host>:1984/stream.html?src=<MAMMOTION_STREAM_NAME>`

## Configuration

Primary environment variables for WS passthrough:

| Variable | Default | Purpose |
| --- | --- | --- |
| `BRIDGE_SCRIPT` | `mammotion_webrtc_bridge.py` | Selects WS passthrough entrypoint |
| `MAMMOTION_EMAIL` | - | Mammotion account email |
| `MAMMOTION_PASSWORD` | - | Mammotion account password |
| `MAMMOTION_DEVICE_NAME` | `first` | Device name in app or `first` |
| `GO2RTC_API_URL` | `http://frigate:1984` | go2rtc REST API base URL |
| `MAMMOTION_STREAM_NAME` | `mammotion` | Stream name managed in go2rtc |
| `MAMMOTION_WHEP_HOST` | container hostname | Host go2rtc uses to reach this service |
| `MAMMOTION_WHEP_PORT` | `8555` | WS signaling listen port |
| `MAMMOTION_GO2RTC_SIGNALING` | `ws` | Kept for compatibility; non-`ws` is ignored and forced to `ws` |
| `MAMMOTION_GO2RTC_RECONCILE_SECONDS` | `20` | Periodic go2rtc registration self-heal interval |
| `MAMMOTION_KEEPALIVE_SECONDS` | `10` | MQTT keepalive cadence |
| `MAMMOTION_RECONNECT_BACKOFF_SECONDS` | `8` | Retry delay for cloud/session reconnect |
| `MAMMOTION_AGORA_REJOIN_BACKOFF_SECONDS` | `3` | Short backoff before a new Agora join after failure/cleanup |
| `VIDEO_ONLY` | `false` | Answer only video media (audio/backchannel disabled) |
| `MAMMOTION_WEBRTC_VIDEO_ONLY` | `false` | Legacy alias for `VIDEO_ONLY` |
| `KEEPALIVE_AFTER_LAST_CLIENT_SECONDS` | `60` | Keep upstream alive window after last downstream disconnect |
| `KEEPALIVE_SECONDS` | `60` | Legacy alias for `KEEPALIVE_AFTER_LAST_CLIENT_SECONDS` |
| `MAMMOTION_AGORA_RTP_TIMEOUT_SECONDS` | `20` | Mark upstream unhealthy when media activity is stale |
| `MAMMOTION_MIN_SESSION_LIFETIME_SECONDS` | `30` | Anti-flap minimum lifetime before non-definitive replacement |

## Reliability behavior

- WS session lifecycle handles trickle ICE continuously (fixes open-once/reopen failures).
- Repeated go2rtc offers now reuse healthy upstream Agora sessions instead of forcing replacement.
- Periodic reconciliation re-registers stream after Frigate/go2rtc restarts.
- Keepalive and wakeup logic keep the mower publishing on Agora.
- Fresh login loop recovers from stale cloud sessions.

## Troubleshooting

- Stream appears once then fails on reopen:
  update to `v0.1.2` or newer; this includes WS lifecycle fix.
- Stream missing after Frigate restart:
  verify `MAMMOTION_GO2RTC_RECONCILE_SECONDS` is set (for example `20`).
- Stream not opening in Frigate/go2rtc:
  confirm both containers are on a shared network and `frigate` resolves from
  bridge container.
- CORS error about external `manifest.json` in go2rtc UI:
  usually cosmetic; focus on actual stream errors (`webrtc disconnected`).

## Integration idea for Mammotion-HA

This approach can be embedded in Mammotion-HA by adding an opt-in WS signaling
endpoint and reusing existing Agora token/recovery logic, then having go2rtc
dial HA directly for signaling-only passthrough.

## Releases

Image tags:

- `stable`: latest tagged release
- `latest`: latest main branch build
- `x.y.z`: pinned release version
- `sha-<short>`: pinned commit build

See [CHANGELOG.md](CHANGELOG.md) for details.

## Credits

Built on [pymammotion](https://github.com/mikey0000/PyMammotion) for Mammotion
auth and Agora integration.
