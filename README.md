# Mammotion → go2rtc/Frigate Bridge

Streams a Mammotion robot mower's camera (Yuka, Luba 2, etc.) into
[go2rtc](https://github.com/AlexxIT/go2rtc) / [Frigate](https://frigate.video/)
so you can view and record it like any other camera.

Mammotion only exposes the live view through Agora (a WebRTC SDK) inside their
app. This bridge logs into your Mammotion account, subscribes to that Agora
video as an audience client, transcodes it to a steady 10 fps H.264 stream, and
publishes it to go2rtc over RTSP.

```
Mammotion cloud ── Agora (HEVC) ──▶ bridge (ffmpeg → H.264) ──RTSP──▶ go2rtc ──▶ Frigate / WebRTC / HLS
```

> ⚠️ **Unofficial & unsupported.** This is not endorsed by Mammotion. It logs
> into their cloud the same way the app does, but using it is at your own risk —
> your account could be rate-limited, blocked, or broken by an app update at any
> time. Don't rely on it for anything critical.

> 🔑 **Use a separate (shared) account.** Mammotion accounts are effectively
> single-session: when the bridge logs in, your phone app gets logged out, and
> vice-versa. Create a second Mammotion account, **share the mower with it** from
> your main account (in the app), and give the bridge *that* account's
> credentials. Your main account/app then keeps working normally alongside the
> bridge.

## Requirements

- Docker + Docker Compose
- A running go2rtc, standalone or bundled inside Frigate (Frigate exposes it on port 8554/1984)
- A **secondary** Mammotion account with the mower shared to it (see the note above)

## Quick start

1. **Add the bridge service** to your Compose file with your Mammotion
   credentials inline (see the full example below), or use the standalone
   [docker-compose.mammotion.yml](docker-compose.mammotion.yml).

2. **Add the go2rtc + camera config.** Merge [frigate.example.yaml](frigate.example.yaml)
   into your Frigate `config.yml` (or, for standalone go2rtc, use
   [go2rtc.example.yaml](go2rtc.example.yaml)).

3. **Start it.**
   ```bash
   docker compose up -d
   docker compose restart frigate   # so Frigate picks up the new camera
   ```

The prebuilt image (`ghcr.io/bleialf/mammotion-rtsp-bridge:latest`, amd64 +
arm64) is pulled automatically — no build step needed.

## WebRTC passthrough mode (new in 0.1.2)

In addition to the default RTSP-transcode bridge, this repo now includes an
experimental direct WebRTC passthrough mode designed for go2rtc/Frigate.

Instead of decoding and re-encoding video with ffmpeg, this mode brokers
WebRTC signaling between go2rtc and Mammotion's Agora edge.

```
Mammotion cloud ── Agora WebRTC ──▶ bridge (signaling only) ──▶ go2rtc/Frigate WebRTC
```

Use the dedicated example compose file:

- [docker-compose.webrtc.yml](docker-compose.webrtc.yml)

Key environment variables for passthrough mode:

| Variable | Default | Purpose |
| --- | --- | --- |
| `BRIDGE_SCRIPT` | `mammotion_go2rtc_bridge.py` | Set to `mammotion_webrtc_bridge.py` for passthrough mode |
| `GO2RTC_API_URL` | `http://frigate:1984` | go2rtc API used for stream registration |
| `MAMMOTION_STREAM_NAME` | `mammotion` | Stream name created/maintained in go2rtc |
| `MAMMOTION_WHEP_HOST` | container hostname | Hostname go2rtc uses to reach this bridge |
| `MAMMOTION_WHEP_PORT` | `8555` | WHEP/WS signaling listen port |
| `MAMMOTION_GO2RTC_SIGNALING` | `ws` | WS signaling mode only (`webrtc:ws://.../api/ws?src=...`). Other values are ignored and forced to `ws`. |
| `MAMMOTION_GO2RTC_RECONCILE_SECONDS` | `20` | Periodically re-ensures stream registration after go2rtc/Frigate restarts |
| `MAMMOTION_KEEPALIVE_SECONDS` | `10` | MQTT keep-alive cadence to keep the mower publishing |

Recommended for Frigate/go2rtc right now:

- Set `MAMMOTION_GO2RTC_SIGNALING=ws`.
- WHEP is unsupported here; the bridge now runs WS-only for Frigate/go2rtc.
- Keep `MAMMOTION_GO2RTC_RECONCILE_SECONDS` enabled so stream registration
  self-heals after Frigate restarts.

### Example: bridge alongside Frigate

The bridge just needs to reach Frigate's go2rtc, so the simplest setup is to
drop it into the same Compose file as Frigate (they share a network, so the
`frigate` hostname resolves):

```yaml
services:
  frigate:
    image: ghcr.io/blakeblackshear/frigate:stable
    container_name: frigate
    restart: unless-stopped
    privileged: true
    shm_size: "1gb"
    devices:
      - /dev/dri:/dev/dri          # Intel hwaccel
    volumes:
      - /etc/localtime:/etc/localtime:ro
      - frigate-config:/config
      - /mnt/nvme/frigate:/media/frigate     # adjust to your storage
    environment:
      FRIGATE_RTSP_PASSWORD: "CHANGE_ME"
      LIBVA_DRIVER_NAME: iHD
    ports:
      - "8971:8971"
      - "8554:8554"               # go2rtc RTSP (the bridge publishes here)
      - "1984:1984"               # go2rtc web UI

  mammotion-bridge:
    image: ghcr.io/bleialf/mammotion-rtsp-bridge:stable   # newest release; :latest = bleeding edge
    container_name: mammotion-bridge
    restart: unless-stopped
    environment:
      MAMMOTION_EMAIL: "your@email.com"
      MAMMOTION_PASSWORD: "your-password"
      MAMMOTION_DEVICE_NAME: "first"
      # Defaults to rtsp://frigate:8554/mammotion — only set if different.
      #GO2RTC_PUBLISH_URL: "rtsp://frigate:8554/mammotion"

volumes:
  frigate-config:
```

## Configuration

All settings are environment variables on the bridge service. Only the first
three are required; the rest have sensible defaults.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MAMMOTION_EMAIL` | — | Mammotion account email |
| `MAMMOTION_PASSWORD` | — | Mammotion account password |
| `MAMMOTION_DEVICE_NAME` | `first` | Device name in the app, or `first` to auto-pick |
| `GO2RTC_PUBLISH_URL` | `rtsp://frigate:8554/mammotion` | Where the bridge publishes; stream name must match go2rtc |
| `MAMMOTION_HEARTBEAT_FILE` | unset | File touched per frame for the Docker healthcheck |
| `MAMMOTION_REFRESH_SECONDS` | `1800` | Agora token refresh interval (0 = off) |
| `MAMMOTION_RECONNECT_BACKOFF_SECONDS` | `8` | Delay before retrying after a failure |
| `MAMMOTION_STARTUP_FRAME_TIMEOUT_SECONDS` | `90` | Restart if no first frame after connect |
| `MAMMOTION_SOFT_STALL_TIMEOUT_SECONDS` | `12` | Request a keyframe after this much stall |
| `MAMMOTION_KEYFRAME_REQUEST_COOLDOWN_SECONDS` | `8` | Min seconds between keyframe requests |
| `MAMMOTION_FRAME_STALL_TIMEOUT_SECONDS` | `120` | Hard-restart cycle if frames/publisher stay gone this long |

### go2rtc / Frigate

See [frigate.example.yaml](frigate.example.yaml) (Frigate) or
[go2rtc.example.yaml](go2rtc.example.yaml) (standalone go2rtc). The key points:

- go2rtc needs an **empty stream** named to match `GO2RTC_PUBLISH_URL` (e.g.
  `mammotion:`) — that makes it accept the bridge's RTSP publish.
- The bridge outputs **H.264**, so Frigate's `hwaccel_args` must be an h264
  preset (`preset-intel-qsv-h264` on Intel).
- `roles: [record]` keeps it simple: continuous recording, no detect process.

### Home Assistant dashboard (optional)

If you use [advanced-camera-card](https://card.camera/) with go2rtc WebRTC,
point it at the stream name explicitly (the camera entity name usually won't
match the go2rtc stream name):

```yaml
- camera_entity: camera.mammotion
  live_provider: go2rtc
  go2rtc:
    url: http://<go2rtc-host>:1984
    stream: mammotion
    modes: [webrtc]
```

## How robust it is

The mower drops out of the Agora channel periodically and its cloud session
token expires after a few hours. The bridge tries to handle both automatically:

- **Proactive keep-alive** every 20 s to keep the mower publishing.
- **Recovery** if the mower leaves anyway (wakes it back up, ~2 s gap).
- **Fresh login per cycle** so an expired cloud token never wedges it.
- **Watchdogs** that restart the cycle if frames stall, the publisher stays
  gone, or the ffmpeg subprocess dies.

## Troubleshooting

- **Stream works in go2rtc UI but not in Frigate** → codec preset mismatch.
  The bridge sends H.264; use `preset-intel-qsv-h264` (not h265).
- **Frigate logs `404 Not Found` for the stream** → the bridge isn't publishing.
  Check `docker logs mammotion-bridge`; confirm `Bridge active. Publishing to …`.
- **Frigate `detect` ffmpeg crash-loops on HEVC errors** → you're on an old
  config; the current bridge outputs H.264. Re-pull the image and use the h264 preset.
- **Choppy / never loads in a HA card** → set the explicit `stream:` name in the
  card (see above); the auto entity→stream mapping often picks the wrong name.
- **Phone app keeps logging out** (or bridge logs `refreshToken invalid!!`) →
  you're sharing one account between the app and the bridge. Use a separate
  shared account for the bridge (see the note at the top).
- **Check the live stream directly:** `http://<go2rtc-host>:1984/stream.html?src=mammotion`

## Versions & releases

The image is built and published by GitHub Actions
([.github/workflows/docker-publish.yml](.github/workflows/docker-publish.yml)):

| Image tag | When | Use for |
| --- | --- | --- |
| `:stable` | a release is tagged | **recommended** — newest tagged release |
| `:latest` | every push to `main` | bleeding edge / dev |
| `:1.2.3`, `:1.2`, `:1` | tag `v1.2.3` | pin a release at any precision |
| `:sha-<short>` | every push to `main` | pin an exact commit |

`:stable` only moves when you cut a release; `:latest` moves on every commit to
main. Most people should run `:stable`.

See [CHANGELOG.md](CHANGELOG.md) for what changed between versions.

Cut a release by tagging:

```bash
git tag v1.0.0
git push origin v1.0.0
```

### Building locally

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  --tag ghcr.io/<you>/mammotion-rtsp-bridge:latest --push .
```

Uses [pymammotion](https://github.com/mikey0000/PyMammotion) for the Mammotion
cloud/Agora integration.
