# Mammotion -> go2rtc Bridge (Docker)

This container logs in to Mammotion, subscribes to the Agora video track, and
publishes it as RTSP to go2rtc. ffmpeg runs inside the bridge container, fed
raw HEVC NAL units from the Agora SDK on its stdin, with wall-clock timestamps
applied before the RTSP push — which is the fix for the historic
"Timestamps are unset / Broken pipe" loop.

## Add To Existing Frigate Compose

1. Copy this folder to your Frigate host, for example as `./mammotion-bridge` next to your main compose file.
2. Copy `.env.mammotion.example` to `.env.mammotion` and fill credentials.
3. Add this service to your existing compose:

```yaml
  mammotion-bridge:
    build:
      context: ./mammotion-bridge
      dockerfile: Dockerfile
    container_name: mammotion-bridge
    restart: unless-stopped
    env_file:
      - ./mammotion-bridge/.env.mammotion
    healthcheck:
      test: ["CMD-SHELL", "python -c 'import os,time; p=\"/tmp/mammotion_heartbeat\"; exit(0 if os.path.exists(p) and time.time()-os.path.getmtime(p)<120 else 1)'"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 45s
```

4. Deploy:

```bash
docker compose up -d --build mammotion-bridge
```

## go2rtc Configuration

Add this to your Frigate config (or your standalone `go2rtc.yaml` without the
outer `go2rtc:` key):

```yaml
go2rtc:
  streams:
    mammotion:
```

The empty entry declares a named stream with no source — go2rtc accepts
RTSP publishes to that name (the bridge sends them) and serves whatever
was published to whoever asks for it.

## Frigate Input

```yaml
ffmpeg:
  inputs:
    - path: rtsp://127.0.0.1:8554/mammotion
      roles: [detect, record]
```

## Environment Variables

| Variable | Default | Notes |
| --- | --- | --- |
| `MAMMOTION_EMAIL` | _required_ | Mammotion account |
| `MAMMOTION_PASSWORD` | _required_ | Mammotion account |
| `MAMMOTION_DEVICE_NAME` | first device | Name shown in the Mammotion app |
| `GO2RTC_PUBLISH_URL` | `rtsp://frigate:8554/mammotion` | RTSP URL ffmpeg pushes to |
| `MAMMOTION_REFRESH_SECONDS` | `1800` | Agora token refresh interval (0 = off) |
| `MAMMOTION_HEARTBEAT_FILE` | _unset_ | Path touched on each frame from Agora |

## Publishing Prebuilt Images

This repo includes a GitHub Actions workflow ([.github/workflows/docker-publish.yml](.github/workflows/docker-publish.yml)) that builds a multi-arch image (amd64 + arm64) and pushes it to GitHub Container Registry on every push to `main`. Tags produced:

- `ghcr.io/bleialf/mammotion-rtsp-bridge:latest` — current main
- `ghcr.io/bleialf/mammotion-rtsp-bridge:sha-<short>` — pinned to a commit
- `ghcr.io/bleialf/mammotion-rtsp-bridge:v1.2.3` — when you tag a release

First-time setup on GitHub:
1. Push this repo to `Bleialf/mammotion-rtsp-bridge`.
2. The workflow runs automatically; first build takes a few minutes (subsequent ones use the GHA cache and are fast).
3. After the first successful run, find the image under your repo's "Packages" sidebar. It's private by default — leave it private.

Pulling on the NUC (private image, one-time):
```bash
# Create a Personal Access Token (classic) with `read:packages` at
# https://github.com/settings/tokens, then:
echo $GHCR_PAT | docker login ghcr.io -u Bleialf --password-stdin
```

Deploying / upgrading:
```bash
docker compose pull mammotion-bridge && docker compose up -d mammotion-bridge
```

No more local `--build` step.

## Robustness Features

- automatic Mammotion login retries
- automatic ffmpeg restart on broken pipe (resumes from next keyframe)
- startup watchdog (restarts bridge if no first frame arrives)
- frame stall watchdog (requests a keyframe, then restarts if frames stop)
- periodic Agora token renewal
- Docker healthcheck based on a frame-ingress heartbeat file
