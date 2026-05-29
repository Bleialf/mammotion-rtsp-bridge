FROM python:3.12-slim
# Pinned to 3.12 because PyAV (av) has no prebuilt wheel for 3.14 yet and we
# don't want to pull in a full ffmpeg build toolchain just to compile it.

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/app

WORKDIR /app

# ffmpeg: used by the legacy bridge.
# libsrtp2-1: required by aiortc's SRTP layer (used by the aiortc-relay branch).
# PyAV's manylinux wheel statically links its own ffmpeg + libvpx + libopus,
# so we don't need their dev/runtime packages here.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg ca-certificates libsrtp2-1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY mammotion_go2rtc_bridge.py /app/mammotion_go2rtc_bridge.py
COPY mammotion_webrtc_bridge.py /app/mammotion_webrtc_bridge.py
COPY mammotion_webrtc/ /app/mammotion_webrtc/
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]
