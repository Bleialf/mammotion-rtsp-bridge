#!/usr/bin/env python3
"""Mammotion Agora → RTSP passthrough bridge.

Single-process bridge for delivering the Mammotion mower's H265 camera into
go2rtc/Frigate without ffmpeg, transcoding, or a second WebRTC hop. Flow:

1. Log into Mammotion via pymammotion and fetch the Agora stream
   subscription (appid/channelName/token/uid/areaCode) for one device.
2. Run a supervisor that brings up an aiortc upstream peer connection to
   Agora as a passive viewer and taps the inbound H265 RTP packets
   (:class:`mammotion_webrtc.AgoraToRtspRelay`).
3. Serve those packets verbatim over a minimal RTSP server bound on
   ``MAMMOTION_RTSP_PORT`` (default 8554), mount ``/mammotion``.
4. Register ``rtsp://<self>:<port>/mammotion`` with go2rtc via its REST
   API so Frigate discovers the stream automatically.
5. Keep the mower in the Agora channel by sending MQTT
   ``send_todev_ble_sync`` every ~10s. The publisher times out without
   this nudge; Mammotion has no RTM heartbeat.

Environment variables:
  MAMMOTION_EMAIL / MAMMOTION_PASSWORD     - cloud credentials (required)
  MAMMOTION_DEVICE_NAME                    - device to stream ("" / "first" = first)
  MAMMOTION_RTSP_PORT                      - RTSP listen port (default 8554)
  MAMMOTION_RTSP_HOST                      - host go2rtc uses to reach us
                                             (default: container hostname)
  MAMMOTION_RTSP_BIND                      - bind address (default 0.0.0.0)
  MAMMOTION_STREAM_NAME                    - go2rtc stream name (default mammotion)
  GO2RTC_API_URL                           - go2rtc REST base (default http://frigate:1984)
  MAMMOTION_GO2RTC_RECONCILE_SECONDS       - periodic re-register interval (default 20)
  MAMMOTION_KEEPALIVE_SECONDS              - MQTT keep-alive interval (default 10)
  MAMMOTION_RECONNECT_BACKOFF_SECONDS      - login retry backoff (default 8)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
from typing import Any

from mammotion_webrtc.agora_session import (
    StreamCredentials,
    refresh_agora_context,
)
from mammotion_webrtc.aiortc_relay import AgoraToRtspRelay
from mammotion_webrtc.go2rtc_register import Go2RTCStreamRegistrar
from mammotion_webrtc.rtsp_server import Go2RtcRtspStream

LOGGER = logging.getLogger("mammotion_webrtc_bridge")

# Minimum seconds between get_stream_subscription calls. Each call re-triggers
# the mower's publish but also hits the cloud, so we debounce rapid retries
# to avoid an account lockout.
CREDS_DEBOUNCE_S = 15.0

# Mirrors the AREA_CODE_MAP in the main-branch bridge, but maps to the
# "CN,GLOBAL"-style strings the REST choose_server API expects.
AREA_CODE_STRING_MAP = {
    "AREA_CODE_CN": "CN",
    "AREA_CODE_NA": "NA",
    "AREA_CODE_EU": "EU",
    "AREA_CODE_AS": "AS",
    "AREA_CODE_JP": "JP",
    "AREA_CODE_IN": "IN",
    "AREA_CODE_GLOB": "GLOBAL",
}


def resolve_area_code_string(value: Any) -> str:
    """Map a Mammotion areaCode to a choose_server area_code string."""
    if isinstance(value, str) and value in AREA_CODE_STRING_MAP:
        region = AREA_CODE_STRING_MAP[value]
        return "CN,GLOBAL" if region in ("GLOBAL", "CN") else f"{region},GLOBAL"
    return "CN,GLOBAL"


async def fetch_stream_fields(mammotion: Any, device_name: str) -> dict[str, Any]:
    """Fetch Agora stream subscription fields for one device."""
    selected_name = (device_name or "").strip()
    if not selected_name or selected_name.lower() == "first":
        all_devices = mammotion.device_registry.all_devices
        if not all_devices:
            raise RuntimeError("No devices found in Mammotion account")
        device_handle = all_devices[0]
        selected_name = device_handle.device_name
        LOGGER.info("Auto-selected first device: %s", selected_name)
    else:
        device_handle = mammotion.device_registry.get_by_name(selected_name)
        if device_handle is None:
            raise RuntimeError(f"Device not found: {selected_name}")

    iot_id = device_handle.iot_id
    stream_response = await mammotion.get_stream_subscription(selected_name, iot_id)
    data = getattr(stream_response, "data", None)
    if data is None:
        raise RuntimeError(f"Stream response has no data: {stream_response}")

    return {
        "appid": getattr(data, "appid", None),
        "channelName": getattr(data, "channelName", None),
        "token": getattr(data, "token", None),
        "uid": getattr(data, "uid", None),
        "areaCode": getattr(data, "areaCode", None),
        "iot_id": iot_id,
        "device_name": selected_name,
    }


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


async def main() -> None:
    log_level = os.getenv("MAMMOTION_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    email = os.getenv("MAMMOTION_EMAIL", "")
    password = os.getenv("MAMMOTION_PASSWORD", "")
    device_name = os.getenv("MAMMOTION_DEVICE_NAME", "")
    if not email or not password:
        raise SystemExit(
            "Missing Mammotion credentials. Set MAMMOTION_EMAIL/MAMMOTION_PASSWORD."
        )

    rtsp_port = _env_int("MAMMOTION_RTSP_PORT", 8554)
    rtsp_bind = os.getenv("MAMMOTION_RTSP_BIND", "0.0.0.0")
    rtsp_host = os.getenv("MAMMOTION_RTSP_HOST", socket.gethostname())
    go2rtc_api_url = os.getenv("GO2RTC_API_URL", "http://frigate:1984")
    stream_name = os.getenv("MAMMOTION_STREAM_NAME", "mammotion")
    go2rtc_reconcile_interval = float(_env_int("MAMMOTION_GO2RTC_RECONCILE_SECONDS", 20))
    keepalive_interval = float(_env_int("MAMMOTION_KEEPALIVE_SECONDS", 10))
    reconnect_backoff = _env_int("MAMMOTION_RECONNECT_BACKOFF_SECONDS", 8)

    rtsp_source = f"rtsp://{rtsp_host}:{rtsp_port}/{stream_name}"

    LOGGER.info("Loading Mammotion SDK modules")
    from pymammotion.client import MammotionClient

    stop_async = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_async.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_async.set())

    # Shared state used by the credentials provider and keep-alive loop.
    state: dict[str, Any] = {
        "mammotion": None,
        "device_name": device_name,
        "iot_id": None,
        "credentials": None,
        "creds_fetched_at": 0.0,
    }

    async def _fresh_client() -> Any | None:
        # A fresh client + full login every cycle. The pymammotion refresh
        # token goes stale after a few hours; starting fresh sidesteps a
        # dead session.
        while not stop_async.is_set():
            client = MammotionClient(ha_version="3.4.23")
            try:
                LOGGER.info("Logging in to Mammotion cloud")
                await client.login_and_initiate_cloud(email, password)
                return client
            except Exception:
                LOGGER.exception(
                    "Mammotion login failed; retrying in %ss", reconnect_backoff
                )
                try:
                    await client.stop()
                except Exception:
                    pass
                await asyncio.sleep(reconnect_backoff)
        return None

    def _creds_from_fields(fields: dict[str, Any]) -> StreamCredentials:
        return StreamCredentials(
            app_id=str(fields["appid"]),
            channel=str(fields["channelName"]),
            rtc_token=str(fields["token"]),
            uid=int(fields["uid"]),
            area_code=resolve_area_code_string(fields.get("areaCode")),
        )

    async def credentials_provider() -> StreamCredentials:
        """Return Agora credentials, debounced.

        ``get_stream_subscription`` does double duty: it returns the RTC
        token AND sends the MQTT command that tells the mower to (re)join
        Agora. The relay calls us on every reconnect attempt; without a
        debounce we would hammer the cloud and risk a lockout.
        """
        mammotion = state["mammotion"]
        now = loop.time()
        last = state.get("creds_fetched_at", 0.0)
        cached = state.get("credentials")
        if mammotion is None or (cached is not None and now - last < CREDS_DEBOUNCE_S):
            if cached is None:
                raise RuntimeError("Mammotion credentials not ready")
            return cached
        try:
            fields = await fetch_stream_fields(mammotion, state["device_name"])
            for key in ("appid", "channelName", "token", "uid"):
                if not fields.get(key):
                    raise RuntimeError(f"Missing {key} in stream subscription payload")
            state["device_name"] = fields["device_name"]
            state["iot_id"] = fields["iot_id"]
            state["credentials"] = _creds_from_fields(fields)
            state["creds_fetched_at"] = now
            LOGGER.info("Re-triggered mower publish via get_stream_subscription")
        except Exception:
            LOGGER.exception("Credential refresh failed; using cached if available")
            if cached is None:
                raise
        return state["credentials"]

    async def wake_publisher() -> None:
        """Force the mower into the Agora channel with video on.

        ``send_todev_ble_sync`` with ``sync_type=3`` is Mammotion's BLE wake.
        ``device_agora_join_channel_with_position`` with ``enter_state=1``
        is the explicit "join Agora, video on" command. Both are best-effort.
        """
        mammotion = state["mammotion"]
        device = state.get("device_name")
        if mammotion is None or not device:
            return
        try:
            await mammotion.send_command_with_args(
                device, "send_todev_ble_sync", sync_type=3
            )
        except Exception:
            LOGGER.debug("BLE sync wake-up failed", exc_info=True)
        try:
            await mammotion.send_command_with_args(
                device,
                "device_agora_join_channel_with_position",
                enter_state=1,
            )
        except Exception:
            LOGGER.debug("Force-join Agora channel failed", exc_info=True)

    # Construct the RTSP server now so the relay can hold a reference even
    # before we know the port is bound — start()/stop() are explicit below.
    rtsp_server = Go2RtcRtspStream(
        bind=rtsp_bind,
        port=rtsp_port,
        mount_point=stream_name,
    )
    relay = AgoraToRtspRelay(
        credentials_provider=credentials_provider,
        agora_context_provider=refresh_agora_context,
        rtsp_server=rtsp_server,
        publisher_wakeup=wake_publisher,
    )
    # Wire the RTSP server's "new viewer connected" hook back to the relay
    # so we can opportunistically PLI Agora for a fresh keyframe — without
    # this the new viewer waits up to one full GOP for picture.
    rtsp_server._on_keyframe_request = lambda: asyncio.create_task(relay.request_keyframe())

    relay_started = False
    try:
        # ---- Login + initial credential fetch ----
        # The supervisor cannot start until we have *some* credentials cached,
        # because credentials_provider's first run feeds the initial PC.
        while not stop_async.is_set() and state["mammotion"] is None:
            mammotion = await _fresh_client()
            if mammotion is None:
                break
            state["mammotion"] = mammotion
            try:
                fields = await fetch_stream_fields(mammotion, device_name)
                for key in ("appid", "channelName", "token", "uid"):
                    if not fields.get(key):
                        raise RuntimeError(
                            f"Missing {key} in stream subscription payload"
                        )
                state["device_name"] = fields["device_name"]
                state["iot_id"] = fields["iot_id"]
                state["credentials"] = _creds_from_fields(fields)
                state["creds_fetched_at"] = loop.time()
                LOGGER.info(
                    "Stream subscription ready for device %s (channel=%s)",
                    fields["device_name"],
                    fields["channelName"],
                )
            except Exception:
                LOGGER.exception("Initial stream fetch failed; retrying")
                try:
                    await mammotion.stop()
                except Exception:
                    pass
                state["mammotion"] = None
                await asyncio.sleep(reconnect_backoff)

        if stop_async.is_set():
            return

        # ---- Start RTSP server + relay supervisor ----
        await rtsp_server.start()
        LOGGER.info(
            "RTSP server up: %s (bind=%s:%d)",
            rtsp_source,
            rtsp_bind,
            rtsp_port,
        )
        await relay.start()
        relay_started = True

        # ---- Register the go2rtc stream + reconcile loop + keep-alive ----
        next_keepalive = loop.time()
        next_go2rtc_reconcile = loop.time()
        while not stop_async.is_set():
            now = loop.time()
            mammotion = state["mammotion"]
            if mammotion is None:
                # Lost cloud session — re-login and continue. The relay's
                # own supervisor will keep retrying with cached creds (or
                # fail loudly on token expiry), so we do not need to tear
                # it down here.
                mammotion = await _fresh_client()
                if mammotion is None:
                    break
                state["mammotion"] = mammotion

            if now >= next_go2rtc_reconcile:
                try:
                    async with Go2RTCStreamRegistrar(go2rtc_api_url) as registrar:
                        ok = await registrar.ensure_stream(stream_name, rtsp_source)
                    if ok:
                        LOGGER.debug(
                            "go2rtc stream %s wired to %s", stream_name, rtsp_source
                        )
                    else:
                        LOGGER.warning(
                            "go2rtc stream %s not confirmed; will retry in %.0fs",
                            stream_name,
                            go2rtc_reconcile_interval,
                        )
                except Exception:
                    LOGGER.warning(
                        "go2rtc reconciliation failed; retrying in %.0fs",
                        go2rtc_reconcile_interval,
                        exc_info=True,
                    )
                next_go2rtc_reconcile = now + go2rtc_reconcile_interval

            if now >= next_keepalive:
                try:
                    await mammotion.send_command_with_args(
                        state["device_name"], "send_todev_ble_sync", sync_type=2
                    )
                except Exception:
                    LOGGER.debug("Keep-alive sync failed", exc_info=True)
                    # A failing keep-alive usually means a dead cloud session.
                    # Drop the client; the loop re-logs in on the next tick.
                    try:
                        await mammotion.stop()
                    except Exception:
                        pass
                    state["mammotion"] = None
                next_keepalive = now + keepalive_interval

            try:
                await asyncio.wait_for(stop_async.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
    finally:
        LOGGER.info("Shutting down")
        if relay_started:
            try:
                await relay.stop()
            except Exception:
                LOGGER.exception("Relay stop failed")
        try:
            await rtsp_server.stop()
        except Exception:
            LOGGER.exception("RTSP server stop failed")
        if state["mammotion"] is not None:
            try:
                await state["mammotion"].stop()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
