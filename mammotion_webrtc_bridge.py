#!/usr/bin/env python3
"""Standalone Mammotion Agora -> go2rtc WebRTC passthrough bridge.

This is the entrypoint for the WHEP/WebRTC passthrough approach (branch
``webrtc-passthrough``). Unlike the main-branch ``mammotion_go2rtc_bridge.py``
(which pulls encoded frames and re-muxes through ffmpeg -> RTSP), this service
never touches the media. It:

1. Logs into Mammotion (pymammotion), fetching the Agora stream subscription
   (appid / channelName / token / uid / areaCode), reusing the same pattern as
   the main-branch bridge (``fetch_stream_fields`` / ``_fresh_client``).
2. Starts a standalone aiohttp WHEP server (``mammotion_webrtc.whep_server``)
   that brokers go2rtc's SDP offer into an Agora-derived answer.
3. Registers a go2rtc stream named ``mammotion`` whose source is
   ``webrtc:http://<self-host>:<port>/whep/mammotion`` so go2rtc dials our WHEP
   endpoint and DTLS-SRTPs straight to the Agora edge.
4. Runs the MQTT keep-alive loop (``send_todev_ble_sync`` sync_type=2 every
   ~10s) so the mower's publisher stays in the Agora channel. Mammotion has no
   RTM token, so this MQTT path replaces PetKit's RTM heartbeat.

Environment variables:
  MAMMOTION_EMAIL / MAMMOTION_PASSWORD   - cloud credentials (required)
  MAMMOTION_DEVICE_NAME                  - device to stream ("" / "first" = first)
  MAMMOTION_WHEP_PORT                    - WHEP listen port (default 8555)
  MAMMOTION_WHEP_HOST                    - host go2rtc uses to reach us
                                           (default: container hostname)
  MAMMOTION_WHEP_BIND                    - bind address (default 0.0.0.0)
  MAMMOTION_WHEP_TOKEN                   - optional static bearer token
  GO2RTC_API_URL                         - go2rtc REST base (default http://frigate:1984)
  MAMMOTION_STREAM_NAME                  - go2rtc stream name (default mammotion)
  MAMMOTION_GO2RTC_RECONCILE_SECONDS     - periodic re-register interval
                                           (default 20)
  MAMMOTION_KEEPALIVE_SECONDS            - MQTT keep-alive interval (default 10)
  MAMMOTION_RECONNECT_BACKOFF_SECONDS    - login retry backoff (default 8)
  MAMMOTION_AGORA_REJOIN_BACKOFF_SECONDS - Agora rejoin backoff (default 3)
  MAMMOTION_WEBRTC_VIDEO_ONLY            - answer audio as inactive (default false)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
from typing import Any

from aiohttp import web

from mammotion_webrtc.whep_server import StreamCredentials, create_whep_app
from mammotion_webrtc.go2rtc_register import Go2RTCStreamRegistrar

LOGGER = logging.getLogger("mammotion_webrtc_bridge")

# Minimum seconds between get_stream_subscription calls. Each call re-triggers
# the mower to publish but also hits the cloud, so we debounce go2rtc's rapid
# WHEP retries to avoid an account lockout.
CREDS_DEBOUNCE_S = 15.0

# Mirrors the AREA_CODE_MAP in the main-branch bridge, but maps to the
# "CN,GLOBAL"-style strings that Agora's choose_server REST API expects (not the
# integer bitmask used by the native Agora SDK on main).
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
    """Map a Mammotion areaCode to a choose_server area_code string.

    TODO(mammotion): unverified. The main-branch bridge maps areaCode to the
    native SDK's integer bitmask; the REST choose_server API instead wants a
    comma list like "CN,GLOBAL". We default to "CN,GLOBAL" (Agora's own default)
    which works for global apps. If the mower's app id is region-locked this may
    need tuning.
    """
    if isinstance(value, str) and value in AREA_CODE_STRING_MAP:
        region = AREA_CODE_STRING_MAP[value]
        return "CN,GLOBAL" if region in ("GLOBAL", "CN") else f"{region},GLOBAL"
    return "CN,GLOBAL"


async def fetch_stream_fields(mammotion: Any, device_name: str) -> dict[str, Any]:
    """Fetch Agora stream subscription fields for one device.

    Identical pattern to the main-branch bridge's ``fetch_stream_fields``.
    """
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


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


async def main() -> None:
    """Run the bridge: login, WHEP server, go2rtc registration, keep-alive."""
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

    whep_port = _env_int("MAMMOTION_WHEP_PORT", 8555)
    whep_bind = os.getenv("MAMMOTION_WHEP_BIND", "0.0.0.0")
    whep_host = os.getenv("MAMMOTION_WHEP_HOST", socket.gethostname())
    whep_token = os.getenv("MAMMOTION_WHEP_TOKEN") or None
    go2rtc_api_url = os.getenv("GO2RTC_API_URL", "http://frigate:1984")
    stream_name = os.getenv("MAMMOTION_STREAM_NAME", "mammotion")
    go2rtc_signaling = (os.getenv("MAMMOTION_GO2RTC_SIGNALING", "ws") or "ws").strip().lower()
    go2rtc_reconcile_interval = float(_env_int("MAMMOTION_GO2RTC_RECONCILE_SECONDS", 20))
    keepalive_interval = float(_env_int("MAMMOTION_KEEPALIVE_SECONDS", 10))
    reconnect_backoff = _env_int("MAMMOTION_RECONNECT_BACKOFF_SECONDS", 8)
    agora_rejoin_backoff = float(
        _env_int("MAMMOTION_AGORA_REJOIN_BACKOFF_SECONDS", 3)
    )
    video_only = _env_bool("MAMMOTION_WEBRTC_VIDEO_ONLY", False)

    LOGGER.info("Loading Mammotion SDK modules")
    from pymammotion.client import MammotionClient

    stop_async = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_async.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_async.set())

    # Shared mutable state used by the credentials provider and keep-alive loop.
    # The credentials provider is called on every new WHEP session (and on
    # Agora token renewal), so it must always read the *current* client and
    # device. We mutate these in place across reconnect cycles.
    state: dict[str, Any] = {
        "mammotion": None,
        "device_name": device_name,
        "iot_id": None,
    }

    async def _fresh_client() -> Any | None:
        # A fresh client + full login every cycle. The pymammotion refresh token
        # goes stale after a few hours; starting fresh sidesteps a dead session.
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
            device_id=str(fields.get("iot_id") or ""),
            area_code=resolve_area_code_string(fields.get("areaCode")),
        )

    async def credentials_provider() -> StreamCredentials:
        """Return Agora RTC credentials, re-triggering the mower to publish.

        get_stream_subscription does double duty: it returns the RTC token AND
        sends the MQTT command that tells the mower to (re)join Agora and start
        publishing video (the publish window is only ~50s). So on a viewer
        connect we must re-fetch to wake the publisher — but debounced, so
        go2rtc's rapid WHEP retries don't hammer the cloud (which risks an
        account lockout). Within the debounce window we return the cached creds.
        """
        mammotion = state["mammotion"]
        now = loop.time()
        last = state.get("creds_fetched_at", 0.0)
        cached = state.get("credentials")
        if mammotion is None or (cached is not None and now - last < CREDS_DEBOUNCE_S):
            if cached is None:
                raise RuntimeError("Mammotion credentials not ready")
            LOGGER.info(
                "Reusing cached stream subscription device=%s channel=%s age=%.1fs",
                cached.device_id or state.get("iot_id") or "<unknown>",
                cached.channel,
                now - last,
            )
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
            LOGGER.info(
                "Refreshed stream subscription device=%s channel=%s",
                fields["iot_id"],
                fields["channelName"],
            )
        except Exception:
            LOGGER.exception("Credential refresh failed; using cached if available")
            if cached is None:
                raise
        return state["credentials"]

    async def wake_publisher() -> None:
        """Force the mower into the Agora channel with video on.

        Fired at the start of each new WHEP session. ``send_todev_ble_sync``
        with ``sync_type=3`` is Mammotion's BLE wake (mirrors mikey0000's HA
        flow on offer open). ``device_agora_join_channel_with_position`` with
        ``enter_state=1`` is ``vi_switch=1`` — explicit "join Agora channel,
        video on". Both are best-effort; failures don't block negotiation.
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

    # ---- Start the WHEP aiohttp server (long-lived) ----
    app = create_whep_app(
        credentials_provider,
        auth_token=whep_token,
        publisher_wakeup=wake_publisher,
        reconnect_backoff_seconds=agora_rejoin_backoff,
        video_only=video_only,
    )
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, whep_bind, whep_port)
    await site.start()
    if go2rtc_signaling != "ws":
        LOGGER.warning(
            "Ignoring MAMMOTION_GO2RTC_SIGNALING=%s; only ws is supported "
            "because WHEP is currently unreliable with Frigate/go2rtc",
            go2rtc_signaling,
        )
        go2rtc_signaling = "ws"
    whep_source = f"webrtc:ws://{whep_host}:{whep_port}/api/ws?src={stream_name}"
    LOGGER.info(
        "WHEP server listening on %s:%s; go2rtc signaling=%s source=%s video_only=%s "
        "agora_rejoin_backoff=%.1fs",
        whep_bind,
        whep_port,
        go2rtc_signaling,
        whep_source,
        video_only,
        agora_rejoin_backoff,
    )

    registrar: Go2RTCStreamRegistrar | None = None
    try:
        # ---- Login + initial credential fetch (so the provider works) ----
        while not stop_async.is_set():
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
                continue

            # ---- Register the go2rtc stream (idempotent) ----
            try:
                registrar = Go2RTCStreamRegistrar(go2rtc_api_url)
                async with registrar:
                    ok = await registrar.ensure_stream(stream_name, whep_source)
                if ok:
                    LOGGER.info(
                        "Registered go2rtc stream %s -> %s", stream_name, whep_source
                    )
                else:
                    LOGGER.warning(
                        "go2rtc stream registration did not confirm; go2rtc may "
                        "register lazily on first viewer"
                    )
            except Exception:
                LOGGER.exception("go2rtc stream registration failed (continuing)")

            # ---- Keep-alive loop ----
            # Proactive keep-alive over MQTT. The mower's publisher leaves the
            # Agora channel after ~50s without a viewer-present signal. PetKit
            # used an RTM heartbeat; Mammotion has none, so we use MQTT
            # send_todev_ble_sync(sync_type=2) instead.
            next_keepalive = loop.time()
            next_go2rtc_reconcile = loop.time()
            while not stop_async.is_set():
                now = loop.time()
                if now >= next_go2rtc_reconcile:
                    try:
                        registrar = Go2RTCStreamRegistrar(go2rtc_api_url)
                        async with registrar:
                            ok = await registrar.ensure_stream(stream_name, whep_source)
                        if ok:
                            LOGGER.debug(
                                "go2rtc stream %s registration is healthy", stream_name
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
                        # A failing keep-alive usually means a dead cloud
                        # session; break to re-login with a fresh client.
                        break
                    next_keepalive = now + keepalive_interval
                try:
                    await asyncio.wait_for(stop_async.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass

            # Cycle ended (stop or keep-alive failure). Tear down the client so
            # the next cycle logs in fresh.
            try:
                await mammotion.stop()
            except Exception:
                LOGGER.exception("Mammotion stop failed")
            state["mammotion"] = None
            if not stop_async.is_set():
                LOGGER.info("Re-logging into Mammotion in %ss", reconnect_backoff)
                await asyncio.sleep(reconnect_backoff)
    finally:
        LOGGER.info("Shutting down")
        if state["mammotion"] is not None:
            try:
                await state["mammotion"].stop()
            except Exception:
                pass
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
