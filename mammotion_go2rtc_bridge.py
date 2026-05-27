#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import queue
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class Settings:
    mammotion_email: str
    mammotion_password: str
    mammotion_device_name: str
    rtsp_publish_url: str
    refresh_seconds: int
    reconnect_backoff_seconds: int
    startup_frame_timeout_seconds: int
    soft_stall_timeout_seconds: int
    frame_stall_timeout_seconds: int
    keyframe_request_cooldown_seconds: int
    heartbeat_file: str
    dump_stream_json: bool


def parse_args() -> Settings:
    parser = argparse.ArgumentParser(
        description="Subscribe to Mammotion Agora video and publish it as RTSP to go2rtc"
    )
    parser.add_argument("--email", default=os.getenv("MAMMOTION_EMAIL", ""))
    parser.add_argument("--password", default=os.getenv("MAMMOTION_PASSWORD", ""))
    parser.add_argument("--device", default=os.getenv("MAMMOTION_DEVICE_NAME", ""))
    parser.add_argument(
        "--rtsp-url",
        default=os.getenv("GO2RTC_PUBLISH_URL", "rtsp://frigate:8554/mammotion"),
        help="RTSP URL to publish to (default: rtsp://frigate:8554/mammotion)",
    )
    parser.add_argument(
        "--refresh-seconds",
        type=int,
        default=int(os.getenv("MAMMOTION_REFRESH_SECONDS", "1800")),
    )
    parser.add_argument(
        "--reconnect-backoff-seconds",
        type=int,
        default=int(os.getenv("MAMMOTION_RECONNECT_BACKOFF_SECONDS", "8")),
    )
    parser.add_argument(
        "--startup-frame-timeout-seconds",
        type=int,
        default=int(os.getenv("MAMMOTION_STARTUP_FRAME_TIMEOUT_SECONDS", "90")),
    )
    parser.add_argument(
        "--soft-stall-timeout-seconds",
        type=int,
        default=int(os.getenv("MAMMOTION_SOFT_STALL_TIMEOUT_SECONDS", "12")),
    )
    parser.add_argument(
        "--frame-stall-timeout-seconds",
        type=int,
        default=int(os.getenv("MAMMOTION_FRAME_STALL_TIMEOUT_SECONDS", "120")),
    )
    parser.add_argument(
        "--keyframe-request-cooldown-seconds",
        type=int,
        default=int(os.getenv("MAMMOTION_KEYFRAME_REQUEST_COOLDOWN_SECONDS", "8")),
    )
    parser.add_argument(
        "--heartbeat-file",
        default=os.getenv("MAMMOTION_HEARTBEAT_FILE", "/tmp/mammotion_heartbeat"),
        help="Touched on every frame; the Docker healthcheck reads it. Set empty to disable.",
    )
    parser.add_argument("--dump-stream-json", action="store_true")
    args = parser.parse_args()

    if not args.email or not args.password:
        raise SystemExit(
            "Missing Mammotion credentials. Set MAMMOTION_EMAIL/MAMMOTION_PASSWORD."
        )

    return Settings(
        mammotion_email=args.email,
        mammotion_password=args.password,
        mammotion_device_name=args.device,
        rtsp_publish_url=args.rtsp_url,
        refresh_seconds=max(0, args.refresh_seconds),
        reconnect_backoff_seconds=max(1, args.reconnect_backoff_seconds),
        startup_frame_timeout_seconds=max(10, args.startup_frame_timeout_seconds),
        soft_stall_timeout_seconds=max(3, args.soft_stall_timeout_seconds),
        frame_stall_timeout_seconds=max(10, args.frame_stall_timeout_seconds),
        keyframe_request_cooldown_seconds=max(2, args.keyframe_request_cooldown_seconds),
        heartbeat_file=(args.heartbeat_file or "").strip(),
        dump_stream_json=bool(args.dump_stream_json),
    )


AREA_CODE_MAP = {
    "AREA_CODE_CN": 0x00000001,
    "AREA_CODE_NA": 0x00000002,
    "AREA_CODE_EU": 0x00000004,
    "AREA_CODE_AS": 0x00000008,
    "AREA_CODE_JP": 0x00000010,
    "AREA_CODE_IN": 0x00000020,
    "AREA_CODE_GLOB": 0xFFFFFFFF,
}


def resolve_area_code(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return AREA_CODE_MAP.get(value, AREA_CODE_MAP["AREA_CODE_GLOB"])
    return AREA_CODE_MAP["AREA_CODE_GLOB"]


class _ConnectionObserver:
    def __init__(self, parent: "AgoraToRtsp") -> None:
        self.parent = parent

    def __getattr__(self, name):
        def _noop(*args, **kwargs):
            return 0
        return _noop

    def on_connected(self, agora_rtc_conn, conn_info, reason):
        logging.info("Agora connected: reason=%s", reason)
        self.parent.connected_at_ts = time.time()
        self.parent.connected_event.set()
        ret = self.parent.connection.get_local_user().subscribe_all_video(
            self.parent._video_sub_options_cls(encodedFrameOnly=True)
        )
        logging.info("subscribe_all_video(encodedFrameOnly=True) -> %s", ret)

    def on_disconnected(self, agora_rtc_conn, conn_info, reason):
        logging.warning("Agora disconnected: reason=%s", reason)

    def on_user_joined(self, agora_rtc_conn, user_id):
        logging.info("Remote user joined: %s", user_id)
        self.parent.remote_uid = str(user_id)
        self.parent.peer_online = True
        self.parent.peer_offline_since = 0.0
        ret = self.parent.connection.get_local_user().subscribe_video(
            user_id, self.parent._video_sub_options_cls(encodedFrameOnly=True)
        )
        logging.info("subscribe_video(user=%s) -> %s", user_id, ret)
        self.parent.request_keyframe(reason="user_joined")

    def on_error(self, agora_rtc_conn, error_code, error_msg):
        logging.error("Agora error: code=%s msg=%s", error_code, error_msg)

    def on_user_offline(self, agora_rtc_conn, user_id, reason):
        # The mower's Agora publisher routinely leaves the channel after ~50s
        # with reason=0 (clean quit). The Mammotion app handles this as a
        # recovery flow (see Mammotion-HA agora_websocket.py): poke the device
        # via an MQTT command + refresh stream subscription. The device then
        # rejoins on its own within a few seconds.
        logging.info(
            "Remote user offline: uid=%s reason=%s -- scheduling recovery",
            user_id,
            reason,
        )
        self.parent.peer_online = False
        if self.parent.peer_offline_since == 0.0:
            self.parent.peer_offline_since = time.time()
        self.parent.waiting_for_keyframe = True
        self.parent._schedule_recovery()

    def on_user_left(self, agora_rtc_conn, user_id, reason):
        logging.info(
            "Remote user left: uid=%s reason=%s -- scheduling recovery",
            user_id,
            reason,
        )
        self.parent.peer_online = False
        if self.parent.peer_offline_since == 0.0:
            self.parent.peer_offline_since = time.time()
        self.parent.waiting_for_keyframe = True
        self.parent._schedule_recovery()


class _LocalUserObserver:
    def __init__(self, parent: "AgoraToRtsp") -> None:
        self.parent = parent

    def __getattr__(self, name):
        def _noop(*args, **kwargs):
            return 0
        return _noop

    def on_user_video_track_subscribed(self, agora_local_user, user_id, info, track):
        logging.info(
            "Video track subscribed: user=%s codec=%s",
            user_id,
            getattr(info, "codec_type", "?"),
        )

    def on_video_subscribe_state_changed(
        self, agora_local_user, channel, user_id, old_state, new_state, elapse
    ):
        logging.info(
            "Video subscribe state: user=%s old=%s new=%s", user_id, old_state, new_state
        )


class _EncodedFrameObserver:
    def __init__(self, parent: "AgoraToRtsp") -> None:
        self.parent = parent

    def on_encoded_video_frame(self, uid, image_buffer, length, info):
        parent = self.parent
        parent.remote_uid = str(uid)
        codec = int(getattr(info, "codec_type", 0) or 0)
        if codec != 3:
            if not parent._warned_non_hevc:
                parent._warned_non_hevc = True
                logging.warning("Ignoring non-HEVC frame from Agora (codec=%s)", codec)
            return

        frame_type = int(getattr(info, "frame_type", 0) or 0)
        if parent.waiting_for_keyframe:
            if frame_type not in (1, 3):
                return
            parent.waiting_for_keyframe = False
            logging.info("Keyframe received (frame_type=%s)", frame_type)

        if not parent._ensure_ffmpeg_started():
            return

        now = time.time()
        parent.frames_seen += 1
        parent.last_frame_ts = now
        if parent.first_frame_ts == 0.0:
            parent.first_frame_ts = now
            logging.info("First encoded frame received from Agora uid=%s codec=H265", uid)
        parent._touch_heartbeat(now)

        try:
            parent.frame_queue.put_nowait(image_buffer)
        except queue.Full:
            parent.frames_dropped += 1


class AgoraToRtsp:
    # Mirror the Mammotion HA integration's peer-recovery timing (see
    # custom_components/mammotion/agora_websocket.py: PEER_REJOIN_DEBOUNCE_SECS
    # and PEER_RECOVER_COOLDOWN_SECS). 2s debounce lets a naturally fast rejoin
    # happen without a wake-up poke; 15s cooldown prevents thrash if the device
    # is genuinely unreachable.
    PEER_REJOIN_DEBOUNCE_SECS = 2.0
    PEER_RECOVER_COOLDOWN_SECS = 15.0

    def __init__(self, rtsp_url: str, area_code: int, heartbeat_file: str = "") -> None:
        self.rtsp_url = rtsp_url
        self.area_code = area_code
        self.heartbeat_file = heartbeat_file

        self.connected_event = threading.Event()
        self.stop_event = threading.Event()
        self.frame_queue: queue.Queue[bytes] = queue.Queue(maxsize=512)

        self.frames_seen = 0
        self.frames_dropped = 0
        self.first_frame_ts = 0.0
        self.last_frame_ts = 0.0
        self.connected_at_ts = 0.0
        self.remote_uid: str | None = None
        self.peer_online = False
        # Wall-clock time the publisher went offline (0 = currently online).
        # Drives the "publisher gone too long" watchdog so a dead cloud session
        # (failed wake-ups) actually forces a cycle restart instead of hanging.
        self.peer_offline_since = 0.0
        self.waiting_for_keyframe = True
        self._last_keyframe_request_ts = 0.0
        self._last_heartbeat_write_ts = 0.0
        self._warned_non_hevc = False

        self._ffmpeg: subprocess.Popen[bytes] | None = None
        self._ffmpeg_lock = threading.Lock()
        self._writer_thread: threading.Thread | None = None

        self.service = None
        self.connection = None
        self._video_sub_options_cls = None

        # Set via configure_recovery() before start() — needed so the recovery
        # task can call back into pymammotion from the Agora SDK thread.
        self._mammotion: Any = None
        self._device_name: str = ""
        self._iot_id: str = ""
        self._loop: asyncio.AbstractEventLoop | None = None
        self._recovery_future: Any = None
        self._last_recovery_ts = 0.0

        self._conn_observer = _ConnectionObserver(self)
        self._local_user_observer = _LocalUserObserver(self)
        self._encoded_observer = _EncodedFrameObserver(self)

    def configure_recovery(
        self,
        mammotion: Any,
        device_name: str,
        iot_id: str,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Wire up the bits needed to recover the publisher when it drops."""
        self._mammotion = mammotion
        self._device_name = device_name
        self._iot_id = iot_id
        self._loop = loop

    def _schedule_recovery(self) -> None:
        """Called from Agora SDK thread on user_left/user_offline."""
        loop = self._loop
        if loop is None or loop.is_closed() or self._mammotion is None:
            return
        try:
            fut = asyncio.run_coroutine_threadsafe(self._recover(), loop)
        except RuntimeError:
            return
        self._recovery_future = fut

    async def _recover(self) -> None:
        await asyncio.sleep(self.PEER_REJOIN_DEBOUNCE_SECS)

        if self.peer_online:
            # The mower rejoined under its own steam within the debounce window.
            return
        if self.stop_event.is_set():
            return

        now = time.monotonic()
        if now - self._last_recovery_ts < self.PEER_RECOVER_COOLDOWN_SECS:
            logging.debug("Recovery suppressed (cooldown)")
            return
        self._last_recovery_ts = now

        logging.info(
            "Publisher still gone after %.0fs -- sending wake-up command",
            self.PEER_REJOIN_DEBOUNCE_SECS,
        )
        try:
            await self._mammotion.send_command_with_args(
                self._device_name, "send_todev_ble_sync", sync_type=3
            )
        except Exception:
            logging.exception("Wake-up command failed")
        try:
            await self._mammotion.get_stream_subscription(
                self._device_name, self._iot_id
            )
        except Exception:
            logging.exception("Stream subscription refresh failed")

    def _touch_heartbeat(self, now: float) -> None:
        if not self.heartbeat_file or now - self._last_heartbeat_write_ts < 2.0:
            return
        try:
            with open(self.heartbeat_file, "w", encoding="utf-8") as f:
                f.write(str(int(now)))
            self._last_heartbeat_write_ts = now
        except Exception:
            logging.exception("Failed to update heartbeat file: %s", self.heartbeat_file)

    def request_keyframe(self, reason: str = "") -> bool:
        if self.connection is None or self.remote_uid is None:
            return False
        local_user = self.connection.get_local_user()
        try:
            if hasattr(local_user, "send_intra_request"):
                local_user.send_intra_request(self.remote_uid)
            elif hasattr(local_user, "_send_intra_request"):
                local_user._send_intra_request(self.remote_uid)
            else:
                return False
        except Exception:
            logging.exception("Failed to request intra frame from user=%s", self.remote_uid)
            return False
        self.waiting_for_keyframe = True
        self._last_keyframe_request_ts = time.time()
        logging.warning(
            "Requested intra frame from uid=%s%s",
            self.remote_uid,
            f" (reason={reason})" if reason else "",
        )
        return True

    def _ensure_ffmpeg_started(self) -> bool:
        with self._ffmpeg_lock:
            if self._ffmpeg is None:
                self._start_ffmpeg_locked()
            return self._ffmpeg is not None

    def _start_ffmpeg_locked(self) -> None:
        # Transcode HEVC -> constant 10fps H.264. Smoother playback than
        # passing Mammotion's bursty variable-rate stream through with copy.
        # This is the exact config that produced a healthy H.264 stream
        # earlier; the long-term wedging we saw was a dead cloud session +
        # watchdog bug (fixed in main()), not this transcode. No -err_detect
        # / -ec flags here — those broke ffmpeg startup. Frigate consumers
        # must use an h264 hwaccel preset (preset-intel-qsv-h264 on Intel).
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-fflags",
            "+genpts+discardcorrupt",
            "-use_wallclock_as_timestamps",
            "1",
            "-f",
            "hevc",
            "-i",
            "pipe:0",
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-pix_fmt",
            "yuv420p",
            "-r",
            "10",
            "-vsync",
            "cfr",
            "-g",
            "20",
            "-bf",
            "0",
            "-f",
            "rtsp",
            "-rtsp_transport",
            "tcp",
            "-muxdelay",
            "0",
            "-muxpreload",
            "0",
            self.rtsp_url,
        ]
        logging.info("Starting ffmpeg -> %s", self.rtsp_url)
        try:
            self._ffmpeg = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        except FileNotFoundError:
            logging.error("ffmpeg binary not found in PATH")
            return
        self._writer_thread = threading.Thread(
            target=self._writer_loop, name="ffmpeg-writer", daemon=True
        )
        self._writer_thread.start()

    def _writer_loop(self) -> None:
        while not self.stop_event.is_set():
            # Proactive subprocess health check. Without this, if ffmpeg exits
            # while frames aren't currently flowing (e.g. during the ~3s
            # peer-recovery gap), we wouldn't detect it until the next write —
            # by which point go2rtc has been serving 404s for a long time.
            with self._ffmpeg_lock:
                ff = self._ffmpeg
            if ff is not None and ff.poll() is not None:
                logging.warning(
                    "ffmpeg subprocess exited (returncode=%s); will restart on next keyframe",
                    ff.returncode,
                )
                with self._ffmpeg_lock:
                    self._cleanup_ffmpeg_locked()
                self.waiting_for_keyframe = True
                while True:
                    try:
                        self.frame_queue.get_nowait()
                    except queue.Empty:
                        break
                # Ask Mammotion for an immediate keyframe so we restart fast
                # instead of waiting for the next natural one (~5s).
                self.request_keyframe(reason="ffmpeg_exited")
                continue

            try:
                chunk = self.frame_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            stdin = ff.stdin if ff is not None else None
            if stdin is None:
                continue
            try:
                stdin.write(chunk)
                stdin.flush()
            except BrokenPipeError:
                logging.warning("ffmpeg stdin broken; will restart on next keyframe")
                with self._ffmpeg_lock:
                    self._cleanup_ffmpeg_locked()
                self.waiting_for_keyframe = True
                while True:
                    try:
                        self.frame_queue.get_nowait()
                    except queue.Empty:
                        break

    def _cleanup_ffmpeg_locked(self) -> None:
        if self._ffmpeg is None:
            return
        try:
            if self._ffmpeg.stdin:
                self._ffmpeg.stdin.close()
        except Exception:
            pass
        try:
            self._ffmpeg.terminate()
        except Exception:
            pass
        self._ffmpeg = None

    def start(self, appid: str, channel: str, token: str, uid: str) -> None:
        from agora.rtc.agora_base import (
            AgoraServiceConfig,
            ChannelProfileType,
            ClientRoleType,
            RTCConnConfig,
            RtcConnectionPublishConfig,
            VideoSubscriptionOptions,
        )
        from agora.rtc.agora_service import AgoraService

        self._video_sub_options_cls = VideoSubscriptionOptions
        self.service = AgoraService()
        service_config = AgoraServiceConfig(
            appid=appid,
            area_code=self.area_code,
            channel_profile=ChannelProfileType.CHANNEL_PROFILE_LIVE_BROADCASTING,
            enable_video=1,
            # Audio path must be enabled so Mammotion's publisher considers us a
            # real audience; without it the device stops sending video after
            # ~50s. We never play the audio back, just keep the channel alive.
            enable_audio_device=0,
            enable_audio_processor=1,
            use_string_uid=0,
        )
        ret = self.service.initialize(service_config)
        if ret != 0:
            raise RuntimeError(f"Agora service initialize failed: {ret}")

        conn_config = RTCConnConfig(
            auto_subscribe_audio=1,
            auto_subscribe_video=1,
            enable_audio_recording_or_playout=0,
            client_role_type=ClientRoleType.CLIENT_ROLE_AUDIENCE,
            channel_profile=ChannelProfileType.CHANNEL_PROFILE_LIVE_BROADCASTING,
        )
        publish_config = RtcConnectionPublishConfig(
            is_publish_audio=False, is_publish_video=False
        )
        self.connection = self.service.create_rtc_connection(conn_config, publish_config)
        if self.connection is None:
            raise RuntimeError("Failed to create RTC connection")

        self.connection.register_observer(self._conn_observer)
        self.connection.register_local_user_observer(self._local_user_observer)
        self.connection.register_video_encoded_frame_observer(self._encoded_observer)

        ret = self.connection.connect(token, channel, uid)
        if ret != 0:
            raise RuntimeError(f"Agora connect failed: {ret}")

    def renew_token(self, token: str) -> None:
        if self.connection is None:
            return
        ret = self.connection.renew_token(token)
        logging.info("renew_token -> %s", ret)

    def stop(self) -> None:
        self.stop_event.set()
        if self.connection is not None:
            try:
                self.connection.disconnect()
            except Exception:
                logging.exception("Error while disconnecting Agora connection")
            try:
                self.connection.release()
            except Exception:
                logging.exception("Error while releasing Agora connection")
            self.connection = None
        if self.service is not None:
            try:
                self.service.release()
            except Exception:
                logging.exception("Error while releasing Agora service")
            self.service = None
        with self._ffmpeg_lock:
            self._cleanup_ffmpeg_locked()


async def fetch_stream_fields(mammotion: Any, device_name: str) -> dict[str, Any]:
    selected_name = (device_name or "").strip()
    if not selected_name or selected_name.lower() == "first":
        all_devices = mammotion.device_registry.all_devices
        if not all_devices:
            raise RuntimeError("No devices found in Mammotion account")
        device_handle = all_devices[0]
        selected_name = device_handle.device_name
        logging.info("Auto-selected first device: %s", selected_name)
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


async def main() -> None:
    settings = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    logging.info("Loading Mammotion SDK modules")
    from pymammotion.client import MammotionClient

    stop_async = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_async.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_async.set())

    async def _fresh_client() -> "Any | None":
        # A *new* client + full username/password login every cycle. The
        # pymammotion refresh token goes stale after a few hours
        # ("refreshToken invalid!!"); reusing the same client then leaves us
        # with a dead cloud session that can't send wake-up commands. Starting
        # fresh sidesteps the dead refresh token entirely.
        while not stop_async.is_set():
            client = MammotionClient(ha_version="3.4.23")
            try:
                logging.info("Logging in to Mammotion cloud")
                await client.login_and_initiate_cloud(
                    settings.mammotion_email, settings.mammotion_password
                )
                return client
            except Exception:
                logging.exception(
                    "Mammotion login failed; retrying in %ss",
                    settings.reconnect_backoff_seconds,
                )
                try:
                    await client.stop()
                except Exception:
                    pass
                await asyncio.sleep(settings.reconnect_backoff_seconds)
        return None

    while not stop_async.is_set():
        bridge: AgoraToRtsp | None = None
        mammotion: Any = None
        try:
            mammotion = await _fresh_client()
            if mammotion is None:
                break  # stop requested during login

            fields = await fetch_stream_fields(mammotion, settings.mammotion_device_name)
            if settings.dump_stream_json:
                with open("agora_stream.json", "w", encoding="utf-8") as f:
                    json.dump(fields, f, indent=2, ensure_ascii=False)
                logging.info("Saved stream subscription to agora_stream.json")

            for key in ("appid", "channelName", "token", "uid"):
                if not fields.get(key):
                    raise RuntimeError(f"Missing {key} in stream subscription payload")

            bridge = AgoraToRtsp(
                rtsp_url=settings.rtsp_publish_url,
                area_code=resolve_area_code(fields.get("areaCode")),
                heartbeat_file=settings.heartbeat_file,
            )
            bridge.configure_recovery(
                mammotion=mammotion,
                device_name=fields["device_name"],
                iot_id=fields["iot_id"],
                loop=loop,
            )
            bridge.start(
                appid=str(fields["appid"]),
                channel=str(fields["channelName"]),
                token=str(fields["token"]),
                uid=str(fields["uid"]),
            )

            if not bridge.connected_event.wait(timeout=25):
                raise RuntimeError("Timed out waiting for Agora connection")

            logging.info("Bridge active. Publishing to %s", settings.rtsp_publish_url)

            next_refresh = (
                time.time() + settings.refresh_seconds
                if settings.refresh_seconds > 0
                else None
            )
            # Proactive keep-alive. The Mammotion app sends
            # send_todev_ble_sync(sync_type=2) every ~20s to tell the device a
            # viewer is present; without it the device leaves the Agora channel
            # after ~50s. Sending it ourselves should keep the publisher online
            # continuously instead of relying on reactive recovery after it drops.
            keepalive_interval = 20.0
            next_keepalive = time.time() + keepalive_interval
            while not stop_async.is_set() and not bridge.stop_event.is_set():
                await asyncio.sleep(1.0)
                now = time.time()

                if now >= next_keepalive:
                    try:
                        await mammotion.send_command_with_args(
                            fields["device_name"], "send_todev_ble_sync", sync_type=2
                        )
                    except Exception:
                        logging.debug("Keep-alive sync failed", exc_info=True)
                    next_keepalive = now + keepalive_interval

                if next_refresh is not None and now >= next_refresh:
                    refreshed = await mammotion.refresh_stream_subscription(
                        settings.mammotion_device_name, fields["iot_id"]
                    )
                    data = getattr(refreshed, "data", None)
                    if not data or not getattr(data, "token", None):
                        raise RuntimeError("Token refresh response missing token")
                    bridge.renew_token(str(getattr(data, "token")))
                    next_refresh = now + settings.refresh_seconds

                if (
                    bridge.connected_at_ts > 0
                    and bridge.first_frame_ts == 0
                    and now - bridge.connected_at_ts
                    > settings.startup_frame_timeout_seconds
                ):
                    raise RuntimeError("No first frame received after startup timeout")

                if bridge.last_frame_ts > 0 and bridge.peer_online:
                    # While peer_online is False the bridge is mid-recovery
                    # (publisher dropped, wake-up scheduled) — silence the
                    # stall watchdog instead of restarting the whole cycle.
                    stall_age = now - bridge.last_frame_ts
                    if (
                        stall_age > settings.soft_stall_timeout_seconds
                        and now - bridge._last_keyframe_request_ts
                        >= settings.keyframe_request_cooldown_seconds
                    ):
                        bridge.request_keyframe(reason=f"stall_{int(stall_age)}s")
                    if stall_age > settings.frame_stall_timeout_seconds:
                        raise RuntimeError("Frame stream stalled")

                # Publisher-gone-too-long watchdog. Fires even while
                # peer_online is False, so a dead cloud session (recovery
                # wake-ups failing) forces a full cycle restart — which
                # re-logins with a fresh MammotionClient next iteration.
                if (
                    not bridge.peer_online
                    and bridge.peer_offline_since > 0
                    and now - bridge.peer_offline_since
                    > settings.frame_stall_timeout_seconds
                ):
                    raise RuntimeError(
                        "Publisher gone too long (likely dead cloud session); "
                        "restarting cycle"
                    )
        except Exception:
            if stop_async.is_set():
                break
            logging.exception(
                "Bridge cycle failed; reconnecting in %ss",
                settings.reconnect_backoff_seconds,
            )
            await asyncio.sleep(settings.reconnect_backoff_seconds)
        finally:
            if bridge is not None:
                logging.info(
                    "Cycle stopping. Frames seen=%s dropped=%s",
                    bridge.frames_seen,
                    bridge.frames_dropped,
                )
                bridge.stop()
            # Tear down the per-cycle cloud client so the next cycle logs in
            # fresh (new tokens) rather than inheriting a dead session.
            if mammotion is not None:
                try:
                    await mammotion.stop()
                except Exception:
                    logging.exception("Mammotion stop failed")
                mammotion = None


if __name__ == "__main__":
    asyncio.run(main())
