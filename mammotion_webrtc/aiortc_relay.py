"""Agora → RTSP H265 passthrough relay.

Owns one upstream aiortc ``RTCPeerConnection`` that subscribes to the
mower's Agora channel as a passive viewer, and forwards every received
H265 RTP packet to an embedded :class:`Go2RtcRtspStream` that go2rtc
consumes via ``rtsp://``. Path:

    Mower → Agora SFU → aiortc upstream PC → RTP tap → RTSP → go2rtc

The aiortc codec stack is monkey-patched in :mod:`.h265_patch` so the SDP
offer/answer can carry H265 PT 100 and the receiver does not drop packets
or crash its decoder thread. Once the patches are in place aiortc is happy
to ferry the H265 bytes through SRTP and out the other side; we never
decode anything.

Lifecycle:

* Eager start: ``await start()`` kicks off the supervisor task. Frigate is
  always-on, so a lazy/idle-grace teardown adds no value and risks the
  upstream not being ready when the first viewer asks.
* Self-healing: the supervisor reconnects on upstream failure with a
  bounded backoff. Each retry refetches credentials via the provider so
  expired Agora tokens are handled implicitly.
* ``await stop()`` tears down both the PC and the RTSP server cleanly.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time

from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.mediastreams import MediaStreamTrack
from aiortc.rtp import RtpPacket, unwrap_rtx

from . import h265_patch
from .agora_edge import AgoraWebSocketHandler
from .agora_session import (
    AgoraContextProvider,
    CredentialsProvider,
    PublisherWakeup,
)
from .rtsp_server import Go2RtcRtspStream

# Apply the H265 patches once at import time. The bridge entrypoint imports
# this module before constructing any RTCPeerConnection, so this is safe.
h265_patch.apply()

LOGGER = logging.getLogger(__name__)


class AgoraToRtspRelay:
    """Bridge from Agora WebRTC to a local RTSP server.

    One instance owns one upstream PC and one RTSP server. The RTSP server
    is shared across all go2rtc consumers (typically just one — go2rtc
    multiplexes its own viewers internally).
    """

    def __init__(
        self,
        *,
        credentials_provider: CredentialsProvider,
        agora_context_provider: AgoraContextProvider,
        rtsp_server: Go2RtcRtspStream,
        publisher_wakeup: PublisherWakeup = None,
        cheap_recovery: PublisherWakeup = None,
        upstream_track_timeout_seconds: float = 15.0,
        reconnect_backoff_seconds: float = 5.0,
        max_reconnect_backoff_seconds: float = 60.0,
        no_rtp_watchdog_seconds: float = 30.0,
        cheap_recovery_wait_seconds: float = 10.0,
    ) -> None:
        self._credentials_provider = credentials_provider
        self._agora_context_provider = agora_context_provider
        self._rtsp_server = rtsp_server
        self._publisher_wakeup = publisher_wakeup
        # Best-effort "nudge the publisher without tearing down the upstream"
        # callback. The watchdog fires this first; only if RTP doesn't resume
        # within cheap_recovery_wait_seconds do we escalate to full teardown.
        self._cheap_recovery = cheap_recovery
        self._upstream_track_timeout = upstream_track_timeout_seconds
        self._reconnect_backoff = reconnect_backoff_seconds
        self._max_reconnect_backoff = max_reconnect_backoff_seconds
        self._no_rtp_watchdog = no_rtp_watchdog_seconds
        self._cheap_recovery_wait = cheap_recovery_wait_seconds

        self._stop_event = asyncio.Event()
        self._upstream_pc: RTCPeerConnection | None = None
        self._upstream_handler: AgoraWebSocketHandler | None = None
        self._supervisor_task: asyncio.Task[None] | None = None
        # The receiver associated with the inbound video track. Cached so
        # the RTSP server can ask us to PLI upstream when a new client joins.
        self._video_receiver = None
        # SSRC of the inbound H265 stream — needed for the PLI we send on
        # behalf of new RTSP clients. None until the first packet arrives.
        self._video_ssrc: int | None = None
        # Set when the upstream PC reaches "connected"; cleared on teardown.
        self._upstream_ready = asyncio.Event()
        # Monotonic-ns timestamp of the last forwarded H265 RTP packet. Used
        # by the no-RTP watchdog to detect a publisher that has gone silent
        # while the ICE/DTLS connection is still nominally healthy.
        self._last_rtp_ns: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._supervisor_task is not None and not self._supervisor_task.done():
            return
        self._stop_event.clear()
        self._supervisor_task = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._supervisor_task is not None:
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except (asyncio.CancelledError, Exception):
                pass
            self._supervisor_task = None
        await self._teardown_upstream()

    async def request_keyframe(self) -> None:
        """Send a PLI to Agora asking for an IDR.

        Called by the RTSP server when a new client connects, so the viewer
        does not have to wait for the next naturally-scheduled keyframe.
        Silently no-ops if upstream is not ready or the SSRC is unknown.
        """
        receiver = self._video_receiver
        ssrc = self._video_ssrc
        if receiver is None or ssrc is None:
            return
        try:
            await receiver._send_rtcp_pli(ssrc)
        except Exception:  # noqa: BLE001
            LOGGER.debug("PLI to upstream failed", exc_info=True)

    # ------------------------------------------------------------------
    # Supervisor loop — reconnect-with-backoff
    # ------------------------------------------------------------------

    async def _supervise(self) -> None:
        backoff = self._reconnect_backoff
        while not self._stop_event.is_set():
            try:
                await self._run_one_upstream()
                # Clean exit (upstream disconnected on its own). Reset backoff
                # so a steady-state churn does not push retry delay up.
                backoff = self._reconnect_backoff
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "Upstream connection failed; will retry in %.1fs", backoff
                )
            finally:
                await self._teardown_upstream()

            if self._stop_event.is_set():
                return
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                # If we got here without timing out, stop was requested.
                return
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, self._max_reconnect_backoff)

    async def _run_one_upstream(self) -> None:
        """Bring up one Agora subscription and pump RTP until it dies."""
        LOGGER.info("Starting upstream Agora subscription")

        if self._publisher_wakeup is not None:
            try:
                await self._publisher_wakeup()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Publisher wakeup failed; continuing anyway")

        credentials = await self._credentials_provider()
        agora_response = await self._agora_context_provider(credentials)

        pc = RTCPeerConnection()
        pc.addTransceiver("video", direction="recvonly")
        pc.addTransceiver("audio", direction="recvonly")

        # Per-attempt state for the track and connection-state events.
        track_received = asyncio.Event()
        connection_failed = asyncio.Event()
        video_track_ref: dict[str, MediaStreamTrack] = {}

        @pc.on("track")
        def _on_track(track: MediaStreamTrack) -> None:
            LOGGER.info(
                "Upstream aiortc track received kind=%s id=%s", track.kind, track.id
            )
            if track.kind != "video" or "video" in video_track_ref:
                return
            video_track_ref["video"] = track
            self._install_rtp_tap(pc, track)
            track_received.set()

        @pc.on("connectionstatechange")
        def _on_state() -> None:
            state = pc.connectionState
            LOGGER.info("Upstream aiortc connectionState=%s", state)
            if state == "connected":
                self._upstream_ready.set()
            elif state in ("failed", "closed", "disconnected"):
                self._upstream_ready.clear()
                connection_failed.set()

        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)

        handler = AgoraWebSocketHandler(
            prefer_instant_video=True,
            subscribe_retry_delay=1.0,
            subscribe_retry_attempts=3,
            declare_remote_video_ssrc=True,
        )
        session_id = secrets.token_hex(16)

        # Track these so _teardown_upstream can clean them up even if we
        # bail out partway through this method.
        self._upstream_pc = pc
        self._upstream_handler = handler

        answer_sdp = await handler.connect_and_join(
            live_feed=credentials.to_agora_credentials(),
            offer_sdp=pc.localDescription.sdp,
            session_id=session_id,
            app_id=credentials.app_id,
            agora_response=agora_response,
        )
        if not answer_sdp:
            raise RuntimeError("Agora signaling returned no answer SDP")

        await pc.setRemoteDescription(
            RTCSessionDescription(sdp=answer_sdp, type="answer")
        )

        try:
            await asyncio.wait_for(
                track_received.wait(), timeout=self._upstream_track_timeout
            )
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"Upstream video track did not arrive within "
                f"{self._upstream_track_timeout:.1f}s"
            )

        LOGGER.info("Upstream ready (session=%s); pumping RTP to RTSP server", session_id)

        # Reset the watchdog clock at the moment we're ready to receive. Some
        # initial silence is normal while the publisher uploads parameter sets
        # and the first IDR; the watchdog only fires if the silence persists.
        self._last_rtp_ns = time.monotonic_ns()

        async def _no_rtp_watchdog() -> None:
            # Poll twice per window so the worst-case detection latency is
            # ~0.5 * window and we don't spin too aggressively.
            interval = max(1.0, self._no_rtp_watchdog / 2)
            threshold_ns = int(self._no_rtp_watchdog * 1e9)
            tried_cheap_recovery = False
            while True:
                await asyncio.sleep(interval)
                idle_ns = time.monotonic_ns() - self._last_rtp_ns
                if idle_ns < threshold_ns:
                    # Stream healthy; if we'd previously tried a cheap
                    # recovery, reset the flag so a future stall gets one too.
                    tried_cheap_recovery = False
                    continue

                # Stalled. First try a cheap recovery (refresh_fpv) — one
                # MQTT message vs. tearing down the upstream + new Agora
                # session + fresh credentials. Only escalate if that doesn't
                # restore RTP within cheap_recovery_wait_seconds.
                if (
                    not tried_cheap_recovery
                    and self._cheap_recovery is not None
                ):
                    LOGGER.info(
                        "No upstream RTP for %.1fs; trying cheap recovery "
                        "(refresh_fpv) before teardown",
                        idle_ns / 1e9,
                    )
                    tried_cheap_recovery = True
                    try:
                        await self._cheap_recovery()
                    except Exception:  # noqa: BLE001
                        LOGGER.debug("cheap recovery raised", exc_info=True)
                    # Give the mower a chance to react. Check RTP after the
                    # configured wait — if it resumed, the next loop iter
                    # sees idle_ns under threshold and resets the flag.
                    await asyncio.sleep(self._cheap_recovery_wait)
                    if time.monotonic_ns() - self._last_rtp_ns < threshold_ns:
                        LOGGER.info("Cheap recovery worked — RTP resumed")
                        tried_cheap_recovery = False
                        continue
                    LOGGER.warning(
                        "Cheap recovery didn't restore RTP; escalating to "
                        "full teardown"
                    )

                LOGGER.warning(
                    "No upstream RTP for %.1fs (publisher likely stalled); "
                    "tearing down to force reconnect",
                    (time.monotonic_ns() - self._last_rtp_ns) / 1e9,
                )
                connection_failed.set()
                return

        # Hold until the connection dies, the watchdog fires, or we are asked
        # to stop. The actual RTP forwarding happens in the receiver-hook
        # coroutine; this task just supervises.
        stop_wait = asyncio.create_task(self._stop_event.wait())
        failed_wait = asyncio.create_task(connection_failed.wait())
        watchdog_task = asyncio.create_task(_no_rtp_watchdog())
        try:
            await asyncio.wait(
                {stop_wait, failed_wait, watchdog_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (stop_wait, failed_wait, watchdog_task):
                if not task.done():
                    task.cancel()

    # ------------------------------------------------------------------
    # RTP tap — the actual passthrough
    # ------------------------------------------------------------------

    def _install_rtp_tap(self, pc: RTCPeerConnection, track: MediaStreamTrack) -> None:
        """Hook the receiver's RTP handler so each H265 packet goes to RTSP.

        We wrap ``receiver._handle_rtp_packet`` rather than tapping deeper
        because that point lets us still call the original handler — which
        keeps RTCP RR / NACK / PLI flowing back to Agora. Without those,
        Agora's edge eventually decides the subscription is stale and
        either stops sending or drops the session.

        The original handler is left in charge of RTX unwrapping; we mirror
        that work locally so our tap sees the recovered packet too. Losing
        RTX recovery would translate to visible glitches on any path with
        non-trivial loss.
        """
        receiver = None
        for transceiver in pc.getTransceivers():
            if transceiver.receiver is not None and transceiver.receiver.track is track:
                receiver = transceiver.receiver
                break
        if receiver is None:
            LOGGER.error("Could not locate RTCRtpReceiver for video track")
            return

        self._video_receiver = receiver
        original_handler = receiver._handle_rtp_packet
        rtsp_server = self._rtsp_server

        # Pull the receiver's private name-mangled attributes once. These
        # are stable across aiortc 1.13.x — if a future version reorganises
        # them, the patch reapplication step in h265_patch.py is where we
        # would notice.
        codecs_by_pt = receiver._RTCRtpReceiver__codecs  # noqa: SLF001
        rtx_ssrc_map = receiver._RTCRtpReceiver__rtx_ssrc  # noqa: SLF001

        relay_self = self

        async def tap(packet: RtpPacket, arrival_time_ms: int) -> None:
            try:
                effective = _unwrap_rtx_if_needed(packet, codecs_by_pt, rtx_ssrc_map)
                codec = codecs_by_pt.get(effective.payload_type)
                if codec is not None and codec.mimeType.lower() in h265_patch.H265_MIMETYPES:
                    ssrc_just_learned = relay_self._video_ssrc is None
                    if ssrc_just_learned:
                        relay_self._video_ssrc = effective.ssrc
                    if effective.payload:
                        relay_self._last_rtp_ns = time.monotonic_ns()
                        rtsp_server.push_rtp(
                            payload=bytes(effective.payload),
                            timestamp=effective.timestamp,
                            marker=bool(effective.marker),
                        )
                    if ssrc_just_learned:
                        # First H265 packet of a new upstream session. The
                        # mower is mid-GOP and we got handed P-frames — no
                        # parameter sets in them, so RTSP DESCRIBE would
                        # block its 10 s wait and go2rtc would time out at
                        # 5 s. Fire one PLI immediately to force the mower
                        # to emit a fresh IDR (which prepends VPS/SPS/PPS).
                        # Detached task so the tap stays non-blocking.
                        LOGGER.info(
                            "First H265 RTP received (ssrc=%d); requesting IDR",
                            effective.ssrc,
                        )
                        asyncio.create_task(relay_self.request_keyframe())
            except Exception:  # noqa: BLE001
                LOGGER.exception("RTP tap failed; dropping packet")
            await original_handler(packet, arrival_time_ms=arrival_time_ms)

        receiver._handle_rtp_packet = tap

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    async def _teardown_upstream(self) -> None:
        handler = self._upstream_handler
        pc = self._upstream_pc
        self._upstream_handler = None
        self._upstream_pc = None
        self._video_receiver = None
        self._video_ssrc = None
        self._upstream_ready.clear()

        if handler is not None:
            try:
                await handler.disconnect()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Agora handler disconnect failed")
        if pc is not None:
            try:
                await pc.close()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Upstream PC close failed")


def _unwrap_rtx_if_needed(
    packet: RtpPacket,
    codecs_by_pt: dict,
    rtx_ssrc_map: dict,
) -> RtpPacket:
    """Mirror :meth:`RTCRtpReceiver._handle_rtp_packet`'s RTX unwrap.

    Returns the original packet unchanged if it is not RTX or if any input
    needed for unwrap is missing. The receiver's own copy of this logic
    still runs (we did not disturb the original handler), so this is purely
    for our tap.
    """
    codec = codecs_by_pt.get(packet.payload_type)
    if codec is None or codec.name.lower() != "rtx":
        return packet
    original_ssrc = rtx_ssrc_map.get(packet.ssrc)
    apt = codec.parameters.get("apt")
    if (
        original_ssrc is None
        or not isinstance(apt, int)
        or apt not in codecs_by_pt
        or len(packet.payload) < 2
    ):
        return packet
    try:
        return unwrap_rtx(packet, payload_type=apt, ssrc=original_ssrc)
    except Exception:  # noqa: BLE001
        return packet
