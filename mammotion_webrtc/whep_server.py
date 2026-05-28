"""Standalone WHEP server brokering go2rtc's SDP offer to an Agora answer.

Port of the PetKit HA integration's ``whep_proxy.py`` upstream half plus the
Agora-context refresh that lived in PetKit's ``camera.py``
(``_refresh_agora_context`` / ``_filter_candidates``). The PetKit
``HomeAssistantView`` classes are replaced with plain ``aiohttp.web`` handlers.

Flow per WHEP request (``POST /whep/{stream}``):

1. go2rtc POSTs its SDP offer (``application/sdp``).
2. We obtain fresh Agora credentials (appid/channel/rtc_token/uid) via the
   injected :class:`StreamCredentialsProvider` (backed by pymammotion).
3. We call Agora ``choose_server`` to get edge gateways + TURN.
4. :class:`AgoraWebSocketHandler` joins ``join_v3`` as audience, subscribes to
   the publisher's H.265 video SSRC, and synthesizes an SDP answer pointing at
   the Agora edge.
5. We return ``201`` with the answer (``application/sdp``) + a ``Location``
   header. PATCH (trickle ICE) / DELETE on the session resource are supported.

Differences from PetKit, all because Mammotion is not PetKit:

* No HA auth wrapper. An optional static bearer token (env ``MAMMOTION_WHEP_TOKEN``)
  can gate requests; by default the server is open because it is meant to be
  reachable only by the co-located go2rtc.
* No second "proxy to internal go2rtc stream" manager. In this design go2rtc
  dials *our* WHEP endpoint directly, so the upstream session IS the public
  session.
* No RTM (``AgoraRTMSignaling``). Mammotion has no rtm_token; the publisher is
  kept alive over MQTT by the entrypoint, not RTM start_live/heartbeat.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import aiohttp
from aiohttp import web

from .agora_edge import (
    AgoraAPIClient,
    AgoraCredentials,
    AgoraDuplicateJoinError,
    AgoraResponse,
    AgoraWebSocketHandler,
    RTCIceCandidateInit,
    SERVICE_IDS,
)

LOGGER = logging.getLogger(__name__)

# PetKit refreshed RTM tokens every 20 minutes; we keep the same cadence for
# the RTC token renewal (renew_token is triggered by Agora's expiry events, but
# this provides a proactive backstop).
TOKEN_REFRESH_INTERVAL_SECONDS = 20 * 60
MAX_JOIN_ATTEMPTS = 2
DEFAULT_MIN_SESSION_LIFETIME_SECONDS = 30.0
DEFAULT_RTP_TIMEOUT_SECONDS = 20.0
DEFAULT_KEEPALIVE_SECONDS = 60.0


@dataclass
class StreamCredentials:
    """Agora RTC credentials for one Mammotion stream subscription.

    Mirrors the subset of pymammotion ``get_stream_subscription`` data the
    Agora join flow needs (``fetch_stream_fields`` in the main-branch bridge
    returns these same keys).
    """

    app_id: str
    channel: str
    rtc_token: str
    uid: int
    # Mammotion device/IoT ID for lifecycle logging and duplicate-session traces.
    device_id: str = ""
    # "CN,GLOBAL"-style area code string accepted by choose_server. Defaults to
    # global; the entrypoint maps Mammotion's areaCode if it can.
    area_code: str = "CN,GLOBAL"

    def to_agora_credentials(self) -> AgoraCredentials:
        """Adapt to the join-flow credential object."""
        return AgoraCredentials(
            rtc_token=self.rtc_token,
            channel_id=self.channel,
            uid=self.uid,
            app_id=self.app_id,
        )


# A provider returns fresh credentials each time it is awaited. The entrypoint
# wires this to pymammotion's get_stream_subscription so each new WHEP session
# starts with a current token.
StreamCredentialsProvider = Callable[[], Awaitable[StreamCredentials]]


def filter_agora_candidates(
    candidates: list[RTCIceCandidateInit],
    agora_response: AgoraResponse,
) -> list[RTCIceCandidateInit]:
    """Prefer relay/srflx candidates and drop host candidates.

    Ported from PetKit ``camera.py:_filter_candidates``.
    """
    valid_ips = {addr.ip for addr in (agora_response.get_turn_addresses() or [])}

    def is_valid(cand: str) -> bool:
        if "typ srflx" in cand or "typ prflx" in cand:
            return True
        if "typ relay" in cand:
            return not valid_ips or any(ip in cand for ip in valid_ips)
        return False

    filtered = [c for c in candidates if is_valid(c.candidate or "")]
    return filtered or candidates


async def refresh_agora_context(credentials: StreamCredentials) -> AgoraResponse:
    """Fetch Agora gateway + TURN endpoints.

    Ported from PetKit ``camera.py:_refresh_agora_context`` (which called
    ``AgoraAPIClient.choose_server``). The PetKit app id constant is replaced
    by ``credentials.app_id`` from the Mammotion stream subscription.
    """
    async with AgoraAPIClient() as agora_client:
        return await agora_client.choose_server(
            app_id=credentials.app_id,
            token=credentials.rtc_token,
            channel_name=credentials.channel,
            user_id=int(credentials.uid),
            area_code=credentials.area_code,
            service_flags=[
                SERVICE_IDS["CHOOSE_SERVER"],
                SERVICE_IDS["CLOUD_PROXY_FALLBACK"],
            ],
        )


@dataclass
class AgoraUpstreamSession:
    """One active upstream Agora session for a stream/device."""

    # Public stream name exposed to go2rtc/WHEP.
    stream: str
    # Generated local lifecycle/session identifier.
    session_id: str
    # Agora channel currently joined for this stream.
    channel: str
    # Mammotion device/IoT ID associated with the channel.
    device_id: str
    # Downstream client that triggered setup, if known.
    owner_client_id: str
    agora_handler: AgoraWebSocketHandler
    created_at: float
    refresh_task: asyncio.Task[None] | None = None


@dataclass
class DownstreamClientSession:
    """One downstream go2rtc consumer attached to an upstream session."""

    session_id: str
    owner_client_id: str
    upstream_session_id: str
    created_at: float


@dataclass
class StreamRuntimeState:
    """Per-stream lifecycle state."""

    active_agora_session: AgoraUpstreamSession | None = None
    setup_task: asyncio.Task[AgoraUpstreamSession] | None = None
    cleanup_task: asyncio.Task[None] | None = None
    last_rtp_at: float = 0.0
    last_successful_join_at: float = 0.0
    healthy: bool = False
    unhealthy_reason: str = ""
    connected_clients: dict[str, DownstreamClientSession] = field(default_factory=dict)
    owner_current_session: dict[str, str] = field(default_factory=dict)


class MammotionWhepManager:
    """Manage one direct Agora session per stream for WHEP consumers.

    Port of PetKit ``PetkitAgoraUpstreamManager`` minus the RTM signaling and
    minus HA bookkeeping. Sessions are keyed by stream name (Mammotion exposes
    a single mower stream; the manager still supports several).
    """

    def __init__(
        self,
        credentials_provider: StreamCredentialsProvider,
        *,
        publisher_wakeup: Callable[[], Awaitable[None]] | None = None,
        reconnect_backoff_seconds: float = 3.0,
        video_only: bool = False,
        keep_agora_session_alive: bool = False,
        keepalive_seconds: float = DEFAULT_KEEPALIVE_SECONDS,
        min_session_lifetime_seconds: float = DEFAULT_MIN_SESSION_LIFETIME_SECONDS,
        rtp_timeout_seconds: float = DEFAULT_RTP_TIMEOUT_SECONDS,
    ) -> None:
        """Store the credentials provider and per-stream session state."""
        self._credentials_provider = credentials_provider
        self._publisher_wakeup = publisher_wakeup
        self._lock = asyncio.Lock()
        self._loop = asyncio.get_running_loop()
        self._states: dict[str, StreamRuntimeState] = {}
        self._stream_locks: dict[str, asyncio.Lock] = {}
        self._reconnect_backoff_seconds = reconnect_backoff_seconds
        self._video_only = video_only
        self._keep_agora_session_alive = keep_agora_session_alive
        self._keepalive_seconds = keepalive_seconds
        self._min_session_lifetime_seconds = min_session_lifetime_seconds
        self._rtp_timeout_seconds = rtp_timeout_seconds
        self._retry_after: dict[str, float] = {}

    async def _get_state(self, stream: str) -> StreamRuntimeState:
        """Return/create state for one stream."""
        async with self._lock:
            return self._states.setdefault(stream, StreamRuntimeState())

    async def _get_stream_lock(self, stream: str) -> asyncio.Lock:
        """Return the serialized lifecycle lock for one stream."""
        async with self._lock:
            return self._stream_locks.setdefault(stream, asyncio.Lock())

    @staticmethod
    def _session_log_context(
        stream: str,
        session_id: str = "",
        channel: str = "",
        device_id: str = "",
    ) -> str:
        """Build a consistent session log context string."""
        parts = [f"stream={stream}"]
        if device_id:
            parts.append(f"device={device_id}")
        if channel:
            parts.append(f"channel={channel}")
        if session_id:
            parts.append(f"session={session_id}")
        return " ".join(parts)

    async def _wait_for_reconnect_backoff(self, stream: str) -> None:
        """Sleep until the stream is allowed to join again."""
        retry_at = self._retry_after.get(stream, 0.0)
        now = self._loop.time()
        if retry_at <= now:
            return
        delay = retry_at - now
        LOGGER.info("Reconnect backoff stream=%s delay=%.1fs", stream, delay)
        await asyncio.sleep(delay)

    def _set_reconnect_backoff(self, stream: str) -> None:
        """Delay the next join attempt for one stream."""
        self._retry_after[stream] = (
            self._loop.time() + self._reconnect_backoff_seconds
        )

    async def _handle_join_failure(
        self,
        stream: str,
        agora_handler: AgoraWebSocketHandler | None,
    ) -> None:
        """Tear down failed local join state and apply reconnect backoff."""
        if agora_handler is not None:
            await agora_handler.disconnect()
        self._set_reconnect_backoff(stream)

    async def create_session(
        self,
        stream: str,
        offer_sdp: str,
        *,
        pion_compat: bool = False,
        owner_client_id: str | None = None,
    ) -> tuple[str, str]:
        """Attach one downstream offer to an active upstream Agora session."""
        # TEMP diagnostic: log go2rtc's offer so we can see its DTLS setup role
        # and offered codecs vs. the answer we build.
        LOGGER.info("go2rtc WHEP offer SDP:\n%s", offer_sdp)
        owner = owner_client_id or secrets.token_hex(8)
        setup_task: asyncio.Task[AgoraUpstreamSession] | None = None
        should_setup = False
        stream_lock = await self._get_stream_lock(stream)
        async with stream_lock:
            state = await self._get_state(stream)
            existing = state.active_agora_session
            if existing is not None:
                healthy, reason, definitive_failure = self._session_health(state, existing)
                age = max(0.0, self._loop.time() - existing.created_at)
                if healthy:
                    LOGGER.info(
                        "Reusing active Agora session %s",
                        self._session_log_context(
                            stream,
                            existing.session_id,
                            existing.channel,
                            existing.device_id,
                        ),
                    )
                    return await self._attach_downstream_locked(
                        stream,
                        state,
                        existing,
                        offer_sdp,
                        pion_compat=pion_compat,
                        owner_client_id=owner,
                    )
                if age < self._min_session_lifetime_seconds and not definitive_failure:
                    LOGGER.info(
                        "Anti-flap guard keeping young session %s age=%.1fs reason=%s",
                        self._session_log_context(
                            stream,
                            existing.session_id,
                            existing.channel,
                            existing.device_id,
                        ),
                        age,
                        reason,
                    )
                    return await self._attach_downstream_locked(
                        stream,
                        state,
                        existing,
                        offer_sdp,
                        pion_compat=pion_compat,
                        owner_client_id=owner,
                    )
                LOGGER.info(
                    "Cleaning unhealthy session %s reason=%s",
                    self._session_log_context(
                        stream,
                        existing.session_id,
                        existing.channel,
                        existing.device_id,
                    ),
                    reason,
                )
                await self._close_upstream_locked(
                    stream,
                    state,
                    expected_upstream_session_id=existing.session_id,
                    reason="unhealthy",
                )
            if state.setup_task is not None and not state.setup_task.done():
                setup_task = state.setup_task
                LOGGER.info("Waiting for in-progress setup stream=%s", stream)
            else:
                should_setup = True

        if should_setup:
            await self._wait_for_reconnect_backoff(stream)
            stream_lock = await self._get_stream_lock(stream)
            async with stream_lock:
                state = await self._get_state(stream)
                if state.setup_task is not None and not state.setup_task.done():
                    setup_task = state.setup_task
                else:
                    setup_task = asyncio.create_task(
                        self._setup_upstream_session(
                            stream,
                            offer_sdp,
                            pion_compat=pion_compat,
                            owner_client_id=owner,
                        )
                    )
                    state.setup_task = setup_task

        if setup_task is None:
            raise RuntimeError(f"Unable to initialize setup task for stream={stream}")

        try:
            setup_session = await setup_task
        except Exception:
            stream_lock = await self._get_stream_lock(stream)
            async with stream_lock:
                state = await self._get_state(stream)
                if state.setup_task is setup_task:
                    state.setup_task = None
                state.healthy = False
                state.unhealthy_reason = "setup_failed"
            raise

        stream_lock = await self._get_stream_lock(stream)
        async with stream_lock:
            state = await self._get_state(stream)
            if state.setup_task is setup_task:
                state.setup_task = None
            if state.active_agora_session is None:
                state.active_agora_session = setup_session
            existing = state.active_agora_session
            if existing is None:
                raise RuntimeError("Active Agora session missing after setup")
            if existing.session_id != setup_session.session_id:
                await setup_session.agora_handler.disconnect()

            return await self._attach_downstream_locked(
                stream,
                state,
                existing,
                offer_sdp,
                pion_compat=pion_compat,
                owner_client_id=owner,
            )

    async def _setup_upstream_session(
        self,
        stream: str,
        offer_sdp: str,
        *,
        pion_compat: bool,
        owner_client_id: str,
    ) -> AgoraUpstreamSession:
        """Create one upstream Agora session for a stream."""
        if self._publisher_wakeup is not None:
            try:
                await self._publisher_wakeup()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Publisher wakeup failed (continuing)")

        credentials = await self._credentials_provider()
        for field_name in ("app_id", "channel", "rtc_token"):
            if not getattr(credentials, field_name, None):
                raise RuntimeError(
                    f"Stream credentials missing {field_name}; cannot start Agora"
                )

        agora_response = await refresh_agora_context(credentials)
        if agora_response is None:
            raise RuntimeError("Failed to retrieve Agora edge servers")

        async def refresh_rtc_token() -> str | None:
            try:
                refreshed = await self._credentials_provider()
            except Exception:  # noqa: BLE001
                return None
            return refreshed.rtc_token or None

        session_id = secrets.token_hex(16)

        def _on_connection_lost(reason: str) -> None:
            with contextlib.suppress(RuntimeError):
                asyncio.get_running_loop().create_task(
                    self.mark_unhealthy(
                        stream,
                        expected_upstream_session_id=session_id,
                        reason=reason,
                    )
                )

        def _on_media_activity() -> None:
            with contextlib.suppress(RuntimeError):
                asyncio.get_running_loop().create_task(
                    self.touch_rtp(
                        stream,
                        expected_upstream_session_id=session_id,
                    )
                )

        def _build_agora_handler() -> AgoraWebSocketHandler:
            agora_handler = AgoraWebSocketHandler(
                rtc_token_provider=refresh_rtc_token,
                prefer_instant_video=True,
                subscribe_retry_delay=1.0,
                subscribe_retry_attempts=3,
                declare_remote_video_ssrc=True,
                disable_audio_answer=self._video_only,
                pion_compat=pion_compat,
                on_connection_lost=_on_connection_lost,
                on_media_activity=_on_media_activity,
            )
            agora_handler.set_log_context(
                stream=stream,
                session_id=session_id,
                channel=credentials.channel,
                device_id=credentials.device_id,
            )

            for line in offer_sdp.splitlines():
                stripped = line.strip()
                if stripped.startswith("a=candidate:"):
                    agora_handler.add_ice_candidate(
                        RTCIceCandidateInit(candidate=stripped.removeprefix("a="))
                    )

            agora_handler.candidates = filter_agora_candidates(
                agora_handler.candidates,
                agora_response,
            )
            return agora_handler

        LOGGER.info(
            "Creating Agora session %s",
            self._session_log_context(
                stream,
                session_id,
                credentials.channel,
                credentials.device_id,
            ),
        )

        agora_handler: AgoraWebSocketHandler | None = None
        answer_sdp: str | None = None
        for attempt in range(MAX_JOIN_ATTEMPTS):
            agora_handler = _build_agora_handler()
            try:
                answer_sdp = await agora_handler.connect_and_join(
                    live_feed=credentials.to_agora_credentials(),
                    offer_sdp=offer_sdp,
                    session_id=session_id,
                    app_id=credentials.app_id,
                    agora_response=agora_response,
                )
                break
            except AgoraDuplicateJoinError:
                await self._handle_join_failure(stream, agora_handler)
                LOGGER.warning(
                    "Agora duplicate join detected %s attempt=%d",
                    self._session_log_context(
                        stream,
                        session_id,
                        credentials.channel,
                        credentials.device_id,
                    ),
                    attempt + 1,
                )
                if attempt + 1 >= MAX_JOIN_ATTEMPTS:
                    raise RuntimeError(
                        "Agora duplicate join persisted after retry: "
                        + self._session_log_context(
                            stream,
                            session_id,
                            credentials.channel,
                            credentials.device_id,
                        )
                    )
                await self._wait_for_reconnect_backoff(stream)
                credentials = await self._credentials_provider()
                agora_response = await refresh_agora_context(credentials)
                continue
            except Exception:
                await self._handle_join_failure(stream, agora_handler)
                raise

        if agora_handler is None or not answer_sdp:
            await self._handle_join_failure(stream, agora_handler)
            raise RuntimeError("Agora upstream negotiation did not return an SDP answer")

        now = self._loop.time()
        session = AgoraUpstreamSession(
            stream=stream,
            session_id=session_id,
            channel=credentials.channel,
            device_id=credentials.device_id,
            owner_client_id=owner_client_id,
            agora_handler=agora_handler,
            created_at=now,
        )
        stream_lock = await self._get_stream_lock(stream)
        async with stream_lock:
            state = await self._get_state(stream)
            state.active_agora_session = session
            state.last_rtp_at = now
            state.last_successful_join_at = now
            state.healthy = True
            state.unhealthy_reason = ""
            self._retry_after[stream] = 0.0

        LOGGER.info(
            "Agora session active %s",
            self._session_log_context(
                stream,
                session_id,
                credentials.channel,
                credentials.device_id,
            ),
        )
        return session

    async def _attach_downstream_locked(
        self,
        stream: str,
        state: StreamRuntimeState,
        upstream: AgoraUpstreamSession,
        offer_sdp: str,
        *,
        pion_compat: bool,
        owner_client_id: str,
    ) -> tuple[str, str]:
        """Attach one downstream client session to an existing upstream session."""
        answer_sdp = upstream.agora_handler.build_answer_for_offer(
            offer_sdp,
            pion_compat=pion_compat,
        )
        if not answer_sdp:
            raise RuntimeError("Failed to generate SDP answer from active Agora session")

        stale_client_session = state.owner_current_session.get(owner_client_id)
        if stale_client_session:
            state.connected_clients.pop(stale_client_session, None)

        downstream_session_id = secrets.token_hex(16)
        state.connected_clients[downstream_session_id] = DownstreamClientSession(
            session_id=downstream_session_id,
            owner_client_id=owner_client_id,
            upstream_session_id=upstream.session_id,
            created_at=self._loop.time(),
        )
        state.owner_current_session[owner_client_id] = downstream_session_id

        if state.cleanup_task is not None and not state.cleanup_task.done():
            state.cleanup_task.cancel()
        state.cleanup_task = None
        return downstream_session_id, answer_sdp

    def _session_health(
        self,
        state: StreamRuntimeState,
        session: AgoraUpstreamSession,
    ) -> tuple[bool, str, bool]:
        """Return (healthy, reason, definitive_failure)."""
        if not session.agora_handler.is_connected:
            state.healthy = False
            state.unhealthy_reason = "websocket_closed"
            return False, "websocket_closed", True
        if not state.healthy:
            reason = state.unhealthy_reason or "unhealthy"
            definitive = reason in {"websocket_closed", "p2p_lost", "setup_failed"}
            return False, reason, definitive
        if self._rtp_timeout_seconds > 0 and state.last_rtp_at > 0:
            age = self._loop.time() - state.last_rtp_at
            if age > self._rtp_timeout_seconds:
                state.healthy = False
                state.unhealthy_reason = "rtp_timeout"
                return False, "rtp_timeout", False
        return True, "healthy", False

    async def add_session_candidates(
        self,
        stream: str,
        session_id: str,
        sdp_fragment: str,
    ) -> bool:
        """Forward trickled ICE candidates for one active session."""
        stream_lock = await self._get_stream_lock(stream)
        async with stream_lock:
            state = await self._get_state(stream)
            downstream = state.connected_clients.get(session_id)
            upstream = state.active_agora_session
            if downstream is None or upstream is None:
                return False
            if downstream.upstream_session_id != upstream.session_id:
                return False

            added = 0
            for candidate in _parse_trickle_candidates(sdp_fragment):
                upstream.agora_handler.add_ice_candidate(candidate)
                added += 1

            if added:
                LOGGER.debug(
                    "Collected %d trickle candidates for %s",
                    added,
                    self._session_log_context(
                        stream,
                        upstream.session_id,
                        upstream.channel,
                        upstream.device_id,
                    ),
                )
            return True

    async def add_session_candidate(
        self,
        stream: str,
        session_id: str,
        candidate: str,
    ) -> bool:
        """Forward one trickled ICE candidate for an active session."""
        stream_lock = await self._get_stream_lock(stream)
        async with stream_lock:
            state = await self._get_state(stream)
            downstream = state.connected_clients.get(session_id)
            upstream = state.active_agora_session
            if downstream is None or upstream is None:
                return False
            if downstream.upstream_session_id != upstream.session_id:
                return False

            upstream.agora_handler.add_ice_candidate(
                RTCIceCandidateInit(candidate=candidate)
            )
            return True

    async def _close_upstream_locked(
        self,
        stream: str,
        state: StreamRuntimeState,
        *,
        expected_upstream_session_id: str | None = None,
        reason: str,
    ) -> bool:
        """Close one active upstream Agora session while holding stream lock."""
        session = state.active_agora_session
        if session is None:
            return False
        if (
            expected_upstream_session_id
            and session.session_id != expected_upstream_session_id
        ):
            LOGGER.info(
                "Skipping stale cleanup stream=%s expected_session=%s active_session=%s",
                stream,
                expected_upstream_session_id,
                session.session_id,
            )
            return False

        LOGGER.info(
            "Session cleanup start %s reason=%s",
            self._session_log_context(
                stream,
                session.session_id,
                session.channel,
                session.device_id,
            ),
            reason,
        )

        if state.setup_task is not None and not state.setup_task.done():
            state.setup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await state.setup_task
            state.setup_task = None
        if state.cleanup_task is not None and not state.cleanup_task.done():
            current_task = asyncio.current_task()
            state.cleanup_task.cancel()
            if state.cleanup_task is not current_task:
                with contextlib.suppress(asyncio.CancelledError):
                    await state.cleanup_task
        state.cleanup_task = None

        await session.agora_handler.disconnect()
        state.active_agora_session = None
        state.healthy = False
        state.unhealthy_reason = reason
        state.last_rtp_at = 0.0
        state.connected_clients.clear()
        state.owner_current_session.clear()

        LOGGER.info(
            "Session cleanup complete %s",
            self._session_log_context(
                stream,
                session.session_id,
                session.channel,
                session.device_id,
            ),
        )
        return True

    async def close_session(
        self,
        stream: str,
        *,
        expected_session_id: str | None = None,
        reason: str = "requested",
    ) -> bool:
        """Detach one downstream session and optionally close upstream."""
        stream_lock = await self._get_stream_lock(stream)
        async with stream_lock:
            state = await self._get_state(stream)
            if not expected_session_id:
                return False

            downstream = state.connected_clients.pop(expected_session_id, None)
            if downstream is None:
                return False
            state.owner_current_session.pop(downstream.owner_client_id, None)
            return await self._maybe_cleanup_upstream_locked(
                stream,
                state,
                owner_client_id=downstream.owner_client_id,
                reason=reason,
            )

    async def close_owner_session(
        self,
        stream: str,
        *,
        owner_client_id: str,
        reason: str,
    ) -> None:
        """Detach downstream state owned by one websocket client."""
        stream_lock = await self._get_stream_lock(stream)
        async with stream_lock:
            state = await self._get_state(stream)
            downstream_id = state.owner_current_session.pop(owner_client_id, None)
            if downstream_id:
                state.connected_clients.pop(downstream_id, None)
            await self._maybe_cleanup_upstream_locked(
                stream,
                state,
                owner_client_id=owner_client_id,
                reason=reason,
            )

    async def _maybe_cleanup_upstream_locked(
        self,
        stream: str,
        state: StreamRuntimeState,
        *,
        owner_client_id: str,
        reason: str,
    ) -> bool:
        """Close or schedule-close upstream when no downstream clients remain."""
        upstream = state.active_agora_session
        if upstream is None:
            return False
        if state.connected_clients:
            return True
        if owner_client_id != upstream.owner_client_id:
            LOGGER.info(
                "Skipping upstream cleanup for non-owner disconnect stream=%s owner=%s active_owner=%s",
                stream,
                owner_client_id,
                upstream.owner_client_id,
            )
            return True

        if self._keep_agora_session_alive:
            if state.cleanup_task is not None and not state.cleanup_task.done():
                state.cleanup_task.cancel()
            state.cleanup_task = asyncio.create_task(
                self._delayed_cleanup(
                    stream,
                    expected_upstream_session_id=upstream.session_id,
                    delay_seconds=self._keepalive_seconds,
                )
            )
            LOGGER.info(
                "Keeping Agora session alive stream=%s session=%s for %.1fs",
                stream,
                upstream.session_id,
                self._keepalive_seconds,
            )
            return True

        return await self._close_upstream_locked(
            stream,
            state,
            expected_upstream_session_id=upstream.session_id,
            reason=reason,
        )

    async def _delayed_cleanup(
        self,
        stream: str,
        *,
        expected_upstream_session_id: str,
        delay_seconds: float,
    ) -> None:
        """Delayed cleanup to keep healthy upstream alive across reconnects."""
        try:
            await asyncio.sleep(delay_seconds)
            stream_lock = await self._get_stream_lock(stream)
            async with stream_lock:
                state = await self._get_state(stream)
                if state.connected_clients:
                    return
                await self._close_upstream_locked(
                    stream,
                    state,
                    expected_upstream_session_id=expected_upstream_session_id,
                    reason="keepalive expired",
                )
        except asyncio.CancelledError:
            raise

    async def mark_unhealthy(
        self,
        stream: str,
        *,
        expected_upstream_session_id: str,
        reason: str,
    ) -> None:
        """Mark active session unhealthy from Agora callbacks."""
        stream_lock = await self._get_stream_lock(stream)
        async with stream_lock:
            state = await self._get_state(stream)
            session = state.active_agora_session
            if session is None or session.session_id != expected_upstream_session_id:
                return
            state.healthy = False
            state.unhealthy_reason = reason

    async def touch_rtp(
        self,
        stream: str,
        *,
        expected_upstream_session_id: str,
    ) -> None:
        """Update last RTP-ish activity timestamp for health checks."""
        stream_lock = await self._get_stream_lock(stream)
        async with stream_lock:
            state = await self._get_state(stream)
            session = state.active_agora_session
            if session is None or session.session_id != expected_upstream_session_id:
                return
            now = self._loop.time()
            state.last_rtp_at = now
            state.healthy = True
            state.unhealthy_reason = ""

    async def close_all(self) -> None:
        """Close all active sessions."""
        async with self._lock:
            streams = list(self._states)
        for stream in streams:
            stream_lock = await self._get_stream_lock(stream)
            async with stream_lock:
                state = await self._get_state(stream)
                await self._close_upstream_locked(
                    stream,
                    state,
                    expected_upstream_session_id=(
                        state.active_agora_session.session_id
                        if state.active_agora_session
                        else None
                    ),
                    reason="application shutdown",
                )


def _parse_trickle_candidates(sdp_fragment: str) -> list[RTCIceCandidateInit]:
    """Extract trickled ICE candidates from a WHEP SDP fragment.

    Ported from PetKit ``whep_proxy.py:_parse_trickle_candidates`` but using the
    local SDPParser. The local parser does not split candidate attributes into a
    structured ``candidates`` list, so we read ``a=candidate:`` lines directly.
    """
    candidates: list[RTCIceCandidateInit] = []
    media_index = -1
    current_mid: str | None = None

    for raw_line in sdp_fragment.splitlines():
        line = raw_line.strip()
        if line.startswith("m="):
            media_index += 1
            current_mid = None
            continue
        if line.startswith("a=mid:"):
            current_mid = line.removeprefix("a=mid:")
            continue
        if not line.startswith("a=candidate:"):
            continue

        candidates.append(
            RTCIceCandidateInit(
                candidate=line.removeprefix("a="),
                sdp_mid=current_mid,
                sdp_m_line_index=media_index if media_index >= 0 else None,
            )
        )
    return candidates


# ---------------------------------------------------------------------------
# aiohttp.web handlers (replacing PetKit HomeAssistantView classes)
# ---------------------------------------------------------------------------

_MANAGER_KEY = web.AppKey("mammotion_whep_manager", MammotionWhepManager)
_TOKEN_KEY = web.AppKey("mammotion_whep_token", object)

# Permissive CORS so a browser WHEP test page can hit the bridge directly for
# diagnostics. WHEP normally runs server-to-server (go2rtc → us); a browser
# served from a different origin needs Access-Control-Allow-Origin or the
# preflight OPTIONS fails and fetch() can't even send the POST.
_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, PATCH, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Authorization, Content-Type",
    "Access-Control-Expose-Headers": "Location",
    "Access-Control-Max-Age": "600",
}


@web.middleware
async def _cors_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=_CORS_HEADERS)
    response = await handler(request)
    for key, value in _CORS_HEADERS.items():
        response.headers.setdefault(key, value)
    return response


def _check_auth(request: web.Request) -> web.Response | None:
    """Optional static bearer-token gate (env MAMMOTION_WHEP_TOKEN)."""
    expected = request.app.get(_TOKEN_KEY)
    if not expected:
        return None

    header = request.headers.get("Authorization", "")
    token = request.query.get("token")
    if header == f"Bearer {expected}" or token == expected:
        return None
    return web.Response(status=401, text="Authentication required")


async def _handle_whep_post(request: web.Request) -> web.Response:
    """Receive go2rtc's SDP offer; return the Agora-derived SDP answer."""
    auth_error = _check_auth(request)
    if auth_error is not None:
        return auth_error

    stream = request.match_info["stream"]
    offer_sdp = await request.text()
    if not offer_sdp or not offer_sdp.strip():
        return web.Response(status=400, text="Empty SDP offer")

    user_agent = request.headers.get("User-Agent", "")
    pion_compat = "Go-http-client" in user_agent or "go2rtc" in user_agent.lower()
    LOGGER.info(
        "WHEP client stream=%s user_agent=%s pion_compat=%s",
        stream,
        user_agent or "<empty>",
        pion_compat,
    )

    manager = request.app[_MANAGER_KEY]
    try:
        session_id, answer_sdp = await manager.create_session(
            stream,
            offer_sdp,
            pion_compat=pion_compat,
        )
    except (OSError, RuntimeError, ValueError, aiohttp.ClientError) as err:
        LOGGER.error("WHEP negotiation failed for %s: %s", stream, err)
        return web.Response(status=502, text=str(err))

    return web.Response(
        status=201,
        text=answer_sdp,
        content_type="application/sdp",
        headers={"Location": f"{request.path}/{session_id}"},
    )


async def _handle_whep_patch(request: web.Request) -> web.Response:
    """Accept trickled ICE candidates for one active WHEP session."""
    auth_error = _check_auth(request)
    if auth_error is not None:
        return auth_error

    stream = request.match_info["stream"]
    session_id = request.match_info["session_id"]
    body = await request.text()

    manager = request.app[_MANAGER_KEY]
    if not await manager.add_session_candidates(stream, session_id, body):
        return web.Response(status=404, text="No active WHEP session")
    return web.Response(status=204)


async def _handle_whep_delete(request: web.Request) -> web.Response:
    """Tear down one active WHEP session."""
    auth_error = _check_auth(request)
    if auth_error is not None:
        return auth_error

    stream = request.match_info["stream"]
    session_id = request.match_info["session_id"]
    manager = request.app[_MANAGER_KEY]
    if not await manager.close_session(
        stream,
        expected_session_id=session_id,
        reason="whep delete",
    ):
        return web.Response(status=404, text="No active WHEP session")
    return web.Response(status=200, text="Session closed")


async def _handle_go2rtc_ws(request: web.Request) -> web.StreamResponse:
    """go2rtc-compatible `/api/ws` endpoint for `webrtc:ws://.../api/ws?src=...`.

    The go2rtc WS client sends `webrtc/offer` first and trickles
    `webrtc/candidate` messages asynchronously. Our Agora join flow needs the
    candidate list up front, so we briefly buffer trickled candidates before
    generating the answer.
    """
    auth_error = _check_auth(request)
    if auth_error is not None:
        return auth_error

    stream = (request.query.get("src") or "").strip()
    if not stream:
        return web.Response(status=400, text="Missing ?src=...")

    # Do not enforce aiohttp heartbeat pings here. go2rtc's WS client can keep
    # the signaling socket mostly idle after offer/answer, and strict heartbeat
    # timeouts can tear down an otherwise healthy producer session.
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    manager = request.app[_MANAGER_KEY]
    ws_client_id = secrets.token_hex(8)
    current_session_id: str | None = None
    pending_candidates: list[str] = []

    try:
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT:
                continue

            try:
                payload = json.loads(msg.data)
            except json.JSONDecodeError:
                continue

            msg_type = str(payload.get("type") or "")
            if msg_type == "webrtc/candidate":
                candidate = str(payload.get("value") or "").strip()
                if not candidate.startswith("candidate:"):
                    continue

                if current_session_id:
                    await manager.add_session_candidate(
                        stream,
                        current_session_id,
                        candidate,
                    )
                else:
                    pending_candidates.append(candidate)
                continue

            if msg_type != "webrtc/offer":
                continue

            offer_sdp = str(payload.get("value") or "")
            if not offer_sdp.strip():
                await ws.send_json(
                    {
                        "type": "error",
                        "value": "webrtc/offer: empty SDP",
                    }
                )
                continue

            if pending_candidates:
                trickle_lines = [f"a={candidate}" for candidate in pending_candidates]
                offer_sdp = offer_sdp.rstrip() + "\r\n" + "\r\n".join(trickle_lines) + "\r\n"
                pending_candidates.clear()

            try:
                if current_session_id:
                    LOGGER.info(
                        "go2rtc WS received replacement offer stream=%s old_session=%s",
                        stream,
                        current_session_id,
                    )
                session_id, answer_sdp = await manager.create_session(
                    stream,
                    offer_sdp,
                    pion_compat=True,
                    owner_client_id=ws_client_id,
                )
                current_session_id = session_id
                await ws.send_json({"type": "webrtc/answer", "value": answer_sdp})
                LOGGER.info("go2rtc WS session established stream=%s id=%s", stream, session_id)
            except Exception as err:  # noqa: BLE001
                LOGGER.error("go2rtc WS negotiation failed for %s: %s", stream, err)
                await ws.send_json({"type": "error", "value": f"webrtc/offer: {err}"})
    finally:
        await manager.close_owner_session(
            stream,
            owner_client_id=ws_client_id,
            reason="go2rtc websocket disconnected",
        )

    return ws


def create_whep_app(
    credentials_provider: StreamCredentialsProvider,
    *,
    auth_token: str | None = None,
    publisher_wakeup: Callable[[], Awaitable[None]] | None = None,
    reconnect_backoff_seconds: float = 3.0,
    video_only: bool = False,
    keep_agora_session_alive: bool = False,
    keepalive_seconds: float = DEFAULT_KEEPALIVE_SECONDS,
    min_session_lifetime_seconds: float = DEFAULT_MIN_SESSION_LIFETIME_SECONDS,
    rtp_timeout_seconds: float = DEFAULT_RTP_TIMEOUT_SECONDS,
) -> web.Application:
    """Build the standalone aiohttp WHEP application.

    Routes:
      * ``POST   /whep/{stream}``                  -> negotiate (offer->answer)
      * ``PATCH  /whep/{stream}/{session_id}``     -> trickle ICE
      * ``DELETE /whep/{stream}/{session_id}``     -> teardown
    """
    app = web.Application(middlewares=[_cors_middleware])
    manager = MammotionWhepManager(
        credentials_provider,
        publisher_wakeup=publisher_wakeup,
        reconnect_backoff_seconds=reconnect_backoff_seconds,
        video_only=video_only,
        keep_agora_session_alive=keep_agora_session_alive,
        keepalive_seconds=keepalive_seconds,
        min_session_lifetime_seconds=min_session_lifetime_seconds,
        rtp_timeout_seconds=rtp_timeout_seconds,
    )
    app[_MANAGER_KEY] = manager
    app[_TOKEN_KEY] = auth_token

    app.router.add_post("/whep/{stream}", _handle_whep_post)
    app.router.add_patch("/whep/{stream}/{session_id}", _handle_whep_patch)
    app.router.add_delete("/whep/{stream}/{session_id}", _handle_whep_delete)
    app.router.add_get("/api/ws", _handle_go2rtc_ws)

    async def _on_cleanup(_app: web.Application) -> None:
        await manager.close_all()

    app.on_cleanup.append(_on_cleanup)
    return app
