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
from dataclasses import dataclass

import aiohttp
from aiohttp import web

from .agora_edge import (
    AgoraAPIClient,
    AgoraCredentials,
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
    """One direct Agora session backing a WHEP consumer (go2rtc)."""

    session_id: str
    agora_handler: AgoraWebSocketHandler
    refresh_task: asyncio.Task[None] | None = None


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
    ) -> None:
        """Store the credentials provider and per-stream session state."""
        self._credentials_provider = credentials_provider
        self._publisher_wakeup = publisher_wakeup
        self._lock = asyncio.Lock()
        self._sessions: dict[str, AgoraUpstreamSession] = {}

    async def create_session(
        self,
        stream: str,
        offer_sdp: str,
        *,
        pion_compat: bool = False,
    ) -> tuple[str, str]:
        """Create or replace the Agora session for one stream."""
        # TEMP diagnostic: log go2rtc's offer so we can see its DTLS setup role
        # and offered codecs vs. the answer we build.
        LOGGER.info("go2rtc WHEP offer SDP:\n%s", offer_sdp)
        await self.close_session(stream)

        # Force the mower into the Agora channel with video on BEFORE we open
        # the upstream WS. Mammotion's publisher otherwise idles when the only
        # subscriber is go2rtc (no app viewer). Best-effort: never block negotiation.
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
            # TODO(mammotion): unlike PetKit there is no RTM update_tokens step.
            # We re-fetch the stream subscription to obtain a fresh rtc_token
            # for Agora's renew_token. The provider must return a current token.
            try:
                refreshed = await self._credentials_provider()
            except Exception:  # noqa: BLE001
                return None
            return refreshed.rtc_token or None

        def _on_connection_lost() -> None:
            # Schedule cleanup so a dropped Agora session frees go2rtc to redial.
            with contextlib.suppress(RuntimeError):
                asyncio.get_running_loop().create_task(self.close_session(stream))

        agora_handler = AgoraWebSocketHandler(
            rtc_token_provider=refresh_rtc_token,
            prefer_instant_video=True,
            subscribe_retry_delay=1.0,
            subscribe_retry_attempts=3,
            declare_remote_video_ssrc=True,
            pion_compat=pion_compat,
            # Audio is answered with the natural sendonly direction, even
            # though Agora doesn't publish audio for Mammotion mowers. With
            # disable_audio_answer=True we emit a=inactive on mid=1, and on
            # BUNDLE Pion gets confused: it never starts ICE checks, Agora
            # times out (p2p_lost). mikey0000's working HA impl never sets
            # audio to inactive.
            on_connection_lost=_on_connection_lost,
            # video_codec defaults to h265 (Mammotion).
        )

        # Collect inline ICE candidates from the offer (PetKit did this in the
        # manager before join_v3).
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

        # NOTE(mammotion): PetKit started RTM (start_live + heartbeat) here. We
        # intentionally skip it. See DESIGN-webrtc-passthrough.md "Port notes".

        session_id = secrets.token_hex(16)
        try:
            answer_sdp = await agora_handler.connect_and_join(
                live_feed=credentials.to_agora_credentials(),
                offer_sdp=offer_sdp,
                session_id=session_id,
                app_id=credentials.app_id,
                agora_response=agora_response,
            )
        except Exception:
            await agora_handler.disconnect()
            raise

        if not answer_sdp:
            await agora_handler.disconnect()
            raise RuntimeError("Agora upstream negotiation did not return an SDP answer")

        session = AgoraUpstreamSession(
            session_id=session_id,
            agora_handler=agora_handler,
        )
        async with self._lock:
            self._sessions[stream] = session

        return session_id, answer_sdp

    async def add_session_candidates(
        self,
        stream: str,
        session_id: str,
        sdp_fragment: str,
    ) -> bool:
        """Forward trickled ICE candidates for one active session."""
        async with self._lock:
            session = self._sessions.get(stream)
        if session is None or session.session_id != session_id:
            return False

        added = 0
        for candidate in _parse_trickle_candidates(sdp_fragment):
            session.agora_handler.add_ice_candidate(candidate)
            added += 1

        if added:
            LOGGER.debug("Collected %d trickle candidates for %s", added, stream)
        return True

    async def add_session_candidate(
        self,
        stream: str,
        session_id: str,
        candidate: str,
    ) -> bool:
        """Forward one trickled ICE candidate for an active session."""
        async with self._lock:
            session = self._sessions.get(stream)
        if session is None or session.session_id != session_id:
            return False

        session.agora_handler.add_ice_candidate(
            RTCIceCandidateInit(candidate=candidate)
        )
        return True

    async def close_session(self, stream: str) -> bool:
        """Close the Agora session for one stream."""
        async with self._lock:
            session = self._sessions.pop(stream, None)

        if session is None:
            return False

        if session.refresh_task is not None:
            session.refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await session.refresh_task

        await session.agora_handler.disconnect()
        return True

    async def close_all(self) -> None:
        """Close all active sessions."""
        async with self._lock:
            streams = list(self._sessions)
        for stream in streams:
            await self.close_session(stream)


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
    # session_id is part of the route but close is keyed by stream.
    manager = request.app[_MANAGER_KEY]
    if not await manager.close_session(stream):
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

    ws = web.WebSocketResponse(heartbeat=20.0)
    await ws.prepare(request)

    manager = request.app[_MANAGER_KEY]
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
                session_id, answer_sdp = await manager.create_session(
                    stream,
                    offer_sdp,
                    pion_compat=True,
                )
                current_session_id = session_id
                await ws.send_json({"type": "webrtc/answer", "value": answer_sdp})
                LOGGER.info("go2rtc WS session established stream=%s id=%s", stream, session_id)
            except Exception as err:  # noqa: BLE001
                LOGGER.error("go2rtc WS negotiation failed for %s: %s", stream, err)
                await ws.send_json({"type": "error", "value": f"webrtc/offer: {err}"})
    finally:
        if current_session_id:
            await manager.close_session(stream)

    return ws


def create_whep_app(
    credentials_provider: StreamCredentialsProvider,
    *,
    auth_token: str | None = None,
    publisher_wakeup: Callable[[], Awaitable[None]] | None = None,
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
