"""Agora edge client: choose-server REST + WebSocket join_v3 signaling.

This module is a standalone merge of two PetKit HA integration files
(MIT, (c) 2024-2026 @Jezza34000):

* ``agora_api.py`` -> :class:`AgoraResponse`, :class:`AgoraAPIClient`,
  :class:`EdgeAddress`, :class:`ICEServer`, ``RESPONSE_FLAGS``, ``SERVICE_IDS``.
* ``agora_websocket.py`` -> :class:`AgoraWebSocketHandler`, the join_v3 client
  that subscribes to the publisher's video SSRC and synthesizes the SDP answer.

Home Assistant / PetKit specifics removed for the standalone port:

* ``from homeassistant...`` / ``from custom_components...`` -> none.
* ``LOGGER`` -> ``logging.getLogger(__name__)``.
* ``pypetkitapi.LiveFeed`` -> :class:`AgoraCredentials` (a tiny dataclass fed
  from pymammotion ``get_stream_subscription`` fields).
* ``sdp_transform.parse`` -> the local :class:`mammotion_webrtc.sdp.SDPParser`.
* ``webrtc_models.RTCIceCandidateInit`` -> the local :class:`RTCIceCandidateInit`.

Mammotion-specific change: the subscribe codec is ``h265`` (not ``h264``); see
the ``codec=`` arguments below and the "Port notes" in
``DESIGN-webrtc-passthrough.md``.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import secrets
import ssl
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Self

import aiohttp
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .sdp import SDPParser, parse_offer_to_ortc

LOGGER = logging.getLogger(__name__)

# Codec we ask Agora's edge to deliver (subscribe / join_v3 ``codec`` field).
# Mammotion's app defaults to H265, but the SFU also advertises VP8 and the
# mower honors VP8 when asked — and VP8 is the only path that survives Pion
# (go2rtc's WebRTC stack) intact. The SDP builders themselves stay
# codec-agnostic; they pick whatever PT the publisher actually emits.
DEFAULT_VIDEO_CODEC = "vp8"


# ---------------------------------------------------------------------------
# Local replacements for PetKit's external dependencies
# ---------------------------------------------------------------------------


@dataclass
class RTCIceCandidateInit:
    """Minimal stand-in for ``webrtc_models.RTCIceCandidateInit``.

    Only the attributes the Agora client touches are modelled.
    """

    candidate: str
    sdp_mid: str | None = None
    sdp_m_line_index: int | None = None


@dataclass
class AgoraCredentials:
    """Replacement for PetKit's ``LiveFeed`` in the Agora join flow.

    PetKit's ``LiveFeed`` carried both RTC and RTM credentials. Mammotion only
    provides the RTC half (appid/channel/token/uid) via
    ``get_stream_subscription``; there is no RTM token. Only the two attributes
    the join path reads are kept: ``rtc_token`` and ``channel_id``.
    """

    rtc_token: str
    channel_id: str
    uid: int = 0
    app_id: str = ""


# ---------------------------------------------------------------------------
# agora_api.py port
# ---------------------------------------------------------------------------

# Service IDs in request payload (request_bodies[].buffer.service_ids)
SERVICE_IDS: dict[str, int] = {
    "CHOOSE_SERVER": 11,
    "CLOUD_PROXY": 18,
    "CLOUD_PROXY_5": 20,
    "CLOUD_PROXY_FALLBACK": 26,
}

# Flags in response payload (response_body[].buffer.flag)
RESPONSE_FLAGS: dict[str, int] = {
    "CHOOSE_SERVER": 4096,
    "CLOUD_PROXY": 1048576,
    "CLOUD_PROXY_5": 4194304,
    "CLOUD_PROXY_FALLBACK": 4194310,
}


def derive_password(uid: int | str) -> str:
    """Derive TURN password from Agora uid."""
    return hashlib.sha256(str(uid).encode("utf-8")).hexdigest()


@dataclass
class EdgeAddress:
    """Agora edge address entry."""

    ip: str
    port: int
    username: str | None = None
    credentials: str | None = None
    ticket: str | None = None
    fingerprint: str | None = None


@dataclass
class ICEServer:
    """RTCIceServer-like structure."""

    urls: str | list[str]
    username: str | None = None
    credential: str | None = None


@dataclass
class AgoraResponse:
    """Parsed Agora choose-server response."""

    code: int
    addresses: list[EdgeAddress]
    ticket: str
    uid: int
    cid: int
    cname: str
    server_ts: int
    detail: dict[str, Any]
    flag: int
    opid: int
    responses: dict[int, dict[str, Any]] | None = None

    @classmethod
    def from_api_response(cls, response_data: dict[str, Any]) -> AgoraResponse:
        """Parse /api/v2/transpond/webrtc response payload."""
        response_body = response_data.get("response_body", [])
        if not response_body:
            raise ValueError("Agora response_body is empty")

        detail_base = response_data.get("detail", {}) or {}
        responses_by_flag: dict[int, dict[str, Any]] = {}

        for response_item in response_body:
            buffer = response_item.get("buffer", {}) or {}
            code = int(buffer.get("code", -1))
            if code != 0:
                LOGGER.debug(
                    "Skipping Agora response buffer with non-zero code=%s flag=%s",
                    code,
                    buffer.get("flag"),
                )
                continue

            flag = int(buffer.get("flag", 0))
            uid = int(buffer.get("uid", 0))
            ticket = str(buffer.get("cert", ""))
            edges_services = buffer.get("edges_services", []) or []

            detail = {
                **detail_base,
                **(buffer.get("detail", {}) or {}),
            }

            username = str(detail.get("8", "") or "")
            credentials = str(detail.get("4", "") or "")
            if not username:
                username = str(uid)
            if not credentials:
                credentials = derive_password(uid)
            if not username:
                username = "test"
            if not credentials:
                credentials = "111111"

            # detail[19] contains semicolon-separated fingerprints (optional)
            fingerprints: list[str] = []
            fingerprint_str = str(detail.get("19", "") or "")
            if fingerprint_str:
                fingerprints = [
                    fingerprint.strip()
                    for fingerprint in fingerprint_str.split(";")
                    if fingerprint.strip()
                ]

            addresses = [
                EdgeAddress(
                    ip=str(edge.get("ip", "")),
                    port=int(edge.get("port", 0)),
                    username=username,
                    credentials=credentials,
                    ticket=ticket,
                    fingerprint=(
                        fingerprints[index] if index < len(fingerprints) else None
                    ),
                )
                for index, edge in enumerate(edges_services)
                if edge.get("ip") and edge.get("port")
            ]

            responses_by_flag[flag] = {
                "code": code,
                "addresses": addresses,
                "ticket": ticket,
                "uid": uid,
                "cid": int(buffer.get("cid", 0)),
                "cname": str(buffer.get("cname", "")),
                "detail": detail,
                "flag": flag,
            }

        if not responses_by_flag:
            raise ValueError("Agora API response did not contain a successful buffer")

        primary = responses_by_flag.get(RESPONSE_FLAGS["CHOOSE_SERVER"])
        if primary is None:
            primary = next(iter(responses_by_flag.values()))

        return cls(
            code=int(primary.get("code", -1)),
            addresses=primary.get("addresses", []),
            ticket=str(primary.get("ticket", "")),
            uid=int(primary.get("uid", 0)),
            cid=int(primary.get("cid", 0)),
            cname=str(primary.get("cname", "")),
            server_ts=int(response_data.get("enter_ts", int(time.time() * 1000))),
            detail=primary.get("detail", {}) or {},
            flag=int(primary.get("flag", 0)),
            opid=int(response_data.get("opid", 0)),
            responses=responses_by_flag,
        )

    def get_responses_by_flag(self, flag: int) -> dict[str, Any] | None:
        """Return parsed response block for one Agora flag."""
        if not self.responses:
            return None
        return self.responses.get(flag)

    def get_gateway_addresses(self) -> list[EdgeAddress]:
        """Return gateway addresses (flag 4096)."""
        if self.responses:
            response = self.responses.get(RESPONSE_FLAGS["CHOOSE_SERVER"])
            if response:
                return response.get("addresses", [])
        if self.flag == RESPONSE_FLAGS["CHOOSE_SERVER"]:
            return self.addresses
        return []

    def get_turn_addresses(self) -> list[EdgeAddress]:
        """Return TURN addresses (flag 4194310)."""
        if self.responses:
            response = self.responses.get(RESPONSE_FLAGS["CLOUD_PROXY_FALLBACK"])
            if response:
                return response.get("addresses", [])
        if self.flag == RESPONSE_FLAGS["CLOUD_PROXY_FALLBACK"]:
            return self.addresses
        return []

    def get_ice_servers(
        self,
        use_all_turn_servers: bool = False,
        new_turn_mode: int = 4,
    ) -> list[ICEServer]:
        """Convert TURN endpoints into RTCIceServer objects."""
        turn_addresses = self.get_turn_addresses() or self.addresses
        if not turn_addresses:
            return []

        addresses = turn_addresses if use_all_turn_servers else turn_addresses[:1]
        servers: list[ICEServer] = []

        for address in addresses:
            if new_turn_mode in (1, 4):
                servers.append(
                    ICEServer(
                        urls=f"turn:{address.ip}:3478?transport=udp",
                        username=address.username,
                        credential=address.credentials,
                    )
                )
            if new_turn_mode in (2, 4):
                servers.append(
                    ICEServer(
                        urls=f"turn:{address.ip}:3478?transport=tcp",
                        username=address.username,
                        credential=address.credentials,
                    )
                )
            if new_turn_mode in (3, 4):
                servers.append(
                    ICEServer(
                        urls=(
                            "turns:"
                            f"{address.ip.replace('.', '-')}.edge.agora.io:443"
                            "?transport=tcp"
                        ),
                        username=address.username,
                        credential=address.credentials,
                    )
                )

        return servers

    def to_ap_response(self, flag: int | None = None) -> dict[str, Any]:
        """Convert response to join_v3 ap_response payload format."""
        if flag is not None and self.responses:
            response = self.responses.get(flag)
            if response:
                return {
                    "code": response["code"],
                    "server_ts": self.server_ts,
                    "uid": response["uid"],
                    "cid": response["cid"],
                    "cname": response["cname"],
                    "detail": response["detail"],
                    "flag": response["flag"],
                    "opid": self.opid,
                    "cert": response["ticket"],
                    "ticket": response["ticket"],
                }

        return {
            "code": self.code,
            "server_ts": self.server_ts,
            "uid": self.uid,
            "cid": self.cid,
            "cname": self.cname,
            "detail": self.detail,
            "flag": self.flag,
            "opid": self.opid,
            "cert": self.ticket,
            "ticket": self.ticket,
        }


class AgoraAPIClient:
    """Agora choose-server API client."""

    WEBCS_DOMAIN = [
        "webrtc2-ap-web-1.agora.io",
        "webrtc2-ap-web-2.agora.io",
        "webrtc2-ap-web-3.agora.io",
        "webrtc2-ap-web-4.agora.io",
    ]
    WEBCS_DOMAIN_BACKUP = [
        "webrtc2-ap-web-5.agora.io",
        "webrtc2-ap-web-6.agora.io",
    ]

    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        """Initialize API client."""
        self.session = session
        self._own_session = session is None

    async def __aenter__(self) -> Self:
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        """Context manager exit."""
        if self._own_session and self.session:
            await self.session.close()

    async def choose_server(
        self,
        app_id: str,
        token: str,
        channel_name: str,
        user_id: int,
        string_uid: str | None = None,
        role: int = 1,
        area_code: str = "CN,GLOBAL",
        service_flags: list[int] | None = None,
        sid: str | None = None,
        proxy_server: str | None = None,
    ) -> AgoraResponse:
        """Request gateway + TURN servers for a channel/token."""
        if string_uid is None:
            string_uid = str(user_id)
        if service_flags is None:
            service_flags = [
                SERVICE_IDS["CHOOSE_SERVER"],
                SERVICE_IDS["CLOUD_PROXY_FALLBACK"],
            ]
        if sid is None:
            sid = str(secrets.randbelow(2**31))

        payload = self._build_request_payload(
            app_id=app_id,
            token=token,
            channel_name=channel_name,
            user_id=user_id,
            string_uid=string_uid,
            service_flags=service_flags,
            sid=sid,
            uri=22,
            role=role,
            area_code=area_code,
        )
        response = await self._make_api_call(payload, proxy_server=proxy_server)
        return AgoraResponse.from_api_response(response)

    @staticmethod
    def _merge_objects(*objects: dict[str, Any] | None) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for obj in objects:
            if obj is None:
                continue
            merged.update({k: v for k, v in obj.items() if v is not None})
        return merged

    def _build_request_payload(
        self,
        app_id: str,
        token: str,
        channel_name: str,
        user_id: int,
        string_uid: str,
        service_flags: list[int],
        sid: str,
        uri: int,
        role: int,
        area_code: str,
    ) -> dict[str, Any]:
        client_ts = int(time.time() * 1000)
        opid = secrets.randbelow(10**12)

        detail = self._merge_objects(
            {"11": area_code},
            {"17": str(role)} if role else None,
            {"22": area_code},
            # Included to match Agora SDK behavior.
            {"6": string_uid} if string_uid else None,
        )

        return {
            "appid": app_id,
            "client_ts": client_ts,
            "opid": opid,
            "sid": sid,
            "request_bodies": [
                {
                    "uri": uri,
                    "buffer": {
                        "cname": channel_name,
                        "detail": detail,
                        "key": token,
                        "service_ids": service_flags,
                        "uid": user_id,
                    },
                }
            ],
        }

    async def _make_api_call(
        self,
        request_payload: dict[str, Any],
        proxy_server: str | None = None,
    ) -> dict[str, Any]:
        session = self.session
        should_close = False
        if session is None:
            session = aiohttp.ClientSession()
            if self._own_session:
                self.session = session
            else:
                should_close = True

        try:
            for domain in [
                *self.WEBCS_DOMAIN,
                *self.WEBCS_DOMAIN_BACKUP,
            ]:
                try:
                    return await self._call_endpoint(
                        session,
                        domain,
                        request_payload,
                        proxy_server,
                    )
                except (TimeoutError, aiohttp.ClientError, ValueError) as err:
                    LOGGER.debug("Agora endpoint %s failed: %s", domain, err)
                    continue

            raise RuntimeError("All Agora endpoints failed")
        finally:
            if should_close:
                await session.close()

    async def _call_endpoint(
        self,
        session: aiohttp.ClientSession,
        domain: str,
        request_payload: dict[str, Any],
        proxy_server: str | None = None,
    ) -> dict[str, Any]:
        if proxy_server:
            url = (
                f"https://{proxy_server}/ap/?url="
                f"{domain}/api/v2/transpond/webrtc?v=2"
            )
        else:
            url = f"https://{domain}/api/v2/transpond/webrtc?v=2"

        form_data = aiohttp.FormData()
        form_data.add_field(
            "request",
            json.dumps(request_payload),
            content_type="application/json",
        )

        async with session.post(
            url,
            data=form_data,
            timeout=aiohttp.ClientTimeout(total=10),
            ssl=False,
        ) as response:
            if response.status != 200:
                raise ValueError(
                    f"Agora API returned status={response.status}: "
                    f"{await response.text()}"
                )
            return await response.json()


# ---------------------------------------------------------------------------
# agora_websocket.py port
# ---------------------------------------------------------------------------


def _create_ws_ssl_context() -> ssl.SSLContext:
    """Create a secure SSL context for Agora edge WebSocket."""
    ssl_context = ssl.create_default_context()
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

    return ssl_context


_SSL_CONTEXT = _create_ws_ssl_context()


@dataclass
class OfferSdpInfo:
    """Selected pieces of the browser offer SDP used for answer generation."""

    parsed_sdp: dict[str, Any]
    fingerprint: str
    ice_ufrag: str
    ice_pwd: str
    audio_extensions: list[dict[str, Any]]
    video_extensions: list[dict[str, Any]]
    audio_direction: str
    video_direction: str
    extmap_allow_mixed: bool
    setup_role: str


class AgoraWebSocketHandler:
    """WebSocket handler for Agora join_v3 signaling."""

    def __init__(
        self,
        rtc_token_provider: Callable[[], Awaitable[str | None]] | None = None,
        *,
        prefer_instant_video: bool = False,
        subscribe_retry_delay: float = 0.0,
        subscribe_retry_attempts: int = 0,
        declare_remote_video_ssrc: bool = False,
        disable_audio_answer: bool = False,
        pion_compat: bool = False,
        on_connection_lost: Callable[[], None] | None = None,
        video_codec: str = DEFAULT_VIDEO_CODEC,
    ) -> None:
        """Initialize runtime state."""
        self._websocket: ClientConnection | None = None
        self._connection_state = "DISCONNECTED"
        self._message_handlers: dict[str, Callable[[dict[str, Any]], Any]] = {}
        self._disconnect_task: asyncio.Task[None] | None = None
        self._on_connection_lost = on_connection_lost

        self.candidates: list[RTCIceCandidateInit] = []
        self._online_users: set[int] = set()
        self._video_streams: dict[int, dict[str, Any]] = {}
        self._subscribed_video_streams: set[tuple[int, int]] = set()

        self._message_loop_task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None
        self._subscribe_retry_task: asyncio.Task[None] | None = None

        self._joined = False
        self._answer_sdp: str | None = None
        self._pending_answer_ortc: dict[str, Any] | None = None
        self._pending_offer_info: OfferSdpInfo | None = None
        self._rtc_token: str | None = None
        self._rtc_token_provider = rtc_token_provider
        self._prefer_instant_video = prefer_instant_video
        self._subscribe_retry_delay = subscribe_retry_delay
        self._subscribe_retry_attempts = subscribe_retry_attempts
        self._declare_remote_video_ssrc = declare_remote_video_ssrc
        self._disable_audio_answer = disable_audio_answer
        self._pion_compat = pion_compat
        # Mammotion publishes H.265 (see DEFAULT_VIDEO_CODEC). PetKit hardcoded
        # "h264" inline; here it is configurable but defaults to h265.
        self._video_codec = video_codec

        self._setup_message_handlers()

    def _setup_message_handlers(self) -> None:
        """Register incoming message handlers."""
        self._message_handlers = {
            "answer": self._handle_answer,
            "on_p2p_lost": self._handle_p2p_lost,
            "error": self._handle_error,
            "on_rtp_capability_change": self._handle_rtp_capability_change,
            "on_user_online": self._handle_user_online,
            "on_add_video_stream": self._handle_add_video_stream,
        }

    def add_ice_candidate(self, candidate: RTCIceCandidateInit) -> None:
        """Collect browser ICE candidates before join_v3."""
        self.candidates.append(candidate)

    async def connect_and_join(
        self,
        live_feed: AgoraCredentials,
        offer_sdp: str,
        session_id: str,
        app_id: str,
        agora_response: AgoraResponse,
    ) -> str | None:
        """Connect to Agora edge WebSocket and return answer SDP."""
        self._rtc_token = live_feed.rtc_token

        offer_info = self._parse_offer_sdp(offer_sdp)
        if offer_info is None:
            LOGGER.error("Failed to parse offer SDP")
            return None

        ortc_info = parse_offer_to_ortc(offer_sdp)
        if not ortc_info:
            LOGGER.error("Failed to build ORTC capabilities from offer")
            return None

        # Add gathered candidates to ORTC offer before join_v3.
        gathered_candidates = self._convert_candidates_to_ortc()
        if gathered_candidates:
            ortc_info.setdefault("iceParameters", {})[
                "candidates"
            ] = gathered_candidates
        LOGGER.debug(
            "Agora join_v3: session=%s gathered_candidates=%d",
            session_id,
            len(gathered_candidates),
        )

        gateway_addresses = agora_response.get_gateway_addresses()
        if not gateway_addresses:
            LOGGER.warning(
                "No gateway addresses in flag 4096; using fallback addresses"
            )
            gateway_addresses = agora_response.addresses

        LOGGER.debug(
            "Agora join_v3: trying %d gateway addresses",
            len(gateway_addresses),
        )
        for gateway in gateway_addresses:
            edge_ip_dashed = gateway.ip.replace(".", "-")
            ws_url = f"wss://{edge_ip_dashed}.edge.agora.io:{gateway.port}"

            try:
                async with asyncio.timeout(10):
                    websocket = await connect(
                        ws_url,
                        ssl=_SSL_CONTEXT,
                        ping_timeout=30,
                        close_timeout=30,
                    )

                self._websocket = websocket
                self._connection_state = "CONNECTED"
                LOGGER.info("Connected to Agora WebSocket: %s", ws_url)

                join_message = self._create_join_message(
                    live_feed=live_feed,
                    session_id=session_id,
                    app_id=app_id,
                    ortc_info=ortc_info,
                    agora_response=agora_response,
                )
                await websocket.send(json.dumps(join_message))
                LOGGER.debug("Sent join_v3 message")

                answer_sdp = await self._wait_for_join_response(
                    websocket=websocket,
                    offer_info=offer_info,
                    agora_response=agora_response,
                )

                if answer_sdp:
                    self._message_loop_task = asyncio.create_task(
                        self._message_loop(websocket)
                    )
                    self._ping_task = asyncio.create_task(self._ping_loop())
                    if self._subscribe_retry_attempts > 0:
                        self._subscribe_retry_task = asyncio.create_task(
                            self._subscribe_retry_loop()
                        )
                    return answer_sdp

                await websocket.close()
                self._websocket = None

            except TimeoutError:
                LOGGER.warning("WebSocket connection timeout for %s", ws_url)
                await self.disconnect()
                continue
            except (WebSocketException, json.JSONDecodeError, OSError) as err:
                LOGGER.warning("WebSocket signaling failed for %s: %s", ws_url, err)
                await self.disconnect()
                continue

        LOGGER.error("Failed to negotiate with all Agora edge gateways")
        self._connection_state = "DISCONNECTED"
        return None

    async def _wait_for_join_response(
        self,
        websocket: ClientConnection,
        offer_info: OfferSdpInfo,
        agora_response: AgoraResponse,
    ) -> str | None:
        """Wait for join success / answer after join_v3."""
        try:
            async with asyncio.timeout(15):
                async for raw_message in websocket:
                    try:
                        response = json.loads(raw_message)
                    except json.JSONDecodeError:
                        LOGGER.debug("Dropped non-JSON websocket payload")
                        continue

                    message_type = response.get("_type", "")
                    # TEMP diagnostic: log every message during the join phase so
                    # we can find where Mammotion's edge delivers the ORTC params
                    # (PetKit got them in the join-success _message; Mammotion's
                    # join-success only carries {code,level,role}).
                    LOGGER.info(
                        "WS-join <- type=%s result=%s body=%s",
                        message_type,
                        response.get("_result"),
                        json.dumps(response)[:1500],
                    )
                    if message_type in self._message_handlers:
                        result = await self._message_handlers[message_type](response)
                        if isinstance(result, str) and result:
                            return result

                    if response.get("_result") == "success":
                        answer = await self._handle_join_success(
                            response=response,
                            offer_info=offer_info,
                            agora_response=agora_response,
                        )
                        if answer:
                            return answer

        except TimeoutError:
            if (
                self._pending_answer_ortc is not None
                and self._pending_offer_info is not None
            ):
                LOGGER.warning(
                    "Timeout waiting for announced video stream; "
                    "falling back to early SDP answer"
                )
                answer = self._finalize_pending_answer()
                if answer:
                    return answer
            LOGGER.error("Timeout waiting for join_v3 response")
        except WebSocketException as err:
            LOGGER.error("WebSocket error while waiting for join response: %s", err)
            self._connection_state = "DISCONNECTED"

        return None

    async def _message_loop(self, websocket: ClientConnection) -> None:
        """Process messages after join success."""
        LOGGER.debug("Started Agora background message loop")
        try:
            async for raw_message in websocket:
                try:
                    response = json.loads(raw_message)
                except json.JSONDecodeError:
                    continue

                message_type = response.get("_type", "")
                LOGGER.debug("WS <- [%s] %s", message_type, response)

                if message_type in self._message_handlers:
                    await self._message_handlers[message_type](response)

                if message_type == "on_token_privilege_will_expire":
                    LOGGER.warning("Agora token expiring soon, sending renew_token")
                    await self._send_renew_token()
                elif message_type == "on_token_privilege_did_expire":
                    LOGGER.error("Agora token expired")

        except asyncio.CancelledError:
            LOGGER.debug("Agora message loop cancelled")
            raise
        except WebSocketException as err:
            LOGGER.warning("Agora message loop closed: %s", err)
            self._fire_connection_lost()
        finally:
            self._connection_state = "DISCONNECTED"

    async def _ping_loop(self) -> None:
        """Keep WebSocket session alive with ping messages."""
        try:
            while self._websocket and self._connection_state == "CONNECTED":
                await asyncio.sleep(3)
                if not self._websocket:
                    break
                ping_message = {
                    "_id": secrets.token_hex(3),
                    "_type": "ping",
                }
                await self._websocket.send(json.dumps(ping_message))
        except asyncio.CancelledError:
            LOGGER.debug("Agora ping loop cancelled")
            raise
        except (WebSocketException, OSError) as err:
            LOGGER.debug("Agora ping loop ended: %s", err)

    async def _send_renew_token(self) -> None:
        """Send renew_token with current rtc token."""
        if self._rtc_token_provider:
            try:
                refreshed_token = await self._rtc_token_provider()
            except Exception as err:  # noqa: BLE001
                LOGGER.debug("Failed to refresh RTC token for renew_token: %s", err)
                return
            if not refreshed_token:
                LOGGER.debug(
                    "RTC token refresh returned empty value; skipping renew_token"
                )
                return
            self._rtc_token = refreshed_token

        if not self._websocket or not self._rtc_token:
            return

        renew_message = {
            "_id": secrets.token_hex(3),
            "_type": "renew_token",
            "_message": {"token": self._rtc_token},
        }
        await self._websocket.send(json.dumps(renew_message))

    async def _handle_join_success(
        self,
        response: dict[str, Any],
        offer_info: OfferSdpInfo,
        agora_response: AgoraResponse,
    ) -> str | None:
        """Handle join_v3 success and generate browser answer SDP."""
        message = response.get("_message", {})
        ortc = message.get("ortc", {})
        if not ortc:
            # Mammotion sends two success messages: the join_v3 response (which
            # DOES carry _message.ortc) and a set_client_role ack ({code,level,
            # role}). This is the latter — ignore it; the ortc one is handled
            # separately and sets the pending answer.
            LOGGER.debug("Success message without ORTC (role ack); ignoring: %s", message)
            return None

        await self._send_set_client_role(role="host", level=0)
        await self._register_existing_video_streams(message)

        # Inject auth fingerprints if not present in ORTC payload.
        dtls_parameters = ortc.setdefault("dtlsParameters", {})
        fingerprints = dtls_parameters.setdefault("fingerprints", [])

        seen = {
            str(item.get("fingerprint", "")).lower()
            for item in fingerprints
            if item.get("fingerprint")
        }

        gateway_addresses = (
            agora_response.get_gateway_addresses() or agora_response.addresses
        )
        for address in gateway_addresses:
            if not address.fingerprint:
                continue

            fingerprint_algorithm = "sha-256"
            fingerprint_value = address.fingerprint
            if " " in fingerprint_value:
                parts = fingerprint_value.split()
                if len(parts) == 2:
                    fingerprint_algorithm = parts[0]
                    fingerprint_value = parts[1]

            if fingerprint_value.lower() in seen:
                continue

            fingerprints.append(
                {
                    "hashFunction": fingerprint_algorithm,
                    "fingerprint": fingerprint_value,
                }
            )
            seen.add(fingerprint_value.lower())

        self._pending_answer_ortc = ortc
        self._pending_offer_info = offer_info

        if self._declare_remote_video_ssrc and not any(
            isinstance(stream.get("ssrcId"), int)
            for stream in self._video_streams.values()
        ):
            LOGGER.debug(
                "Waiting for on_add_video_stream before finalizing SDP answer"
            )
            return None

        return self._finalize_pending_answer()

    async def _handle_answer(self, response: dict[str, Any]) -> str | None:
        """Handle direct `answer` message containing SDP."""
        message = response.get("_message", {})
        answer_sdp = message.get("sdp")
        if answer_sdp:
            self._answer_sdp = answer_sdp
            return answer_sdp
        return None

    async def _handle_p2p_lost(self, response: dict[str, Any]) -> None:
        """Handle p2p_lost signaling."""
        LOGGER.warning(
            "Agora p2p_lost: code=%s error=%s",
            response.get("error_code"),
            response.get("error_str"),
        )
        self._disconnect_task = asyncio.create_task(self.disconnect())
        self._fire_connection_lost()

    async def _handle_error(self, response: dict[str, Any]) -> None:
        """Handle generic Agora signaling errors."""
        message = response.get("_message", {})
        LOGGER.error("Agora error message: %s", message.get("error", message))

    async def _handle_rtp_capability_change(self, response: dict[str, Any]) -> None:
        """Handle capability updates."""
        LOGGER.debug("Agora rtp capability change: %s", response.get("_message", {}))

    async def _handle_user_online(self, response: dict[str, Any]) -> None:
        """Track online users."""
        message = response.get("_message", {})
        uid = message.get("uid")
        if isinstance(uid, int):
            self._online_users.add(uid)

    async def _handle_add_video_stream(self, response: dict[str, Any]) -> str | None:
        """Auto-subscribe to newly announced video stream."""
        message = response.get("_message", {})
        uid = message.get("uid")
        ssrc_id = message.get("ssrcId")
        rtx_ssrc_id = message.get("rtxSsrcId")
        cname = message.get("cname")
        publisher_pt = message.get("pt")
        is_video = bool(message.get("video"))

        if not isinstance(uid, int) or not is_video:
            return None

        LOGGER.info(
            "Agora on_add_video_stream: uid=%s ssrc=%s rtx_ssrc=%s pt=%s",
            uid,
            ssrc_id,
            rtx_ssrc_id,
            publisher_pt,
        )
        self._video_streams[uid] = {
            "ssrcId": ssrc_id,
            "rtxSsrcId": rtx_ssrc_id,
            "cname": cname,
            "pt": publisher_pt if isinstance(publisher_pt, int) else None,
        }

        if isinstance(ssrc_id, int):
            await self._subscribe_video_stream(uid=uid, ssrc_id=ssrc_id)
            if (
                self._pending_answer_ortc is not None
                and self._pending_offer_info is not None
            ):
                return self._finalize_pending_answer()

        return None

    def _finalize_pending_answer(self) -> str | None:
        """Generate the deferred SDP answer once enough stream metadata exists."""
        if self._pending_answer_ortc is None or self._pending_offer_info is None:
            return self._answer_sdp

        answer_sdp = self._generate_answer_sdp(
            self._pending_answer_ortc,
            self._pending_offer_info,
        )
        if answer_sdp:
            self._joined = True
            self._answer_sdp = answer_sdp
            self._pending_answer_ortc = None
            self._pending_offer_info = None
            # TEMP diagnostic: full answer SDP so we can verify candidates /
            # setup role / ufrag against go2rtc's ICE attempt.
            LOGGER.info("Generated answer SDP:\n%s", answer_sdp)
            return answer_sdp
        return None

    async def _send_set_client_role(
        self, role: str = "audience", level: int = 1
    ) -> None:
        """Send set_client_role signaling message."""
        if not self._websocket:
            return

        message = {
            "_id": secrets.token_hex(3),
            "_type": "set_client_role",
            "_message": {
                "role": role,
                "level": level,
                "client_ts": int(time.time() * 1000),
            },
        }
        await self._websocket.send(json.dumps(message))

    async def _send_subscribe(
        self,
        stream_id: int,
        ssrc_id: int,
        codec: str | None = None,
        stream_type: str = "video",
        mode: str = "live",
        p2p_id: int = 1,
        twcc: bool = True,
        rtx: bool = True,
        extend: str = "",
    ) -> None:
        """Send subscribe message for one remote stream."""
        if not self._websocket:
            return

        if codec is None:
            codec = self._video_codec

        LOGGER.debug(
            "Agora subscribe: stream_id=%s ssrc_id=%s codec=%s",
            stream_id,
            ssrc_id,
            codec,
        )
        message = {
            "_id": secrets.token_hex(3),
            "_type": "subscribe",
            "_message": {
                "stream_id": stream_id,
                "stream_type": stream_type,
                "mode": mode,
                "codec": codec,
                "p2p_id": p2p_id,
                "twcc": twcc,
                "rtx": rtx,
                "extend": extend,
                "ssrcId": ssrc_id,
            },
        }
        await self._websocket.send(json.dumps(message))
        self._subscribed_video_streams.add((stream_id, ssrc_id))

    async def _subscribe_video_stream(self, uid: int, ssrc_id: int) -> None:
        """Subscribe once per `(uid, ssrc_id)` pair."""
        if (uid, ssrc_id) in self._subscribed_video_streams:
            return
        # codec defaults to self._video_codec (h265 for Mammotion).
        await self._send_subscribe(stream_id=uid, ssrc_id=ssrc_id)

    async def _register_existing_video_streams(self, payload: Any) -> None:
        """Subscribe to any already-published video streams in join payload."""
        streams = self._find_existing_video_streams(payload)
        if streams:
            LOGGER.debug(
                "Agora join_v3: found %d existing video streams in join payload",
                len(streams),
            )

        for uid, ssrc_id in streams:
            self._video_streams.setdefault(uid, {"ssrcId": ssrc_id})
            await self._subscribe_video_stream(uid=uid, ssrc_id=ssrc_id)

    @classmethod
    def _find_existing_video_streams(cls, payload: Any) -> list[tuple[int, int]]:
        """Walk a join payload and extract existing video stream descriptors."""
        found: list[tuple[int, int]] = []

        def _visit(node: Any) -> None:
            if isinstance(node, dict):
                stream = cls._extract_existing_video_stream(node)
                if stream is not None:
                    found.append(stream)

                for value in node.values():
                    _visit(value)
                return

            if isinstance(node, list):
                for item in node:
                    _visit(item)

        _visit(payload)

        deduped: list[tuple[int, int]] = []
        seen: set[tuple[int, int]] = set()
        for stream in found:
            if stream in seen:
                continue
            seen.add(stream)
            deduped.append(stream)
        return deduped

    @staticmethod
    def _extract_existing_video_stream(node: dict[str, Any]) -> tuple[int, int] | None:
        """Extract one existing video stream descriptor when present."""
        uid = node.get("uid")
        ssrc_id = node.get("ssrcId")
        has_video_marker = (
            node.get("video") is True
            or node.get("stream_type") == "video"
            or node.get("type") == "video"
            or node.get("codec") in {"h264", "h265", "video"}
            or node.get("rtxSsrcId") is not None
        )
        if not has_video_marker:
            return None
        if not isinstance(uid, int) or not isinstance(ssrc_id, int):
            return None
        return (uid, ssrc_id)

    async def _subscribe_retry_loop(self) -> None:
        """Retry subscribe shortly after join for WHEP-style consumers."""
        try:
            for attempt in range(self._subscribe_retry_attempts):
                await asyncio.sleep(self._subscribe_retry_delay)
                if not self._websocket or self._connection_state != "CONNECTED":
                    return

                pending = [
                    (uid, data.get("ssrcId"))
                    for uid, data in self._video_streams.items()
                    if isinstance(data.get("ssrcId"), int)
                ]
                if not pending:
                    continue

                LOGGER.debug(
                    "Agora subscribe retry %d/%d: known_streams=%d",
                    attempt + 1,
                    self._subscribe_retry_attempts,
                    len(pending),
                )
                for uid, ssrc_id in pending:
                    await self._send_subscribe(
                        stream_id=uid,
                        ssrc_id=ssrc_id,
                    )
        except asyncio.CancelledError:
            LOGGER.debug("Agora subscribe retry loop cancelled")
            raise

    def _create_join_message(
        self,
        live_feed: AgoraCredentials,
        session_id: str,
        app_id: str,
        ortc_info: dict[str, Any],
        agora_response: AgoraResponse,
    ) -> dict[str, Any]:
        """Build join_v3 message payload."""
        process_id = (
            f"process-{secrets.token_hex(4)}-{secrets.token_hex(2)}-"
            f"{secrets.token_hex(2)}-{secrets.token_hex(2)}-{secrets.token_hex(6)}"
        )

        return {
            "_id": secrets.token_hex(3),
            "_type": "join_v3",
            "_message": {
                "p2p_id": 1,
                "session_id": session_id,
                "app_id": app_id,
                "channel_key": live_feed.rtc_token,
                "channel_name": live_feed.channel_id,
                "sdk_version": "4.24.0",
                "browser": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/142.0.0.0 Safari/537.36"
                ),
                "process_id": process_id,
                "mode": "live",
                # Mammotion publishes H.265.
                "codec": self._video_codec,
                "role": "host",
                "has_changed_gateway": False,
                "ap_response": agora_response.to_ap_response(
                    RESPONSE_FLAGS["CHOOSE_SERVER"]
                ),
                "extend": "",
                "details": {},
                "features": {"rejoin": True},
                "attributes": {
                    "userAttributes": {
                        "enableAudioMetadata": False,
                        "enableAudioPts": False,
                        "enablePublishedUserList": True,
                        "maxSubscription": 50,
                        "enableUserLicenseCheck": True,
                        "enableRTX": True,
                        "enableInstantVideo": self._prefer_instant_video,
                        "enableDataStream2": False,
                        "enableAutFeedback": True,
                        "enableUserAutoRebalanceCheck": True,
                        "enableXR": True,
                        "enableLossbasedBwe": True,
                        "enableAutCC": True,
                        "enablePreallocPC": False,
                        "enablePubTWCC": False,
                        "enableSubTWCC": True,
                        "enablePubRTX": True,
                        "enableSubRTX": True,
                    }
                },
                "join_ts": int(time.time() * 1000),
                "ortc": ortc_info,
            },
        }

    def _convert_candidates_to_ortc(self) -> list[dict[str, Any]]:
        """Convert browser ICE candidates to Agora ORTC format."""
        converted: list[dict[str, Any]] = []

        for candidate in self.candidates:
            candidate_string = candidate.candidate
            if not candidate_string:
                continue

            candidate_string = candidate_string.removeprefix("candidate:")

            parts = candidate_string.split()
            if len(parts) < 8:
                continue

            try:
                converted.append(
                    {
                        "foundation": parts[0],
                        "ip": parts[4],
                        "port": int(parts[5]),
                        "priority": int(parts[3]),
                        "protocol": parts[2],
                        "type": parts[7],
                    }
                )
            except (TypeError, ValueError):
                continue

        return converted

    @staticmethod
    def _parse_offer_sdp(offer_sdp: str) -> OfferSdpInfo | None:
        """Parse browser SDP offer using the local SDPParser.

        PetKit used ``sdp_transform.parse`` here; the local SDPParser emits the
        same field names the rest of this method reads (``iceUfrag``, ``icePwd``,
        ``fingerprint.hash``, ``ext[].value/.uri``, ``direction``, ``setup``,
        ``groups[].mids``, ``mid``, ``payloads``) plus ``extmapAllowMixed``.
        """
        try:
            parsed_sdp = SDPParser.parse(offer_sdp)

            fingerprint = ""
            if "fingerprint" in parsed_sdp:
                fingerprint = parsed_sdp["fingerprint"].get("hash", "")
            else:
                for media in parsed_sdp.get("media", []):
                    if "fingerprint" in media:
                        fingerprint = media["fingerprint"].get("hash", "")
                        break

            ice_ufrag = parsed_sdp.get("iceUfrag", "")
            ice_pwd = parsed_sdp.get("icePwd", "")
            if not ice_ufrag or not ice_pwd:
                for media in parsed_sdp.get("media", []):
                    if not ice_ufrag and "iceUfrag" in media:
                        ice_ufrag = media["iceUfrag"]
                    if not ice_pwd and "icePwd" in media:
                        ice_pwd = media["icePwd"]
                    if ice_ufrag and ice_pwd:
                        break

            audio_extensions: list[dict[str, Any]] = []
            video_extensions: list[dict[str, Any]] = []
            audio_direction = "sendrecv"
            video_direction = "sendrecv"

            for media in parsed_sdp.get("media", []):
                media_type = media.get("type")
                direction = media.get("direction", "sendrecv")

                if media_type == "audio":
                    audio_direction = direction
                elif media_type == "video":
                    video_direction = direction

                for extension in media.get("ext", []):
                    entry = {
                        "entry": extension.get("value"),
                        "extensionName": extension.get("uri"),
                    }
                    if media_type == "audio":
                        audio_extensions.append(entry)
                    elif media_type == "video":
                        video_extensions.append(entry)

            extmap_allow_mixed = bool(parsed_sdp.get("extmapAllowMixed", False))

            setup_role = "actpass"
            for media in parsed_sdp.get("media", []):
                if "setup" in media:
                    setup_role = media["setup"]
                    break

            return OfferSdpInfo(
                parsed_sdp=parsed_sdp,
                fingerprint=fingerprint,
                ice_ufrag=ice_ufrag,
                ice_pwd=ice_pwd,
                audio_extensions=audio_extensions,
                video_extensions=video_extensions,
                audio_direction=audio_direction,
                video_direction=video_direction,
                extmap_allow_mixed=extmap_allow_mixed,
                setup_role=setup_role,
            )
        except (TypeError, ValueError, KeyError) as err:
            LOGGER.error("Failed to parse offer SDP: %s", err)
            return None

    # Agora's media servers do not generate transport-cc feedback. The official
    # Web SDK (AgoraRTC_N, mungMediaDesc) strips both the transport-wide-cc
    # extmap and the per-payload `a=rtcp-fb:* transport-cc` lines before feeding
    # the synthesized remote SDP into the browser PeerConnection. We mirror that
    # munging in the answer SDP we hand to go2rtc/Pion — without it the consumer
    # negotiates transport-cc, waits for feedback that never arrives, and ICE
    # eventually times out (p2p_lost on Agora side).
    _TWCC_URI = (
        "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01"
    )

    @staticmethod
    def _dedupe_rtcp_feedbacks(
        feedbacks: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Drop duplicate ``(type, parameter)`` entries while preserving order.

        Agora's ORTC reply contains duplicate ``nack``/``nack pli`` entries.
        Chrome tolerates duplicate ``a=rtcp-fb`` lines; Pion (go2rtc) is
        stricter and may reject the whole m-line.
        """
        seen: set[tuple[str, str]] = set()
        out: list[dict[str, Any]] = []
        for feedback in feedbacks or []:
            key = (str(feedback.get("type", "")), str(feedback.get("parameter", "")))
            if key in seen:
                continue
            seen.add(key)
            out.append(feedback)
        return out

    @classmethod
    def _strip_transport_cc(cls, capabilities: dict[str, Any]) -> dict[str, Any]:
        """Return a copy of caps with transport-cc removed + rtcp-fb deduped."""
        munged = dict(capabilities)
        for ext_key in ("audioExtensions", "videoExtensions"):
            munged[ext_key] = [
                ext for ext in (munged.get(ext_key) or [])
                if ext.get("extensionName") != cls._TWCC_URI
            ]
        for codec_key in ("audioCodecs", "videoCodecs"):
            new_codecs = []
            for codec in (munged.get(codec_key) or []):
                codec_copy = dict(codec)
                rtcp_fbs = [
                    fb for fb in (codec.get("rtcpFeedbacks") or [])
                    if fb.get("type") != "transport-cc"
                ]
                codec_copy["rtcpFeedbacks"] = cls._dedupe_rtcp_feedbacks(rtcp_fbs)
                new_codecs.append(codec_copy)
            munged[codec_key] = new_codecs
        return munged

    @classmethod
    def _select_rtp_capabilities(cls, ortc: dict[str, Any]) -> dict[str, Any]:
        """Return the preferred RTP capability block from Agora ORTC data."""
        rtp_capabilities = ortc.get("rtpCapabilities", {})
        caps = (
            rtp_capabilities.get("sendrecv")
            or rtp_capabilities.get("recv")
            or rtp_capabilities.get("send")
            or rtp_capabilities
        )
        return cls._strip_transport_cc(caps)

    @staticmethod
    def _extract_fingerprint(dtls_parameters: dict[str, Any]) -> str:
        """Return the DTLS fingerprint advertised by Agora."""
        fingerprints = dtls_parameters.get("fingerprints", []) or []
        if not fingerprints:
            return ""

        primary = fingerprints[0]
        algorithm = primary.get("hashFunction") or primary.get("algorithm") or "sha-256"
        fingerprint_value = primary.get("fingerprint", "")
        return f"{algorithm} {fingerprint_value}" if fingerprint_value else ""

    def _build_candidate_lines(self, candidates: list[dict[str, Any]]) -> list[str]:
        """Translate Agora ICE candidates into SDP lines.

        Agora's ORTC payload reports ``foundation="udpcandidate"`` (string).
        libwebrtc (mikey0000's HA case) accepts that; Pion in go2rtc accepts it
        too per RFC 5245 but only IPv4 is reachable from a typical Docker
        bridge network, so we drop IPv6 host candidates.

        Appends ``a=end-of-candidates`` so Pion (go2rtc) knows the candidate
        list is complete and can start ICE checks immediately rather than
        waiting for trickle that will never arrive.
        """
        candidate_lines: list[str] = []
        for index, candidate in enumerate(candidates):
            ip = str(candidate.get("ip", ""))
            if not ip or ":" in ip:
                # Skip empty + IPv6 host candidates — Docker bridges typically
                # have no IPv6 route, and an unreachable candidate in front of
                # the working IPv4 one delays ICE for the worse.
                continue
            if self._pion_compat:
                # Pion is stricter than browser libwebrtc with non-numeric
                # foundations seen in Agora ORTC payloads.
                foundation = str(index + 1)
            else:
                foundation = candidate.get("foundation") or f"candidate{index}"
            protocol = candidate.get("protocol", "udp")
            priority = candidate.get("priority", 2103266323)
            port = candidate.get("port", 0)
            candidate_type = candidate.get("type", "host")

            line = (
                "a=candidate:"
                f"{foundation} 1 {protocol} {priority} {ip} {port} typ {candidate_type}"
            )
            if candidate.get("generation") is not None:
                line += f" generation {candidate['generation']}"
            candidate_lines.append(line)
        if candidate_lines:
            candidate_lines.append("a=end-of-candidates")
        return candidate_lines

    @staticmethod
    def _answer_direction(offer_direction: str) -> str:
        """Map the offered direction into the matching answer direction."""
        if offer_direction == "sendonly":
            return "recvonly"
        if offer_direction == "recvonly":
            return "sendonly"
        if offer_direction == "sendrecv":
            return "sendrecv"
        return "inactive"

    def _primary_video_stream(self) -> dict[str, Any] | None:
        """Return the first announced video stream that exposes an SSRC."""
        if not self._declare_remote_video_ssrc:
            return None

        for stream in self._video_streams.values():
            if isinstance(stream.get("ssrcId"), int):
                return stream
        return None

    @staticmethod
    def _bundle_mids(offer_info: OfferSdpInfo) -> str:
        """Return the BUNDLE mids declared by the offer."""
        bundle_group = (
            offer_info.parsed_sdp.get("groups", [{}])[0]
            if offer_info.parsed_sdp.get("groups")
            else {}
        )
        return bundle_group.get("mids", "0 1")

    @staticmethod
    def _build_session_sdp_lines(
        bundle_mids: str,
        *,
        extmap_allow_mixed: bool,
    ) -> list[str]:
        """Build the session-level SDP prefix for the answer."""
        sdp_lines = [
            "v=0",
            "o=- 0 0 IN IP4 127.0.0.1",
            "s=AgoraGateway",
            "t=0 0",
            f"a=group:BUNDLE {bundle_mids}",
            "a=ice-lite",
        ]
        if extmap_allow_mixed:
            sdp_lines.append("a=extmap-allow-mixed")
        sdp_lines.append("a=msid-semantic: WMS")
        return sdp_lines

    @staticmethod
    def _payload_types_for_media(
        codecs: list[dict[str, Any]],
        media: dict[str, Any],
    ) -> list[str]:
        """Return negotiated payload types for one media section."""
        payload_types = [str(codec.get("payloadType")) for codec in codecs]
        if payload_types:
            return payload_types
        return str(media.get("payloads", "")).split()

    def _build_transport_lines(
        self,
        media_type: str,
        payloads: str,
        *,
        ice_ufrag: str,
        ice_pwd: str,
        fingerprint: str,
        mid: str,
    ) -> list[str]:
        """Build the transport lines for one media section."""
        return [
            f"m={media_type} 9 UDP/TLS/RTP/SAVPF {payloads}",
            "c=IN IP4 127.0.0.1",
            "a=rtcp:9 IN IP4 0.0.0.0",
            f"a=ice-ufrag:{ice_ufrag}",
            f"a=ice-pwd:{ice_pwd}",
            *([] if self._pion_compat else ["a=ice-options:trickle"]),
            f"a=fingerprint:{fingerprint}",
            "a=setup:active",
            f"a=mid:{mid}",
        ]

    # MID is excluded: Agora's edge hardcodes mid=2 for video internally, but
    # the offer has video at mid=1 — if MID extension is negotiated the browser
    # discards all video RTP due to MID mismatch (see mikey0000's PR #392).
    _SKIP_EXT_URIS = frozenset({"urn:ietf:params:rtp-hdrext:sdes:mid"})

    @classmethod
    def _build_extension_lines(
        cls,
        offer_extensions: list[dict[str, Any]],
        answer_extensions: list[dict[str, Any]],
    ) -> list[str]:
        """Build negotiated extmap lines for one media section."""
        offer_ext_map = {
            extension.get("extensionName"): extension.get("entry")
            for extension in offer_extensions
        }
        return [
            f"a=extmap:{offer_ext_map[extension_name]} {extension_name}"
            for extension in answer_extensions
            if (extension_name := extension.get("extensionName")) in offer_ext_map
            and extension_name not in cls._SKIP_EXT_URIS
        ]

    @staticmethod
    def _build_codec_lines(codecs: list[dict[str, Any]]) -> list[str]:
        """Build codec-specific SDP lines for one media section."""
        codec_lines: list[str] = []
        for codec in codecs:
            payload_type = codec.get("payloadType")
            rtp_map = codec.get("rtpMap", {})
            codec_name = rtp_map.get("encodingName", "")
            clock_rate = rtp_map.get("clockRate", 90000)
            encoding_parameters = rtp_map.get("encodingParameters")

            if encoding_parameters:
                codec_lines.append(
                    "a=rtpmap:"
                    f"{payload_type} {codec_name}/{clock_rate}/{encoding_parameters}"
                )
            else:
                codec_lines.append(f"a=rtpmap:{payload_type} {codec_name}/{clock_rate}")

            for feedback in codec.get("rtcpFeedbacks", []):
                feedback_type = feedback.get("type")
                feedback_parameter = feedback.get("parameter")
                if feedback_parameter:
                    codec_lines.append(
                        "a=rtcp-fb:"
                        f"{payload_type} {feedback_type} {feedback_parameter}"
                    )
                else:
                    codec_lines.append(f"a=rtcp-fb:{payload_type} {feedback_type}")

            fmtp = codec.get("fmtp", {})
            parameters = fmtp.get("parameters", {}) if fmtp else {}
            if parameters:
                parameter_string = ";".join(
                    f"{key}={value}" for key, value in parameters.items()
                )
                codec_lines.append(f"a=fmtp:{payload_type} {parameter_string}")

        return codec_lines

    @staticmethod
    def _build_video_ssrc_lines(
        primary_video_stream: dict[str, Any] | None,
    ) -> list[str]:
        """Build SSRC lines for the announced remote video stream."""
        if primary_video_stream is None:
            return []

        video_ssrc = primary_video_stream.get("ssrcId")
        if not isinstance(video_ssrc, int):
            return []

        rtx_ssrc = primary_video_stream.get("rtxSsrcId")
        cname = primary_video_stream.get("cname") or "agora"
        ssrc_lines = [
            "a=msid:agora agora-video",
            f"a=ssrc:{video_ssrc} cname:{cname}",
            f"a=ssrc:{video_ssrc} msid:agora agora-video",
            f"a=ssrc:{video_ssrc} mslabel:agora",
            f"a=ssrc:{video_ssrc} label:agora-video",
        ]
        if isinstance(rtx_ssrc, int):
            ssrc_lines.append(f"a=ssrc-group:FID {video_ssrc} {rtx_ssrc}")
            ssrc_lines.append(f"a=ssrc:{rtx_ssrc} cname:{cname}")
        return ssrc_lines

    def _build_media_section_lines(
        self,
        media: dict[str, Any],
        *,
        index: int,
        caps: dict[str, Any],
        offer_info: OfferSdpInfo,
        ice_ufrag: str,
        ice_pwd: str,
        fingerprint: str,
        candidate_lines: list[str],
        primary_video_stream: dict[str, Any] | None,
    ) -> list[str]:
        """Build the SDP lines for one audio or video media section."""
        media_type = media.get("type", "audio")
        answer_direction = self._answer_direction(media.get("direction", "sendonly"))
        if media_type == "audio" and self._disable_audio_answer:
            answer_direction = "inactive"

        codecs = (
            caps.get("audioCodecs", []) or []
            if media_type == "audio"
            else caps.get("videoCodecs", []) or []
        )
        # For the video section: when the publisher's actual payload type is
        # known (from on_add_video_stream `pt`), narrow the answer to just that
        # PT. The Mammotion publisher only sends one codec at a time (H.265 on
        # PT 100). go2rtc otherwise sees H264 + H265 in our answer and picks
        # the first match (H264), then never receives matching RTP because the
        # publisher is on H265 — symptom: probe shows an h264 receiver, no
        # video reaches the consumer, Agora eventually p2p_lost.
        if media_type == "video" and primary_video_stream is not None:
            publisher_pt = primary_video_stream.get("pt")
            if isinstance(publisher_pt, int):
                matching = [c for c in codecs if c.get("payloadType") == publisher_pt]
                if matching:
                    codecs = matching
        answer_extensions = (
            caps.get("audioExtensions", []) or []
            if media_type == "audio"
            else caps.get("videoExtensions", []) or []
        )
        offer_extensions = (
            offer_info.audio_extensions
            if media_type == "audio"
            else offer_info.video_extensions
        )
        payloads = " ".join(self._payload_types_for_media(codecs, media))
        mid = str(media.get("mid", str(index)))

        sdp_lines = self._build_transport_lines(
            media_type,
            payloads,
            ice_ufrag=ice_ufrag,
            ice_pwd=ice_pwd,
            fingerprint=fingerprint,
            mid=mid,
        )
        sdp_lines.extend(candidate_lines)
        sdp_lines.extend(
            self._build_extension_lines(offer_extensions, answer_extensions)
        )
        sdp_lines.extend([f"a={answer_direction}", "a=rtcp-mux", "a=rtcp-rsize"])
        sdp_lines.extend(self._build_codec_lines(codecs))
        if media_type == "video":
            sdp_lines.extend(self._build_video_ssrc_lines(primary_video_stream))
        return sdp_lines

    def _generate_answer_sdp(
        self,
        ortc: dict[str, Any],
        offer_info: OfferSdpInfo,
    ) -> str | None:
        """Generate answer SDP from Agora ORTC response."""
        try:
            ice_parameters = ortc.get("iceParameters", {})
            caps = self._select_rtp_capabilities(ortc)
            ice_ufrag = ice_parameters.get("iceUfrag") or secrets.token_hex(4)
            ice_pwd = ice_parameters.get("icePwd") or secrets.token_hex(16)
            fingerprint = self._extract_fingerprint(ortc.get("dtlsParameters", {}))
            if not fingerprint:
                LOGGER.error("Missing DTLS fingerprint in Agora ORTC response")
                return None

            media_sections = offer_info.parsed_sdp.get("media", []) or []
            if not media_sections:
                return None

            sdp_lines = self._build_session_sdp_lines(
                self._bundle_mids(offer_info),
                extmap_allow_mixed=offer_info.extmap_allow_mixed,
            )
            candidate_lines = self._build_candidate_lines(
                ice_parameters.get("candidates", []) or []
            )
            primary_video_stream = self._primary_video_stream()

            for index, media in enumerate(media_sections):
                sdp_lines.extend(
                    self._build_media_section_lines(
                        media,
                        index=index,
                        caps=caps,
                        offer_info=offer_info,
                        ice_ufrag=ice_ufrag,
                        ice_pwd=ice_pwd,
                        fingerprint=fingerprint,
                        candidate_lines=candidate_lines,
                        primary_video_stream=primary_video_stream,
                    )
                )

            answer_sdp = "\r\n".join(sdp_lines) + "\r\n"
            return answer_sdp if self._validate_sdp(answer_sdp) else None
        except (AttributeError, TypeError, ValueError) as err:
            LOGGER.error("Failed to generate answer SDP: %s", err)
            return None

    @staticmethod
    def _validate_sdp(sdp: str) -> bool:
        """Validate mandatory SDP lines."""
        if not sdp.strip():
            return False

        has_version = False
        has_origin = False
        has_session_name = False
        has_timing = False
        media_count = 0

        for line in sdp.split("\r\n"):
            if line.startswith("v="):
                has_version = True
            elif line.startswith("o="):
                has_origin = True
            elif line.startswith("s="):
                has_session_name = True
            elif line.startswith("t="):
                has_timing = True
            elif line.startswith("m="):
                media_count += 1

        return (
            has_version
            and has_origin
            and has_session_name
            and has_timing
            and media_count >= 1
        )

    @property
    def is_connected(self) -> bool:
        """Return websocket connectivity state."""
        return self._connection_state == "CONNECTED"

    def _fire_connection_lost(self) -> None:
        """Notify the owner that the Agora connection dropped unexpectedly."""
        if self._on_connection_lost is not None:
            self._on_connection_lost()
            self._on_connection_lost = None

    async def disconnect(self) -> None:
        """Close websocket and cancel background tasks."""
        tasks_to_wait: list[asyncio.Task[None]] = []
        current_task = asyncio.current_task()

        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            if self._ping_task is not current_task:
                tasks_to_wait.append(self._ping_task)
        self._ping_task = None

        if self._subscribe_retry_task and not self._subscribe_retry_task.done():
            self._subscribe_retry_task.cancel()
            if self._subscribe_retry_task is not current_task:
                tasks_to_wait.append(self._subscribe_retry_task)
        self._subscribe_retry_task = None

        if self._message_loop_task and not self._message_loop_task.done():
            self._message_loop_task.cancel()
            if self._message_loop_task is not current_task:
                tasks_to_wait.append(self._message_loop_task)
        self._message_loop_task = None

        if tasks_to_wait:
            await asyncio.gather(*tasks_to_wait, return_exceptions=True)

        if self._websocket:
            with contextlib.suppress(WebSocketException):
                await self._websocket.close()
            self._websocket = None

        self._joined = False
        self._answer_sdp = None
        self._pending_answer_ortc = None
        self._pending_offer_info = None
        self._connection_state = "DISCONNECTED"
        self._video_streams.clear()
        self._subscribed_video_streams.clear()
