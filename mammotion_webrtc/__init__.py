"""Mammotion WebRTC passthrough package.

A standalone port of the PetKit Home Assistant integration's Agora->go2rtc
WebRTC passthrough (MIT, (c) 2024-2026 @Jezza34000). It joins the Agora
channel as audience over Agora's private WebSocket signaling protocol,
synthesizes an SDP answer pointing at the Agora edge gateway, and exposes a
standalone WHEP endpoint that go2rtc dials. Media never flows through Python.

Mammotion-specific changes versus the PetKit reference are documented in
``DESIGN-webrtc-passthrough.md`` under "Port notes". The biggest ones:

* Credentials come from pymammotion ``get_stream_subscription`` (appid,
  channelName, token, uid) instead of PetKit's ``LiveFeed`` / coordinator.
* The subscribe codec is ``h265`` (Mammotion publishes H.265) rather than
  ``h264``.
* There is NO RTM token; the PetKit RTM start_live/heartbeat path is omitted
  and the publisher is kept alive over MQTT (``send_todev_ble_sync``).
"""

from __future__ import annotations

__all__ = [
    "SDPParser",
    "parse_offer_to_ortc",
    "AgoraWebSocketHandler",
    "AgoraAPIClient",
    "AgoraResponse",
    "StreamCredentials",
    "MammotionWhepManager",
    "create_whep_app",
    "Go2RTCStreamRegistrar",
]

from .sdp import SDPParser, parse_offer_to_ortc
from .agora_edge import (
    AgoraAPIClient,
    AgoraResponse,
    AgoraWebSocketHandler,
)
from .whep_server import (
    MammotionWhepManager,
    StreamCredentials,
    create_whep_app,
)
from .go2rtc_register import Go2RTCStreamRegistrar
