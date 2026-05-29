"""Lightweight Agora session glue used by the bridge entrypoint.

Holds the data classes and callback aliases the relay needs to fetch fresh
Agora credentials and edge-gateway context from Mammotion's cloud. Lives
in its own module so the relay does not have to import the (now defunct)
WHEP server just to get :class:`StreamCredentials`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .agora_edge import (
    AgoraAPIClient,
    AgoraCredentials,
    AgoraResponse,
    SERVICE_IDS,
)


@dataclass
class StreamCredentials:
    """Agora RTC credentials for one Mammotion stream subscription.

    Mirrors the subset of pymammotion ``get_stream_subscription`` data the
    Agora join flow needs.
    """

    app_id: str
    channel: str
    rtc_token: str
    uid: int
    # ``"CN,GLOBAL"``-style area code string accepted by ``choose_server``.
    # Defaults to global; the entrypoint maps Mammotion's areaCode if it can.
    area_code: str = "CN,GLOBAL"

    def to_agora_credentials(self) -> AgoraCredentials:
        return AgoraCredentials(
            rtc_token=self.rtc_token,
            channel_id=self.channel,
            uid=self.uid,
            app_id=self.app_id,
        )


CredentialsProvider = Callable[[], Awaitable[StreamCredentials]]
AgoraContextProvider = Callable[[StreamCredentials], Awaitable[AgoraResponse]]
PublisherWakeup = Callable[[], Awaitable[None]] | None


async def refresh_agora_context(credentials: StreamCredentials) -> AgoraResponse:
    """Fetch Agora gateway + TURN endpoints for one set of credentials."""
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
