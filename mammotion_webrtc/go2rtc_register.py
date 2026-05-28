"""Register a Mammotion stream into go2rtc via its REST API.

Port of the REST-registration core of PetKit's ``go2rtc_stream.py``
(``PetkitGo2RTCStreamManager.async_ensure_stream`` + ``_async_call_api`` +
``_async_stream_matches`` + ``_async_get_streams``).

Everything tied to Home Assistant is dropped: the HA-managed go2rtc URL
discovery, signed HA WHEP source URLs (``async_sign_path``), RTSP base URL
derivation, the legacy-stream migration, and the camera resolution. The base
URL and the WebRTC source are passed in explicitly by the entrypoint.

The go2rtc REST contract is unchanged: ``POST api/streams?dst=<name>&src=<src>``
(with PUT/PATCH fallbacks), and ``GET api/streams`` to check the producer is
already wired so registration is idempotent.
"""

from __future__ import annotations

import logging
from http import HTTPStatus
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from aiohttp import ClientError, ClientSession, ClientTimeout

LOGGER = logging.getLogger(__name__)

_GO2RTC_API_PATH = "api/streams"
_REQUEST_TIMEOUT = ClientTimeout(total=10)


class Go2RTCStreamRegistrar:
    """Register one fixed go2rtc stream pointing at our WHEP source."""

    def __init__(
        self,
        base_url: str,
        session: ClientSession | None = None,
    ) -> None:
        """Store the go2rtc API base URL and (optionally) a shared session."""
        self._base_url = self._normalize_url(base_url)
        self._session = session
        self._own_session = session is None

    async def __aenter__(self) -> "Go2RTCStreamRegistrar":
        if self._session is None:
            self._session = ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        if self._own_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _require_session(self) -> ClientSession:
        if self._session is None:
            self._session = ClientSession()
        return self._session

    async def ensure_stream(
        self,
        stream_name: str,
        source: str,
        *,
        raise_on_failure: bool = False,
    ) -> bool:
        """Ensure go2rtc has ``stream_name`` wired to ``source``.

        Returns True if the stream exists with the expected producer after the
        call. Mirrors PetKit's POST/PUT/PATCH method ladder.
        """
        if await self._stream_matches(stream_name, source):
            LOGGER.debug("go2rtc stream %s already wired to %s", stream_name, source)
            return True

        methods: tuple[tuple[str, dict[str, str]], ...] = (
            ("post", {"dst": stream_name, "src": source}),
            ("put", {"name": stream_name, "src": source}),
            ("patch", {"name": stream_name, "src": source}),
            ("patch", {"dst": stream_name, "src": source}),
        )

        statuses: list[str] = []
        for method, params in methods:
            status, detail = await self._call_api(method, params)
            detail_suffix = f" ({detail})" if detail else ""
            statuses.append(f"{method.upper()}={status}{detail_suffix}")
            if status in (
                HTTPStatus.OK,
                HTTPStatus.CREATED,
                HTTPStatus.NO_CONTENT,
            ):
                return True
            if await self._stream_matches(stream_name, source):
                return True

        LOGGER.warning(
            "Failed to register go2rtc stream %s (%s)",
            stream_name,
            ", ".join(statuses),
        )
        if raise_on_failure:
            raise RuntimeError(
                f"Failed to register go2rtc stream {stream_name}"
                f" ({', '.join(statuses)})"
            )
        return False

    async def remove_stream(self, stream_name: str) -> bool:
        """Remove the go2rtc stream if it exists."""
        for params in ({"dst": stream_name}, {"name": stream_name}):
            status, _ = await self._call_api("delete", params)
            if status in (HTTPStatus.OK, HTTPStatus.NO_CONTENT):
                return True
            if status == HTTPStatus.NOT_FOUND:
                continue
        return False

    async def _stream_matches(self, stream_name: str, source: str) -> bool:
        """Return whether go2rtc already has the expected producer source."""
        streams = await self._get_streams()
        if streams is None:
            return False

        stream = streams.get(stream_name)
        if not isinstance(stream, dict):
            return False

        producers = stream.get("producers") or []
        normalized_source = self._normalize_source_url(source)
        return any(
            isinstance(producer, dict)
            and self._normalize_source_url(str(producer.get("url", "")))
            == normalized_source
            for producer in producers
        )

    async def _get_streams(self) -> dict[str, dict] | None:
        """Fetch the current go2rtc streams payload."""
        session = self._require_session()
        try:
            async with session.get(
                urljoin(self._base_url, _GO2RTC_API_PATH),
                timeout=_REQUEST_TIMEOUT,
            ) as response:
                if response.status != HTTPStatus.OK:
                    return None
                payload = await response.json()
        except (ClientError, TimeoutError, ValueError) as err:
            LOGGER.debug("Failed to query go2rtc streams: %s", err)
            return None

        if not isinstance(payload, dict):
            return None
        return payload

    async def _call_api(
        self, method: str, params: dict[str, str]
    ) -> tuple[int, str | None]:
        """Call the go2rtc API and return HTTP status plus error detail."""
        session = self._require_session()
        request = getattr(session, method)
        try:
            async with request(
                urljoin(self._base_url, _GO2RTC_API_PATH),
                params=params,
                timeout=_REQUEST_TIMEOUT,
            ) as response:
                await response.read()
                return response.status, None
        except (ClientError, TimeoutError) as err:
            LOGGER.debug("go2rtc %s failed for %s: %s", method.upper(), params, err)
            return 0, str(err)

    @staticmethod
    def _normalize_url(url: str) -> str:
        """Normalize one go2rtc base URL for API calls."""
        return url.rstrip("/") + "/"

    @staticmethod
    def _normalize_source_url(source: str) -> str:
        """Normalize go2rtc producer URLs for stable comparisons."""
        if not source:
            return source

        raw_url = source
        if ":" in source:
            prefix, remainder = source.split(":", 1)
            if urlsplit(remainder).scheme in {"http", "https"}:
                raw_url = remainder
                # go2rtc stores HTTP-backed WebRTC producers as plain HTTP URLs
                # in /api/streams, so the transport wrapper must not participate
                # in equality checks.
                if prefix != "webrtc":
                    raw_url = f"{prefix}:{remainder}"

        parts = urlsplit(raw_url)
        if not parts.scheme:
            return source.rstrip("/")

        filtered_query = urlencode(
            [
                (key, value)
                for key, value in parse_qsl(parts.query, keep_blank_values=True)
                if key != "authSig"
            ],
            doseq=True,
        )
        normalized = urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path.rstrip("/"),
                filtered_query,
                "",
            )
        )
        return normalized.rstrip("/")
