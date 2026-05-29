"""Minimal asyncio RTSP server serving one H265 stream over TCP-interleaved RTP.

Just enough RTSP to satisfy go2rtc/Frigate's RTSP client:

* ``OPTIONS`` / ``DESCRIBE`` / ``SETUP`` (TCP-interleaved only) / ``PLAY`` /
  ``GET_PARAMETER`` / ``TEARDOWN``.
* One mount point, set at construction (``rtsp://host:port/<mount>``).
* No authentication. The bridge is meant to listen only on the docker
  network shared with go2rtc.
* No RTP-over-UDP. TCP-interleaved is simpler (no extra socket per session,
  no NAT-traversal nonsense) and is what go2rtc picks anyway when the client
  is on the same host network.

The RTP packets fed in via :meth:`Go2RtcRtspStream.push_rtp` are H265
already in their RFC 7798 packetisation; we rebuild the 12-byte RTP header
with our own SSRC/seq/PT but leave the payload untouched. This is the whole
trick that makes the relay work: we never look inside the H265 payload, we
just transport it.

The SDP returned to DESCRIBE is built once we have seen at least one VPS,
SPS and PPS NAL — sprop-vps / sprop-sps / sprop-pps are inlined so the
client can prime its decoder before the first IDR. If the client connects
before parameter sets have arrived, DESCRIBE blocks (with a timeout) until
they do.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import random
import struct
import time
from dataclasses import dataclass, field

LOGGER = logging.getLogger(__name__)

# Dynamic PT we expose on the RTSP side. Need not match Agora's PT 100;
# what matters is that the SDP a=rtpmap line agrees with the PT inside the
# RTP header we send.
RTSP_H265_PAYLOAD_TYPE = 96

# H265 NAL unit types we care about for SDP fmtp generation. Reference:
# RFC 7798 section 1.1.4 / H.265 Table 7-1.
_H265_NAL_VPS = 32
_H265_NAL_SPS = 33
_H265_NAL_PPS = 34
_H265_NAL_AP = 48  # Aggregation packet (multiple NALs in one RTP payload)
_H265_NAL_FU = 49  # Fragmentation unit (NAL split across packets)

# H265 IRAP (Intra Random Access Point) NAL types: any of these lets a
# decoder start producing pictures.
_H265_IRAP_RANGE = range(16, 24)

# How long DESCRIBE blocks waiting for VPS/SPS/PPS before giving up with
# 503. Frigate's RTSP client retries on 503 with backoff, so a tight bound
# is fine — the upstream rarely needs more than ~2s to produce the first
# parameter set after IDR.
_PARAMETER_SET_WAIT_SECONDS = 10.0

# Cap the per-session send buffer. A slow client must not be allowed to grow
# the queue unboundedly — if it lags this far behind we drop it. The number
# is generous (≈4s at 8 Mbps H265) and protects against a Frigate-side stall.
_SESSION_QUEUE_HIGH_WATER = 1024


@dataclass
class _ParameterSets:
    """Latest VPS/SPS/PPS seen on the inbound stream.

    Kept as raw NAL units (the 2-byte NAL header is INCLUDED — that is what
    sprop-* in RFC 7798 expects, and what most decoders want when they see
    these bytes prepended to their input stream).
    """

    vps: bytes | None = None
    sps: bytes | None = None
    pps: bytes | None = None
    ready: asyncio.Event = field(default_factory=asyncio.Event)

    def update(self, vps: bytes | None, sps: bytes | None, pps: bytes | None) -> bool:
        changed = False
        if vps is not None and vps != self.vps:
            self.vps = vps
            changed = True
        if sps is not None and sps != self.sps:
            self.sps = sps
            changed = True
        if pps is not None and pps != self.pps:
            self.pps = pps
            changed = True
        if self.vps and self.sps and self.pps and not self.ready.is_set():
            self.ready.set()
        return changed


def _scan_h265_nals_for_parameter_sets(
    payload: bytes,
) -> tuple[bytes | None, bytes | None, bytes | None]:
    """Pick out VPS/SPS/PPS NAL units from one H265 RTP payload.

    Handles single NAL, AP (aggregation), and the start of a fragmented
    parameter set (rare but valid). For FU continuations and unrelated VCL
    payloads we return all-None.
    """
    if len(payload) < 2:
        return None, None, None

    nal_type = (payload[0] >> 1) & 0x3F
    vps = sps = pps = None

    if nal_type == _H265_NAL_AP:
        # Aggregation packet: skip 2-byte payload header, then iterate
        # length-prefixed NALs until end of buffer.
        offset = 2
        while offset + 2 <= len(payload):
            (nal_len,) = struct.unpack(">H", payload[offset : offset + 2])
            offset += 2
            if nal_len == 0 or offset + nal_len > len(payload):
                break
            nal = payload[offset : offset + nal_len]
            offset += nal_len
            if len(nal) < 2:
                continue
            t = (nal[0] >> 1) & 0x3F
            if t == _H265_NAL_VPS:
                vps = nal
            elif t == _H265_NAL_SPS:
                sps = nal
            elif t == _H265_NAL_PPS:
                pps = nal
    elif nal_type == _H265_NAL_FU:
        # Fragmented NAL — only the first fragment carries the original
        # NAL type in the FU header. We could reassemble parameter sets
        # across fragments, but in practice the mower sends them as single
        # NALs (they're tiny). Skip.
        return None, None, None
    elif nal_type in (_H265_NAL_VPS, _H265_NAL_SPS, _H265_NAL_PPS):
        # Single NAL containing the parameter set. The payload IS the NAL
        # (NAL header bytes 0-1 + RBSP).
        if nal_type == _H265_NAL_VPS:
            vps = bytes(payload)
        elif nal_type == _H265_NAL_SPS:
            sps = bytes(payload)
        else:
            pps = bytes(payload)

    return vps, sps, pps


def _is_h265_irap(payload: bytes) -> bool:
    """Best-effort IRAP detection for one RTP payload.

    Catches IRAPs that arrive as a single NAL or as the first fragment of
    a FU. AP-wrapped IRAPs are caught by scanning the inner NALs. False
    negatives are fine: we use this only to opportunistically PLI upstream
    when we have not seen a keyframe recently.
    """
    if len(payload) < 2:
        return False
    nal_type = (payload[0] >> 1) & 0x3F
    if nal_type in _H265_IRAP_RANGE:
        return True
    if nal_type == _H265_NAL_FU and len(payload) >= 3:
        fu_header = payload[2]
        start_bit = (fu_header >> 7) & 0x1
        original_type = fu_header & 0x3F
        return bool(start_bit) and original_type in _H265_IRAP_RANGE
    if nal_type == _H265_NAL_AP:
        offset = 2
        while offset + 2 <= len(payload):
            (nal_len,) = struct.unpack(">H", payload[offset : offset + 2])
            offset += 2
            if nal_len == 0 or offset + nal_len > len(payload):
                break
            inner = payload[offset]
            offset += nal_len
            if ((inner >> 1) & 0x3F) in _H265_IRAP_RANGE:
                return True
    return False


def _build_rtp_packet(
    *,
    payload: bytes,
    payload_type: int,
    sequence_number: int,
    timestamp: int,
    ssrc: int,
    marker: bool,
) -> bytes:
    """Pack one RTP packet with no header extension and no CSRCs."""
    first = 0x80  # V=2, P=0, X=0, CC=0
    second = (0x80 if marker else 0x00) | (payload_type & 0x7F)
    header = struct.pack(
        ">BBHII", first, second, sequence_number & 0xFFFF, timestamp & 0xFFFFFFFF, ssrc & 0xFFFFFFFF
    )
    return header + payload


@dataclass
class _RtpFrame:
    """One payload to forward, queued per session."""

    payload: bytes
    timestamp: int
    marker: bool


class _RtspSession:
    """One connected RTSP client."""

    def __init__(
        self,
        stream: "Go2RtcRtspStream",
        writer: asyncio.StreamWriter,
        peer: str,
    ) -> None:
        self.id = f"{random.randint(0, 0xFFFFFFFF):08x}"
        self.stream = stream
        self.writer = writer
        self.peer = peer
        self.ssrc = random.randint(1, 0xFFFFFFFE)
        # RTP sequence number starts random per RFC 3550. Clients tolerate this.
        self.sequence_number = random.randint(0, 0xFFFF)
        # RTSP TCP-interleaved channel for RTP. Set during SETUP.
        self.rtp_channel: int | None = None
        # RTCP channel (we don't actually emit RTCP, but we have to remember
        # what the client asked for so it does not get confused).
        self.rtcp_channel: int | None = None
        self.queue: asyncio.Queue[_RtpFrame | None] = asyncio.Queue()
        self.playing = False
        self._writer_task: asyncio.Task[None] | None = None
        self._closed = False
        # When PLAY starts, request a fresh keyframe upstream so the client
        # has something to decode within a frame or two instead of waiting
        # for the next natural IDR.
        self.want_keyframe = True

    def queue_frame(self, frame: _RtpFrame) -> None:
        if self._closed or not self.playing:
            return
        if self.queue.qsize() >= _SESSION_QUEUE_HIGH_WATER:
            LOGGER.warning(
                "RTSP session %s (peer %s) lagging beyond %d packets; dropping",
                self.id,
                self.peer,
                _SESSION_QUEUE_HIGH_WATER,
            )
            self._closed = True
            self.queue.put_nowait(None)
            return
        try:
            self.queue.put_nowait(frame)
        except asyncio.QueueFull:  # defensive — Queue is unbounded by default
            self._closed = True

    async def start_playback(self) -> None:
        if self._writer_task is not None:
            return
        self.playing = True
        self._writer_task = asyncio.create_task(self._writer_loop())

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.playing = False
        # Wake the writer so it can exit cleanly.
        try:
            self.queue.put_nowait(None)
        except Exception:  # noqa: BLE001
            pass
        if self._writer_task is not None:
            try:
                await asyncio.wait_for(self._writer_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._writer_task.cancel()
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:  # noqa: BLE001
            pass

    async def _writer_loop(self) -> None:
        try:
            while True:
                item = await self.queue.get()
                if item is None or self._closed:
                    return
                if self.rtp_channel is None:
                    # Client called PLAY without SETUP — should not happen,
                    # but discard rather than crash.
                    continue
                packet = _build_rtp_packet(
                    payload=item.payload,
                    payload_type=RTSP_H265_PAYLOAD_TYPE,
                    sequence_number=self.sequence_number,
                    timestamp=item.timestamp,
                    ssrc=self.ssrc,
                    marker=item.marker,
                )
                self.sequence_number = (self.sequence_number + 1) & 0xFFFF
                # Interleaved binary frame framing: $<channel><length BE><RTP>
                framed = b"$" + bytes([self.rtp_channel]) + struct.pack(">H", len(packet)) + packet
                try:
                    self.writer.write(framed)
                    await self.writer.drain()
                except (ConnectionError, BrokenPipeError, OSError) as exc:
                    LOGGER.info(
                        "RTSP session %s (peer %s) write failed: %s — dropping",
                        self.id,
                        self.peer,
                        exc,
                    )
                    return
        except asyncio.CancelledError:
            return
        except Exception:  # noqa: BLE001
            LOGGER.exception("RTSP session %s writer crashed", self.id)


class Go2RtcRtspStream:
    """RTSP server hosting one Mammotion video stream.

    Lifecycle is owned by the caller: instantiate, ``await start()``, push
    RTP packets in with :meth:`push_rtp`, and ``await stop()`` on shutdown.
    Each accepted RTSP client gets its own session loop; failures of one
    session never affect the others or the upstream.
    """

    def __init__(
        self,
        *,
        bind: str = "0.0.0.0",
        port: int = 8554,
        mount_point: str = "mammotion",
        on_keyframe_request: "callable | None" = None,
    ) -> None:
        self._bind = bind
        self._port = port
        self._mount = mount_point.strip("/")
        # Bridge callback invoked when we want upstream to send a fresh IDR
        # (new client connected, parameter sets stale, etc). Best-effort; the
        # bridge will turn this into an RTCP PLI.
        self._on_keyframe_request = on_keyframe_request

        self._server: asyncio.base_events.Server | None = None
        self._params = _ParameterSets()
        self._sessions: dict[str, _RtspSession] = {}
        self._lock = asyncio.Lock()
        self._started_at_ns = time.monotonic_ns()

    # ------------------------------------------------------------------
    # Public API used by the relay
    # ------------------------------------------------------------------

    @property
    def url_path(self) -> str:
        return f"/{self._mount}"

    @property
    def active_session_count(self) -> int:
        return sum(1 for s in self._sessions.values() if s.playing)

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await asyncio.start_server(
            self._handle_client, self._bind, self._port
        )
        sockets = self._server.sockets or ()
        LOGGER.info(
            "RTSP server listening on %s (mount=%s, sockets=%s)",
            f"{self._bind}:{self._port}",
            self.url_path,
            ", ".join(str(s.getsockname()) for s in sockets),
        )

    async def stop(self) -> None:
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            await session.close()
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:  # noqa: BLE001
                pass
            self._server = None

    def push_rtp(self, *, payload: bytes, timestamp: int, marker: bool) -> None:
        """Forward one H265 RTP payload to every active session.

        ``payload`` is the byte string the upstream RTP carried (after the
        12-byte RTP header) — i.e. RFC 7798 packetised H265. We do not
        decode or inspect it beyond looking for parameter-set NALs.
        """
        vps, sps, pps = _scan_h265_nals_for_parameter_sets(payload)
        if vps or sps or pps:
            self._params.update(vps, sps, pps)

        frame = _RtpFrame(payload=payload, timestamp=timestamp, marker=marker)
        for session in self._sessions.values():
            session.queue_frame(frame)

    def request_keyframe_if_needed(self) -> None:
        """Hook for the relay to fire a PLI on its own schedule."""
        if self._on_keyframe_request is not None:
            try:
                self._on_keyframe_request()
            except Exception:  # noqa: BLE001
                LOGGER.debug("on_keyframe_request callback raised", exc_info=True)

    # ------------------------------------------------------------------
    # RTSP wire handling
    # ------------------------------------------------------------------

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer_info = writer.get_extra_info("peername")
        peer = f"{peer_info[0]}:{peer_info[1]}" if peer_info else "unknown"
        session = _RtspSession(self, writer, peer)
        LOGGER.info("RTSP client connected from %s (session=%s)", peer, session.id)
        async with self._lock:
            self._sessions[session.id] = session
        try:
            while True:
                request = await self._read_request(reader)
                if request is None:
                    return
                method, url, headers, body = request
                LOGGER.debug(
                    "RTSP < %s %s session=%s cseq=%s",
                    method,
                    url,
                    session.id,
                    headers.get("cseq"),
                )
                await self._dispatch(session, method, url, headers, body)
                if method == "TEARDOWN":
                    return
        except (ConnectionError, asyncio.IncompleteReadError, BrokenPipeError):
            return
        except Exception:  # noqa: BLE001
            LOGGER.exception("RTSP session %s aborted", session.id)
        finally:
            async with self._lock:
                self._sessions.pop(session.id, None)
            await session.close()
            LOGGER.info(
                "RTSP client disconnected %s (session=%s, active=%d)",
                peer,
                session.id,
                self.active_session_count,
            )

    async def _read_request(
        self, reader: asyncio.StreamReader
    ) -> tuple[str, str, dict[str, str], bytes] | None:
        """Read one RTSP text request. Returns None on EOF."""
        # Request line.
        line = await reader.readline()
        if not line:
            return None
        try:
            request_line = line.decode("ascii", errors="replace").strip()
        except Exception:  # noqa: BLE001
            return None
        if not request_line:
            return None
        parts = request_line.split(" ", 2)
        if len(parts) < 3:
            return None
        method, url, _version = parts

        headers: dict[str, str] = {}
        while True:
            raw = await reader.readline()
            if not raw or raw in (b"\r\n", b"\n"):
                break
            try:
                name, _, value = raw.decode("ascii", errors="replace").partition(":")
            except Exception:  # noqa: BLE001
                continue
            headers[name.strip().lower()] = value.strip()

        body = b""
        if "content-length" in headers:
            try:
                length = int(headers["content-length"])
            except ValueError:
                length = 0
            if length > 0:
                body = await reader.readexactly(length)

        return method.upper(), url, headers, body

    async def _dispatch(
        self,
        session: _RtspSession,
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes,
    ) -> None:
        cseq = headers.get("cseq", "0")
        if method == "OPTIONS":
            await self._respond(session, 200, "OK", cseq, {
                "Public": "OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN, GET_PARAMETER",
            })
        elif method == "DESCRIBE":
            await self._handle_describe(session, url, cseq, headers)
        elif method == "SETUP":
            await self._handle_setup(session, url, cseq, headers)
        elif method == "PLAY":
            await self._handle_play(session, cseq)
        elif method in ("GET_PARAMETER", "SET_PARAMETER"):
            await self._respond(session, 200, "OK", cseq, {
                "Session": session.id,
            })
        elif method == "TEARDOWN":
            await self._respond(session, 200, "OK", cseq, {
                "Session": session.id,
            })
        else:
            await self._respond(session, 405, "Method Not Allowed", cseq, {})

    async def _handle_describe(
        self,
        session: _RtspSession,
        url: str,
        cseq: str,
        headers: dict[str, str],
    ) -> None:
        # Wait (with a bound) for VPS/SPS/PPS to arrive so we can put
        # sprop-* into the SDP fmtp. Frigate's RTSP client retries on 503.
        if not self._params.ready.is_set():
            try:
                await asyncio.wait_for(
                    self._params.ready.wait(), timeout=_PARAMETER_SET_WAIT_SECONDS
                )
            except asyncio.TimeoutError:
                LOGGER.warning(
                    "DESCRIBE from %s timed out waiting for H265 parameter sets",
                    session.peer,
                )
                await self._respond(session, 503, "Service Unavailable", cseq, {})
                return

        sdp = self._build_sdp(url)
        body = sdp.encode("ascii")
        await self._respond(
            session,
            200,
            "OK",
            cseq,
            {
                "Content-Type": "application/sdp",
                "Content-Base": self._content_base(url),
                "Content-Length": str(len(body)),
            },
            body=body,
        )

    async def _handle_setup(
        self,
        session: _RtspSession,
        url: str,
        cseq: str,
        headers: dict[str, str],
    ) -> None:
        transport_header = headers.get("transport", "")
        rtp_channel, rtcp_channel = _parse_interleaved_channels(transport_header)
        if rtp_channel is None:
            LOGGER.warning(
                "SETUP from %s requested unsupported transport %r — refusing",
                session.peer,
                transport_header,
            )
            await self._respond(session, 461, "Unsupported Transport", cseq, {})
            return
        session.rtp_channel = rtp_channel
        session.rtcp_channel = rtcp_channel
        transport_response = (
            f"RTP/AVP/TCP;unicast;interleaved={rtp_channel}-"
            f"{rtcp_channel if rtcp_channel is not None else rtp_channel + 1}"
            f";ssrc={session.ssrc:08X}"
        )
        await self._respond(session, 200, "OK", cseq, {
            "Session": session.id,
            "Transport": transport_response,
        })

    async def _handle_play(self, session: _RtspSession, cseq: str) -> None:
        if session.rtp_channel is None:
            await self._respond(session, 455, "Method Not Valid in This State", cseq, {})
            return
        # Best-effort: ask the relay to nudge upstream for a fresh IDR so
        # the new viewer is not waiting for the next natural keyframe.
        self.request_keyframe_if_needed()
        await session.start_playback()
        await self._respond(session, 200, "OK", cseq, {
            "Session": session.id,
            "Range": "npt=0.000-",
            "RTP-Info": (
                f"url={self._control_url('trackID=0')};"
                f"seq={session.sequence_number};rtptime=0"
            ),
        })

    async def _respond(
        self,
        session: _RtspSession,
        status: int,
        reason: str,
        cseq: str,
        headers: dict[str, str],
        *,
        body: bytes = b"",
    ) -> None:
        lines = [f"RTSP/1.0 {status} {reason}", f"CSeq: {cseq}"]
        for name, value in headers.items():
            lines.append(f"{name}: {value}")
        if body and "content-length" not in {k.lower() for k in headers}:
            lines.append(f"Content-Length: {len(body)}")
        message = ("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + body
        try:
            session.writer.write(message)
            await session.writer.drain()
        except (ConnectionError, BrokenPipeError, OSError):
            return

    # ------------------------------------------------------------------
    # SDP construction
    # ------------------------------------------------------------------

    def _content_base(self, url: str) -> str:
        if url.endswith("/"):
            return url
        return url + "/"

    def _control_url(self, track_id: str) -> str:
        # Track URLs are relative — go2rtc/ffmpeg both honour the
        # Content-Base header to resolve them against the request URL.
        return track_id

    def _build_sdp(self, request_url: str) -> str:
        sprop_vps = base64.b64encode(self._params.vps or b"").decode("ascii")
        sprop_sps = base64.b64encode(self._params.sps or b"").decode("ascii")
        sprop_pps = base64.b64encode(self._params.pps or b"").decode("ascii")
        # Session id derived from server start time keeps Wireshark traces
        # legible; nothing depends on it being globally unique.
        session_id = int(self._started_at_ns / 1_000_000_000)
        lines = [
            "v=0",
            f"o=- {session_id} {session_id} IN IP4 0.0.0.0",
            "s=Mammotion",
            "t=0 0",
            "a=control:*",
            f"m=video 0 RTP/AVP {RTSP_H265_PAYLOAD_TYPE}",
            "c=IN IP4 0.0.0.0",
            f"a=rtpmap:{RTSP_H265_PAYLOAD_TYPE} H265/90000",
            (
                f"a=fmtp:{RTSP_H265_PAYLOAD_TYPE} "
                f"sprop-vps={sprop_vps};sprop-sps={sprop_sps};sprop-pps={sprop_pps}"
            ),
            "a=control:trackID=0",
        ]
        return "\r\n".join(lines) + "\r\n"


def _parse_interleaved_channels(
    transport_header: str,
) -> tuple[int | None, int | None]:
    """Pull ``interleaved=A-B`` out of an RTSP Transport header.

    Returns ``(rtp_channel, rtcp_channel)`` or ``(None, None)`` if the
    transport is not RTP/AVP/TCP interleaved (the only flavour we accept).
    """
    if "RTP/AVP/TCP" not in transport_header.upper():
        return None, None
    for chunk in transport_header.split(";"):
        chunk = chunk.strip()
        if chunk.lower().startswith("interleaved="):
            spec = chunk.split("=", 1)[1]
            if "-" in spec:
                a, b = spec.split("-", 1)
                try:
                    return int(a), int(b)
                except ValueError:
                    return None, None
            try:
                rtp = int(spec)
                return rtp, rtp + 1
            except ValueError:
                return None, None
    # No interleaved= field → default to 0-1.
    return 0, 1
