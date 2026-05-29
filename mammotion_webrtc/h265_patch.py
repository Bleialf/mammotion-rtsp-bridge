"""Monkey-patch aiortc to accept H265 as a passthrough codec.

aiortc 1.13 has zero H265 support — no encoder, no decoder, no depacketizer.
Without these patches three things go wrong, in order:

1. ``CODECS["video"]`` does not contain H265, so the offer aiortc generates
   does not list ``H265/90000``. Agora's edge answers with an H265-only video
   m-line, no common codec is found, and the upstream PC never starts.
2. If we did get H265 into the SDP, ``RTCRtpReceiver._handle_rtp_packet``
   drops every packet with PT 100 because ``self.__codecs.get(100)`` is None
   (the receiver only stores codecs from the negotiated SDP, but it would
   never have been negotiated).
3. If we did register H265 with the receiver, the decoder thread calls
   ``get_decoder(codec)`` on the first packet and crashes — there is no
   H265 decoder in aiortc.

We only need RTP passthrough (not decode), so we:

* Insert ``video/H265`` (PT 100) and matching ``video/rtx`` (PT 101) at the
  *front* of ``CODECS["video"]`` so it is preferred in offers/answers.
* Replace ``get_decoder`` in the receiver module's namespace with a wrapper
  that hands back a no-op :class:`Decoder` for H265. ``depayload`` already
  returns the raw RTP payload when it does not recognise the codec, which
  is exactly what we want — the jitter buffer accumulates the H265-packetised
  bytes and the no-op decoder discards them while the rest of the receiver
  (RTCP RR / NACK / PLI back to Agora) keeps running.

The actual H265 RTP bytes are tapped *before* depayload by the relay (see
:mod:`.aiortc_relay`) — this patch only exists to stop aiortc from rejecting
the codec at SDP, packet-routing, or decoder-spawn time.

Apply once, ideally at process startup before any ``RTCPeerConnection`` is
constructed. Subsequent calls are no-ops.
"""

from __future__ import annotations

import logging

from aiortc import codecs as _codecs
from aiortc import rtcrtpreceiver as _receiver
from aiortc.codecs.base import Decoder
from aiortc.jitterbuffer import JitterFrame
from aiortc.rtcrtpparameters import RTCRtcpFeedback, RTCRtpCodecParameters

LOGGER = logging.getLogger(__name__)

# PT used for H265 / H265-RTX in the offer aiortc generates. These have to
# fall outside the dynamic range already claimed by aiortc's built-in codec
# init (VP8=97, VP8-RTX=98, H264=99, H264-RTX=100, H264=101, H264-RTX=102).
# Agora replaces the PT with whatever it picks in the answer; the negotiated
# value is what shows up in subsequent RTP packet headers. The constant is
# only used at offer-creation time.
H265_OFFER_PAYLOAD_TYPE = 103
H265_RTX_OFFER_PAYLOAD_TYPE = 104

H265_MIMETYPES = frozenset({"video/h265", "video/hevc"})

_applied = False


class _NoOpDecoder(Decoder):
    """Discard frames silently.

    We never want decoded frames — the relay forwards raw RTP. This satisfies
    the decoder thread's contract (``decode(JitterFrame) -> list[Frame]``)
    without pulling in libde265 / ffmpeg.
    """

    def decode(self, encoded_frame: JitterFrame) -> list:  # type: ignore[override]
        return []


def _patched_get_decoder(codec):
    if codec.mimeType.lower() in H265_MIMETYPES:
        return _NoOpDecoder()
    return _original_get_decoder(codec)


_original_get_decoder = _codecs.get_decoder


def apply() -> None:
    """Install the H265 patches. Safe to call multiple times."""
    global _applied
    if _applied:
        return

    h265 = RTCRtpCodecParameters(
        mimeType="video/H265",
        clockRate=90000,
        payloadType=H265_OFFER_PAYLOAD_TYPE,
        rtcpFeedback=[
            RTCRtcpFeedback(type="nack"),
            RTCRtcpFeedback(type="nack", parameter="pli"),
            RTCRtcpFeedback(type="goog-remb"),
        ],
        parameters={},
    )
    h265_rtx = RTCRtpCodecParameters(
        mimeType="video/rtx",
        clockRate=90000,
        payloadType=H265_RTX_OFFER_PAYLOAD_TYPE,
        parameters={"apt": H265_OFFER_PAYLOAD_TYPE},
    )

    # Mutate the list in place — peerconnection.py imported the dict object
    # by reference at module load. Skip if a video/H265 entry is already
    # present (idempotent re-apply); otherwise append at the end. We do not
    # care about ordering — Agora picks its own preferred codec in the
    # answer, and ``find_common_codecs`` matches on mimeType not position.
    video_codecs = _codecs.CODECS["video"]
    already_present = any(
        c.mimeType.lower() in H265_MIMETYPES for c in video_codecs
    )
    if not already_present:
        video_codecs.append(h265)
        video_codecs.append(h265_rtx)

    # The receiver module did ``from .codecs import get_decoder`` at import
    # time, so its module-level binding is a copy of the original function.
    # Patching ``_codecs.get_decoder`` alone is not enough; we have to swap
    # the name in the receiver module too.
    _receiver.get_decoder = _patched_get_decoder

    _applied = True
    LOGGER.info(
        "Patched aiortc: H265 (offer PT=%d, RTX PT=%d) accepted as passthrough",
        H265_OFFER_PAYLOAD_TYPE,
        H265_RTX_OFFER_PAYLOAD_TYPE,
    )
