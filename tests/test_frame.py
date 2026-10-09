import pytest

import kbus
from kbus.core.frame import HEADER, FrameCodec
from kbus.message import make_message


def test_round_trip():
    codec = FrameCodec(kbus.Limits())
    frame = codec.encode(make_message({"id": "1", "kind": "send", "op": "è"}, b"\x00\xff"))
    message = codec.decode(frame)
    assert message.meta == {"id": "1", "kind": "send", "op": "è"}
    assert message.payload == b"\x00\xff"


def test_encode_over_limit():
    codec = FrameCodec(kbus.Limits(max_frame=10))
    with pytest.raises(kbus.FrameTooLarge):
        codec.encode(kbus.Message(payload=b"x" * 20))


def test_decode_rejects_bad_magic_and_oversize():
    frame = FrameCodec(kbus.Limits()).encode(kbus.Message(payload=b"x" * 20))
    with pytest.raises(kbus.ProtocolError):
        FrameCodec(kbus.Limits()).decode(b"XBUS" + frame[4:])
    with pytest.raises(kbus.ProtocolError):
        FrameCodec(kbus.Limits(max_frame=10)).decode(frame)
    assert len(frame) == HEADER.size + 2 + 20
