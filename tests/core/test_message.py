import pytest

import kbus


def test_defaults():
    message = kbus.Message()
    assert message.meta == {}
    assert message.payload == b""


def test_meta_and_payload_are_kept():
    message = kbus.Message(meta={"op": "http", "n": 3}, payload=b"\x00\xff")
    assert message.meta == {"op": "http", "n": 3}
    assert message.payload == b"\x00\xff"


def test_payload_only():
    assert kbus.Message(payload=b"chunk").meta == {}


@pytest.mark.parametrize("key", ["id", "kind", "route", "from", "via"])
def test_reserved_meta_keys_are_refused(key):
    with pytest.raises(ValueError):
        kbus.Message(meta={key: "x"})


def test_meta_must_be_a_dict():
    with pytest.raises(TypeError):
        kbus.Message(meta=["op"])


def test_payload_must_be_bytes():
    with pytest.raises(TypeError):
        kbus.Message(payload="text")


def test_errors_share_one_base():
    for name in (
        "RemoteError", "NoSuchMember", "NoSuchRoute", "NoSuchInstance", "Refused",
        "LinkLost", "Unreachable", "FrameTooLarge", "ProtocolError", "Overloaded",
        "Rejected", "Aborted",
    ):
        assert issubclass(getattr(kbus, name), kbus.Error)
    assert issubclass(kbus.Error, Exception)


def test_limits_defaults():
    limits = kbus.Limits()
    assert limits.max_frame == 16 * 2**20
    assert limits.max_meta == 64 * 2**10
    assert limits.max_route == 4 * 2**10
    assert limits.max_pending == 1024
    assert limits.stream_window == 32
    assert limits.write_buffer == 4 * 2**20
