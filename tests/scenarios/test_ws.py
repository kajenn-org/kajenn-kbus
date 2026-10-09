import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK, InvalidStatus

from .conftest import wait_stats


async def test_echo_text_and_bytes(http_server, running_app):
    async with connect(f"ws://{http_server}/ws") as ws:
        await ws.send("hi")
        assert await ws.recv() == "echo:hi"
        await ws.send(b"\x00\x01")
        assert await ws.recv() == b"echo:\x00\x01"


async def test_client_closes(http_server, running_app, gateway_member):
    async with connect(f"ws://{http_server}/ws") as ws:
        await ws.send("hi")
        await ws.recv()
    await wait_stats(gateway_member, ws_closed_by_client=1)


async def test_app_closes(http_server, running_app):
    async with connect(f"ws://{http_server}/ws") as ws:
        await ws.send("bye")
        with pytest.raises(ConnectionClosedOK):
            await ws.recv()
        assert ws.close_code == 1000


async def test_app_dying_mid_session_closes_the_socket(http_server, running_app):
    if not running_app.can_die:
        pytest.skip("the in-process app cannot be killed")
    async with connect(f"ws://{http_server}/ws") as ws:
        await ws.send("hi")
        assert await ws.recv() == "echo:hi"
        await running_app.kill()
        with pytest.raises(ConnectionClosed):
            await ws.recv()
        assert ws.close_code == 1011


async def test_app_absent_is_refused_at_the_handshake(http_server):
    with pytest.raises(InvalidStatus) as info:
        async with connect(f"ws://{http_server}/ws"):
            pass
    assert info.value.response.status_code == 403
