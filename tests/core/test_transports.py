import asyncio

import pytest

import kbus


@pytest.fixture
def remote(transport, address):
    if transport == "pipe":
        pytest.skip("listen/connect apply to socket transports only")
    return address


async def test_listener_yields_one_connection_per_connect(remote):
    async with kbus.core.listen(remote) as listener:
        c1 = await kbus.core.connect(listener.address)
        c2 = await kbus.core.connect(listener.address)
        s1 = await anext(aiter(listener))
        s2 = await anext(aiter(listener))
    for conn in (c1, c2, s1, s2):
        await conn.close()


async def test_bound_address_is_concrete(remote):
    async with kbus.core.listen(remote) as listener:
        assert listener.address.startswith(remote.split("://")[0] + "://")
        assert ":0/" not in listener.address


async def test_connect_to_nothing_raises_unreachable(remote, tmp_path):
    if remote.startswith("unix://"):
        target = f"unix://{tmp_path}/nobody.sock"
    else:
        async with kbus.core.listen(remote) as listener:
            target = listener.address
    with pytest.raises(kbus.Unreachable):
        await kbus.core.connect(target)


async def test_listener_close_keeps_accepted_connections(remote):
    listener = kbus.core.listen(remote)
    await listener.__aenter__()
    a = await kbus.core.connect(listener.address)
    b = await anext(aiter(listener))
    target = listener.address
    await listener.close()

    async def handler(message, reply):
        await reply.send(kbus.Message(payload=b"alive"))

    b.handler = handler
    assert (await a.call(kbus.Message())).payload == b"alive"
    with pytest.raises(kbus.Unreachable):
        await kbus.core.connect(target)
    await a.close()
    await b.close()


async def test_peer_process_death_is_link_lost(make_pair, transport):
    if transport == "pipe":
        pytest.skip("no transport to drop in-process")
    a, b = await make_pair()

    async def handler(message, reply):
        await asyncio.Event().wait()

    b.handler = handler
    task = asyncio.create_task(a.call(kbus.Message()))
    await asyncio.sleep(0.05)
    await b.close()
    with pytest.raises(kbus.LinkLost):
        await task


async def test_payload_bytes_survive_the_wire(pair):
    a, b = pair
    payload = bytes(range(256)) * 4096

    async def handler(message, reply):
        await reply.send(message)

    b.handler = handler
    reply = await a.call(kbus.Message(meta={"k": "v"}, payload=payload))
    assert reply.payload == payload
    assert reply.meta["k"] == "v"
