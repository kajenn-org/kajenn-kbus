import asyncio

import pytest

import kbus


async def blocking(message, reply):
    await asyncio.Event().wait()


async def test_peer_close_gives_link_lost_to_pending_call(pair):
    a, b = pair
    b.handler = blocking
    task = asyncio.create_task(a.call(kbus.Message()))
    await asyncio.sleep(0.05)
    await b.close()
    with pytest.raises(kbus.LinkLost):
        await task


async def test_call_after_close_fails_immediately(pair):
    a, b = pair
    await a.close()
    with pytest.raises(kbus.LinkLost):
        await a.call(kbus.Message())
    with pytest.raises(kbus.LinkLost):
        await a.send(kbus.Message())


async def test_wait_closed_after_orderly_close(pair):
    a, b = pair
    await a.close()
    assert await a.wait_closed() is None
    assert await b.wait_closed() is None
    assert a.closed and b.closed


async def test_overloaded_when_too_many_pending(make_pair):
    a, b = await make_pair(limits_a=kbus.Limits(max_pending=2))
    started = []

    async def handler(message, reply):
        started.append(1)
        await asyncio.Event().wait()

    b.handler = handler
    t1 = asyncio.create_task(a.call(kbus.Message()))
    t2 = asyncio.create_task(a.call(kbus.Message()))
    await asyncio.sleep(0.05)
    with pytest.raises(kbus.Overloaded):
        await a.call(kbus.Message())
    assert len(started) == 2
    t1.cancel()
    t2.cancel()


async def test_frame_too_large_on_send(make_pair):
    a, b = await make_pair(limits_a=kbus.Limits(max_frame=100))
    seen = []

    async def handler(message, reply):
        seen.append(message)
        await reply.send(kbus.Message())

    b.handler = handler
    with pytest.raises(kbus.FrameTooLarge):
        await a.call(kbus.Message(payload=b"x" * 200))
    with pytest.raises(kbus.FrameTooLarge):
        await a.send(kbus.Message(payload=b"x" * 200))
    assert seen == []
    await a.call(kbus.Message(payload=b"x" * 10))
    assert len(seen) == 1


async def test_meta_too_large_on_send(make_pair):
    a, b = await make_pair(limits_a=kbus.Limits(max_meta=64))
    with pytest.raises(kbus.FrameTooLarge):
        await a.send(kbus.Message(meta={"k": "v" * 200}))


async def test_frame_too_large_on_receive_closes_the_connection(make_pair):
    a, b = await make_pair(limits_b=kbus.Limits(max_frame=100))
    seen = []

    async def handler(message, reply):
        seen.append(message)
        await reply.send(kbus.Message())

    b.handler = handler
    with pytest.raises(kbus.LinkLost):
        await a.call(kbus.Message(payload=b"x" * 200))
    reason = await b.wait_closed()
    assert isinstance(reason, kbus.ProtocolError)
    assert seen == []
