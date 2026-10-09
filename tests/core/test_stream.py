import asyncio

import pytest

import kbus


async def collect(stream):
    return [m.payload for m in [x async for x in stream]]


async def test_open_delivers_both_directions_in_order(pair):
    a, b = pair
    seen = {}

    async def handler(message, stream):
        seen["kind"] = message.meta["kind"]
        seen["op"] = message.meta["op"]
        seen["in"] = await collect(stream)
        for i in range(2):
            await stream.send(kbus.Message(payload=b"back%d" % i))
        await stream.close()

    b.handler = handler
    async with a.open(kbus.Message(meta={"op": "up"})) as stream:
        for i in range(3):
            await stream.send(kbus.Message(payload=b"out%d" % i))
        await stream.close()
        back = await collect(stream)
    assert seen == {"kind": "open", "op": "up", "in": [b"out0", b"out1", b"out2"]}
    assert back == [b"back0", b"back1"]


async def test_nothing_lost_over_many_messages(pair):
    a, b = pair

    async def handler(message, stream):
        async for incoming in stream:
            await stream.send(incoming)
        await stream.close()

    b.handler = handler
    sent = [b"%05d" % i for i in range(500)]
    async with a.open(kbus.Message()) as stream:

        async def producer():
            for payload in sent:
                await stream.send(kbus.Message(payload=payload))
            await stream.close()

        task = asyncio.create_task(producer())
        back = await collect(stream)
        await task
    assert back == sent


async def test_stream_messages_carry_meta(pair):
    a, b = pair

    async def handler(message, stream):
        async for incoming in stream:
            await stream.send(kbus.Message(meta={"echo": incoming.meta["n"]}))
        await stream.close()

    b.handler = handler
    async with a.open(kbus.Message()) as stream:
        await stream.send(kbus.Message(meta={"n": 7}))
        await stream.close()
        back = [m.meta["echo"] async for m in stream]
    assert back == [7]


async def test_sender_waits_when_window_is_full(make_pair):
    a, b = await make_pair(limits_a=kbus.Limits(stream_window=2))
    release = asyncio.Event()
    received = []

    async def handler(message, stream):
        await release.wait()
        async for incoming in stream:
            received.append(incoming.payload)
        await stream.close()

    b.handler = handler
    async with a.open(kbus.Message()) as stream:
        await stream.send(kbus.Message(payload=b"1"))
        await stream.send(kbus.Message(payload=b"2"))
        third = asyncio.create_task(stream.send(kbus.Message(payload=b"3")))
        await asyncio.sleep(0.2)
        assert not third.done()
        release.set()
        await asyncio.wait_for(third, 1)
        await stream.close()
        await collect(stream)
    assert received == [b"1", b"2", b"3"]


async def test_close_ends_only_my_direction(pair):
    a, b = pair
    client_closed = asyncio.Event()

    async def handler(message, stream):
        assert await collect(stream) == []
        await client_closed.wait()
        await stream.send(kbus.Message(payload=b"late"))
        await stream.close()

    b.handler = handler
    async with a.open(kbus.Message()) as stream:
        await stream.close()
        client_closed.set()
        assert await collect(stream) == [b"late"]
        assert stream.finished


async def test_abort_reaches_the_other_side(pair):
    a, b = pair
    outcome = {}
    done = asyncio.Event()

    async def handler(message, stream):
        try:
            await collect(stream)
        except kbus.Aborted as exc:
            outcome["iter"] = exc.reason
        try:
            await stream.send(kbus.Message())
        except kbus.Aborted as exc:
            outcome["send"] = exc.reason
        done.set()

    b.handler = handler
    async with a.open(kbus.Message()) as stream:
        await stream.send(kbus.Message())
        await stream.abort("changed my mind")
        assert stream.finished
    await asyncio.wait_for(done.wait(), 1)
    assert outcome == {"iter": "changed my mind", "send": "changed my mind"}


async def test_leaving_the_block_unfinished_aborts(pair):
    a, b = pair
    outcome = {}
    done = asyncio.Event()

    async def handler(message, stream):
        try:
            await collect(stream)
        except kbus.Aborted as exc:
            outcome["reason"] = exc.reason
        done.set()

    b.handler = handler
    async with a.open(kbus.Message()) as stream:
        await stream.send(kbus.Message())
    await asyncio.wait_for(done.wait(), 1)
    assert isinstance(outcome["reason"], str)


async def test_handler_returning_unfinished_aborts(pair):
    a, b = pair

    async def handler(message, stream):
        await stream.send(kbus.Message(payload=b"one"))

    b.handler = handler
    async with a.open(kbus.Message()) as stream:
        first = await anext(aiter(stream))
        assert first.payload == b"one"
        with pytest.raises(kbus.Aborted):
            await collect(stream)


async def test_connection_drop_gives_link_lost_on_both_sides(pair):
    a, b = pair
    outcome = {}
    done = asyncio.Event()

    async def handler(message, stream):
        try:
            await collect(stream)
        except kbus.LinkLost:
            outcome["b"] = True
        done.set()

    b.handler = handler
    async with a.open(kbus.Message()) as stream:
        await stream.send(kbus.Message())
        await asyncio.sleep(0.05)
        await a.close()
        with pytest.raises(kbus.LinkLost):
            await collect(stream)
    await asyncio.wait_for(done.wait(), 1)
    assert outcome == {"b": True}


async def test_streams_and_calls_share_one_connection(pair):
    a, b = pair

    async def handler(message, second):
        if message.meta["kind"] == "call":
            await second.send(kbus.Message(payload=b"reply"))
            return
        async for incoming in second:
            await second.send(incoming)
        await second.close()

    b.handler = handler
    async with a.open(kbus.Message()) as s1, a.open(kbus.Message()) as s2:
        await s1.send(kbus.Message(payload=b"s1"))
        reply = await a.call(kbus.Message())
        await s2.send(kbus.Message(payload=b"s2"))
        await s1.close()
        await s2.close()
        assert await collect(s1) == [b"s1"]
        assert await collect(s2) == [b"s2"]
    assert reply.payload == b"reply"


async def test_cancelling_the_opener_aborts_the_stream(pair):
    a, b = pair
    outcome = {}
    done = asyncio.Event()

    async def handler(message, stream):
        try:
            await collect(stream)
        except kbus.Aborted:
            outcome["aborted"] = True
        done.set()

    b.handler = handler

    async def opener():
        async with a.open(kbus.Message()) as stream:
            await stream.send(kbus.Message())
            await asyncio.Event().wait()

    task = asyncio.create_task(opener())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(done.wait(), 1)
    assert outcome == {"aborted": True}


async def test_open_to_a_closed_connection_fails_immediately(pair):
    a, b = pair
    await b.close()
    await a.wait_closed()
    with pytest.raises(kbus.LinkLost):
        async with a.open(kbus.Message()):
            pass


async def test_stream_frame_too_large(make_pair):
    a, b = await make_pair(limits_a=kbus.Limits(max_frame=100))

    async def handler(message, stream):
        await collect(stream)
        await stream.close()

    b.handler = handler
    async with a.open(kbus.Message()) as stream:
        with pytest.raises(kbus.FrameTooLarge):
            await stream.send(kbus.Message(payload=b"x" * 200))
        await stream.close()
        await collect(stream)
