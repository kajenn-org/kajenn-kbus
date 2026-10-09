import asyncio

import pytest

import kbus


async def echo(message, reply):
    await reply.send(kbus.Message(meta={"seen": message.meta["op"]}, payload=message.payload))


async def test_call_round_trip(pair):
    a, b = pair
    b.handler = echo
    reply = await a.call(kbus.Message(meta={"op": "ping"}, payload=b"\x01\x02"))
    assert reply.meta["seen"] == "ping"
    assert reply.payload == b"\x01\x02"


async def test_handler_sees_kind_and_id(pair):
    a, b = pair
    seen = {}

    async def handler(message, reply):
        seen.update(message.meta)
        await reply.send(kbus.Message())

    b.handler = handler
    await a.call(kbus.Message(meta={"op": "x"}))
    assert seen["kind"] == "call"
    assert isinstance(seen["id"], str) and seen["id"]
    assert seen["op"] == "x"


async def test_ids_are_unique(pair):
    a, b = pair
    ids = []

    async def handler(message, reply):
        ids.append(message.meta["id"])
        await reply.send(kbus.Message())

    b.handler = handler
    for _ in range(5):
        await a.call(kbus.Message())
    assert len(set(ids)) == 5


async def test_reply_fail_becomes_remote_error(pair):
    a, b = pair

    async def handler(message, reply):
        await reply.fail(ValueError("bad order"))

    b.handler = handler
    with pytest.raises(kbus.RemoteError) as info:
        await a.call(kbus.Message())
    assert info.value.type == "ValueError"
    assert info.value.message == "bad order"
    assert isinstance(info.value.traceback, str)


async def test_handler_exception_becomes_remote_error(pair):
    a, b = pair

    async def handler(message, reply):
        raise KeyError("missing")

    b.handler = handler
    with pytest.raises(kbus.RemoteError) as info:
        await a.call(kbus.Message())
    assert info.value.type == "KeyError"
    assert "KeyError" in info.value.traceback


async def test_handler_returning_without_reply(pair):
    a, b = pair

    async def handler(message, reply):
        return None

    b.handler = handler
    with pytest.raises(kbus.RemoteError) as info:
        await a.call(kbus.Message())
    assert info.value.type == "NoReply"


async def test_many_calls_replies_in_any_order(pair):
    a, b = pair

    async def handler(message, reply):
        n = message.meta["n"]
        await asyncio.sleep(0.01 * (5 - n))
        await reply.send(kbus.Message(meta={"n": n}))

    b.handler = handler
    replies = await asyncio.gather(*(a.call(kbus.Message(meta={"n": n})) for n in range(5)))
    assert [r.meta["n"] for r in replies] == [0, 1, 2, 3, 4]


async def test_both_directions(pair):
    a, b = pair

    async def a_handler(message, reply):
        await reply.send(kbus.Message(meta={"who": "a"}))

    async def b_handler(message, reply):
        inner = await b.call(kbus.Message(meta={"op": "callback"}))
        await reply.send(kbus.Message(meta={"who": "b", "inner": inner.meta["who"]}))

    a.handler = a_handler
    b.handler = b_handler
    reply = await a.call(kbus.Message(meta={"op": "outer"}))
    assert reply.meta == {"who": "b", "inner": "a"}


async def test_timeout_cancels_the_handler(pair):
    a, b = pair
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(message, reply):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    b.handler = handler
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.2):
            await a.call(kbus.Message())
    await asyncio.wait_for(cancelled.wait(), 1)


async def test_cancelled_error_is_the_standard_one(pair):
    a, b = pair

    async def handler(message, reply):
        await asyncio.Event().wait()

    b.handler = handler
    task = asyncio.create_task(a.call(kbus.Message()))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError) as info:
        await task
    assert type(info.value) is asyncio.CancelledError
