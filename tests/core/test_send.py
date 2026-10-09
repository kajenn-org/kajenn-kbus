import asyncio

import kbus


async def test_send_delivers_without_reply(pair):
    a, b = pair
    got = asyncio.Queue()

    async def handler(message, second):
        await got.put((message.meta, message.payload, second))

    b.handler = handler
    await a.send(kbus.Message(meta={"op": "note"}, payload=b"n"))
    meta, payload, second = await asyncio.wait_for(got.get(), 1)
    assert meta["kind"] == "send"
    assert meta["op"] == "note"
    assert payload == b"n"
    assert second is None


async def test_handler_error_on_send_stays_on_the_other_side(pair):
    a, b = pair
    calls = []

    async def handler(message, second):
        calls.append(message.meta["kind"])
        if message.meta["kind"] == "send":
            raise RuntimeError("logged there")
        await second.send(kbus.Message())

    b.handler = handler
    await a.send(kbus.Message())
    reply = await a.call(kbus.Message())
    assert reply.payload == b""
    assert calls == ["send", "call"]
