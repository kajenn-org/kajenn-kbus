import asyncio

import pytest

import kbus


async def echo(message, reply):
    await reply.send(kbus.Message(meta={"seen_route": message.meta["route"],
                                        "seen_from": message.meta["from"]},
                                  payload=message.payload))


async def test_listen_returns_a_concrete_address(dispatcher, tmp_path):
    address = await dispatcher.listen(f"unix://{tmp_path}/a.sock")
    assert address == f"unix://{tmp_path}/a.sock"
    ws = await dispatcher.listen("ws://127.0.0.1:0/bus")
    assert ws.startswith("ws://127.0.0.1:") and ":0/" not in ws


async def test_route_call(members):
    await members("billing", echo)
    shop = await members("shop")
    reply = await shop.call("billing.total", kbus.Message(payload=b'{"order": 4}'))
    assert reply.meta["seen_route"] == "total"
    assert reply.meta["seen_from"] == "shop"
    assert reply.payload == b'{"order": 4}'


async def test_route_without_remainder(members):
    await members("billing", echo)
    shop = await members("shop")
    reply = await shop.call("billing", kbus.Message())
    assert reply.meta["seen_route"] == ""


async def test_dispatcher_writes_kind_from_route_via(members):
    seen = {}

    async def handler(message, reply):
        seen.update(message.meta)
        await reply.send(kbus.Message())

    await members("billing", handler)
    shop = await members("shop")
    await shop.call("billing.a.b", kbus.Message(meta={"user": "giovanni"}))
    assert seen["kind"] == "call"
    assert seen["route"] == "a.b"
    assert seen["from"] == "shop"
    assert seen["via"] == []
    assert seen["user"] == "giovanni"


async def test_no_such_member_is_immediate(members):
    shop = await members("shop")
    with pytest.raises(kbus.NoSuchMember):
        async with asyncio.timeout(1):
            await shop.call("nobody.x", kbus.Message())


async def test_route_send(members):
    got = asyncio.Queue()

    async def handler(message, second):
        await got.put((message.meta["route"], message.meta["from"], second))

    await members("billing", handler)
    shop = await members("shop")
    await shop.send("billing.note", kbus.Message())
    assert await asyncio.wait_for(got.get(), 1) == ("note", "shop", None)


async def test_route_open(members):
    async def handler(message, stream):
        assert message.meta["route"] == "feed"
        assert message.meta["from"] == "shop"
        async for incoming in stream:
            await stream.send(incoming)
        await stream.close()

    await members("billing", handler)
    shop = await members("shop")
    async with shop.open("billing.feed", kbus.Message()) as stream:
        await stream.send(kbus.Message(payload=b"a"))
        await stream.send(kbus.Message(payload=b"b"))
        await stream.close()
        assert [m.payload async for m in stream] == [b"a", b"b"]


async def test_wrong_secret_is_rejected(attach):
    member = kbus.Member("shop", secret="wrong", handler=None)
    with pytest.raises(kbus.Rejected):
        await member.connect(attach)


async def test_unknown_name_is_rejected(attach):
    member = kbus.Member("ghost", secret="whatever", handler=None)
    with pytest.raises(kbus.Rejected):
        await member.connect(attach)


async def test_unknown_name_without_secret_is_rejected(attach):
    member = kbus.Member("ghost", secret=None, handler=None)
    with pytest.raises(kbus.Rejected):
        await member.connect(attach)


async def test_dispatcher_closes_right_after_a_member_closes(dispatcher, attach):
    billing = kbus.Member("billing", secret="b-secret", handler=None)
    await billing.connect(attach)
    await billing.close()
    async with asyncio.timeout(5):
        await dispatcher.close()


async def test_name_in_use_is_rejected_until_disconnect(attach, members):
    first = await members("shop")
    second = kbus.Member("shop", secret="s-secret", handler=None)
    with pytest.raises(kbus.Rejected):
        await second.connect(attach)
    await first.close()
    await second.connect(attach)
    await second.close()


async def test_member_handler_errors_become_remote_error(members):
    async def handler(message, reply):
        raise ValueError("no")

    await members("billing", handler)
    shop = await members("shop")
    with pytest.raises(kbus.RemoteError) as info:
        await shop.call("billing.total", kbus.Message())
    assert info.value.type == "ValueError"


async def test_member_disconnect_with_pending_calls(attach, members):
    async def handler(message, reply):
        await asyncio.Event().wait()

    billing = kbus.Member("billing", secret="b-secret", handler=handler)
    await billing.connect(attach)
    shop = await members("shop")
    task = asyncio.create_task(shop.call("billing.total", kbus.Message()))
    await asyncio.sleep(0.05)
    await billing.close()
    with pytest.raises(kbus.LinkLost):
        await task
    with pytest.raises(kbus.NoSuchMember):
        await shop.call("billing.total", kbus.Message())


async def test_inproc_member_uses_dispatcher_limits(placement, tmp_path):
    if placement != "inproc":
        pytest.skip("the dispatcher's limits apply to in-process members")
    dispatcher = kbus.Dispatcher(secrets={"shop": "s", "billing": "b"},
                                 limits=kbus.Limits(max_pending=1))
    started = []

    async def handler(message, reply):
        started.append(1)
        await asyncio.Event().wait()

    billing = kbus.Member("billing", secret="b", handler=handler)
    await billing.connect(dispatcher)
    shop = kbus.Member("shop", secret="s", handler=None)
    await shop.connect(dispatcher)
    first = asyncio.create_task(shop.call("billing.x", kbus.Message()))
    await asyncio.sleep(0.05)
    with pytest.raises(kbus.Overloaded):
        await shop.call("billing.x", kbus.Message())
    assert started == [1]
    first.cancel()
    await shop.close()
    await billing.close()
    await dispatcher.close()


async def test_route_over_max_route_is_not_sent(members):
    seen = []

    async def handler(message, reply):
        seen.append(message)
        await reply.send(kbus.Message())

    await members("billing", handler)
    shop = await members("shop", limits=kbus.Limits(max_route=16))
    with pytest.raises(kbus.FrameTooLarge):
        await shop.call("billing." + "x" * 100, kbus.Message())
    await shop.call("billing.total", kbus.Message())
    assert len(seen) == 1


async def test_route_over_max_route_on_receive_closes_the_connection(placement, tmp_path):
    if placement == "inproc":
        pytest.skip("an in-process member sends with the dispatcher's limits")
    dispatcher = kbus.Dispatcher(secrets={"shop": "s", "billing": "b"},
                                 limits=kbus.Limits(max_route=16))
    if placement == "unix":
        address = await dispatcher.listen(f"unix://{tmp_path}/app.sock")
    else:
        address = await dispatcher.listen("ws://127.0.0.1:0/bus")
    seen = []

    async def handler(message, reply):
        seen.append(message)
        await reply.send(kbus.Message())

    billing = kbus.Member("billing", secret="b", handler=handler)
    await billing.connect(address)
    shop = kbus.Member("shop", secret="s", handler=None)
    await shop.connect(address)
    with pytest.raises(kbus.LinkLost):
        await shop.call("billing." + "x" * 100, kbus.Message())
    assert seen == []
    await shop.close()
    await billing.close()
    await dispatcher.close()
