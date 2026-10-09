import asyncio

import pytest

import kbus

WORKER_SECRETS = {"alfa_0003": "w3", "alfa_0004": "w4"}


async def worker_handler(message, reply):
    await reply.send(kbus.Message(
        meta={"seen_route": message.meta["route"], "seen_from": message.meta["from"],
              "seen_via": message.meta["via"]},
        payload=message.payload))


async def group_handler(message, reply):
    await reply.send(kbus.Message(meta={"group": message.meta["route"]}))


@pytest.fixture
async def group(members, placement, tmp_path):
    group = await members("alfa", group_handler,
                          dispatcher=kbus.Dispatcher(secrets=WORKER_SECRETS))
    if placement == "inproc":
        target = group.dispatcher
    elif placement == "unix":
        target = await group.dispatcher.listen(f"unix://{tmp_path}/alfa.sock")
    else:
        target = await group.dispatcher.listen("ws://127.0.0.1:0/bus")
    worker = kbus.Member("alfa_0003", secret="w3", handler=worker_handler)
    await worker.connect(target)
    yield group
    await worker.close()


async def test_route_to_nested_member(group, members):
    shop = await members("shop")
    reply = await shop.call("alfa.alfa_0003.http", kbus.Message(payload=b"body"))
    assert reply.meta["seen_route"] == "http"
    assert reply.meta["seen_from"] == "shop"
    assert reply.meta["seen_via"] == ["alfa"]
    assert reply.payload == b"body"


async def test_route_to_the_group_itself(group, members):
    shop = await members("shop")
    reply = await shop.call("alfa.assign", kbus.Message())
    assert reply.meta["group"] == "assign"


async def test_nested_missing_member(group, members):
    shop = await members("shop")
    with pytest.raises(kbus.NoSuchMember):
        await shop.call("alfa.alfa_0004.http", kbus.Message())


async def test_nested_stream(group, members):
    async def feed(message, stream):
        async for incoming in stream:
            await stream.send(kbus.Message(payload=incoming.payload.upper()))
        await stream.close()

    worker = kbus.Member("alfa_0004", secret="w4", handler=feed)
    await worker.connect(group.dispatcher)
    shop = await members("shop")
    async with shop.open("alfa.alfa_0004.feed", kbus.Message()) as stream:
        await stream.send(kbus.Message(payload=b"a"))
        await stream.close()
        assert [m.payload async for m in stream] == [b"A"]
    await worker.close()


async def test_group_on_reply_sees_replies_passing_through(group, members):
    seen = []

    @group.on_reply
    async def observe(meta):
        seen.append(meta["seen_route"])

    shop = await members("shop")
    await shop.call("alfa.alfa_0003.http", kbus.Message())
    await shop.call("alfa.alfa_0003.snapshot", kbus.Message())
    assert seen == ["http", "snapshot"]


async def test_worker_disconnect_gives_link_lost_through_the_group(group, members):
    async def blocking(message, reply):
        await asyncio.Event().wait()

    worker = kbus.Member("alfa_0004", secret="w4", handler=blocking)
    await worker.connect(group.dispatcher)
    shop = await members("shop")
    task = asyncio.create_task(shop.call("alfa.alfa_0004.http", kbus.Message()))
    await asyncio.sleep(0.05)
    await worker.close()
    with pytest.raises(kbus.LinkLost):
        await task


async def test_error_replies_carry_meta_through_on_reply(group, members):
    async def failing(message, reply):
        await reply.fail(ValueError("boom"), meta={"worker_events": ["oom"], "worker_snapshot": 3})

    worker = kbus.Member("alfa_0004", secret="w4", handler=failing)
    await worker.connect(group.dispatcher)
    seen = []

    @group.on_reply
    async def observe(meta):
        seen.append(meta)

    shop = await members("shop")
    with pytest.raises(kbus.RemoteError) as info:
        await shop.call("alfa.alfa_0004.run", kbus.Message())
    assert info.value.type == "ValueError"
    assert info.value.meta["worker_events"] == ["oom"]
    assert info.value.meta["worker_snapshot"] == 3
    assert info.value.meta["kind"] == "error"
    assert info.value.meta["error"] == "ValueError"
    assert seen == [info.value.meta]
    await worker.close()


async def test_kbus_errors_carry_meta_too(group, members):
    shop = await members("shop")
    seen = []

    @shop.on_reply
    async def observe(meta):
        seen.append(meta)

    with pytest.raises(kbus.NoSuchMember) as info:
        await shop.call("alfa.alfa_0004.http", kbus.Message())
    assert info.value.meta["kind"] == "error"
    assert info.value.meta["error"] == "NoSuchMember"
    assert seen == [info.value.meta]
