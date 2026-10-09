import asyncio

import pytest

import kbus


async def ok(message, reply):
    await reply.send(kbus.Message(meta={"answer": 1}))


async def test_on_message_sees_meta_and_can_refuse(dispatcher, members):
    seen = []

    @dispatcher.on_message
    async def policy(meta):
        seen.append(dict(meta))
        if meta["route"].startswith("billing.admin") and meta["from"] != "root":
            raise kbus.Refused("admin only")

    calls = []

    async def handler(message, reply):
        calls.append(message.meta["route"])
        await reply.send(kbus.Message())

    await members("billing", handler)
    shop = await members("shop")
    root = await members("root")
    with pytest.raises(kbus.Refused):
        await shop.call("billing.admin.reset", kbus.Message())
    await root.call("billing.admin.reset", kbus.Message())
    await shop.call("billing.total", kbus.Message())
    assert calls == ["admin.reset", "total"]
    assert [(m["from"], m["route"]) for m in seen] == [
        ("shop", "billing.admin.reset"), ("root", "billing.admin.reset"), ("shop", "billing.total"),
    ]


async def test_hooks_cannot_change_meta(dispatcher, members):
    @dispatcher.on_message
    async def tamper(meta):
        meta["from"] = "root"
        meta["user"] = "mallory"

    seen = {}

    async def handler(message, reply):
        seen.update(message.meta)
        await reply.send(kbus.Message())

    await members("billing", handler)
    shop = await members("shop")
    await shop.call("billing.total", kbus.Message(meta={"user": "giovanni"}))
    assert seen["from"] == "shop"
    assert seen["user"] == "giovanni"


async def test_on_reply_sees_every_reply_before_the_caller(members):
    await members("billing", ok)
    shop = await members("shop")
    order = []

    @shop.on_reply
    async def observe(meta):
        order.append(("hook", meta["answer"]))

    reply = await shop.call("billing.total", kbus.Message())
    order.append(("caller", reply.meta["answer"]))
    assert order == [("hook", 1), ("caller", 1)]


async def test_on_reply_cannot_change_nested_meta(members):
    async def nested(message, reply):
        await reply.send(kbus.Message(meta={"data": {"x": 1}}))

    await members("billing", nested)
    shop = await members("shop")

    @shop.on_reply
    async def tamper(meta):
        meta["data"]["x"] = 999

    reply = await shop.call("billing.total", kbus.Message())
    assert reply.meta["data"] == {"x": 1}


async def test_on_disconnect_reports_name_and_reason(dispatcher, attach):
    gone = asyncio.Queue()

    @dispatcher.on_disconnect
    async def on_gone(name, reason):
        await gone.put((name, reason))

    billing = kbus.Member("billing", secret="b-secret", handler=ok)
    await billing.connect(attach)
    await billing.close()
    name, reason = await asyncio.wait_for(gone.get(), 1)
    assert name == "billing"
    assert reason is None
