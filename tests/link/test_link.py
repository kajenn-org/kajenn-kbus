import asyncio

import pytest

import kbus

from .conftest import FAST, TOKEN


async def test_laptop_calls_server(server, laptop):
    await asyncio.wait_for(laptop["link"].wait_reachable(), 2)
    assert laptop["link"].reachable
    reply = await laptop["cli"].call("server:kb.ask", kbus.Message(meta={"user": "giovanni"}))
    assert reply.meta["seen_route"] == "ask"
    assert reply.meta["seen_from"] == "laptop"
    assert reply.meta["user"] == "giovanni"


async def test_server_calls_laptop_on_the_same_connection(server, laptop):
    await asyncio.wait_for(laptop["link"].wait_reachable(), 2)
    reply = await server["ops"].call("laptop:claude.notify", kbus.Message())
    assert reply.meta["seen_route"] == "notify"
    assert reply.meta["seen_from"] == "server"


async def test_both_directions_at_once(server, laptop):
    await asyncio.wait_for(laptop["link"].wait_reachable(), 2)
    up = [laptop["cli"].call("server:kb.ask", kbus.Message()) for _ in range(5)]
    down = [server["ops"].call("laptop:claude.notify", kbus.Message()) for _ in range(5)]
    replies = await asyncio.gather(*up, *down)
    assert [r.meta["seen_from"] for r in replies] == ["laptop"] * 5 + ["server"] * 5


async def test_stream_across_the_link(server, laptop):
    async def feed(message, stream):
        async for incoming in stream:
            await stream.send(kbus.Message(payload=incoming.payload[::-1]))
        await stream.close()

    server["kb"].handler = feed
    await asyncio.wait_for(laptop["link"].wait_reachable(), 2)
    async with laptop["cli"].open("server:kb.feed", kbus.Message()) as stream:
        await stream.send(kbus.Message(payload=b"abc"))
        await stream.close()
        assert [m.payload async for m in stream] == [b"cba"]


async def test_unknown_instance_is_immediate(server, laptop):
    with pytest.raises(kbus.NoSuchInstance):
        async with asyncio.timeout(1):
            await laptop["cli"].call("mars:kb.ask", kbus.Message())


async def test_members_of_the_other_side_are_not_local(server, laptop):
    await asyncio.wait_for(laptop["link"].wait_reachable(), 2)
    with pytest.raises(kbus.NoSuchMember):
        await laptop["cli"].call("kb.ask", kbus.Message())


async def test_link_down_is_unreachable_and_reconnects(server, laptop, port):
    link = laptop["link"]
    await asyncio.wait_for(link.wait_reachable(), 2)
    await server["acceptor"].close()
    await asyncio.wait_for(link.wait_unreachable(), 2)
    assert not link.reachable
    with pytest.raises(kbus.Unreachable):
        async with asyncio.timeout(1):
            await laptop["cli"].call("server:kb.ask", kbus.Message())
    acceptor = kbus.LinkAcceptor(tokens={TOKEN: "laptop"})
    server["dispatcher"].attach_acceptor(acceptor)
    await acceptor.listen(f"ws://127.0.0.1:{port}/link")
    try:
        await asyncio.wait_for(link.wait_reachable(), 5)
        reply = await laptop["cli"].call("server:kb.ask", kbus.Message())
        assert reply.meta["seen_from"] == "laptop"
    finally:
        await acceptor.close()


async def test_pending_calls_get_link_lost_when_the_link_drops(server, laptop):
    async def blocking(message, reply):
        await asyncio.Event().wait()

    server["kb"].handler = blocking
    link = laptop["link"]
    await asyncio.wait_for(link.wait_reachable(), 2)
    task = asyncio.create_task(laptop["cli"].call("server:kb.ask", kbus.Message()))
    await asyncio.sleep(0.1)
    await server["acceptor"].close()
    with pytest.raises(kbus.LinkLost):
        await asyncio.wait_for(task, 2)


async def test_wrong_token_is_rejected(server, port):
    dispatcher = kbus.Dispatcher(secrets={})
    link = kbus.Link("intruder", token="nope", url=f"ws://127.0.0.1:{port}/link",
                     reconnect=kbus.Reconnect(first=0.05, max=0.1, factor=2.0))
    dispatcher.attach_link("server", link)
    with pytest.raises(kbus.Rejected):
        await asyncio.wait_for(link.wait_reachable(), 2)
    assert not link.reachable
    await link.close()
    await dispatcher.close()


async def test_reconnect_defaults():
    r = kbus.Reconnect()
    assert (r.first, r.max, r.factor) == (1.0, 30.0, 2.0)


async def test_duplicate_token_waits_for_the_first_link_to_drop(server, laptop, port):
    await asyncio.wait_for(laptop["link"].wait_reachable(), 2)
    other = kbus.Dispatcher(secrets={})
    second = kbus.Link("laptop-bis", token=TOKEN, url=f"ws://127.0.0.1:{port}/link",
                       reconnect=kbus.Reconnect(first=0.05, max=0.1, factor=2.0))
    other.attach_link("server", second)
    await asyncio.sleep(0.3)
    assert not second.reachable
    assert laptop["link"].reachable
    assert (await laptop["cli"].call("server:kb.ask", kbus.Message())).meta["seen_from"] == "laptop"
    await laptop["link"].close()
    await asyncio.wait_for(second.wait_reachable(), 2)
    assert second.reachable
    await other.close()


async def test_route_over_a_link_cannot_name_an_instance(server, laptop):
    await asyncio.wait_for(laptop["link"].wait_reachable(), 2)
    with pytest.raises(kbus.Refused):
        await laptop["cli"].call("server:laptop:claude.notify", kbus.Message())


async def test_dispatcher_close_closes_attached_links(server, port):
    dispatcher = kbus.Dispatcher(secrets={})
    link = kbus.Link("laptop", token=TOKEN, url=f"ws://127.0.0.1:{port}/link", reconnect=FAST)
    dispatcher.attach_link("server", link)
    await asyncio.wait_for(link.wait_reachable(), 2)
    await dispatcher.close()
    assert not link.reachable
    await asyncio.wait_for(link.wait_unreachable(), 1)
