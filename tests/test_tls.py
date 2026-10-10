"""wss:// end to end: a listener with a certificate, clients verifying it with an ``ssl`` context.

The certificate authority is a throwaway one (``trustme``): a client trusting it
gets through, a client trusting only the system's authorities is refused.
"""

import socket
import ssl

import pytest
import trustme

import kbus


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def tls():
    authority = trustme.CA()
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    authority.issue_cert("127.0.0.1").configure_cert(server)
    client = ssl.create_default_context()
    authority.configure_trust(client)
    return server, client


async def echo(message, reply):
    await reply.send(kbus.Message(meta={"seen": message.meta["route"]}, payload=message.payload))


async def echo_payload(message, reply):
    await reply.send(kbus.Message(payload=message.payload))


async def test_core_connect_verifies_the_listener_with_the_given_context(tls):
    server_ssl, client_ssl = tls
    async with kbus.core.listen(f"wss://127.0.0.1:{free_port()}/bus", ssl=server_ssl) as listener:
        client = await kbus.core.connect(listener.address, ssl=client_ssl)
        accepted = await anext(aiter(listener))
        accepted.handler = echo_payload
        reply = await client.call(kbus.Message(payload=b"\x00tls"))
        await client.close()
        await accepted.close()
    assert reply.payload == b"\x00tls"


async def test_a_member_joins_a_dispatcher_over_wss(tls):
    server_ssl, client_ssl = tls
    dispatcher = kbus.Dispatcher(secrets={"billing": "b", "shop": "s"})
    address = await dispatcher.listen(f"wss://127.0.0.1:{free_port()}/bus", ssl=server_ssl)
    billing = kbus.Member("billing", secret="b", handler=echo)
    shop = kbus.Member("shop", secret="s", handler=None)
    try:
        await billing.connect(address, ssl=client_ssl)
        await shop.connect(address, ssl=client_ssl)
        reply = await shop.call("billing.total", kbus.Message(payload=b"42"))
    finally:
        await shop.close()
        await billing.close()
        await dispatcher.close()
    assert (reply.meta["seen"], reply.payload) == ("total", b"42")


async def test_a_member_without_the_authority_is_unreachable(tls):
    server_ssl, _ = tls
    dispatcher = kbus.Dispatcher(secrets={"billing": "b"})
    address = await dispatcher.listen(f"wss://127.0.0.1:{free_port()}/bus", ssl=server_ssl)
    try:
        with pytest.raises(kbus.Unreachable):
            await kbus.Member("billing", secret="b", handler=echo).connect(address)
    finally:
        await dispatcher.close()


async def test_a_link_reaches_its_acceptor_over_wss(tls):
    server_ssl, client_ssl = tls
    port = free_port()
    server = kbus.Dispatcher(secrets={"kb": "k"})
    acceptor = kbus.LinkAcceptor(tokens={"t": "laptop"})
    server.attach_acceptor(acceptor)
    await acceptor.listen(f"wss://127.0.0.1:{port}/link", ssl=server_ssl)
    kb = kbus.Member("kb", secret="k", handler=echo)
    await kb.connect(server)
    laptop = kbus.Dispatcher(secrets={"cli": "x"})
    link = kbus.Link("laptop", token="t", url=f"wss://127.0.0.1:{port}/link", ssl=client_ssl,
                     reconnect=kbus.Reconnect(first=0.05, max=0.2, factor=2.0))
    laptop.attach_link("server", link)
    cli = kbus.Member("cli", secret="x", handler=None)
    await cli.connect(laptop)
    try:
        await link.wait_reachable()
        reply = await cli.call("server:kb.ask", kbus.Message(payload=b"q"))
    finally:
        await cli.close()
        await laptop.close()
        await kb.close()
        await acceptor.close()
        await server.close()
    assert (reply.meta["seen"], reply.payload) == ("ask", b"q")
