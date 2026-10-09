"""Two applications: ``laptop`` opens a Link to ``server``, which accepts by token."""

import socket

import pytest

import kbus

TOKEN = "t-laptop"
FAST = kbus.Reconnect(first=0.05, max=0.2, factor=2.0)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def kb_handler(message, reply):
    await reply.send(kbus.Message(meta={"seen_route": message.meta["route"],
                                        "seen_from": message.meta["from"],
                                        "user": message.meta.get("user")}))


async def claude_handler(message, reply):
    await reply.send(kbus.Message(meta={"seen_route": message.meta["route"],
                                        "seen_from": message.meta["from"]}))


@pytest.fixture
def port():
    return free_port()


@pytest.fixture
async def server(port):
    dispatcher = kbus.Dispatcher(secrets={"kb": "k", "ops": "o"})
    acceptor = kbus.LinkAcceptor(tokens={TOKEN: "laptop"})
    dispatcher.attach_acceptor(acceptor)
    await acceptor.listen(f"ws://127.0.0.1:{port}/link")
    kb = kbus.Member("kb", secret="k", handler=kb_handler)
    await kb.connect(dispatcher)
    ops = kbus.Member("ops", secret="o", handler=None)
    await ops.connect(dispatcher)
    yield {"dispatcher": dispatcher, "acceptor": acceptor, "kb": kb, "ops": ops}
    await ops.close()
    await kb.close()
    await acceptor.close()
    await dispatcher.close()


@pytest.fixture
async def laptop(port):
    dispatcher = kbus.Dispatcher(secrets={"claude": "c", "cli": "x"})
    link = kbus.Link("laptop", token=TOKEN, url=f"ws://127.0.0.1:{port}/link", reconnect=FAST)
    dispatcher.attach_link("server", link)
    claude = kbus.Member("claude", secret="c", handler=claude_handler)
    await claude.connect(dispatcher)
    cli = kbus.Member("cli", secret="x", handler=None)
    await cli.connect(dispatcher)
    yield {"dispatcher": dispatcher, "link": link, "claude": claude, "cli": cli}
    await cli.close()
    await claude.close()
    await link.close()
    await dispatcher.close()
