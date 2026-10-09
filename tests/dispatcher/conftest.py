"""A dispatcher and a way to connect members to it, parametrised on placement.

``attach`` is what a Member.connect() receives: the Dispatcher object itself,
a unix address, or a ws address. Tests never name a placement.
"""

import pytest

import kbus

PLACEMENTS = ["inproc", "unix", "ws"]
SECRETS = {
    "shop": "s-secret",
    "billing": "b-secret",
    "root": "r-secret",
    "alfa": "a-secret",
    "kb": "k-secret",
}


@pytest.fixture(params=PLACEMENTS)
def placement(request):
    return request.param


@pytest.fixture
async def dispatcher():
    dispatcher = kbus.Dispatcher(secrets=SECRETS)
    yield dispatcher
    await dispatcher.close()


@pytest.fixture
async def attach(placement, dispatcher, tmp_path):
    if placement == "inproc":
        return dispatcher
    if placement == "unix":
        return await dispatcher.listen(f"unix://{tmp_path}/app.sock")
    return await dispatcher.listen("ws://127.0.0.1:0/bus")


@pytest.fixture
async def members(attach):
    opened = []

    async def make(name, handler=None, secret=None, **kwargs):
        member = kbus.Member(name, secret=secret or SECRETS[name], handler=handler, **kwargs)
        await member.connect(attach)
        opened.append(member)
        return member

    yield make
    for member in reversed(opened):
        await member.close()
