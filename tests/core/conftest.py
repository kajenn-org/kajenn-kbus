"""Two connected endpoints, parametrised on the three transports.

Replaces the Phase 1 version. Every test in tests/core/ goes through
``make_pair`` or ``pair`` and runs once per transport.
"""

import pytest

import kbus

TRANSPORTS = ["pipe", "unix", "ws"]


@pytest.fixture(params=TRANSPORTS)
def transport(request):
    return request.param


@pytest.fixture
def address(transport, tmp_path):
    if transport == "unix":
        return f"unix://{tmp_path}/kbus.sock"
    if transport == "ws":
        return "ws://127.0.0.1:0/bus"
    return None


@pytest.fixture
async def make_pair(transport, address):
    opened = []

    async def make(limits_a=None, limits_b=None):
        if transport == "pipe":
            a, b = kbus.core.pipe(limits_a=limits_a, limits_b=limits_b)
        else:
            async with kbus.core.listen(address, limits=limits_b) as listener:
                a = await kbus.core.connect(listener.address, limits=limits_a)
                b = await anext(aiter(listener))
        opened.append((a, b))
        return a, b

    yield make
    for a, b in opened:
        await a.close()
        await b.close()


@pytest.fixture
async def pair(make_pair):
    return await make_pair()
