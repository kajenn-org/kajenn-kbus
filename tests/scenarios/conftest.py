"""Scenario 1 of docs/design/scenarios.md: a real HTTP server in front of kbus.

Three placements of the application, same gateway code:

- ``inproc``: the App lives in the test process, member connected to the
  Dispatcher object.
- ``unix``: the App is a child process, member connected over a Unix socket.
- ``ws``: the App is a child process, member connected over WebSocket.

The HTTP server is uvicorn on a free port, serving ``gateway.Gateway``.
"""

import asyncio
import json
import os
import socket
import sys
from pathlib import Path

import httpx
import pytest
import uvicorn

import kbus

from .app import App
from .gateway import Gateway

PLACEMENTS = ["inproc", "unix", "ws"]
SECRETS = {"app": "a-secret", "gw": "g-secret"}
APP_SCRIPT = Path(__file__).with_name("app.py")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class AppHandle:
    def __init__(self, process=None):
        self.process = process

    @property
    def can_die(self):
        return self.process is not None

    async def kill(self):
        self.process.kill()
        await self.process.wait()

    async def stop(self):
        if self.process is not None and self.process.returncode is None:
            self.process.kill()
            await self.process.wait()


@pytest.fixture(params=PLACEMENTS)
def placement(request):
    return request.param


@pytest.fixture
async def dispatcher():
    dispatcher = kbus.Dispatcher(secrets=SECRETS)
    yield dispatcher
    await dispatcher.close()


@pytest.fixture
async def address(placement, dispatcher, tmp_path):
    if placement == "inproc":
        return None
    if placement == "unix":
        return await dispatcher.listen(f"unix://{tmp_path}/app.sock")
    return await dispatcher.listen("ws://127.0.0.1:0/bus")


@pytest.fixture
async def gateway_member(dispatcher):
    member = kbus.Member("gw", secret=SECRETS["gw"], handler=None)
    await member.connect(dispatcher)
    yield member
    await member.close()


@pytest.fixture
async def http_server(gateway_member):
    port = free_port()
    config = uvicorn.Config(Gateway(gateway_member), host="127.0.0.1", port=port,
                            log_level="warning", lifespan="off")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    yield f"127.0.0.1:{port}"
    server.should_exit = True
    await task


@pytest.fixture
async def app_process(placement, dispatcher, address):
    if placement == "inproc":
        member = kbus.Member("app", secret=SECRETS["app"], handler=App().handler)
        await member.connect(dispatcher)
        yield AppHandle()
        await member.close()
        return
    env = dict(os.environ, KBUS_ADDRESS=address, KBUS_NAME="app", KBUS_SECRET=SECRETS["app"])
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(APP_SCRIPT), env=env, stdout=asyncio.subprocess.PIPE)
    line = await asyncio.wait_for(process.stdout.readline(), 10)
    assert line.strip() == b"ready", line
    handle = AppHandle(process)
    yield handle
    await handle.stop()


async def stats(gateway_member):
    reply = await gateway_member.call("app.stats", kbus.Message())
    return json.loads(reply.payload)


async def wait_stats(gateway_member, **expected):
    async with asyncio.timeout(5):
        while True:
            try:
                current = await stats(gateway_member)
            except kbus.NoSuchMember:
                current = None
            if current is not None and all(current[k] == v for k, v in expected.items()):
                return current
            await asyncio.sleep(0.05)


@pytest.fixture
async def running_app(app_process, gateway_member):
    await wait_stats(gateway_member)
    return app_process


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=10) as client:
        yield client
