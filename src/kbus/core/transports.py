# Copyright 2026 Softwell S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Transports under a Connection. The pipe passes Message objects in memory;
unix sockets and WebSockets carry encoded frames."""

import asyncio
import contextlib
import ssl as ssl_module
from typing import Any
from urllib.parse import urlsplit

from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.server import Server, ServerConnection, serve as ws_serve
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI
from websockets.frames import CloseCode

from ..errors import LinkLost, ProtocolError, Unreachable
from ..limits import Limits
from ..message import Message
from .connection import Connection
from .frame import HEADER, FrameCodec


class PipeTransport:
    """One end of an in-memory pipe: an inbox, the peer's inbox, own limits."""

    def __init__(
        self, inbox: "asyncio.Queue[Message | None]", outbox: "asyncio.Queue[Message | None]", limits: Limits
    ) -> None:
        self.inbox = inbox
        self.outbox = outbox
        self.codec = FrameCodec(limits)
        self.closed = False

    async def write(self, message: Message) -> None:
        self.codec.check_outgoing(message)
        if self.closed:
            raise LinkLost("connection closed")
        self.outbox.put_nowait(message)

    async def read(self) -> Message | None:
        message = await self.inbox.get()
        if message is not None:
            self.codec.check_incoming(message)
        return message

    async def close(self) -> None:
        self.closed = True
        self.outbox.put_nowait(None)


def pipe(limits_a: Limits | None = None, limits_b: Limits | None = None) -> tuple[Connection, Connection]:
    """Two connected ends in the same process."""
    limits_a = limits_a or Limits()
    limits_b = limits_b or Limits()
    a_inbox: asyncio.Queue[Message | None] = asyncio.Queue()
    b_inbox: asyncio.Queue[Message | None] = asyncio.Queue()
    a = Connection(PipeTransport(a_inbox, b_inbox, limits_a), limits_a)
    b = Connection(PipeTransport(b_inbox, a_inbox, limits_b), limits_b)
    return a, b


class StreamTransport:
    """One end of a unix socket: frames back to back on the byte stream."""

    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, limits: Limits
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.codec = FrameCodec(limits)
        writer.transport.set_write_buffer_limits(high=limits.write_buffer)

    async def write(self, message: Message) -> None:
        frame = self.codec.encode(message)
        if self.writer.is_closing():
            raise LinkLost("connection closed")
        self.writer.write(frame)
        try:
            await self.writer.drain()
        except ConnectionError as exc:
            raise LinkLost(f"connection lost: {exc}") from exc

    async def read(self) -> Message | None:
        try:
            header = await self.reader.readexactly(HEADER.size)
            meta_len, payload_len = self.codec.decode_header(header)
            body = await self.reader.readexactly(meta_len + payload_len)
        except (asyncio.IncompleteReadError, ConnectionError):
            return None
        return self.codec.decode(header + body)

    async def close(self) -> None:
        self.writer.close()
        with contextlib.suppress(ConnectionError):
            await self.writer.wait_closed()


class WebSocketTransport:
    """One end of a WebSocket: one binary message per frame."""

    def __init__(self, websocket, limits: Limits) -> None:
        self.websocket = websocket
        self.codec = FrameCodec(limits)

    async def write(self, message: Message) -> None:
        frame = self.codec.encode(message)
        try:
            await self.websocket.send(frame)
        except ConnectionClosed as exc:
            raise LinkLost(f"connection lost: {exc}") from exc

    async def read(self) -> Message | None:
        try:
            frame = await self.websocket.recv()
        except ConnectionClosed as exc:
            if exc.sent is not None and exc.sent.code == CloseCode.MESSAGE_TOO_BIG:
                raise ProtocolError(f"frame over max_size: {exc}") from exc
            return None
        if not isinstance(frame, bytes):
            raise ProtocolError("text message on a kbus WebSocket")
        return self.codec.decode(frame)

    async def close(self) -> None:
        await self.websocket.close()


def ws_options(limits: Limits, ssl: ssl_module.SSLContext | None) -> dict[str, Any]:
    """Keyword arguments shared by the WebSocket client and server."""
    options: dict[str, Any] = {
        "compression": None,
        "max_size": HEADER.size + limits.max_frame + limits.max_meta,
        "write_limit": limits.write_buffer,
    }
    if ssl is not None:
        options["ssl"] = ssl
    return options


def check_scheme(address: str) -> str:
    scheme = urlsplit(address).scheme
    if scheme not in ("unix", "ws", "wss"):
        raise ValueError(f"unsupported address {address!r}: use unix://, ws:// or wss://")
    return scheme


async def connect(
    address: str, *, limits: Limits | None = None, ssl: ssl_module.SSLContext | None = None
) -> Connection:
    """Open a connection to a listener at ``address``."""
    limits = limits or Limits()
    scheme = check_scheme(address)
    try:
        if scheme == "unix":
            reader, writer = await asyncio.open_unix_connection(urlsplit(address).path)
            return Connection(StreamTransport(reader, writer, limits), limits)
        websocket = await ws_connect(address, **ws_options(limits, ssl))
    except (OSError, InvalidURI, InvalidHandshake) as exc:
        raise Unreachable(f"cannot connect to {address}: {exc}") from exc
    return Connection(WebSocketTransport(websocket, limits), limits)


class Listener:
    """Accepts connections at one address; an async iterator of them."""

    def __init__(self, address: str, limits: Limits, ssl: ssl_module.SSLContext | None) -> None:
        self.requested = address
        self.limits = limits
        self.ssl = ssl
        self.scheme = check_scheme(address)
        self._address: str | None = None
        self._server: asyncio.Server | Server | None = None
        self._accepted: asyncio.Queue[Connection] = asyncio.Queue()

    @property
    def address(self) -> str:
        if self._address is None:
            raise RuntimeError("the listener is not bound yet: enter it first")
        return self._address

    async def __aenter__(self) -> "Listener":
        parts = urlsplit(self.requested)
        if self.scheme == "unix":
            self._server = await asyncio.start_unix_server(self.accept_stream, parts.path)
            self._address = self.requested
        else:
            self._server = await ws_serve(
                self.accept_websocket, parts.hostname, parts.port, **ws_options(self.limits, self.ssl)
            )
            port = next(iter(self._server.sockets)).getsockname()[1]
            self._address = f"{self.scheme}://{parts.hostname}:{port}{parts.path}"
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    def __aiter__(self) -> "Listener":
        return self

    async def __anext__(self) -> Connection:
        return await self._accepted.get()

    async def close(self) -> None:
        """Stop accepting; connections already accepted stay open."""
        if self._server is None:
            return
        if isinstance(self._server, Server):
            self._server.close(close_connections=False)
        else:
            self._server.close()

    def accept_stream(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._accepted.put_nowait(Connection(StreamTransport(reader, writer, self.limits), self.limits))

    async def accept_websocket(self, websocket: ServerConnection) -> None:
        connection = Connection(WebSocketTransport(websocket, self.limits), self.limits)
        self._accepted.put_nowait(connection)
        await connection.wait_closed()


def listen(
    address: str, *, limits: Limits | None = None, ssl: ssl_module.SSLContext | None = None
) -> Listener:
    """A listener for ``address``; it binds when entered."""
    return Listener(address, limits or Limits(), ssl)
