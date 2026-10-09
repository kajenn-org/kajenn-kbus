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
"""One end of a transport: calls and sends in both directions."""

import asyncio
import contextlib
import json
import logging
import traceback
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from .. import errors
from ..errors import LinkLost, Overloaded, ProtocolError, RemoteError
from ..limits import Limits
from ..message import Message, make_message
from .stream import KBUS_KEYS, Stream

STREAM_KINDS = ("data", "close", "abort", "credit")
ERROR_META_KEYS = ("id", "kind", "error")


class Reply:
    """The answer to one incoming call, bound to its id."""

    def __init__(self, connection: "Connection", id: str) -> None:
        self.connection = connection
        self.id = id
        self.sent = False

    async def send(self, message: Message) -> None:
        await self.deliver(make_message(dict(message.meta, id=self.id, kind="reply"), message.payload))

    async def fail(self, exc: BaseException, meta: dict[str, Any] | None = None) -> None:
        """Answer with an error. A kbus error reaches the caller as its own class;
        a ``RemoteError`` passes through unchanged; anything else becomes a ``RemoteError``.
        ``meta`` travels with the error reply; a kbus error forwards its own ``meta`` by default."""
        if meta is None:
            meta = getattr(exc, "meta", {})
        if isinstance(exc, RemoteError):
            await self.fail_as(exc.type, exc.message, exc.traceback, meta=meta)
            return
        tb = "".join(traceback.format_exception(exc))
        await self.fail_as(
            type(exc).__name__, str(exc), tb, kbus=isinstance(exc, errors.Error), meta=meta
        )

    async def fail_as(
        self, type: str, message: str, tb: str, kbus: bool = False, meta: dict[str, Any] | None = None
    ) -> None:
        payload = json.dumps({"type": type, "message": message, "traceback": tb, "kbus": kbus}).encode()
        meta = {k: v for k, v in (meta or {}).items() if k not in ERROR_META_KEYS}
        await self.deliver(make_message(dict(meta, id=self.id, kind="error", error=type), payload))

    async def deliver(self, message: Message) -> None:
        if self.sent:
            raise RuntimeError(f"call {self.id} already answered")
        await self.connection.transport.write(message)
        self.sent = True


Handler = Callable[[Message, Any], Awaitable[None]]


class Connection:
    """Correlates calls and replies, and streams, by id over one transport."""

    def __init__(self, transport: Any, limits: Limits) -> None:
        self.handler: Handler | None = None
        # False leaves the grant of an incoming stream to its handler (relays).
        self.grant_streams = True
        self.transport = transport
        self.limits = limits
        self._pending: dict[str, asyncio.Future[Message]] = {}
        self._served: dict[str, asyncio.Task[None]] = {}
        self._streams: dict[str, Stream] = {}
        self._stream_tasks: set[asyncio.Task[None]] = set()
        self._closed = False
        self._reason: Exception | None = None
        self._done = asyncio.Event()
        self._read_task = asyncio.get_running_loop().create_task(self.read_loop())

    @property
    def closed(self) -> bool:
        return self._closed

    async def call(self, message: Message) -> Message:
        if self.closed:
            raise LinkLost("connection closed")
        if len(self._pending) >= self.limits.max_pending:
            raise Overloaded(f"{self.limits.max_pending} calls already pending")
        id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self._pending[id] = future
        try:
            await self.transport.write(make_message(dict(message.meta, id=id, kind="call"), message.payload))
            answer = await future
        except asyncio.CancelledError:
            if not self.closed:
                await self.transport.write(make_message({"id": id, "kind": "cancel"}, b""))
            raise
        finally:
            del self._pending[id]
        if answer.meta["kind"] == "error":
            info = json.loads(answer.payload)
            exc: errors.Error
            if info["kbus"]:
                exc = errors.by_name(info["type"])(info["message"])
            else:
                exc = RemoteError(info["type"], info["message"], info["traceback"])
            exc.meta = {key: value for key, value in answer.meta.items() if key != "id"}
            raise exc
        meta = {key: value for key, value in answer.meta.items() if key not in KBUS_KEYS}
        return make_message(meta, answer.payload)

    async def send(self, message: Message) -> None:
        if self.closed:
            raise LinkLost("connection closed")
        id = uuid.uuid4().hex
        await self.transport.write(make_message(dict(message.meta, id=id, kind="send"), message.payload))

    @contextlib.asynccontextmanager
    async def open(self, message: Message) -> AsyncIterator[Stream]:
        if self.closed:
            raise LinkLost("connection closed")
        id = uuid.uuid4().hex
        stream = Stream(self, id)
        self._streams[id] = stream
        try:
            await self.transport.write(make_message(dict(message.meta, id=id, kind="open"), message.payload))
        except BaseException:
            self.forget(id)
            raise
        reason = "left the block unfinished"
        try:
            await stream.grant()
            await stream.accepted()
            yield stream
        except BaseException as exc:
            reason = f"opener raised {type(exc).__name__}"
            raise
        finally:
            await stream.abort(reason)

    def forget(self, id: str) -> None:
        """Drop a finished stream; later messages for its id are ignored."""
        self._streams.pop(id, None)

    async def close(self) -> None:
        if not self.closed:
            self._read_task.cancel()
        await self.finish(None)

    async def wait_closed(self) -> Exception | None:
        await self._done.wait()
        return self._reason

    async def read_loop(self) -> None:
        try:
            while (message := await self.transport.read()) is not None:
                self.dispatch(message)
        except ProtocolError as exc:
            await self.finish(exc)
            return
        except Exception as exc:
            await self.finish(ProtocolError(f"malformed message from the peer: {exc!r}"))
            return
        await self.finish(None)

    def handle(self) -> Handler:
        """The installed handler; a connection without one cannot serve."""
        if self.handler is None:
            raise TypeError("no handler installed on this connection")
        return self.handler

    def dispatch(self, message: Message) -> None:
        kind = message.meta.get("kind")
        id: str = message.meta["id"]
        if kind in ("call", "send"):
            task = asyncio.get_running_loop().create_task(self.serve(message))
            self._served[id] = task
            task.add_done_callback(lambda _: self._served.pop(id, None))
        elif kind == "open":
            stream = Stream(self, id)
            self._streams[id] = stream
            task = asyncio.get_running_loop().create_task(self.serve_stream(message, stream))
            self._stream_tasks.add(task)
            task.add_done_callback(self._stream_tasks.discard)
        elif kind in STREAM_KINDS:
            open_stream = self._streams.get(id)
            if open_stream is not None:
                open_stream.receive(message)
        elif kind in ("reply", "error"):
            future = self._pending.get(id)
            if future is not None and not future.done():
                future.set_result(message)
        elif kind == "cancel":
            served = self._served.get(id)
            if served is not None:
                served.cancel()
        else:
            raise ProtocolError(f"unknown message kind {kind!r}")

    async def serve(self, message: Message) -> None:
        if message.meta["kind"] == "send":
            try:
                await self.handle()(message, None)
            except Exception:
                logging.getLogger("kbus").exception("handler failed on send %s", message.meta["id"])
            return
        reply = Reply(self, message.meta["id"])
        with contextlib.suppress(LinkLost):
            try:
                await self.handle()(message, reply)
            except Exception as exc:
                await reply.fail(exc)
                return
            if not reply.sent:
                await reply.fail_as("NoReply", "handler returned without replying", "")

    async def serve_stream(self, message: Message, stream: Stream) -> None:
        if self.grant_streams:
            with contextlib.suppress(LinkLost):
                await stream.grant()
        try:
            await self.handle()(message, stream)
        except Exception as exc:
            if isinstance(exc, errors.Error) and not isinstance(exc, RemoteError):
                await stream.refuse(exc)
                return
            logging.getLogger("kbus").exception("handler failed on open %s", message.meta["id"])
            await stream.abort(f"handler raised {type(exc).__name__}: {exc}")
            return
        await stream.abort("handler returned")

    async def finish(self, reason: Exception | None) -> None:
        """End the connection: fail pending calls, stop handlers, close the transport."""
        if self.closed:
            await self._done.wait()
            return
        self._closed = True
        self._reason = reason
        for future in self._pending.values():
            if not future.done():
                future.set_exception(LinkLost("connection closed while the call was pending"))
        for task in self._served.values():
            task.cancel()
        for stream in list(self._streams.values()):
            stream.fail(LinkLost("connection closed while the stream was open"))
        await self.transport.close()
        self._done.set()
