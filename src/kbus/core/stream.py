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
"""A channel opened by one message: a sequence of messages in each direction."""

import asyncio
import contextlib
from typing import Any

from .. import errors
from ..errors import Aborted, LinkLost
from ..message import Message, make_message

KBUS_KEYS = ("id", "kind")


class Stream:
    """One stream of a connection, identified by the id of its ``open`` message.

    Flow control is credit-based: each side grants its ``stream_window`` at
    open and one credit for each message the application consumes. A sender
    keeps at most the smaller of the two windows in flight.
    """

    def __init__(self, connection: Any, id: str) -> None:
        self.connection = connection
        self.id = id
        self._inbox: asyncio.Queue[Message | Exception | None] = asyncio.Queue()
        self._credit = 0
        self._granted = False
        self._wake = asyncio.Event()
        self._answered = asyncio.Event()
        self._sending_closed = False
        self._peer_closed = False
        self._ended = False
        self._error: Exception | None = None

    @property
    def finished(self) -> bool:
        return self._error is not None or (self._sending_closed and self._peer_closed)

    async def send(self, message: Message) -> None:
        if self._sending_closed and self._error is None:
            raise RuntimeError(f"stream {self.id}: my direction is already closed")
        while self._error is None and self._credit <= 0:
            self._wake.clear()
            await self._wake.wait()
        if self._error is not None:
            raise self._error
        self._credit -= 1
        try:
            await self.write("data", message.meta, message.payload)
        except BaseException:
            self._credit += 1
            raise

    async def close(self) -> None:
        if self.finished or self._sending_closed:
            return
        await self.write("close", {}, b"")
        self._sending_closed = True
        self.settle()

    async def abort(self, reason: str) -> None:
        if self.finished:
            return
        self.fail(Aborted(reason))
        with contextlib.suppress(LinkLost):
            await self.write("abort", {"reason": reason}, b"")

    async def refuse(self, exc: errors.Error) -> None:
        """Abort with a kbus error the opener receives as its own class."""
        if self.finished:
            return
        self.fail(exc)
        with contextlib.suppress(LinkLost):
            await self.write("abort", {"reason": str(exc), "error": type(exc).__name__}, b"")

    def __aiter__(self) -> "Stream":
        return self

    async def __anext__(self) -> Message:
        if self._error is not None and self._inbox.empty():
            raise self._error
        if self._ended:
            raise StopAsyncIteration
        item = await self._inbox.get()
        if isinstance(item, Exception):
            raise item
        if item is None:
            self._ended = True
            raise StopAsyncIteration
        if not self.finished and not self._peer_closed:
            await self.write("credit", {"n": 1}, b"")
        meta = {key: value for key, value in item.meta.items() if key not in KBUS_KEYS}
        return make_message(meta, item.payload)

    async def grant(self) -> None:
        """Grant the peer this side's window, once, at open."""
        if not self.finished:
            await self.write("credit", {"n": self.connection.limits.stream_window}, b"")

    async def accepted(self) -> None:
        """Wait for the peer's first grant; a failure that comes before it is raised here."""
        await self._answered.wait()
        if not self._granted and self._error is not None:
            raise self._error

    async def write(self, kind: str, meta: dict, payload: bytes) -> None:
        await self.connection.transport.write(make_message(dict(meta, id=self.id, kind=kind), payload))

    def receive(self, message: Message) -> None:
        """Take one stream message arriving from the peer."""
        kind = message.meta["kind"]
        if kind == "data":
            self._inbox.put_nowait(message)
        elif kind == "close":
            self._peer_closed = True
            self._inbox.put_nowait(None)
            self.settle()
        elif kind == "abort":
            reason = message.meta["reason"]
            error = message.meta.get("error")
            self.fail(Aborted(reason) if error is None else errors.by_name(error)(reason))
        else:
            n = message.meta["n"]
            self._credit += n if self._granted else min(n, self.connection.limits.stream_window)
            self._granted = True
            self._wake.set()
            self._answered.set()

    def fail(self, exc: Exception) -> None:
        """End both directions with ``exc``, raised by every later send or iteration."""
        self._error = exc
        self._inbox.put_nowait(exc)
        self._wake.set()
        self._answered.set()
        self.connection.forget(self.id)

    def settle(self) -> None:
        if self.finished:
            self.connection.forget(self.id)
