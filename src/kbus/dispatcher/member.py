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
"""A named participant connected to a dispatcher."""

import copy
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

from ..core import Connection, Stream, connect, pipe
from ..core.connection import Handler
from ..errors import Error, LinkLost
from ..limits import Limits
from ..message import Message, make_message
from .dispatcher import Dispatcher
from .expose import RouteProxy


class Member:
    """One connection to a dispatcher, admitted under ``name`` with ``secret``.

    A member holding a ``dispatcher`` forwards every message whose next route
    segment names a member of that dispatcher (a name in its secrets); every
    other message goes to ``handler``.
    """

    def __init__(
        self,
        name: str,
        *,
        secret: str,
        handler: Handler | None,
        dispatcher: Dispatcher | None = None,
        limits: Limits | None = None,
    ) -> None:
        self._name = name
        self.secret = secret
        self._handler = handler
        self._dispatcher = dispatcher
        self.limits = limits
        self.connection: Connection | None = None
        self._reply_hooks: list[Callable[[dict], Awaitable[None]]] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def dispatcher(self) -> Dispatcher | None:
        return self._dispatcher

    @property
    def handler(self) -> Handler | None:
        return self._handler

    @handler.setter
    def handler(self, handler: Handler | None) -> None:
        """A handler replaced after ``connect`` serves the next message."""
        self._handler = handler
        if self.connection is not None and self.dispatcher is None:
            self.connection.handler = handler

    @property
    def connected(self) -> Connection:
        """The connection to the dispatcher; a member not yet connected has none."""
        if self.connection is None:
            raise LinkLost(f"member {self.name!r} is not connected")
        return self.connection

    def on_reply(self, hook: Callable[[dict], Awaitable[None]]) -> Callable[[dict], Awaitable[None]]:
        self._reply_hooks.append(hook)
        return hook

    async def connect(self, target: Dispatcher | str) -> None:
        if isinstance(target, Dispatcher):
            connection, other = pipe(self.limits or target.limits, target.limits)
            target.accept(other)
        else:
            connection = await connect(target, limits=self.limits)
        connection.handler = self.handler if self.dispatcher is None else self.serve
        connection.grant_streams = self.dispatcher is None
        try:
            await connection.call(make_message({"hello": self.name, "secret": self.secret}, b""))
        except Error:
            await connection.close()
            raise
        self.connection = connection

    async def close(self) -> None:
        if self.connected.closed:
            return
        await self.connected.send(make_message({"bye": self.name}, b""))
        await self.connected.wait_closed()

    async def call(self, route: str, message: Message) -> Message:
        try:
            reply = await self.connected.call(self.routed(route, message))
        except Error as exc:
            await self.observe(make_message(exc.meta, b""))
            raise
        await self.observe(reply)
        return reply

    async def send(self, route: str, message: Message) -> None:
        await self.connected.send(self.routed(route, message))

    def open(self, route: str, message: Message) -> AbstractAsyncContextManager[Stream]:
        return self.connected.open(self.routed(route, message))

    def routed(self, route: str, message: Message) -> Message:
        return make_message(dict(message.meta, route=route), message.payload)

    def route(self, prefix: str) -> RouteProxy:
        return RouteProxy(self, prefix)

    async def observe(self, reply: Message) -> None:
        for hook in self._reply_hooks:
            await hook(copy.deepcopy(reply.meta))

    async def serve(self, message: Message, second: Any) -> None:
        """Forward to a member of the nested dispatcher, or hand to the handler."""
        dispatcher = self.dispatcher
        if dispatcher is None:
            raise RuntimeError("serve() belongs to a member holding a dispatcher")
        head = message.meta["route"].partition(".")[0]
        if head not in dispatcher.secrets:
            if message.meta["kind"] == "open":
                await second.grant()
            if self.handler is None:
                raise TypeError(f"member {self.name!r} has no handler")
            await self.handler(message, second)
            return
        meta = dict(message.meta, via=[*message.meta["via"], self.name])
        await dispatcher.route(make_message(meta, message.payload), second, on_reply=self.observe)
