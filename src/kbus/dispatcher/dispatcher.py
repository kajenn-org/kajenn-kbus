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
"""Named members inside one application, routed by name."""

import asyncio
import contextlib
import copy
from collections.abc import Awaitable, Callable
from typing import Any

from ..core import Connection, Stream, listen
from ..core.tasks import Tasks, accept_from
from ..errors import (
    Aborted,
    Error,
    LinkLost,
    NoSuchInstance,
    NoSuchMember,
    Refused,
    Rejected,
    Unreachable,
)
from ..limits import Limits
from ..message import Message, make_message


class Dispatcher:
    """Admits members by name and secret and routes messages between them.

    A member's first call carries ``hello`` and ``secret`` and no route; its
    orderly close is a send carrying ``bye``, answered by closing the connection.
    Every other message carries a route whose first segment names the
    destination member, or ``instance:route`` for a member of the application
    at the other end of a link.
    """

    def __init__(self, secrets: dict[str, str], *, limits: Limits | None = None) -> None:
        self.secrets = dict(secrets)
        self.limits = limits or Limits()
        self._members: dict[str, Connection] = {}
        self._connections: set[Connection] = set()
        self._links: dict[str, Any] = {}
        self._acceptors: list = []
        self._listeners: list = []
        self._tasks = Tasks()
        # Closes started by a member's goodbye: close() awaits them, never cancels them.
        self._closing = Tasks()
        self._message_hooks: list[Callable[[dict], Awaitable[None]]] = []
        self._disconnect_hooks: list[Callable[[str, Exception | None], Awaitable[None]]] = []

    def on_message(self, hook: Callable[[dict], Awaitable[None]]) -> Callable[[dict], Awaitable[None]]:
        """Register ``hook``, awaited with a copy of each message's metadata before routing.

        A hook that raises stops the message and the caller gets the error:
        ``Refused`` for a policy. Usable as a decorator."""
        self._message_hooks.append(hook)
        return hook

    def on_disconnect(
        self, hook: Callable[[str, Exception | None], Awaitable[None]]
    ) -> Callable[[str, Exception | None], Awaitable[None]]:
        """Register ``hook``, awaited with the name and the close reason of each
        admitted member that disconnects; the reason is ``None`` for an orderly
        close. Usable as a decorator."""
        self._disconnect_hooks.append(hook)
        return hook

    async def listen(self, address: str, *, ssl=None) -> str:
        """Accept members at ``address`` (``unix://``, ``ws://``, ``wss://`` with ``ssl``).

        Return the bound address."""
        listener = await listen(address, limits=self.limits, ssl=ssl).__aenter__()
        self._listeners.append(listener)
        self._tasks.spawn(accept_from(listener, self.accept))
        return listener.address

    def attach_link(self, instance_name: str, link: Any) -> None:
        """Route ``instance_name:...`` over ``link`` and start it; what arrives on it
        is served as coming from ``instance_name``."""
        self._links[instance_name] = link
        link.attach(self, instance_name)

    def attach_acceptor(self, acceptor: Any) -> None:
        """Serve the links ``acceptor`` admits; each is reachable as ``<name>:...``,
        with the name its token maps to."""
        self._acceptors.append(acceptor)
        acceptor.dispatcher = self

    def instance(self, name: str) -> Connection:
        """The connection of the link to instance ``name``."""
        connection: Connection | None
        if name in self._links:
            connection = self._links[name].connection
        else:
            accepting = [acceptor for acceptor in self._acceptors if name in acceptor.tokens.values()]
            if not accepting:
                raise NoSuchInstance(f"no instance named {name!r}")
            connection = next((a.links[name] for a in accepting if name in a.links), None)
        if connection is None:
            raise Unreachable(f"instance {name!r} is not connected")
        return connection

    async def close(self) -> None:
        """Close listeners, member connections, and every attached link and acceptor."""
        for listener in self._listeners:
            await listener.close()
        self._tasks.cancel()
        for connection in list(self._connections):
            await connection.close()
        for link in self._links.values():
            await link.close()
        for acceptor in self._acceptors:
            await acceptor.close()

    def accept(self, connection: Connection) -> None:
        """Serve one member connection: admission first, then routing."""
        self._connections.add(connection)
        name: str | None = None

        async def handler(message: Message, second: Any) -> None:
            nonlocal name
            meta = message.meta
            if "route" in meta:
                if name is None or self._members.get(name) is not connection:
                    raise Rejected("not admitted")
                await self.serve(name, message, second)
            elif "hello" in meta:
                self.admit(meta["hello"], meta["secret"], connection)
                name = meta["hello"]
                await second.send(Message())
            else:
                if name is None:
                    raise Rejected("not admitted")
                del self._members[name]
                self._closing.spawn(connection.close())

        async def watch() -> None:
            reason = await connection.wait_closed()
            self._connections.discard(connection)
            if name is None:
                return
            if self._members.get(name) is connection:
                del self._members[name]
            for hook in self._disconnect_hooks:
                await hook(name, reason)

        connection.handler = handler
        connection.grant_streams = False
        self._tasks.spawn(watch())

    def admit(self, name: str, secret: str, connection: Connection) -> None:
        if name not in self.secrets or self.secrets[name] != secret:
            raise Rejected(f"unknown name or wrong secret for {name!r}")
        if name in self._members:
            raise Rejected(f"name {name!r} already in use")
        self._members[name] = connection

    async def serve(
        self, sender: str, message: Message, second: Any, *, over_link: bool = False
    ) -> None:
        """Run the hooks on a message from ``sender``, then route it.

        A message that arrived over a link may not name an instance: one hop only.
        """
        if over_link and ":" in message.meta["route"]:
            raise Refused("route over a link cannot name an instance")
        meta = {key: value for key, value in message.meta.items() if key != "id"}
        meta["from"] = sender
        meta["via"] = []
        for hook in self._message_hooks:
            await hook(copy.deepcopy(meta))
        await self.route(make_message(meta, message.payload), second)

    async def route(
        self,
        message: Message,
        second: Any,
        on_reply: Callable[[Message], Awaitable[None]] | None = None,
    ) -> None:
        """Forward ``message`` (carrying ``kind``, ``route``, ``from``, ``via``) to its member.

        Across a link ``from`` and ``via`` stay behind: the other side sets its own.
        ``on_reply`` sees the reply to a call before it goes back through ``second``.
        """
        meta = dict(message.meta)
        kind = meta.pop("kind")
        connection = self.target(meta)
        forwarded = make_message(meta, message.payload)
        if kind == "call":
            try:
                reply = await connection.call(forwarded)
            except Error as exc:
                if on_reply is not None:
                    await on_reply(make_message(exc.meta, b""))
                raise
            if on_reply is not None:
                await on_reply(reply)
            await second.send(reply)
        elif kind == "send":
            await connection.send(forwarded)
        else:
            async with connection.open(forwarded) as stream:
                await second.grant()
                await asyncio.gather(relay(second, stream), relay(stream, second))


    def target(self, meta: dict[str, Any]) -> Connection:
        """The connection to forward to, rewriting ``route`` (and dropping from/via across a link)."""
        instance, sep, route = meta["route"].partition(":")
        if sep:
            del meta["from"], meta["via"]
            meta["route"] = route
            return self.instance(instance)
        head, _, meta["route"] = meta["route"].partition(".")
        connection = self._members.get(head)
        if connection is None:
            raise NoSuchMember(f"no member named {head!r}")
        return connection


async def relay(source: Stream, target: Stream) -> None:
    """Copy one direction of a stream into another, then close or abort it."""
    try:
        async for message in source:
            await target.send(message)
    except Aborted as exc:
        await target.abort(exc.reason)
        return
    except LinkLost:
        await target.abort("link lost")
        return
    with contextlib.suppress(Aborted, LinkLost):
        await target.close()
