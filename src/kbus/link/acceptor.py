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
"""The side of a link that listens and admits by token."""

from typing import Any

from ..core import Connection, Listener, listen
from ..core.tasks import Tasks, accept_from
from ..errors import Refused, Rejected
from ..limits import Limits
from ..message import Message


class LinkAcceptor:
    """Accepts links and names each one after its token.

    ``links`` maps the instance name of every admitted link to its connection.
    Messages arriving on a link are served by the attached dispatcher as
    coming from that instance name.
    """

    def __init__(self, tokens: dict[str, str], *, limits: Limits | None = None) -> None:
        self.tokens = dict(tokens)
        self.limits = limits or Limits()
        self.dispatcher: Any = None
        self.links: dict[str, Connection] = {}
        self._connections: set[Connection] = set()
        self._listeners: list[Listener] = []
        self._tasks = Tasks()

    async def listen(self, url: str, *, ssl=None) -> str:
        listener = await listen(url, limits=self.limits, ssl=ssl).__aenter__()
        self._listeners.append(listener)
        self._tasks.spawn(accept_from(listener, self.accept))
        return listener.address

    async def close(self) -> None:
        for listener in self._listeners:
            await listener.close()
        self._tasks.cancel()
        for connection in list(self._connections):
            await connection.close()
        self.links.clear()

    def accept(self, connection: Connection) -> None:
        """Serve one link: the token first, then routing."""
        self._connections.add(connection)
        name: str | None = None

        async def handler(message: Message, second: Any) -> None:
            nonlocal name
            if "route" in message.meta:
                if name is None:
                    raise Rejected("not admitted")
                await self.dispatcher.serve(name, message, second, over_link=True)
                return
            name = self.admit(message.meta["token"], connection)
            await second.send(Message())

        async def watch() -> None:
            await connection.wait_closed()
            self._connections.discard(connection)
            if name is not None and self.links.get(name) is connection:
                del self.links[name]

        connection.handler = handler
        connection.grant_streams = False
        self._tasks.spawn(watch())

    def admit(self, token: str, connection: Connection) -> str:
        """Register ``connection`` under the name of ``token``.

        A name still linked to a live connection refuses the newcomer, which
        retries after its backoff; the old link is replaced only once it drops.
        """
        if token not in self.tokens:
            raise Rejected("unknown token")
        name = self.tokens[token]
        current = self.links.get(name)
        if current is not None and not current.closed:
            raise Refused("instance already linked")
        self.links[name] = connection
        return name
