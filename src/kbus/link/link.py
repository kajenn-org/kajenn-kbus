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
"""The side of a link that connects, and reconnects after a drop."""

import asyncio
import contextlib
from dataclasses import dataclass
from typing import Any

from ..core import Connection, connect
from ..errors import LinkLost, Refused, Rejected, Unreachable
from ..limits import Limits
from ..message import Message, make_message


@dataclass(frozen=True)
class Reconnect:
    """Backoff between attempts: ``first``, then times ``factor``, capped at ``max``."""

    first: float = 1.0
    max: float = 30.0
    factor: float = 2.0


class Link:
    """One connection to a ``LinkAcceptor``, admitted by ``token``.

    Its first call carries ``hello`` and ``token``. Messages arriving on it are
    served by the attached dispatcher as coming from the instance name given
    to ``attach_link``. A rejected token stops the link; any other drop is
    followed by a new attempt.
    """

    def __init__(
        self,
        name: str,
        *,
        token: str,
        url: str,
        limits: Limits | None = None,
        reconnect: Reconnect = Reconnect(),
    ) -> None:
        self.name = name
        self.token = token
        self.url = url
        self.limits = limits
        self.reconnect = reconnect
        self.dispatcher: Any = None
        self.instance_name: str | None = None
        self._connection: Connection | None = None
        self._rejected: Rejected | None = None
        self._task: asyncio.Task[None] | None = None
        # Set while reachable, and for good once rejected.
        self._up = asyncio.Event()
        self._down = asyncio.Event()
        self._down.set()

    @property
    def reachable(self) -> bool:
        """True while the link is connected."""
        return self._connection is not None and not self._connection.closed

    @property
    def connection(self) -> Connection | None:
        return self._connection if self.reachable else None

    async def wait_reachable(self) -> None:
        """Wait until the link is connected.

        Raises ``Rejected`` when the acceptor refused the token."""
        await self._up.wait()
        if self._rejected is not None:
            raise self._rejected

    async def wait_unreachable(self) -> None:
        """Wait until the link is down."""
        await self._down.wait()

    async def close(self) -> None:
        """Stop the link: the connection is closed and no new attempt follows."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def attach(self, dispatcher: Any, instance_name: str) -> None:
        self.dispatcher = dispatcher
        self.instance_name = instance_name
        self._task = asyncio.get_running_loop().create_task(self.run())

    async def run(self) -> None:
        delay = self.reconnect.first
        while True:
            if await self.attempt():
                delay = self.reconnect.first
            if self._rejected is not None:
                return
            await asyncio.sleep(delay)
            delay = min(delay * self.reconnect.factor, self.reconnect.max)

    async def attempt(self) -> bool:
        """Connect once and stay connected until the drop; True when the token was accepted."""
        try:
            connection = await connect(self.url, limits=self.limits)
        except Unreachable:
            return False
        connection.handler = self.serve
        connection.grant_streams = False
        try:
            await connection.call(make_message({"hello": self.name, "token": self.token}, b""))
            self._connection = connection
            self._down.clear()
            self._up.set()
            await connection.wait_closed()
            return True
        except Rejected as exc:
            self._rejected = exc
            self._up.set()
            return False
        except (LinkLost, Refused):
            return False
        finally:
            self._connection = None
            if self._rejected is None:
                self._up.clear()
            self._down.set()
            await connection.close()

    async def serve(self, message: Message, second: Any) -> None:
        await self.dispatcher.serve(self.instance_name, message, second, over_link=True)
