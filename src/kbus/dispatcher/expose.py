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
"""Python objects as members: marked async methods, JSON arguments and results."""

import inspect
import json
from typing import Any

from ..core.connection import Reply
from ..errors import NoSuchRoute
from ..message import Message


def expose(target: Any) -> Any:
    """Mark an ``async def`` method, or build the handler serving an object's marked methods."""
    if inspect.iscoroutinefunction(target):
        target.__kbus_exposed__ = True
        return target

    async def handler(message: Message, reply: Reply) -> None:
        name = message.meta["route"]
        method = getattr(target, name, None)
        if method is None or not getattr(method, "__kbus_exposed__", False):
            raise NoSuchRoute(f"no exposed method {name!r}")
        result = await method(**json.loads(message.payload))
        await reply.send(Message(payload=json.dumps(result).encode()))

    return handler


class RouteProxy:
    """Calls the exposed methods under ``prefix`` through ``member``, keyword arguments only."""

    def __init__(self, member: Any, prefix: str) -> None:
        self.member = member
        self.prefix = prefix

    def __getattr__(self, name: str) -> Any:
        async def invoke(**kwargs: Any) -> Any:
            message = Message(payload=json.dumps(kwargs).encode())
            reply = await self.member.call(f"{self.prefix}.{name}", message)
            return json.loads(reply.payload)

        return invoke
