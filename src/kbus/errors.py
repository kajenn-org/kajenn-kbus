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
"""Errors raised by kbus. Every one subclasses :class:`Error`."""

from typing import Any


class Error(Exception):
    """Base of every kbus error. ``meta`` holds the metadata of an error reply, if any."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self.meta: dict[str, Any] = {}


class RemoteError(Error):
    """The handler on the other side raised, or replied with ``fail``."""

    def __init__(self, type: str, message: str, traceback: str) -> None:
        super().__init__(f"{type}: {message}")
        self.type = type
        self.message = message
        self.traceback = traceback


class NoSuchMember(Error):
    """No member is connected under the routed name."""


class NoSuchRoute(Error):
    """The member has nothing exposed under the route."""


class NoSuchInstance(Error):
    """No link is known under the instance name."""


class Refused(Error):
    """A dispatcher hook refused the message."""


class LinkLost(Error):
    """The connection dropped while the call or stream was pending."""


class Unreachable(Error):
    """Nothing answered at the address, or the instance link is down."""


class FrameTooLarge(Error):
    """An outgoing frame exceeds the sender's limits; nothing was sent."""


class ProtocolError(Error):
    """An incoming frame violates the protocol or the receiver's limits."""


class Overloaded(Error):
    """Too many calls in flight on the connection; nothing was sent."""


class Rejected(Error):
    """Wrong secret or token, unknown name, or name already in use."""


class Aborted(Error):
    """The stream was aborted by one of its two sides."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def by_name(name: str) -> type[Error]:
    """The kbus error class a peer named in an error reply."""
    for cls in Error.__subclasses__():
        if cls.__name__ == name and cls is not RemoteError:
            return cls
    raise ProtocolError(f"unknown kbus error {name!r} in an error reply")
