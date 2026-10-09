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
"""kbus: transparent transport of opaque payloads plus inspectable metadata."""

from . import core
from .core import Reply, Stream
from .dispatcher import Dispatcher, Member, expose
from .errors import (
    Aborted,
    Error,
    FrameTooLarge,
    LinkLost,
    NoSuchInstance,
    NoSuchMember,
    NoSuchRoute,
    Overloaded,
    ProtocolError,
    Refused,
    Rejected,
    RemoteError,
    Unreachable,
)
from .limits import Limits
from .link import Link, LinkAcceptor, Reconnect
from .message import Message

__version__ = "0.1.0.dev0"

__all__ = [
    "Aborted",
    "Dispatcher",
    "Error",
    "FrameTooLarge",
    "Limits",
    "Link",
    "LinkAcceptor",
    "LinkLost",
    "Member",
    "Message",
    "NoSuchInstance",
    "NoSuchMember",
    "NoSuchRoute",
    "Overloaded",
    "ProtocolError",
    "Reconnect",
    "Refused",
    "Rejected",
    "RemoteError",
    "Reply",
    "Stream",
    "Unreachable",
    "core",
    "expose",
]
