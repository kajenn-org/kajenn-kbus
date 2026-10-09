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
"""The unit kbus carries: inspectable metadata plus an opaque payload."""

from typing import Any

RESERVED_KEYS = frozenset({"id", "kind", "route", "from", "via"})


class Message:
    """A JSON object of metadata and a payload of bytes."""

    __slots__ = ("meta", "payload")

    def __init__(self, meta: dict[str, Any] | None = None, payload: bytes = b"") -> None:
        if meta is None:
            meta = {}
        if not isinstance(meta, dict):
            raise TypeError("Message meta must be a dict")
        if not isinstance(payload, bytes):
            raise TypeError("Message payload must be bytes")
        reserved = RESERVED_KEYS.intersection(meta)
        if reserved:
            raise ValueError(f"Message meta uses keys reserved to kbus: {sorted(reserved)}")
        self.meta = dict(meta)
        self.payload = payload


def make_message(meta: dict[str, Any], payload: bytes) -> Message:
    """Build a message carrying kbus keys, bypassing the public checks."""
    message = Message.__new__(Message)
    message.meta = meta
    message.payload = payload
    return message
