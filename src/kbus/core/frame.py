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
"""Wire format of a frame: header, compact JSON metadata, payload."""

import json
import struct

from ..errors import FrameTooLarge, ProtocolError
from ..limits import Limits
from ..message import Message, make_message

MAGIC = b"KBUS"
VERSION = 1
HEADER = struct.Struct("!4sBII")


class FrameCodec:
    """Encode, decode and size-check frames against one side's limits."""

    def __init__(self, limits: Limits) -> None:
        self.limits = limits

    def encode_meta(self, meta: dict) -> bytes:
        return json.dumps(
            meta, separators=(",", ":"), allow_nan=False, ensure_ascii=False
        ).encode()

    def check(self, meta_len: int, payload_len: int, error: type) -> None:
        """Raise ``error`` when the sizes exceed this side's limits."""
        if meta_len > self.limits.max_meta:
            raise error(f"meta of {meta_len} bytes over max_meta {self.limits.max_meta}")
        if meta_len + payload_len > self.limits.max_frame:
            raise error(
                f"frame of {meta_len + payload_len} bytes over max_frame {self.limits.max_frame}"
            )

    def check_outgoing(self, message: Message) -> None:
        self.check(len(self.encode_meta(message.meta)), len(message.payload), FrameTooLarge)

    def check_incoming(self, message: Message) -> None:
        self.check(len(self.encode_meta(message.meta)), len(message.payload), ProtocolError)

    def encode(self, message: Message) -> bytes:
        meta = self.encode_meta(message.meta)
        self.check(len(meta), len(message.payload), FrameTooLarge)
        return HEADER.pack(MAGIC, VERSION, len(meta), len(message.payload)) + meta + message.payload

    def decode_header(self, header: bytes) -> tuple[int, int]:
        """Return ``(meta_len, payload_len)`` of a valid header."""
        magic, version, meta_len, payload_len = HEADER.unpack(header)
        if magic != MAGIC:
            raise ProtocolError(f"bad magic {magic!r}")
        if version != VERSION:
            raise ProtocolError(f"unsupported version {version}")
        self.check(meta_len, payload_len, ProtocolError)
        return meta_len, payload_len

    def decode(self, frame: bytes) -> Message:
        meta_len, payload_len = self.decode_header(frame[: HEADER.size])
        body = frame[HEADER.size :]
        if len(body) != meta_len + payload_len:
            raise ProtocolError("frame length does not match its header")
        try:
            meta = json.loads(body[:meta_len])
        except ValueError as exc:
            raise ProtocolError(f"meta is not valid JSON: {exc}") from exc
        if not isinstance(meta, dict):
            raise ProtocolError("meta is not a JSON object")
        return make_message(meta, body[meta_len:])
