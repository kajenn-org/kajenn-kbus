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
"""Limits of a connection, one object for every bound."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Limits:
    """Bounds applied by the side that sets them."""

    max_frame: int = 16 * 2**20
    max_meta: int = 64 * 2**10
    max_route: int = 4 * 2**10
    max_pending: int = 1024
    stream_window: int = 32
    write_buffer: int = 4 * 2**20
