#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pyre-unsafe

from typing import List


class Vector:
    def size(self) -> int: ...
    def print_all(self) -> str: ...
    def print_detailed(self) -> str: ...
    def summarize_to_text(self) -> str: ...
    def child_at(self, idx: int) -> Vector: ...
    def null_count(self) -> int: ...
    def is_null_at(self, int) -> bool: ...
    def __getitem__(self, int) -> str: ...
