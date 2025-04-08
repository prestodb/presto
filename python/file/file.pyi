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

from pyvelox.type import Type


class File:
    def __init__(self, path: str, format_str: str) -> None: ...
    def get_schema(self) -> Type: ...

def PARQUET(str) -> File: ...
def DWRF(str) -> File: ...
def NIMBLE(str) -> File: ...
def ORC(str) -> File: ...
def JSON(str) -> File: ...
def TEXT(str) -> File: ...
