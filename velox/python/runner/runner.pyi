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

from typing import Iterator, Optional

from pyvelox.vector import Vector


class LocalRunner:
    def __init__(self, PlanNode) -> None: ...
    def execute(self, max_drivers: Optional[int] = None) -> Iterator[Vector]: ...
    def add_file_split(self, plan_id: str, file_path: str) -> None: ...
    def add_query_config(self, config_name: str, config_value: str) -> None: ...
    def print_plan_with_stats(self) -> str: ...

def register_hive(str) -> None: ...
def register_tpch(str) -> None: ...
def unregister(str) -> None: ...
def unregister_all() -> None: ...
