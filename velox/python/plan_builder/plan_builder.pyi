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

from enum import Enum
from typing import List, Dict, Type, Optional

from pyvelox.file import File
from pyvelox.type import Type


class JoinType(Enum):
    INNER = 1
    LEFT = 2
    RIGHT = 3
    FULL = 4

class PlanNode:
    def name(self) -> str: ...
    def serialize(self) -> str: ...
    def to_string(self) -> str: ...

def deserialize_plan(str): ...

class PlanBuilder:
    def __init__(self) -> None: ...
    def table_scan(
        self,
        output_schema: Type,
        aliases: Dict[str, str] = {},
        subfields: Dict[str, List[int]] = {},
        row_index: str = "",
        connector_id: str = "prism",
        input_files: List[File] = [],
    ) -> PlanBuilder: ...
    def tpch_gen(
        self,
        table_name: str,
        columns: list[str] = [],
        scale_factor: int = 1,
        num_parts: int = 1,
        connector_id: str = "tpch"
    ) -> PlanBuilder: ...
    def table_write(
        self,
        output_file: Optional[File] = None,
        output_path: Optional[File] = None,
        connector_id: str = "hive",
        output_schema: Optional[Type] = None,
    ) -> PlanBuilder: ...
    def get_plan_node(self) -> PlanBuilder: ...
    def new_builder(self) -> PlanBuilder: ...
    def id(self) -> str: ...
