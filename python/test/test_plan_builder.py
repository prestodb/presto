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

import json
import unittest

from pyvelox.type import BIGINT, ROW
from pyvelox.plan_builder import PlanBuilder, PlanNode, deserialize_plan


class TestPyVeloxPlanBuidler(unittest.TestCase):
    def test_plan_builder(self):
        plan_builder = PlanBuilder()
        self.assertEqual(plan_builder.get_plan_node(), None)

        # Table scan. Needs at least the output schema and it needs to be a ROW.
        self.assertRaises(RuntimeError, plan_builder.table_scan)
        self.assertRaises(RuntimeError, plan_builder.table_scan, BIGINT())

        plan_builder.table_scan(ROW(["c0"], [BIGINT()]))
        table_scan = plan_builder.get_plan_node()
        self.assertTrue(isinstance(table_scan, PlanNode))
        self.assertEqual(table_scan.name(), "TableScan")

        # Project.
        self.assertRaises(
            RuntimeError, plan_builder.project, ["c1"]
        )  # c1 non-existent.
        plan_builder.project(["c0 + 1 AS d0", "100"])
        project = plan_builder.get_plan_node()
        self.assertEqual(project.name(), "Project")

        self.assertNotEqual(table_scan.id(), project.id())

        # Filter.
        self.assertRaises(
            RuntimeError, plan_builder.filter, "c0 > 1"
        )  # c0 non-existent.
        plan_builder.filter("d0 - 10 > 100")
        filter_node = plan_builder.get_plan_node()
        self.assertEqual(filter_node.name(), "Filter")

        self.assertNotEqual(filter_node.id(), project.id())

        self.assertEqual(
            str(filter_node),
            "-- Filter[2]\n" "  -- Project[1]\n" "    -- TableScan[0]\n",
        )

    def test_multiple_plan_builders(self):
        """
        Tests that mutliple plan builders (used to create plans with multiple
        pipelines, like containing joins), generate expected disjoins node ids.
        """
        plan_builder1 = PlanBuilder()
        plan_builder1.table_scan(ROW(["c0"], [BIGINT()]))
        self.assertEqual(plan_builder1.get_plan_node().id(), "0")

        # A new independent plan builder.
        plan_builder2 = PlanBuilder()
        plan_builder2.table_scan(ROW(["c0"], [BIGINT()]))
        self.assertEqual(plan_builder2.get_plan_node().id(), "0")

        # Chained plan builders.
        plan_builder1_1 = plan_builder1.new_builder()
        plan_builder1_1.table_scan(ROW(["c0"], [BIGINT()]))
        self.assertEqual(plan_builder1_1.get_plan_node().id(), "1")

        plan_builder1_2 = plan_builder1.new_builder()
        plan_builder1_2.table_scan(ROW(["c0"], [BIGINT()]))
        self.assertEqual(plan_builder1_2.get_plan_node().id(), "2")

        plan_builder1.project([])
        self.assertEqual(plan_builder1.get_plan_node().id(), "3")

    def test_plan_serialization(self):
        plan_builder = (
            PlanBuilder()
            .table_scan(ROW(["c0"], [BIGINT()]))
            .project(["c0 + 1 AS d0", "100"])
            .filter("d0 - 10 > 100")
        )

        plan_node = plan_builder.get_plan_node()
        plan_clone = deserialize_plan(plan_node.serialize())

        self.assertEqual(
            json.loads(plan_node.serialize()), json.loads(plan_clone.serialize())
        )
        self.assertEqual(str(plan_node), str(plan_clone))
        self.assertEqual(plan_node.to_string(), plan_clone.to_string())
