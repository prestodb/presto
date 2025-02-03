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

import unittest
import pyarrow

from velox.py.arrow import to_velox
from velox.py.runner import LocalRunner
from velox.py.plan_builder import PlanBuilder


class TestPyVeloxRunner(unittest.TestCase):
    def test_runner_empty(self):
        plan_builder = PlanBuilder().values()
        runner = LocalRunner(plan_builder.get_plan_node())
        total_size = 0

        for vector in runner.execute():
            total_size += vector.size()
        self.assertEqual(total_size, 0)

    def test_runner_not_executed(self):
        # Ensure it won't hang on destruction when it was not executed.
        plan_builder = PlanBuilder().values()
        LocalRunner(plan_builder.get_plan_node())

    def test_runner_executed_twice(self):
        # Ensure the runner fails if it is executed twice.
        plan_builder = PlanBuilder().values()
        runner = LocalRunner(plan_builder.get_plan_node())
        runner.execute()
        self.assertRaises(RuntimeError, runner.execute)

    def test_runner_with_values(self):
        vectors = []
        batch_size = 10
        num_batches = 10

        for i in range(num_batches):
            array = pyarrow.array(list(range(i * batch_size, (i + 1) * batch_size)))
            batch = pyarrow.record_batch([array], names=["c0"])
            vectors.append(to_velox(batch))

        plan_builder = PlanBuilder().values(vectors)
        runner = LocalRunner(plan_builder.get_plan_node())
        total_size = 0

        for vector in runner.execute():
            total_size += vector.size()
        self.assertEqual(total_size, 100)

    def test_runner_with_join(self):
        batch_size = 10
        array = pyarrow.array([42] * batch_size)
        batch = to_velox(pyarrow.record_batch([array], names=["c0"]))

        plan_builder = PlanBuilder()
        plan_builder.values([batch]).merge_join(
            left_keys=["c0"],
            right_keys=["c0"],
            right_plan_node=(
                plan_builder.new_builder().values([batch]).get_plan_node()
            ),
        )

        runner = LocalRunner(plan_builder.get_plan_node())
        total_size = 0

        for vector in runner.execute():
            total_size += vector.size()
        self.assertEqual(total_size, batch_size * batch_size)
