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
import pyarrow
import tempfile

from velox.py.arrow import to_velox
from velox.py.plan_builder import PlanBuilder
from velox.py.file import DWRF
from velox.py.type import BIGINT, ROW
from velox.py.runner import LocalRunner, register_hive


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

    def test_write_read_file(self):
        # Test writing a batch of data to a dwrf file on disk, then
        # reading it back.
        register_hive()

        # Generate input data.
        batch_size = 10
        array = pyarrow.array([42] * batch_size)
        input_batch = to_velox(pyarrow.record_batch([array], names=["c0"]))

        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = f"{temp_dir}/output_file"

            plan_builder = PlanBuilder()
            plan_builder.values([input_batch]).table_write(
                output_schema=ROW(["c0"], [BIGINT()]),
                output_file=DWRF(output_file),
                connector_id="hive",
            )

            # Execute and write to output file.
            runner = LocalRunner(plan_builder.get_plan_node())
            iterator = runner.execute()
            output = next(iterator)
            self.assertRaises(StopIteration, next, iterator)

            output_file_from_table_writer = self.extract_file(output)
            self.assertEqual(output_file, output_file_from_table_writer)

            # Now scan it back.
            scan_plan_builder = PlanBuilder()
            scan_plan_builder.table_scan(
                output_schema=ROW(["c0"], [BIGINT()]),
                connector_id="hive",
                input_files=[DWRF(output_file)],
            )

            runner = LocalRunner(scan_plan_builder.get_plan_node())
            iterator = runner.execute()
            result = next(iterator)
            self.assertRaises(StopIteration, next, iterator)

            # Ensure the read batch is the same as the one written.
            self.assertEqual(input_batch, result)

    def extract_file(self, output_vector):
        # Parse and return the output file name from the writer's output.
        output_json = json.loads(output_vector.child_at(1)[1])
        self.assertIsNotNone(output_json)

        write_infos = output_json["fileWriteInfos"]
        self.assertTrue(isinstance(write_infos, list))
        self.assertGreater(len(write_infos), 0)
        return write_infos[0].get("targetFileName", "")
