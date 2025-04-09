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
import os
import random
import tempfile
import unittest

import pyarrow
from pyvelox.arrow import to_velox
from pyvelox.file import DWRF
from pyvelox.plan_builder import PlanBuilder
from pyvelox.runner import (
    LocalRunner,
    register_hive,
    register_tpch,
    unregister,
    unregister_all,
)
from pyvelox.type import BIGINT, DOUBLE, ROW, VARCHAR


class TestPyVeloxRunner(unittest.TestCase):
    # prevent
    def setUp(self) -> None:
        unregister_all()

    def tearDown(self) -> None:
        unregister_all()

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

    def test_runner_with_values_order_limit(self):
        vectors = []
        batch_size = 10
        num_batches = 10

        for i in range(num_batches):
            array = pyarrow.array(list(range(i * batch_size, (i + 1) * batch_size)))
            batch = pyarrow.record_batch([array], names=["c0"])
            vectors.append(to_velox(batch))

        plan_builder = (
            PlanBuilder().values(vectors).order_by(["c0 DESC"]).limit(5, offset=2)
        )
        runner = LocalRunner(plan_builder.get_plan_node())

        iterator = runner.execute()
        output = next(iterator)
        self.assertRaises(StopIteration, next, iterator)

        expected_result = to_velox(
            pyarrow.record_batch([pyarrow.array([97, 96, 95, 94, 93])], names=["c0"])
        )
        self.assertEqual(output, expected_result)

    def test_runner_with_hash_join(self):
        batch_size = 100
        probe = list(range(batch_size))
        build = [i for i in probe if i % 2 == 0]
        random.shuffle(probe)
        random.shuffle(build)

        probe_vector = to_velox(
            pyarrow.record_batch([pyarrow.array(probe)], names=["c0"])
        )
        build_vector = to_velox(
            pyarrow.record_batch([pyarrow.array(build)], names=["c1"])
        )

        plan_builder = PlanBuilder()
        plan_builder.values([probe_vector]).hash_join(
            left_keys=["c0"],
            right_keys=["c1"],
            build_plan_node=(
                plan_builder.new_builder().values([build_vector]).get_plan_node()
            ),
            output=["c0"],
        )
        plan_builder.aggregate(aggregations=["sum(c0)"])

        runner = LocalRunner(plan_builder.get_plan_node())
        iterator = runner.execute()
        vector = next(iterator)

        self.assertRaises(StopIteration, next, iterator)
        self.assertEqual(vector.size(), 1)

        result = int(vector.child_at(0)[0])
        self.assertEqual(result, sum(build))

    def test_runner_with_merge_join(self):
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

    def test_runner_with_merge_sort(self):
        array = pyarrow.array([0, 1, 2, 3, 4])
        batch = to_velox(pyarrow.record_batch([array], names=["c0"]))

        plan_builder = PlanBuilder()
        plan_builder.sorted_merge(
            keys=["c0"],
            sources=(
                plan_builder.new_builder().values([batch]).get_plan_node(),
                plan_builder.new_builder().values([batch]).get_plan_node(),
                plan_builder.new_builder().values([batch]).get_plan_node(),
            ),
        )

        runner = LocalRunner(plan_builder.get_plan_node())
        iterator = runner.execute()
        output = next(iterator)
        self.assertRaises(StopIteration, next, iterator)

        expected = to_velox(
            pyarrow.record_batch(
                [pyarrow.array([0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4])],
                names=["c0"],
            )
        )
        self.assertEqual(output, expected)

    def test_register_connectors(self):
        register_hive("conn1")
        self.assertRaises(RuntimeError, register_hive, "conn1")
        register_tpch("conn2")

        unregister("conn1")
        unregister("conn2")
        self.assertRaises(RuntimeError, unregister, "conn3")
        register_tpch("conn1")
        unregister("conn1")
        register_tpch("conn2")

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
                output_file=DWRF(output_file),
                connector_id="hive",
            )

            # Execute and write to output file.
            runner = LocalRunner(plan_builder.get_plan_node())
            iterator = runner.execute()
            output = next(iterator)
            self.assertRaises(StopIteration, next, iterator)
            self.assertNotEqual(runner.print_plan_with_stats(), "")

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

    def test_tpch_gen(self):
        register_tpch("tpch")
        register_hive("hive")

        num_output_files = 16

        # Generate lineitem, write to an output file, then read it back.
        with tempfile.TemporaryDirectory() as temp_dir:
            plan_builder = PlanBuilder()
            plan_builder.tpch_gen(
                table_name="lineitem",
                connector_id="tpch",
                scale_factor=0.001,
                num_parts=num_output_files,
                columns=["l_orderkey", "l_partkey", "l_quantity", "l_comment"],
            ).table_write(
                output_path=DWRF(temp_dir),
                connector_id="hive",
            )

            # Execute and write to output file.
            runner = LocalRunner(plan_builder.get_plan_node())
            output_files = []
            expected_type = ROW(
                ["l_orderkey", "l_partkey", "l_quantity", "l_comment"],
                [BIGINT(), BIGINT(), DOUBLE(), VARCHAR()],
            )

            for vector in runner.execute(max_drivers=num_output_files):
                output_file = os.path.join(temp_dir, self.extract_file(vector))
                output_dwrf_file = DWRF(output_file)

                # Assert files have the right schema.
                self.assertEqual(output_dwrf_file.get_schema(), expected_type)
                output_files.append(output_dwrf_file)

            self.assertEqual(num_output_files, len(output_files))

            # Now scan it back.
            scan_plan_builder = PlanBuilder()
            scan_plan_builder.table_scan(
                output_schema=ROW(["l_orderkey", "l_partkey"], [BIGINT()] * 2),
                connector_id="hive",
                input_files=output_files,
            )

            runner = LocalRunner(scan_plan_builder.get_plan_node())
            output_rows = 0

            for vector in runner.execute():
                output_rows += vector.size()
            self.assertEqual(output_rows, 6005)

    def extract_file(self, output_vector):
        # Parse and return the output file name from the writer's output.
        output_json = json.loads(output_vector.child_at(1)[1])
        self.assertIsNotNone(output_json)

        write_infos = output_json["fileWriteInfos"]
        self.assertTrue(isinstance(write_infos, list))
        self.assertGreater(len(write_infos), 0)
        return write_infos[0].get("targetFileName", "")
