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

import unittest
import pyarrow

from pyvelox.arrow import to_velox, to_arrow
from pyvelox.type import DOUBLE


class TestPyVeloxVector(unittest.TestCase):
    def test_vector_size(self):
        vector = to_velox(pyarrow.array([1, 2, 3, 4, 5, 6]))
        self.assertEqual(len(vector), 6)
        self.assertEqual(vector.size(), 6)

    def test_vector_nulls(self):
        data = [1, 2, None, 4, 5, 6, None, 9, 10]
        vector = to_velox(pyarrow.array(data))
        self.assertEqual(vector.null_count(), 2)

        self.assertTrue(vector.is_null_at(2))
        self.assertTrue(vector.is_null_at(6))
        self.assertFalse(vector.is_null_at(0))
        self.assertFalse(vector.is_null_at(3))
        self.assertFalse(vector.is_null_at(8))

    def test_vector_compare(self):
        data = [1, 2, 3, 4, 5, 6, 7, 9, 10]
        vector1 = to_velox(pyarrow.array(data))
        vector2 = to_velox(to_arrow(vector1))

        # First compare each element.
        for i in range(len(data)):
            self.assertEqual(vector1.compare(vector2, i, i), 0)

        # Then the entire objects at once (__eq__).
        self.assertEqual(vector1, vector2)

        # Failures.
        vector3 = to_velox(pyarrow.array([1, 2, 3, 4, 5]))
        self.assertNotEqual(vector1, vector3)

        vector4 = to_velox(pyarrow.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
        self.assertNotEqual(vector1, vector4)

    def test_vector_print(self):
        vector = to_velox(pyarrow.array([1, 2]))

        expected_header = "[FLAT BIGINT: 2 elements, no nulls]"
        self.assertEqual(str(vector), expected_header)
        self.assertTrue(vector.print_detailed().startswith(expected_header))

        self.assertEqual(vector.print_all(), "0: 1\n1: 2")
        self.assertEqual(vector.summarize_to_text(), "BIGINT 2 rows FLAT 16B\n")

    def test_vector_complex(self):
        # Array/lists.
        data = [[1, 2, 3], [4, None, 6], None, [7]]
        vector = to_velox(pyarrow.array(data))
        self.assertEqual(str(vector), "[ARRAY ARRAY<BIGINT>: 4 elements, 1 nulls]")

        # Maps.
        data = [[{"key": "a", "value": 1}, {"key": "b", "value": 2}]]
        map_type = pyarrow.map_(pyarrow.string(), pyarrow.int32())
        vector = to_velox(pyarrow.array(data, type=map_type))
        self.assertEqual(
            str(vector), "[MAP MAP<VARCHAR,INTEGER>: 1 elements, no nulls]"
        )

        # Row/structs.
        struct_type = pyarrow.struct(
            [("name", pyarrow.string()), ("age", pyarrow.int32())]
        )
        data = [{"name": "John", "age": 23}, {"name": "Mike", "age": 32}]
        vector = to_velox(pyarrow.array(data, type=struct_type))
        self.assertEqual(
            str(vector), "[ROW ROW<name:VARCHAR,age:INTEGER>: 2 elements, no nulls]"
        )

        # Validate children.
        self.assertEqual(
            str(vector.child_at(0)), "[FLAT VARCHAR: 2 elements, no nulls]"
        )
        self.assertEqual(
            str(vector.child_at(1)), "[FLAT INTEGER: 2 elements, no nulls]"
        )

    def test_vector_type(self):
        data = [1.9, 2.45]
        vector = to_velox(pyarrow.array(data))
        self.assertEqual(vector.type(), DOUBLE())
