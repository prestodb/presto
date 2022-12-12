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

import pyvelox.pyvelox as pv
import unittest


class TestVeloxVector(unittest.TestCase):
    def test_from_list(self):
        self.assertTrue(isinstance(pv.from_list([1, 2, 3]), pv.BaseVector))
        self.assertTrue(isinstance(pv.from_list([1, None, None]), pv.BaseVector))
        self.assertTrue(isinstance(pv.from_list(["hello", "world"]), pv.BaseVector))
        with self.assertRaises(TypeError):
            pv.from_list(["hello", 3.14])
        with self.assertRaises(ValueError):
            pv.from_list([None, None, None])
        with self.assertRaises(ValueError):
            pv.from_list([])

    def test_to_string(self):
        self.assertEqual(
            str(pv.from_list([1, 2, 3])),
            "[FLAT BIGINT: 3 elements, no nulls]",
        )
        self.assertEqual(
            str(pv.from_list([1, None, 3])),
            "[FLAT BIGINT: 3 elements, 1 nulls]",
        )

    def test_get_item(self):
        ints = pv.from_list([1, 2, None, None, 3])
        self.assertEqual(ints[0], 1)
        self.assertEqual(ints[1], 2)
        self.assertEqual(ints[2], None)
        self.assertEqual(ints[3], None)
        self.assertEqual(ints[4], 3)

        strs = pv.from_list(["hello", "world", None])
        self.assertEqual(strs[0], "hello")
        self.assertEqual(strs[1], "world")
        self.assertEqual(strs[2], None)
        self.assertNotEqual(strs[0], "world")
        self.assertNotEqual(strs[2], "world")

        with self.assertRaises(IndexError):
            ints[5]
        with self.assertRaises(IndexError):
            ints[-1]
        with self.assertRaises(IndexError):
            strs[1000]
        with self.assertRaises(IndexError):
            strs[-1000]

    def test_set_item(self):
        ints = pv.from_list([1, 2, None, None, 3])
        self.assertEqual(ints[2], None)
        ints[2] = 10
        self.assertEqual(ints[2], 10)
        ints[4] = None
        self.assertEqual(ints[4], None)

        strs = pv.from_list(["googly", "doogly"])
        self.assertEqual(strs[1], "doogly")
        strs[1] = "moogly"
        self.assertEqual(strs[1], "moogly")
        strs[0] = None
        self.assertEqual(strs[0], None)

        with self.assertRaises(IndexError):
            ints[5] = 10
        with self.assertRaises(IndexError):
            ints[-1] = 10
        with self.assertRaises(IndexError):
            strs[1000] = "hi"
        with self.assertRaises(IndexError):
            strs[-1000] = "bye"
        with self.assertRaises(TypeError):
            ints[3] = "ni hao"
        with self.assertRaises(TypeError):
            strs[0] = 2

    def test_length(self):
        ints = pv.from_list([1, 2, None])
        self.assertEqual(len(ints), 3)
        self.assertEqual(ints.size(), 3)

        strs = pv.from_list(["hi", "bye"])
        self.assertEqual(len(strs), 2)
        self.assertEqual(strs.size(), 2)

    def test_numeric_limits(self):
        bigger_than_int32 = pv.from_list([1 << 33])
        self.assertEqual(bigger_than_int32[0], 1 << 33)
        with self.assertRaises(RuntimeError):
            bigger_than_int64 = pv.from_list([1 << 63])
        smaller_than_int64 = pv.from_list([(1 << 62) + (1 << 62) - 1])

    def test_type(self):
        ints = pv.from_list([1, 2, None])
        self.assertEqual(ints.dtype(), pv.BigintType())
        self.assertEqual(ints.typeKind(), pv.TypeKind.BIGINT)

        strs = pv.from_list(["a", "b", None])
        self.assertEqual(strs.dtype(), pv.VarcharType())
        self.assertEqual(strs.typeKind(), pv.TypeKind.VARCHAR)

    def test_misc(self):
        ints = pv.from_list([3, 4, 3, None])

        self.assertTrue(ints.mayHaveNulls())

        self.assertFalse(ints.isLazy())

        self.assertTrue(ints.isNullAt(3))
        self.assertFalse(ints.isNullAt(0))
        with self.assertRaises(IndexError):
            ints.isNullAt(10)
        with self.assertRaises(IndexError):
            ints.isNullAt(-10)

        self.assertEqual(ints.hashValueAt(0), ints.hashValueAt(2))
        self.assertNotEqual(ints.hashValueAt(0), ints.hashValueAt(1))
        with self.assertRaises(IndexError):
            ints.hashValueAt(10)
        with self.assertRaises(IndexError):
            ints.hashValueAt(-10)

        self.assertEqual(ints.encoding(), pv.VectorEncodingSimple.FLAT)

    def test_append(self):
        ints1 = pv.from_list([4, 2, 0, None])
        ints2 = pv.from_list([1, 3, 3, 7])
        self.assertEqual(len(ints1), 4)
        ints1.append(ints2)
        self.assertEqual(len(ints1), 8)
        self.assertEqual(ints1[5], 3)
        self.assertEqual(ints1[7], 7)

        strs1 = pv.from_list(["bork", "cork"])
        strs2 = pv.from_list(["pork", "stork"])
        self.assertEqual(len(strs1), 2)
        strs1.append(strs2)
        self.assertEqual(len(strs1), 4)
        self.assertEqual(strs1[3], "stork")

        with self.assertRaises(TypeError):
            ints2.append(strs2)
