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

import pyarrow as pa
import pyvelox.pyvelox as pv
import unittest


class TestVeloxVector(unittest.TestCase):
    def test_inheritance(self):
        v1 = pv.from_list([1, 2, 3])
        v2 = pv.from_list(["hello", "world"])
        v3 = pv.constant_vector(1000, 10)
        v4 = pv.dictionary_vector(pv.from_list([1, 2, 3]), [0, 0, 1])

        self.assertTrue(isinstance(v1, pv.BaseVector))
        self.assertTrue(isinstance(v2, pv.BaseVector))
        self.assertTrue(isinstance(v3, pv.BaseVector))
        self.assertTrue(isinstance(v4, pv.BaseVector))

        self.assertTrue(isinstance(v1, pv.SimpleVector_BIGINT))
        self.assertTrue(isinstance(v2, pv.SimpleVector_VARBINARY))
        self.assertTrue(isinstance(v3, pv.SimpleVector_BIGINT))
        self.assertTrue(isinstance(v4, pv.SimpleVector_BIGINT))

        self.assertTrue(isinstance(v1, pv.FlatVector_BIGINT))
        self.assertTrue(isinstance(v2, pv.FlatVector_VARBINARY))
        self.assertTrue(isinstance(v3, pv.ConstantVector_BIGINT))
        self.assertTrue(isinstance(v4, pv.DictionaryVector_BIGINT))

        self.assertFalse(isinstance(v1, pv.ConstantVector_BIGINT))
        self.assertFalse(isinstance(v2, pv.ConstantVector_VARBINARY))
        self.assertFalse(isinstance(v3, pv.FlatVector_BIGINT))
        self.assertFalse(isinstance(v4, pv.ConstantVector_BIGINT))

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

    def test_from_list_with_type(self):
        list_a = [0, 1, 3]
        a = pv.from_list(list_a, pv.BooleanType())
        self.assertEqual(a.typeKind().name, "BOOLEAN")
        for i in range(len(a)):
            self.assertTrue(isinstance(a[i], bool))
            self.assertEqual(a[i], bool(list_a[i]))
        self.assertTrue(
            isinstance(
                pv.from_list([None, None, None], pv.VarcharType()), pv.BaseVector
            )
        )
        empty_vector = pv.from_list([], pv.IntegerType())
        self.assertTrue(isinstance(empty_vector, pv.BaseVector))
        with self.assertRaises(IndexError):
            a = empty_vector[0]
        with self.assertRaises(RuntimeError):
            a = pv.from_list(
                [0, 1, 3], pv.VarcharType()
            )  # Conversion not possible from int to varchar
        list_b = [0.2, 1.2, 3.23]
        b = pv.from_list(list_b, pv.RealType())
        for i in range(len(list_b)):
            self.assertNotAlmostEqual(list_b[i], b[i], places=17)

        # dtype as a keyword argument
        integerVector = pv.from_list([1, 3, 11], dtype=pv.IntegerType())
        self.assertTrue(isinstance(integerVector, pv.BaseVector))
        self.assertEqual(integerVector.typeKind().name, "INTEGER")

    def test_constant_encoding(self):
        ints = pv.constant_vector(1000, 10)
        strings = pv.constant_vector("hello", 100)
        null = pv.constant_vector(None, 1000, pv.SmallintType())
        floats = pv.constant_vector(-3.14, 499)
        self.assertEqual(ints.encoding(), pv.VectorEncodingSimple.CONSTANT)
        self.assertEqual(strings.encoding(), pv.VectorEncodingSimple.CONSTANT)
        self.assertEqual(null.encoding(), pv.VectorEncodingSimple.CONSTANT)
        self.assertEqual(floats.encoding(), pv.VectorEncodingSimple.CONSTANT)
        self.assertEqual(len(ints), 10)
        self.assertEqual(len(strings), 100)
        self.assertEqual(len(null), 1000)
        self.assertEqual(len(floats), 499)
        self.assertEqual(ints.typeKind(), pv.TypeKind.BIGINT)
        self.assertEqual(strings.typeKind(), pv.TypeKind.VARCHAR)
        self.assertEqual(null.typeKind(), pv.TypeKind.SMALLINT)
        self.assertEqual(floats.typeKind(), pv.TypeKind.DOUBLE)

        for i in range(len(ints)):
            self.assertEqual(ints[i], 1000)
        for x in ints:
            self.assertEqual(x, 1000)

        for i in range(len(strings)):
            self.assertEqual(strings[i], "hello")
        for x in strings:
            self.assertEqual(x, "hello")

        for i in range(len(null)):
            self.assertEqual(null[i], None)
        for x in null:
            self.assertEqual(x, None)

        for i in range(len(floats)):
            self.assertEqual(floats[i], -3.14)
        for x in floats:
            self.assertEqual(x, -3.14)

        with self.assertRaises(IndexError):
            ints[10]
        with self.assertRaises(TypeError):
            ints[1] = -1

    def test_dictionary_encoding(self):
        base_indices = [0, 0, 1, 0, 2]
        vec = pv.dictionary_vector(pv.from_list([1, 2, 3]), base_indices)
        self.assertTrue(isinstance(vec, pv.DictionaryVector_BIGINT))
        expected_values = [1, 1, 2, 1, 3]
        self.assertEqual(len(vec), len(expected_values))
        for i in range(len(vec)):
            self.assertEqual(vec[i], expected_values[i])

        indices = vec.indices()
        self.assertTrue(isinstance(indices, pv.DictionaryIndices))
        self.assertEqual(len(indices), len(base_indices))
        for i in range(len(indices)):
            self.assertEqual(indices[i], base_indices[i])

        base_vector = pv.from_list([1, 2, 3, 4, 5, 6, 7])
        base_indices = [6, 6]
        vec = pv.dictionary_vector(base_vector, base_indices)
        expected_values = [7, 7]
        for i in range(len(vec)):
            self.assertEqual(vec[i], expected_values[i])

        with self.assertRaises(TypeError):
            pv.dictionary_vector(pv.from_list([1, 2, 3]), ["a", 0, 0, "b"])
        with self.assertRaises(IndexError):
            pv.dictionary_vector(pv.from_list([1, 2, 3]), [1, 2, 1000000])
            pv.dictionary_vector(pv.from_list([1, 2, 3]), [0, -1, -2])

    def test_array_vector(self):
        v1 = pv.from_list([[1, 2, 3], [1, 2, 3]])
        self.assertTrue(isinstance(v1, pv.ArrayVector))
        self.assertTrue(isinstance(v1.elements(), pv.FlatVector_BIGINT))
        self.assertEqual(len(v1), 2)
        expected_flat = [1, 2, 3, 1, 2, 3]
        self.assertEqual(len(expected_flat), len(v1.elements()))
        for i in range(len(expected_flat)):
            self.assertEqual(expected_flat[i], v1.elements()[i])

        v2 = pv.from_list([[1], [1, 2, None]])
        self.assertTrue(isinstance(v2, pv.ArrayVector))
        self.assertTrue(isinstance(v2.elements(), pv.FlatVector_BIGINT))
        self.assertEqual(len(v2), 2)
        expected_flat = [1, 1, 2, None]
        self.assertEqual(len(v2.elements()), len(expected_flat))
        for i in range(len(expected_flat)):
            self.assertEqual(expected_flat[i], v2.elements()[i])

        doubleNested = pv.from_list([[[1, 2], [3, None]], [[1], [2]]])
        self.assertTrue(isinstance(doubleNested, pv.ArrayVector))
        self.assertTrue(isinstance(doubleNested.elements(), pv.ArrayVector))
        self.assertEqual(len(doubleNested), 2)
        elements = doubleNested.elements().elements()
        self.assertTrue(isinstance(elements, pv.FlatVector_BIGINT))
        self.assertEqual(len(elements), 6)
        expected_firstElements = [1, 2, 3, None, 1, 2]
        self.assertEqual(len(elements), len(expected_firstElements))
        for i in range(len(expected_firstElements)):
            self.assertEqual(expected_firstElements[i], elements[i])

        with self.assertRaises(TypeError):
            a = pv.from_list([[[1, 2], [3, 4]], [[1.1], [2.3]]])

        with self.assertRaises(ValueError):
            v = pv.from_list([[None], [None, None, None]])

        with self.assertRaises(TypeError):
            a = pv.from_list([[[1, 2], [3, 4]], [["hello"], ["world"]]])

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

    def test_slice(self):
        a = pv.from_list(list(range(0, 10)))

        b = a.slice(2, 6)
        self.assertEqual(len(b), 4)
        for i in range(4):
            self.assertEqual(b[i], i + 2)

        with self.assertRaises(NotImplementedError):
            c = a.slice(2, 6, 2)

        d = a[3:6]
        self.assertEqual(len(d), 3)
        for i in range(3):
            self.assertEqual(d[i], i + 3)

        with self.assertRaises(NotImplementedError):
            e = a[3:8:3]

    def test_export_to_arrow(self):
        test_cases = [
            ([1, 2, 3], pa.int64()),
            ([1.1, 2.2, 3.3], pa.float64()),
            (["ab", "bc", "ca"], pa.string()),
        ]
        for data, expected_type in test_cases:
            with self.subTest(data=data):
                vector = pv.from_list(data)
                array = pv.export_to_arrow(vector)

                self.assertEqual(array.type, expected_type)
                self.assertEqual(len(array), len(data))
                self.assertListEqual(array.tolist(), data)

    def test_import_from_arrow(self):
        test_cases = [
            ([11, 26, 31], pa.int64(), pv.IntegerType()),
            ([0.1, 2.5, 3.9], pa.float64(), pv.DoubleType()),
            (["az", "by", "cx"], pa.string(), pv.VarcharType()),
        ]
        for data, dtype, expected_type in test_cases:
            with self.subTest(data=data):
                array = pa.array(data, type=dtype)
                velox_vector = pv.import_from_arrow(array)

                self.assertEqual(velox_vector.size(), len(data))
                self.assertTrue(velox_vector.dtype(), expected_type)
                for i in range(0, len(data)):
                    self.assertEqual(velox_vector[i], data[i])

    def test_roundtrip_conversion(self):
        test_cases = [
            ([41, 92, 13], pv.IntegerType()),
            ([17.19, 22.25, 13.3], pv.DoubleType()),
            (["aa1", "bb2", "cc3"], pv.VarcharType()),
        ]
        for data, expected_type in test_cases:
            with self.subTest(data=data):
                vector = pv.from_list(data)
                array = pv.export_to_arrow(vector)

                velox_vector = pv.import_from_arrow(array)
                self.assertEqual(velox_vector.size(), len(data))
                self.assertTrue(velox_vector.dtype(), expected_type)
                for i in range(0, len(data)):
                    self.assertEqual(velox_vector[i], data[i])

    def test_row_vector_basic(self):
        vals = [
            pv.from_list([1, 2, 3]),
            pv.from_list([4.0, 5.0, 6.0]),
            pv.from_list(["a", "b", "c"]),
        ]

        col_names = ["x", "y", "z"]
        rw = pv.row_vector(col_names, vals)
        rw_str = str(rw)
        expected_str = "0: {1, 4, a}\n1: {2, 5, b}\n2: {3, 6, c}"
        assert expected_str == rw_str

    def test_row_vector_with_nulls(self):
        vals = [
            pv.from_list([1, 2, 3, 1, 2]),
            pv.from_list([4, 5, 6, 4, 5]),
            pv.from_list([7, 8, 9, 7, 8]),
            pv.from_list([10, 11, 12, 10, 11]),
        ]

        col_names = ["a", "b", "c", "d"]
        rw = pv.row_vector(col_names, vals, {0: True, 2: True})
        rw_str = str(rw)
        expected_str = (
            "0: null\n1: {2, 5, 8, 11}\n2: null\n3: {1, 4, 7, 10}\n4: {2, 5, 8, 11}"
        )
        assert expected_str == rw_str

    def test_row_vector_comparison(self):
        u = [
            pv.from_list([1, 2, 3]),
            pv.from_list([7, 4, 9]),
            pv.from_list([10, 11, 12]),
        ]

        v = [
            pv.from_list([1, 2, 3]),
            pv.from_list([7, 8, 9]),
            pv.from_list([10, 11, 12]),
        ]

        w = [
            pv.from_list([1, 2, 3]),
            pv.from_list([7, 8, 9]),
        ]

        u_names = ["a", "b", "c"]
        w_names = ["x", "y"]
        u_rw = pv.row_vector(u_names, u)
        v_rw = pv.row_vector(u_names, v)
        w_rw = pv.row_vector(w_names, w)
        y_rw = pv.row_vector(u_names, u)
        x1_rw = pv.row_vector(u_names, u, {0: True, 2: True})
        x2_rw = pv.row_vector(u_names, u, {0: True, 2: True})

        assert u_rw != w_rw  # num of children doesn't match
        assert u_rw != v_rw  # data doesn't match
        assert u_rw == y_rw  # data match
        assert x1_rw == x2_rw  # with null
        assert x1_rw != u_rw  # with and without null
