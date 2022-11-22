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


class TestVeloxTypes(unittest.TestCase):
    def test_types(self):
        # Ensure we support all the basic types
        self.assertTrue(isinstance(pv.BooleanType(), pv.VeloxType))
        self.assertTrue(isinstance(pv.IntegerType(), pv.VeloxType))
        self.assertTrue(isinstance(pv.BigintType(), pv.VeloxType))
        self.assertTrue(isinstance(pv.SmallintType(), pv.VeloxType))
        self.assertTrue(isinstance(pv.TinyintType(), pv.VeloxType))
        self.assertTrue(isinstance(pv.RealType(), pv.VeloxType))
        self.assertTrue(isinstance(pv.DoubleType(), pv.VeloxType))
        self.assertTrue(isinstance(pv.TimestampType(), pv.VeloxType))
        self.assertTrue(isinstance(pv.VarcharType(), pv.VeloxType))
        self.assertTrue(isinstance(pv.VarbinaryType(), pv.VeloxType))

        # Complex types
        self.assertTrue(isinstance(pv.ArrayType(pv.BooleanType()), pv.VeloxType))
        self.assertTrue(
            isinstance(pv.MapType(pv.VarcharType(), pv.VarbinaryType()), pv.VeloxType)
        )
        self.assertTrue(
            isinstance(pv.RowType(["c0"], [pv.BooleanType()]), pv.VeloxType)
        )

    def test_complex_types(self):
        arrayType = pv.ArrayType(pv.BigintType())
        self.assertEquals(arrayType.element_type(), pv.BigintType())

        mapType = pv.MapType(pv.VarcharType(), pv.VarbinaryType())
        self.assertEquals(mapType.key_type(), pv.VarcharType())
        self.assertEquals(mapType.value_type(), pv.VarbinaryType())

        rowType = pv.RowType(
            ["c0", "c1", "c2"], [pv.BooleanType(), pv.BigintType(), pv.VarcharType()]
        )
        self.assertEquals(rowType.size(), 3)
        self.assertEquals(rowType.child_at(0), pv.BooleanType())
        self.assertEquals(rowType.find_child("c1"), pv.BigintType())
        self.assertEquals(rowType.get_child_idx("c1"), 1)
        self.assertEquals(rowType.name_of(1), "c1")
        self.assertEquals(rowType.names(), ["c0", "c1", "c2"])
