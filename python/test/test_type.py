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
from pyvelox.type import (
    Type,
    BIGINT,
    INTEGER,
    SMALLINT,
    TINYINT,
    BOOLEAN,
    REAL,
    DOUBLE,
    VARCHAR,
    VARBINARY,
    ARRAY,
    MAP,
    ROW,
)


class TestPyVeloxTypes(unittest.TestCase):
    def test_simple_types(self):
        self.assertTrue(isinstance(BIGINT(), Type))
        self.assertTrue(isinstance(INTEGER(), Type))
        self.assertTrue(isinstance(SMALLINT(), Type))
        self.assertTrue(isinstance(TINYINT(), Type))
        self.assertTrue(isinstance(BOOLEAN(), Type))
        self.assertTrue(isinstance(REAL(), Type))
        self.assertTrue(isinstance(DOUBLE(), Type))
        self.assertTrue(isinstance(VARCHAR(), Type))
        self.assertTrue(isinstance(VARBINARY(), Type))

    def test_complex_types(self):
        self.assertTrue(isinstance(ARRAY(VARCHAR()), Type))
        self.assertTrue(isinstance(MAP(BIGINT(), VARCHAR()), Type))
        self.assertTrue(
            isinstance(ROW(["c0", "c1"], [INTEGER(), ARRAY(VARCHAR())]), Type)
        )
        self.assertTrue(isinstance(ROW(), Type))

        # Invalid complex types.
        self.assertRaises(TypeError, ARRAY)
        self.assertRaises(TypeError, MAP)
        self.assertRaises(TypeError, MAP, BIGINT())
        self.assertRaises(RuntimeError, ROW, ["col1"])

    def test_to_str(self):
        self.assertEqual("BOOLEAN", str(BOOLEAN()))
        self.assertEqual("VARBINARY", str(VARBINARY()))

        self.assertEqual("ARRAY<DOUBLE>", str(ARRAY(DOUBLE())))
        self.assertEqual("MAP<VARCHAR,DOUBLE>", str(MAP(VARCHAR(), DOUBLE())))
        self.assertEqual(
            "ROW<c0:INTEGER,c1:TINYINT>", str(ROW(["c0", "c1"], [INTEGER(), TINYINT()]))
        )
        velox_type = ROW(
            ["c0", "c1"],
            [MAP(MAP(ARRAY(ARRAY(REAL())), DOUBLE()), BOOLEAN()), TINYINT()],
        )
        self.assertEqual(
            "ROW<c0:MAP<MAP<ARRAY<ARRAY<REAL>>,DOUBLE>,BOOLEAN>,c1:TINYINT>",
            str(velox_type),
        )

    def test_equality(self):
        self.assertEqual(INTEGER(), INTEGER())
        self.assertEqual(
            MAP(MAP(TINYINT(), BOOLEAN()), BOOLEAN()),
            MAP(MAP(TINYINT(), BOOLEAN()), BOOLEAN()),
        )
        self.assertEqual(ROW(["a"], [INTEGER()]), ROW(["a"], [INTEGER()]))

        self.assertNotEqual(BIGINT(), INTEGER())
        self.assertNotEqual(ARRAY(BIGINT()), REAL())
