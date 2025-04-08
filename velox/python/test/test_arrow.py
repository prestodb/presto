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
from pyvelox.vector import Vector


class TestPyVeloxArrow(unittest.TestCase):
    def test_vector_simple(self):
        array = pyarrow.array([1, 2, 3, 4, 5, 6])
        vector = to_velox(array)

        self.assertTrue(isinstance(vector, Vector))
        self.assertEqual(len(vector), 6)

        # TODO: For now we only return the values as strings for printing.
        for i in range(len(array)):
            self.assertEqual(vector[i], str(array[i]))

    def test_roundtrip(self):
        array = pyarrow.array([2, 2, 3, 4, 4, 0])
        array2 = to_arrow(to_velox(array))

        self.assertTrue(isinstance(array2, pyarrow.Array))
        self.assertEqual(array, array2)

    def test_empty(self):
        # TODO: Velox's arrow bridge does not allow missing buffers (even if
        # there are no rows):
        #   https://github.com/facebookincubator/velox/issues/12082

        # vector = to_velox(pyarrow.array([]))
        # self.assertEqual(vector.size(), 0)
        pass
