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

import shutil
import tempfile
import unittest
from os import path

import pyvelox.pyvelox as pv


class TestVeloxVectorSaver(unittest.TestCase):
    def setUp(self):
        # create a temporary directory
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # remove the temporary directory
        shutil.rmtree(self.test_dir)

    def make_flat_vector(self):
        return pv.from_list([1, 2, 3])

    def make_const_vector(self):
        return pv.constant_vector(1000, 10)

    def make_dict_vector(self):
        base_indices = [0, 0, 1, 0, 2]
        return pv.dictionary_vector(pv.from_list([1, 2, 3]), base_indices)

    def test_serde_vector(self):
        data = {
            "flat_vector": self.make_flat_vector(),
            "const_vector": self.make_const_vector(),
            "dict_vector": self.make_dict_vector(),
        }

        paths = {
            "flat_vector": path.join(self.test_dir, "flat.pyvelox"),
            "const_vector": path.join(self.test_dir, "const.pyvelox"),
            "dict_vector": path.join(self.test_dir, "dict.pyvelox"),
        }

        for vec_key, fpath_key in zip(data, paths):
            vec = data[vec_key]
            fpath = paths[fpath_key]
            pv.save_vector(vector=vec, file_path=fpath)
            loaded_vec = pv.load_vector(file_path=fpath)
            self.assertEqual(len(vec), len(loaded_vec))
            self.assertEqual(vec.dtype(), loaded_vec.dtype())
            for i in range(len(vec)):
                self.assertEqual(vec[i], loaded_vec[i])
