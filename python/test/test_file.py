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

from pyvelox.file import PARQUET, DWRF, NIMBLE, ORC, JSON, TEXT, File


class TestPyVeloxFile(unittest.TestCase):
    def test_input_file(self):
        file_parquet = PARQUET("my_parquet_file")
        file_nimble = NIMBLE("/tmp/my_nimble")
        file_dwrf = DWRF("dir/some_file.dwrf")
        file_orc = ORC("orc_file")
        file_json = JSON("dir2/other_file.json")
        file_text = TEXT("a/b/csome_file.txt")

        self.assertEqual(str(file_parquet), "my_parquet_file (parquet)")
        self.assertEqual(str(file_nimble), "/tmp/my_nimble (nimble)")
        self.assertEqual(str(file_dwrf), "dir/some_file.dwrf (dwrf)")
        self.assertEqual(str(file_orc), "orc_file (orc)")
        self.assertEqual(str(file_json), "dir2/other_file.json (json)")
        self.assertEqual(str(file_text), "a/b/csome_file.txt (text)")

    def test_input_file_string(self):
        p = "/any/path"

        self.assertEqual(File(p, "parquet"), PARQUET(p))
        self.assertEqual(File(p, "nimble"), NIMBLE(p))
        self.assertEqual(File(p, "dwrf"), DWRF(p))

        self.assertEqual(File(p, "DWRF"), DWRF(p))
        self.assertEqual(File(p, "dWrF"), DWRF(p))

        self.assertRaises(RuntimeError, File, p, "bla")
        self.assertRaises(RuntimeError, File, p, "unknown")
