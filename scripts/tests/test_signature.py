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
from scripts.signature import bias_signatures
from pathlib import Path
import json


def read_from_file(file_path):
    return Path(file_path).read_text()


def test_bias(base_signatures, contender_signatures):
    return bias_signatures(
        json.loads(base_signatures), json.loads(contender_signatures), 10
    )


class SignatureTest(unittest.TestCase):
    def test_bias(self):
        # Remove a signature
        _, return_value = test_bias(
            """{"reverse": ["(array(T)) -> array(T)"]}""",
            """{"reverse": []}""",
        )

        self.assertEqual(return_value, 1)

        # Add a new signature
        bias_functions, _ = test_bias(
            """{"reverse": ["(array(T)) -> array(T)"]}""",
            """{"reverse": ["(array(T)) -> array(T)"],
                   "foo": ["(varchar) -> varchar"]}""",
        )

        self.assertEqual(bias_functions, "foo=10")

        # Modify a signature.
        bias_functions, _ = test_bias(
            """{"reverse": ["(array(T)) -> array(T)"]}""",
            """{"reverse": ["(array(T)) -> array(T)", "(varchar) -> varchar"]}""",
        )

        self.assertEqual(bias_functions, "reverse=10")

        # Add more than one signature change
        bias_functions, _ = test_bias(
            """{"reverse": ["(array(T)) -> array(T)"]}""",
            """{"reverse": ["(array(T)) -> array(T)"],
                   "foo": ["(varchar) -> varchar"],
                   "bar": ["(varchar) -> varchar"]}""",
        )

        self.assertEqual(bias_functions, "bar=10,foo=10")


if __name__ == "__main__":
    unittest.main()
