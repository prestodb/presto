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

import pathlib
import tempfile

from ..cpp_micro_benchmarks import LocalCppMicroBenchmarks


REPO_ROOT = pathlib.Path(__file__).parent.parent.parent.parent.parent
benchmark = LocalCppMicroBenchmarks()


def test_parse_benchmark_name_no_params():
    tags = benchmark._parse_benchmark_name("Something")
    assert tags == {"name": "Something"}


def test_get_values():
    result = [
        "/some/path/FeatureNormalization.cpp",
        "normalize",
        20.99487392089844,
    ]
    actual = benchmark._get_values(result)
    assert actual == {
        "data": [20.99487392089844],
        "time_unit": "ns",
        "times": [20.99487392089844],
        "unit": "ns",
    }


def test_format_unit():
    assert benchmark._format_unit("bytes_per_second") == "B/s"
    assert benchmark._format_unit("items_per_second") == "i/s"
    assert benchmark._format_unit("foo_per_bar") == "foo_per_bar"


def test_find_binaries():
    with tempfile.TemporaryDirectory() as temp_dir:
        fake_binaries = [pathlib.Path(temp_dir) / name for name in ("blah", "blahblah")]

        for binary in fake_binaries:
            binary.touch(mode=0o777)

        res = benchmark._find_binaries(pathlib.Path(temp_dir))
        assert set(res) == set(fake_binaries)
