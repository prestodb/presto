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

from .. import cpp_micro_benchmarks


def test_parse_benchmark_name_no_params():
    tags = cpp_micro_benchmarks._parse_benchmark_name("Something")
    assert tags == {"name": "Something"}


def test_get_values():
    result = [
        "/some/path/FeatureNormalization.cpp",
        "normalize",
        20.99487392089844,
    ]
    benchmark = cpp_micro_benchmarks.RecordCppMicroBenchmarks()
    actual = benchmark._get_values(result)
    assert actual == {
        "data": [20.99487392089844],
        "time_unit": "ns",
        "times": [20.99487392089844],
        "unit": "ns",
    }


def test_format_unit():
    benchmark = cpp_micro_benchmarks.RecordCppMicroBenchmarks()
    assert benchmark._format_unit("bytes_per_second") == "B/s"
    assert benchmark._format_unit("items_per_second") == "i/s"
    assert benchmark._format_unit("foo_per_bar") == "foo_per_bar"


def test_get_run_command():
    options = {
        "iterations": None,
    }
    actual = cpp_micro_benchmarks.get_run_command("out", options)
    assert actual == [
        "../../../scripts/benchmark-runner.py",
        "run",
        "--path",
        "../../../_build/release/velox/benchmarks/basic/",
        "--dump-path",
        "out",
    ]
