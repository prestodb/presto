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

import copy
import json
import os
import tempfile

import conbench.runner

from veloxbench import _benchmark

RUN_OPTIONS = {
    "iterations": {
        "default": None,
        "type": int,
        "help": "Number of iterations of each benchmark.",
    },
}


# TODO: what other COMMON_OPTIONS?
# https://github.com/ursacomputing/benchmarks/blob/033eee0951adbf41931a2de95caccbac887da6ff/benchmarks/cpp_micro_benchmarks.py#L18-L59
# TODO: the suite filter won't quite work yet either
COMMON_OPTIONS = {
    "suite-filter": {
        "default": None,
        "type": str,
        "help": "Regex filtering benchmark suites.",
    },
}


def get_run_command(output_dir, options):
    # TODO: we'll probably want to unify these runner scripts
    # Must run `make benchmarks-basic-build` before this
    # TODO: deal with the paths better (especially to benchmarks/basic)
    command = [
        "../../../scripts/benchmark-runner.py",
        "run",
        "--path",
        "../../../_build/release/velox/benchmarks/basic/",
        "--dump-path",
        output_dir,
    ]

    # TODO: extend cpp micro benchmarks to allow for iterations
    iterations = options.get("iterations", None)
    if iterations:
        raise NotImplementedError()

    return command


def _parse_benchmark_name(full_name):
    # TODO: Do we need something more complicated?
    # https://github.com/ursacomputing/benchmarks/blob/033eee0951adbf41931a2de95caccbac887da6ff/benchmarks/cpp_micro_benchmarks.py#L86-L103
    if full_name[0] == "%":
        full_name = full_name[1:]
    return {"name": full_name}


@conbench.runner.register_benchmark
class RecordCppMicroBenchmarks(_benchmark.Benchmark):
    """Run the Velox C++ micro benchmarks."""

    external = True
    name = "cpp-micro"
    # see TODO about COMMON_OPTIONS above, set an empty dict for now
    options = copy.deepcopy(COMMON_OPTIONS)
    options.update(**RUN_OPTIONS)
    description = "Run the Velox C++ micro benchmarks."
    iterations = 1
    flags = {"language": "C++"}

    def run(self, **kwargs):
        with tempfile.TemporaryDirectory() as result_dir:
            run_command = get_run_command(result_dir, kwargs)
            self.execute_command(run_command)

            # iterate through files to make the suites
            with os.scandir(result_dir) as result_files:
                for result_file in result_files:
                    suite = result_file.name.replace(".json", "", 1)
                    with open(result_file.path, "r") as f:
                        results = json.load(f)
                    self.conbench.mark_new_batch()
                    for result in results:
                        # Folly benchmark exports line separators by mistake as
                        # an entry in the json file.
                        if result[1] == "-":
                            continue
                        yield self._record_result(suite, result, kwargs)

    def _record_result(self, suite, result, options):
        info, context = {}, {"benchmark_language": "C++"}
        # TODO: we should really name these in the json and not rely on position!
        tags = _parse_benchmark_name(result[1])
        name = tags.pop("name")
        tags["suite"] = suite
        tags["source"] = self.name

        values = self._get_values(result)

        return self.record(
            values,
            tags,
            info,
            context,
            options=options,
            output=result,
            name=name,
        )

    def _get_values(self, result):
        return {
            # Folly always returns in ns, so use that. All benchmarks are times, none are throughput so both data and times have the same unit
            "data": [result[2]],
            "unit": self._format_unit("ns"),
            "times": [result[2]],
            "time_unit": "ns",
        }

    def _format_unit(self, x):
        if x == "bytes_per_second":
            return "B/s"
        if x == "items_per_second":
            return "i/s"
        return x
