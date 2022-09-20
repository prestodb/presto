#!/usr/bin/env python3
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


# Set up the CPP microbenchmarks runner class without importing conbench
# (so users without conbench installed can run this file as a script).
# The conbench integration is set up in implemented_benchmarks.py

import copy
import os
import pathlib
import subprocess
import tempfile


REPO_ROOT = pathlib.Path(__file__).parent.parent.parent.parent

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


class LocalCppMicroBenchmarks:
    """Run the Velox C++ micro benchmarks."""

    external = True
    name = "cpp-micro"
    # see TODO about COMMON_OPTIONS above, set an empty dict for now
    options = copy.deepcopy(COMMON_OPTIONS)
    options.update(**RUN_OPTIONS)
    description = "Run the Velox C++ micro benchmarks."
    iterations = 1
    flags = {"language": "C++"}

    def run(self, result_dir, **kwargs):
        binaries = self._find_binaries(
            REPO_ROOT.joinpath("_build", "release", "velox", "benchmarks", "basic")
        )
        result_dir_path = pathlib.Path(result_dir)

        for binary_path in binaries:
            out_path = result_dir_path / f"{binary_path.name}.json"
            print(f"Executing and dumping results for '{binary_path}' to '{out_path}':")
            run_command = [
                binary_path,
                "--bm_max_secs",
                "10",
                "--bm_max_trials",
                "1000000",
                "--bm_json_verbose",
                out_path,
            ]

            # TODO: extend cpp micro benchmarks to allow for iterations
            iterations = kwargs.get("iterations", None)
            if iterations:
                raise NotImplementedError()

            try:
                print(run_command)
                result = subprocess.run(run_command, check=True)
            except subprocess.CalledProcessError as e:
                print(e.stderr.decode("utf-8"))
                raise e

    @staticmethod
    def _find_binaries(binary_dir: pathlib.Path):
        # Must run `make benchmarks-basic-build` before this
        binaries = [
            path
            for path in binary_dir.glob("*")
            if os.access(path, os.X_OK) and path.is_file()
        ]
        if not binaries:
            raise ValueError(f"No binaries found at path {binary_dir.resolve()}")

        print(f"Found {len(binaries)} binaries to execute")
        return binaries

    @staticmethod
    def _parse_benchmark_name(full_name):
        # TODO: Do we need something more complicated?
        # https://github.com/ursacomputing/benchmarks/blob/033eee0951adbf41931a2de95caccbac887da6ff/benchmarks/cpp_micro_benchmarks.py#L86-L103
        if full_name[0] == "%":
            full_name = full_name[1:]
        return {"name": full_name}

    def _get_values(self, result):
        return {
            # Folly always returns in ns, so use that. All benchmarks are times, none are throughput so both data and times have the same unit
            "data": [result[2]],
            "unit": self._format_unit("ns"),
            "times": [result[2]],
            "time_unit": "ns",
        }

    @staticmethod
    def _format_unit(x):
        if x == "bytes_per_second":
            return "B/s"
        if x == "items_per_second":
            return "i/s"
        return x


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as result_dir:
        LocalCppMicroBenchmarks().run(result_dir=result_dir)
