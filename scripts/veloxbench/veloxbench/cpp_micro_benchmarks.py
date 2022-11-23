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

import argparse
import copy
import os
import pathlib
import re
import subprocess
import sys
import tempfile


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
    description = "Run Velox C++ micro-benchmarks."
    iterations = 1
    flags = {"language": "C++"}

    def run(
        self,
        result_dir,
        binary_path=None,
        binary_filter=None,
        bm_filter=None,
        bm_max_secs=None,
        bm_max_trials=None,
        bm_estimate_time=False,
        **kwargs,
    ):
        if binary_path:
            binary_path = self._normalize_path(binary_path)
        else:
            binary_path = self._default_binary_path()

        binaries = self._find_binaries(binary_path)
        result_dir_path = pathlib.Path(result_dir)

        for binary_path in binaries:
            if binary_filter and not re.search(binary_filter, binary_path.name):
                continue

            out_path = result_dir_path / f"{binary_path.name}.json"
            print(f"Executing and dumping results for '{binary_path}' to '{out_path}':")
            run_command = [
                binary_path,
                "--bm_json_verbose",
                out_path,
            ]

            if bm_max_secs:
                run_command.extend(["--bm_max_secs", str(bm_max_secs)])

            if bm_max_trials:
                run_command.extend(["--bm_max_trials", str(bm_max_trials)])

            if bm_filter:
                run_command.extend(["--bm_regex", bm_filter])

            if bm_estimate_time:
                run_command.append("--bm_estimate_time")

            # TODO: Extend cpp micro benchmarks to allow for iterations.
            if kwargs.get("iterations", None):
                raise NotImplementedError()

            try:
                print(run_command)
                result = subprocess.run(run_command, check=True)
            except subprocess.CalledProcessError as e:
                print(e.stderr.decode("utf-8"))
                raise e

    @staticmethod
    def _find_binaries(binary_path: pathlib.Path):
        print(f"Looking for binaries at '{binary_path}'")

        # Must run `make benchmarks-basic-build` before this
        binaries = [
            path
            for path in binary_path.glob("*")
            if os.access(path, os.X_OK) and path.is_file()
        ]
        if not binaries:
            raise ValueError(f"No binaries found at path '{binary_path.resolve()}'")

        print(f"Found {len(binaries)} benchmark binaries")
        return binaries

    @staticmethod
    def _default_binary_path():
        repo_root = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
        return repo_root.joinpath("_build", "release", "velox", "benchmarks", "basic")

    @staticmethod
    def _normalize_path(binary_path: str) -> pathlib.Path:
        path = pathlib.Path(binary_path)
        if not path.is_absolute():
            path = pathlib.Path.cwd().joinpath(path).resolve()
        return path

    @staticmethod
    def _parse_benchmark_name(full_name: str):
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


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="VeloxBench Client Tool",
        epilog="(c) Meta Platforms 2004-present",
    )
    parser.add_argument(
        "--binary_path",
        default=None,
        help="Directory where benchmark binaries are stored. "
        "Defaults to release build directory.",
    )
    parser.add_argument(
        "--binary_filter",
        default=None,
        help="Filter applied to binary names. "
        "By default execute all binaries found.",
    )
    parser.add_argument(
        "--bm_filter",
        default=None,
        help="Filter applied to benchmark names within binaries. "
        "By default execute all benchmarks.",
    )
    parser.add_argument(
        "--bm_max_secs",
        default=None,
        type=int,
        help="For how many second to run each benchmark in a binary.",
    )
    parser.add_argument(
        "--bm_max_trials",
        default=None,
        type=int,
        help="Maximum number of trials (iterations) executed for each benchmark.",
    )
    parser.add_argument(
        "--bm_estimate_time",
        default=False,
        action="store_true",
        help="Use folly benchmark --bm_estimate_time flag.",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    with tempfile.TemporaryDirectory() as result_dir:
        LocalCppMicroBenchmarks().run(
            result_dir=result_dir,
            binary_path=args.binary_path,
            binary_filter=args.binary_filter,
            bm_filter=args.bm_filter,
            bm_max_secs=args.bm_max_secs,
            bm_max_trials=args.bm_max_trials,
            bm_estimate_time=args.bm_estimate_time,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
