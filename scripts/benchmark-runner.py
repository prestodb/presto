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

import argparse
import os
import subprocess
import sys


_FIND_CMD = "find %s -maxdepth 1 -type f"
_BENCHMARK_CMD = "%s --bm_json_verbose %s --bm_max_secs 10 --bm_epochs 100000"


def execute(cmd, print_stdout=False):
    """
    Executes an external process using Popen.
    Either print the process output or return in a list (based on print_stdout).
    """
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True) as process:
        if print_stdout:
            for line in iter(process.stdout.readline, b""):
                sys.stdout.buffer.write(line)
        else:
            result = process.stdout.readlines()
            return [line.strip().decode("ascii") for line in result]


def run(args):
    files = execute(_FIND_CMD % args.path)

    if not files:
        print("No benchmark binaries found in path '%s'. Aborting..." % args.path)
        return 1

    dump_path = args.dump_path if args.dump_path else os.path.join(args.path, "dumps")
    os.makedirs(dump_path, exist_ok=True)

    print("Benchmark-runner found %i benchmarks binaries to execute." % len(files))

    # Execute and dump results for each benchmark file.
    for file_path in files:
        file_name = os.path.basename(file_path)
        json_file_name = os.path.join(dump_path, "%s.json" % file_name)

        print(
            "Executing and dumping results for '%s' to '%s':"
            % (file_name, json_file_name)
        )
        execute(_BENCHMARK_CMD % (file_path, json_file_name), print_stdout=True)
        print()

    return 0


def compare(args):
    raise Exception("'compare' not implemented yet.")


def parse_args():
    parser = argparse.ArgumentParser(description="Velox Benchmark Runner Utility.")
    parser.set_defaults(func=lambda _: parser.print_help())

    subparsers = parser.add_subparsers(help="Please specify one of the subcommands.")

    parser_run = subparsers.add_parser("run", help="Run benchmarks and dump results.")
    parser_run.add_argument(
        "--path", required=True, help="Path containing the benchmark binaries."
    )
    parser_run.add_argument(
        "--dump-path",
        default=None,
        help="Path where json results will be dumped ('--path/dumps' by default)",
    )
    parser_run.set_defaults(func=run)

    parser_compare = subparsers.add_parser(
        "compare", help="Compare benchmark dumped results."
    )
    parser_compare.set_defaults(func=compare)

    return parser.parse_args()


def main():
    args = parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
