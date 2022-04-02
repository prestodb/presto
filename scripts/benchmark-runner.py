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
import json
import os
import subprocess
import sys


_FIND_BINARIES_CMD = "find %s -maxdepth 1 -type f -executable"
_FIND_JSON_CMD = "find %s -maxdepth 1 -name '*.json' -type f"
_BENCHMARK_CMD = "%s --bm_max_secs 10 --bm_max_trials 1000000"
_BENCHMARK_WITH_DUMP_CMD = _BENCHMARK_CMD + " --bm_json_verbose %s"

_OUTPUT_NUM_COLS = 100


# Cosmetic helper functions.
def color_red(text) -> str:
    return "\033[91m{}\033[00m".format(text) if sys.stdout.isatty() else text


def color_yellow(text) -> str:
    return "\033[93m{}\033[00m".format(text) if sys.stdout.isatty() else text


def color_green(text) -> str:
    return "\033[92m{}\033[00m".format(text) if sys.stdout.isatty() else text


def bold(text) -> str:
    return "\033[1m{}\033[00m".format(text) if sys.stdout.isatty() else text


def execute(cmd, print_stdout=False):
    """
    Executes an external process using Popen.
    Either print the process output or return it in a list (based on print_stdout).
    """
    result = None

    if print_stdout:
        process = subprocess.Popen(cmd, stdout=sys.stdout, shell=True)
    else:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        result = [line.strip().decode("ascii") for line in process.stdout.readlines()]

    if process.wait() != 0:
        raise subprocess.SubprocessError(
            "'{}' returned a non-zero code ({}).".format(cmd, process.returncode)
        )
    return result


def run(args):
    files = execute(_FIND_BINARIES_CMD % args.path)

    if not files:
        print("No benchmark binaries found in path '%s'. Aborting..." % args.path)
        return 1

    if args.dump_path:
        os.makedirs(args.dump_path, exist_ok=True)
    print("Benchmark-runner found %i benchmarks binaries to execute." % len(files))

    # Execute and dump results for each benchmark file.
    for file_path in files:
        file_name = os.path.basename(file_path)

        if args.dump_path:
            json_file_name = os.path.join(args.dump_path, "%s.json" % file_name)
            print(
                "Executing and dumping results for '%s' to '%s':"
                % (file_name, json_file_name)
            )
            cmd = _BENCHMARK_WITH_DUMP_CMD % (file_path, json_file_name)
        else:
            print("Executing '%s':" % file_name)
            cmd = _BENCHMARK_CMD % file_path

        print("$ %s" % cmd)
        execute(cmd, print_stdout=True)
        print()

    return 0


def get_benchmark_handle(file_path, name):
    if name[0] == "%":
        name = name[1:]
    return "{}/{}".format(os.path.basename(file_path), name)


def fmt_runtime(time_ns):
    if time_ns < 1000:
        return "{:.2f}ns".format(time_ns)

    time_usec = time_ns / 1000
    if time_usec < 1000:
        return "{:.2f}us".format(time_usec)
    else:
        return "{:.2f}ms".format(time_usec / 1000)


def compare_file(args, target_data, baseline_data):
    baseline_map = {}
    for row in baseline_data:
        baseline_map[get_benchmark_handle(row[0], row[1])] = row[2]

    passes = []
    faster = []
    failures = []

    for row in target_data:
        # Folly benchmark exports line separators by mistake as an entry on
        # the json file.
        if row[1] == "-":
            continue

        benchmark_handle = get_benchmark_handle(row[0], row[1])
        baseline_result = baseline_map[benchmark_handle]
        target_result = row[2]

        if baseline_result == 0 or target_result == 0:
            delta = 0
        elif baseline_result > target_result:
            delta = 1 - (target_result / baseline_result)
        else:
            delta = (1 - (baseline_result / target_result)) * -1

        if abs(delta) > args.threshold:
            if delta > 0:
                status = color_green("✓ Pass")
                passes.append(benchmark_handle)
                faster.append(benchmark_handle)
            else:
                status = color_red("✗ Fail")
                failures.append(benchmark_handle)
        else:
            status = color_green("✓ Pass")
            passes.append(benchmark_handle)

        suffix = "({} vs {}) {:+.2f}%".format(
            fmt_runtime(baseline_result), fmt_runtime(target_result), delta * 100
        )

        # Prefix length is 12 bytes (considering utf8 and invisible chars).
        spacing = " " * (_OUTPUT_NUM_COLS - (12 + len(benchmark_handle) + len(suffix)))
        print("    {}: {}{}{}".format(status, benchmark_handle, spacing, suffix))

    return passes, faster, failures


def compare(args):
    print(
        "=> Starting comparison using {} ({}%) as threshold.".format(
            args.threshold, args.threshold * 100
        )
    )
    print("=> Values are reported as percentage normalized to the largest values:")
    print("=>    (positive means speedup; negative means regression).")

    # Read file lists from both directories.
    baseline_files = execute(_FIND_JSON_CMD % args.baseline_dump_path)
    target_files = execute(_FIND_JSON_CMD % args.target_dump_path)

    baseline_map = {os.path.basename(f): f for f in baseline_files}
    target_map = {os.path.basename(f): f for f in target_files}

    all_passes = []
    all_faster = []
    all_failures = []

    # Compare json results from each file.
    for file_name, target_path in target_map.items():
        print("=" * _OUTPUT_NUM_COLS)
        print(file_name)
        print("=" * _OUTPUT_NUM_COLS)

        if file_name not in baseline_map:
            print("WARNING: baseline file for '%s' not found. Skipping." % file_name)
            continue

        # Open and read each file.
        with open(target_path) as f:
            target_data = json.load(f)

        with open(baseline_map[file_name]) as f:
            baseline_data = json.load(f)

        passes, faster, failures = compare_file(args, target_data, baseline_data)
        all_passes += passes
        all_faster += faster
        all_failures += failures

    def print_list(names):
        for n in names:
            print("    %s" % n)

    # Print a nice summary of the results:
    print("Summary:")
    if all_passes:
        faster_summary = (
            " ({} are faster):".format(len(all_faster)) if all_faster else ""
        )
        print(color_green("  Pass: %d%s " % (len(all_passes), faster_summary)))
        print_list(all_faster)

    if all_failures:
        print(color_red("  Fail: %d" % len(all_failures)))
        print_list(all_failures)
        return 1
    return 0


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
        help="Path where json results will be dumped. By default only print to stdout.",
    )
    parser_run.set_defaults(func=run)

    parser_compare = subparsers.add_parser(
        "compare", help="Compare benchmark dumped results."
    )
    parser_compare.set_defaults(func=compare)
    parser_compare.add_argument(
        "--baseline-dump-path",
        required=True,
        help="Path where containing base dump results.",
    )
    parser_compare.add_argument(
        "--target-dump-path",
        required=True,
        help="Path where containing target dump results.",
    )
    parser_compare.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=0.2,
        help="Comparison threshold. "
        "Variations larger than this threshold will be reported as failures. "
        "Default 0.2 (20%%).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
