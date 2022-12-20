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
import sys
import tempfile
from veloxbench.veloxbench.cpp_micro_benchmarks import LocalCppMicroBenchmarks

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
                status = color_green("🗲 Pass")
                passes.append((row[0], row[1], delta))
                faster.append((row[0], row[1], delta))
            else:
                status = color_red("✗ Fail")
                failures.append((row[0], row[1], delta))
        else:
            status = color_green("✓ Pass")
            passes.append((row[0], row[1], delta))

        suffix = "({} vs {}) {:+.2f}%".format(
            fmt_runtime(baseline_result), fmt_runtime(target_result), delta * 100
        )

        # Prefix length is 12 bytes (considering utf8 and invisible chars).
        spacing = " " * (_OUTPUT_NUM_COLS - (12 + len(benchmark_handle) + len(suffix)))
        print("    {}: {}{}{}".format(status, benchmark_handle, spacing, suffix))

    return passes, faster, failures


def find_json_files(path):
    json_files = {}
    try:
        with os.scandir(path) as files:
            for file_found in files:
                if file_found.name.endswith(".json"):
                    json_files[file_found.name] = file_found.path
    except:
        pass
    return json_files


def compare(args):
    print(
        "=> Starting comparison using {} ({}%) as threshold.".format(
            args.threshold, args.threshold * 100
        )
    )
    print("=> Values are reported as percentage normalized to the largest values:")
    print("=>    (positive means speedup; negative means regression).")

    # Read file lists from both directories.
    baseline_map = find_json_files(args.baseline_path)
    target_map = find_json_files(args.target_path)

    all_passes = []
    all_faster = []
    all_failures = []

    # Keep track of benchmarks that exceeded the threshold to they can be saved
    # to the rerun_output file.
    rerun_log = {}

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

        if faster or failures:
            rerun_log[file_name] = faster + failures

    def print_list(names):
        for n in names:
            print(
                "    {} ({:+.2f}%)".format(get_benchmark_handle(n[0], n[1]), n[2] * 100)
            )

    # Write rerun log to output file.
    if args.rerun_json_output:
        with open(args.rerun_json_output, "w") as out_file:
            out_file.write(json.dumps(rerun_log, indent=4))

    # Print a nice summary of the results:
    print("Summary ({}% threshold):".format(args.threshold * 100))
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


def run(args):
    kwargs = {
        "output_dir": args.output_path or tempfile.mkdtemp(),
        "binary_path": args.binary_path,
        "binary_filter": args.binary_filter,
        "bm_filter": args.bm_filter,
        "bm_max_secs": args.bm_max_secs,
        "bm_max_trials": args.bm_max_trials,
        "bm_estimate_time": args.bm_estimate_time,
    }

    # In case we only want to rerun failed benchmarks from rerun_json_input.
    if args.rerun_json_input:
        with open(args.rerun_json_input, "r") as input_file:
            json_input = json.load(input_file)

        def gen_binary_filter(json_file_name):
            return "^{}$".format(json_file_name.rstrip(".json"))

        def gen_bm_filter(bm_list):
            return "^{}$".format("|".join([x[1] for x in bm_list]))

        for file_name, bm_list in json_input.items():
            kwargs["binary_filter"] = gen_binary_filter(file_name)
            kwargs["bm_filter"] = gen_bm_filter(bm_list)
            LocalCppMicroBenchmarks().run(**kwargs)

    # Otherwise, run all benchmarks we can find.
    else:
        LocalCppMicroBenchmarks().run(**kwargs)


def parse_args():
    parser = argparse.ArgumentParser(description="Velox Benchmark Runner Utility.")
    parser.set_defaults(func=lambda _: parser.print_help())

    subparsers = parser.add_subparsers(help="Please specify one of the subcommands.")

    # Arguments for the "run" subparser.
    parser_run = subparsers.add_parser("run", help="Run benchmarks and dump results.")
    parser_run.add_argument(
        "--binary_path",
        default=None,
        help="Directory where benchmark binaries are stored. "
        "Defaults to release build directory.",
    )
    parser_run.add_argument(
        "--output_path",
        default=None,
        help="Directory where output json files will be written to. "
        "By default generate a temporary directory.",
    )
    parser_run.add_argument(
        "--binary_filter",
        default=None,
        help="Filter applied to binary names. "
        "By default execute all binaries found.",
    )
    parser_run.add_argument(
        "--bm_filter",
        default=None,
        help="Filter applied to benchmark names within binaries. "
        "By default execute all benchmarks.",
    )
    parser_run.add_argument(
        "--bm_max_secs",
        default=None,
        type=int,
        help="For how many seconds to run each benchmark in a binary.",
    )
    parser_run.add_argument(
        "--bm_max_trials",
        default=None,
        type=int,
        help="Maximum number of trials (iterations) executed for each benchmark.",
    )
    parser_run.add_argument(
        "--bm_estimate_time",
        default=False,
        action="store_true",
        help="Use folly benchmark --bm_estimate_time flag.",
    )
    parser_run.add_argument(
        "--rerun_json_input",
        default=None,
        help="Only binaries and benchmark names read from this json file will "
        "be run. This file needs to be generated using the "
        "--rerun_json_output flag.",
    )
    parser_run.set_defaults(func=run)

    # Arguments for the "compare" subparser.
    parser_compare = subparsers.add_parser(
        "compare", help="Compare benchmark dumped results."
    )
    parser_compare.set_defaults(func=compare)
    parser_compare.add_argument(
        "--baseline_path",
        required=True,
        help="Path where containing base dump results.",
    )
    parser_compare.add_argument(
        "--target_path",
        required=True,
        help="Path where containing target dump results.",
    )
    parser_compare.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=0.05,
        help="Comparison threshold. "
        "Variations larger than this threshold will be reported as failures. "
        "Default 0.05 (5%%).",
    )
    parser_compare.add_argument(
        "--rerun_json_output",
        default=None,
        help="File where the rerun output will be saved. Redo output contains "
        "information about the failed benchmarks (the ones where the variation "
        "exceeded the threshold).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
