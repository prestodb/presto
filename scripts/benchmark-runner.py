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
import pathlib
import re
import subprocess
import sys
import tempfile
import traceback
import uuid

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


def _default_binary_path():
    repo_root = pathlib.Path(__file__).parent.parent.absolute()
    return repo_root.joinpath("_build", "release", "velox", "benchmarks", "basic")


def _normalize_path(binary_path: str) -> pathlib.Path:
    path = pathlib.Path(binary_path)
    if not path.is_absolute():
        path = pathlib.Path.cwd().joinpath(path).resolve()
    return path


def run_all_benchmarks(
    output_dir,
    binary_path=None,
    binary_filter=None,
    bm_filter=None,
    bm_max_secs=None,
    bm_max_trials=None,
    bm_estimate_time=False,
):
    if binary_path:
        binary_path = _normalize_path(binary_path)
    else:
        binary_path = _default_binary_path()

    binaries = _find_binaries(binary_path)
    output_dir_path = pathlib.Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    for binary_path in binaries:
        if binary_filter and not re.search(binary_filter, binary_path.name):
            continue

        out_path = output_dir_path / f"{binary_path.name}.json"
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

        try:
            print(run_command)
            subprocess.run(run_command, check=True)
        except subprocess.CalledProcessError as e:
            print(e.stderr.decode("utf-8"))
            raise e


def upload_results(output_dir, run_id):
    print(f"Uploading results from {output_dir=} to Conbench with {run_id=}")

    # Check there's actually results first
    if not list(pathlib.Path(output_dir).rglob("*.json")):
        print("No results to upload")
        return

    # Import benchadapt inside this function so you don't need it if you're just running benchmarks
    from benchadapt.adapters import FollyAdapter
    from benchadapt.log import log

    # Print the POSTs in the logs
    log.setLevel("DEBUG")

    # benchadapt needs these for logging in to Conbench
    required_env_vars = {"CONBENCH_URL", "CONBENCH_EMAIL", "CONBENCH_PASSWORD"}
    missing_env_vars = required_env_vars - set(os.environ)
    if missing_env_vars:
        print(
            "Not uploading results to Conbench because these env vars are missing: "
            f"{missing_env_vars}"
        )
        return

    # This should work, though it would be much better to get the sha from velox
    # directly so we know we're using the right one (see TODO below)
    commit = os.environ["CIRCLE_SHA1"]

    pr_number_env = os.getenv("CIRCLE_PR_NUMBER", "")
    pr_number = int(pr_number_env) if pr_number_env else None
    run_reason = "pull request" if pr_number else "commit"
    run_name = f"{run_reason}: {commit}"

    conbench_upload_callable = FollyAdapter(
        # Since benchmarks have already run, this run command is a no-op
        command=["ls", output_dir],
        result_dir=output_dir,
        result_fields_override={
            "run_id": run_id,
            "run_name": run_name,
            "run_reason": run_reason,
            "github": {
                "repository": "https://github.com/facebookincubator/velox",
                "pr_number": pr_number,
                "commit": commit,
            },
        },
        result_fields_append={
            "info": {
                # TODO: undo the hard coding with ways to get the values from velox
                # c.f. https://github.com/ursacomputing/benchmarks/blob/033eee0951adbf41931a2de95caccbac887da6ff/benchmarks/_benchmark.py#L30-L48
                "velox_version": "0.0.1",
                "velox_compiler_id": None,
                "velox_compiler_version": None,
            },
            "context": {"velox_compiler_flags": None},
        },
    )

    conbench_upload_callable()


def run(args):
    output_dir = args.output_path or tempfile.mkdtemp()
    kwargs = {
        "output_dir": output_dir,
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
            run_all_benchmarks(**kwargs)

    # Otherwise, run all benchmarks we can find.
    else:
        run_all_benchmarks(**kwargs)

    if args.conbench_upload_run_id:
        try:
            upload_results(output_dir=output_dir, run_id=args.conbench_upload_run_id)
        except Exception:
            print("ERROR caught during uploading results:")
            print(traceback.format_exc())


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
    parser_run.add_argument(
        "--conbench_upload_run_id",
        default=None,
        help="A Conbench run ID unique to this build. If given, the run script will "
        "upload results to Conbench upon completion. Requires the `benchadapt` package "
        "installed and the following env vars set: CIRCLE_SHA1, CIRCLE_PR_NUMBER, "
        "CONBENCH_URL, CONBENCH_EMAIL, CONBENCH_PASSWORD",
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
