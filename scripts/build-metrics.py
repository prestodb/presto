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
import sys
import uuid
from os.path import join, splitext
from pathlib import Path
from typing import Any, Dict, List

from benchadapt import BenchmarkResult
from benchadapt.adapters import BenchmarkAdapter


class BinarySizeAdapter(BenchmarkAdapter):
    """
    Adapter to track build artifact sizes in conbench.
    Expects the `size_file` to be formatted like this:
    <size in bytes> <path/to/binary|arbitrary_name>

    Suite meta data will be library, object or executable
    based on file ending.
    """

    size_file: Path

    def __init__(
        self,
        command: List[str],
        size_file: str,
        build_type: str,
        result_fields_override: Dict[str, Any] = {},
        result_fields_append: Dict[str, Any] = {},
    ) -> None:
        self.size_file = Path(size_file)
        if build_type not in ["debug", "release"]:
            raise ValueError(f"Build type '{build_type}' is not valid!")
        self.build_type = build_type
        super().__init__(command, result_fields_override, result_fields_append)

    def _transform_results(self) -> List[BenchmarkResult]:
        results = []

        batch_id = uuid.uuid4().hex
        with open(self.size_file, "r") as file:
            sizes = [line.strip() for line in file]

        if not sizes:
            raise ValueError("'size_file' is empty!")

        for line in sizes:
            size, path = line.split(maxsplit=1)
            path = path.strip()
            _, ext = splitext(path)
            if ext in [".so", ".a"]:
                suite = "library"
            elif ext == ".o":
                suite = "object"
            else:
                suite = "executable"

            parsed_size = BenchmarkResult(
                run_reason="merge",
                batch_id=batch_id,
                stats={
                    "data": [size],
                    "unit": "B",
                    "iterations": 1,
                },
                tags={
                    "name": path,
                    "suite": suite,
                    "source": f"{self.build_type}_build_metrics_size",
                },
                info={},
                context={"benchmark_language": "C++"},
            )
            results.append(parsed_size)

        return results


class NinjaLogAdapter(BenchmarkAdapter):
    """
    Adapter to extract compile and link times from a .ninja_log.
    Will calculate aggregates for total, compile and link time.
    Suite metadata will be set based on binary ending to object, library or executable.

    Only files in paths beginning with velox/ will be tracked to avoid dependencies.
    """

    ninja_log: Path

    def __init__(
        self,
        command: List[str],
        ninja_log: str,
        build_type: str,
        result_fields_override: Dict[str, Any] = {},
        result_fields_append: Dict[str, Any] = {},
    ) -> None:
        self.ninja_log = Path(ninja_log)
        if build_type not in ["debug", "release"]:
            raise ValueError(f"Build type '{build_type}' is not valid!")
        self.build_type = build_type
        super().__init__(command, result_fields_override, result_fields_append)

    def _transform_results(self) -> List[BenchmarkResult]:
        results = []

        batch_id = uuid.uuid4().hex
        with open(self.ninja_log, "r") as file:
            log_lines = [line.strip() for line in file]

        if not log_lines[0].startswith("# ninja log v"):
            raise ValueError("Malformed Ninja log found!")
        else:
            del log_lines[0]

        ms2sec = lambda x: x / 1000
        get_epoch = lambda l: int(l.split()[2])
        totals = {
            "link_time": 0,
            "compile_time": 0,
            "total_time": 0,
        }

        for line in log_lines:
            start, end, epoch, object_path, _ = line.split()
            start = int(start)
            end = int(end)
            duration = ms2sec(end - start)

            # Don't track dependency times (refine check potentially?)
            if not object_path.startswith("velox"):
                continue

            _, ext = splitext(object_path)
            if ext in [".so", ".a"] or not ext:
                totals["link_time"] += duration
                suite = "linking"
            elif ext == ".o":
                totals["compile_time"] += duration
                suite = "compiling"
            else:
                print(f"Unkown file type found: {object_path}")
                print("Skipping...")
                continue

            time_result = BenchmarkResult(
                run_reason="merge",
                batch_id=batch_id,
                stats={
                    "data": [duration],
                    "unit": "s",
                    "iterations": 1,
                },
                tags={
                    "name": object_path,
                    "suite": suite,
                    "source": f"{self.build_type}_build_metrics_time",
                },
                info={},
                context={"benchmark_language": "C++"},
            )
            results.append(time_result)

        totals["total_time"] = totals["link_time"] + totals["compile_time"]
        for total_name, total in totals.items():
            total_result = BenchmarkResult(
                run_reason="merge",
                batch_id=batch_id,
                stats={
                    "data": [total],
                    "unit": "s",
                    "iterations": 1,
                },
                tags={
                    "name": total_name,
                    "suite": "total",
                    "source": f"{self.build_type}_build_metrics_time",
                },
                info={},
                context={"benchmark_language": "C++"},
            )
            results.append(total_result)

        return results


# find velox -type f -name '*.o' -exec ls -l -BB {} \; | awk '{print $5, $9}' |  sed 's|CMakeFiles/.*dir/||g' > /tmp/object-size


def upload(args):
    print("Uploading Build Metrics")
    pr_number = int(args.pr_number) if args.pr_number else None
    run_reason = "pull request" if pr_number else "commit"
    run_name = f"{run_reason}: {args.sha}"
    sizes = BinarySizeAdapter(
        command=["true"],
        size_file=join(args.base_path, args.size_file),
        build_type=args.build_type,
        result_fields_override={
            "run_id": args.run_id,
            "run_name": run_name,
            "run_reason": run_reason,
            "github": {
                "repository": "https://github.com/facebookincubator/velox",
                "pr_number": pr_number,
                "commit": args.sha,
            },
        },
    )
    sizes()

    times = NinjaLogAdapter(
        command=["true"],
        ninja_log=join(args.base_path, args.ninja_log),
        build_type=args.build_type,
        result_fields_override={
            "run_id": args.run_id,
            "run_name": run_name,
            "run_reason": run_reason,
            "github": {
                "repository": "https://github.com/facebookincubator/velox",
                "pr_number": pr_number,
                "commit": args.sha,
            },
        },
    )
    times()


def parse_args():
    parser = argparse.ArgumentParser(description="Velox Build Metric Utility.")
    parser.set_defaults(func=lambda _: parser.print_help())

    subparsers = parser.add_subparsers(help="Please specify one of the subcommands.")

    upload_parser = subparsers.add_parser(
        "upload", help="Parse and upload build metrics"
    )
    upload_parser.set_defaults(func=upload)
    upload_parser.add_argument(
        "--ninja_log", default=".ninja_log", help="Name of the ninja log file."
    )
    upload_parser.add_argument(
        "--size_file",
        default="object_sizes",
        help="Name of the file containing size information.",
    )
    upload_parser.add_argument(
        "--build_type",
        required=True,
        help="Type of build results come from, e.g. debug or release",
    )
    upload_parser.add_argument(
        "--run_id",
        required=True,
        help="A Conbench run ID unique to this build.",
    )
    upload_parser.add_argument(
        "--sha",
        required=True,
        help="HEAD sha for the result upload to conbench.",
    )
    upload_parser.add_argument(
        "--pr_number",
        default=0,
        help="PR number for the result upload to conbench.",
    )
    upload_parser.add_argument(
        "base_path",
        help="Path in which the .ninja_log and sizes_file are found.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    args.func(args)
