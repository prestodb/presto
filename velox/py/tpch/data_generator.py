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
import logging
import os
import sys

from velox.py.file import File
from velox.py.plan_builder import PlanBuilder
from velox.py.runner import LocalRunner, register_hive, register_tpch


TPCH_CONNECTOR_NAME = "tpch"
HIVE_CONNECTOR_NAME = "hive"


def generate_data(args) -> int:
    """
    Runs PyVelox query plan that generate TPC-H data using the TpchConnector
    in Velox, and writes to a given set of output files.
    """
    output_path = os.path.join(args.path, args.table)

    if not os.path.exists(output_path):
        os.mkdir(output_path)
    elif os.listdir(output_path):
        raise Exception(f"Refusing to write to non-empty directory '{output_path}'.")

    logging.info(
        f"Generating TPC-H '{args.table}' using scale factor "
        f"{args.scale_factor} - ({args.num_files} output files)."
    )

    register_tpch(TPCH_CONNECTOR_NAME)
    register_hive(HIVE_CONNECTOR_NAME)

    plan_builder = PlanBuilder()
    plan_builder.tpch_gen(
        table_name=args.table,
        connector_id=TPCH_CONNECTOR_NAME,
        scale_factor=args.scale_factor,
        num_parts=args.num_files,
    ).table_write(
        output_path=File(path=output_path, format_str=args.file_format),
        connector_id=HIVE_CONNECTOR_NAME,
    )

    # Execute and write to output files.
    runner = LocalRunner(plan_builder.get_plan_node())
    output_files = []
    row_count = 0

    for vector in runner.execute(max_drivers=args.num_files):
        # Parse table writer's output to extract file names and other stats.
        output_json = json.loads(vector.child_at(1)[1])

        row_count += output_json["rowCount"]
        write_path = output_json["writePath"]
        write_infos = output_json["fileWriteInfos"]

        for write_info in write_infos:
            write_file_name = write_info["writeFileName"]
            output_files.append(os.path.join(write_path, write_file_name))

    file_num = 0
    for output_file in output_files:
        os.rename(
            output_file,
            os.path.join(output_path, f"{args.table}-{file_num}.{args.file_format}"),
        )
        file_num += 1

    file_count = len(output_files)
    logging.info(
        f"Written {row_count} records to {file_count} output files at '{output_path}'"
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PyVelox Data Generator Utility for TPC-H."
    )
    parser.add_argument(
        "--path",
        help="The directory (path) where generated files will be stored.",
        type=str,
    )
    parser.add_argument(
        "--table",
        help="The TPC-H table (lineitem by default).",
        type=str,
        default="lineitem",
    )
    parser.add_argument(
        "--num_files",
        help="Number of output files to generate.",
        type=int,
        default=8,
    )
    parser.add_argument(
        "--scale_factor",
        help="TPC-H scale factor for the given table.",
        type=float,
        default=0.001,
    )
    parser.add_argument(
        "--file_format",
        help="The file format to use for the written files (dwrf, parquet, or nimble).",
        type=str,
        default="dwrf",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)-s %(levelname)s:%(lineno)d: %(message)s"
    )
    return generate_data(parse_args())


if __name__ == "__main__":
    sys.exit(main())
