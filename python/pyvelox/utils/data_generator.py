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

from pyvelox.file import File
from pyvelox.plan_builder import PlanBuilder
from pyvelox.runner import LocalRunner, register_hive, register_tpch, unregister

TPCH_CONNECTOR_NAME = "tpch"
HIVE_CONNECTOR_NAME = "hive"


def generate_tpch_data(
    path: str, table="lineitem", num_files=8, scale_factor=0.001, file_format="dwrf"
) -> dict:
    """
    Runs PyVelox query plan that generate TPC-H data using the TpchConnector
    in Velox, and writes to a given set of output files.

    Args:
        path: The directory where generated files will be stored, has to be empty
        table: The TPC-H table to generate
        num_files: Number of output files to generate
        scale_factor: TPC-H scale factor for the given table
        file_format: The file format to use for the written files

    Returns:
        A dictionary containing stats about the generated data:
        {
            "row_count": int,  # Total number of rows written
            "file_count": int,  # Number of files generated
            "output_path": str,  # Full path to the output directory
            "output_files": List[str],  # List of generated file paths
        }
    """
    output_path = os.path.abspath(os.path.join(path, table))

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    elif os.listdir(output_path):
        raise Exception(f"Refusing to write to non-empty directory '{output_path}'.")

    # Hack to prevent an extension from being thrown when the function is run multiple times in the same python session,
    # register_* should handle this or there needs to be a function to check if a connector exits
    unregister(TPCH_CONNECTOR_NAME)
    unregister(HIVE_CONNECTOR_NAME)
    register_tpch(TPCH_CONNECTOR_NAME)
    register_hive(HIVE_CONNECTOR_NAME)

    plan_builder = PlanBuilder()
    plan_builder.tpch_gen(
        table_name=table,
        connector_id=TPCH_CONNECTOR_NAME,
        scale_factor=scale_factor,
        num_parts=num_files,
    ).table_write(
        output_path=File(path=output_path, format_str=file_format),
        connector_id=HIVE_CONNECTOR_NAME,
    )

    # Execute and write to output files.
    runner = LocalRunner(plan_builder.get_plan_node())
    output_files = []
    row_count = 0

    for vector in runner.execute(max_drivers=num_files):
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
        new_path = os.path.join(output_path, f"{table}-{file_num}.{file_format}")
        os.rename(output_file, new_path)
        file_num += 1

    file_count = len(output_files)

    return {
        "row_count": row_count,
        "file_count": file_count,
        "output_path": output_path,
        "output_files": output_files,
    }


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
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)-s %(levelname)s:%(lineno)d: %(message)s"
    )
    logging.info(
        f"Generating TPC-H '{args.table}' using scale factor "
        f"{args.scale_factor} - ({args.num_files} output files)."
    )

    result = generate_tpch_data(**vars(args))

    logging.info(
        f"Written {result.row_count} records to {result.file_count} output files at '{result.output_path}'" # pyre-ignore
    )

    return 0 if result else 1


if __name__ == "__main__":
    sys.exit(main())
