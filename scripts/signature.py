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
import sys

import pyvelox.pyvelox as pv
from deepdiff import DeepDiff


# Utility to export and diff function signatures.


# From https://stackoverflow.com/questions/287871/how-do-i-print-colored-text-to-the-terminal
class bcolors:
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    BOLD = "\033[1m"


def export(args):
    """Exports Velox function signatures."""
    if args.spark:
        pv.register_spark_signatures()

    if args.presto:
        pv.register_presto_signatures()

    signatures = pv.get_function_signatures()

    # Convert signatures to json
    jsoned_signatures = {}
    for key in signatures.keys():
        jsoned_signatures[key] = [str(value) for value in signatures[key]]

    # Persist to file
    json.dump(jsoned_signatures, args.output_file)
    return 0


def diff(args):
    """Diffs Velox function signatures."""
    first_signatures = json.load(args.first)
    second_signatures = json.load(args.second)
    delta = DeepDiff(
        first_signatures, second_signatures, ignore_order=True, report_repetition=True
    )
    exit_status = 0
    if delta:
        if "dictionary_item_removed" in delta:
            print(
                f"Signature removed: {bcolors.FAIL}{delta['dictionary_item_removed']}"
            )
            exit_status = 1

        if "values_changed" in delta:
            print(f"Signature changed: {bcolors.FAIL}{delta['values_changed']}")
            exit_status = 1

        if "repetition_change" in delta:
            print(f"Signature repeated: {bcolors.FAIL}{delta['repetition_change']}")
            exit_status = 1

        if "iterable_item_removed" in delta:
            print(
                f"Iterable item removed: {bcolors.FAIL}{delta['iterable_item_removed']}"
            )
            exit_status = 1

        print(f"Found differences: {bcolors.OKGREEN}{delta}")

    else:
        print(f"{bcolors.BOLD}No differences found.")

    return exit_status


def parse_args():
    global parser

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="""Velox Function Signature Utility""",
    )

    command = parser.add_subparsers(dest="command")
    export_command_parser = command.add_parser("export")
    export_command_parser.add_argument("--spark", action="store_true")
    export_command_parser.add_argument("--presto", action="store_false")
    export_command_parser.add_argument("output_file", type=argparse.FileType("w"))

    diff_command_parser = command.add_parser("diff")
    diff_command_parser.add_argument("first", type=argparse.FileType("r"))
    diff_command_parser.add_argument("second", type=argparse.FileType("r"))

    parser.set_defaults(command="help")

    return parser.parse_args()


def main():
    args = parse_args()
    return globals()[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
