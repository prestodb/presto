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
    pv.clear_signatures()

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


def diff_signatures(base_signatures, contender_signatures):
    """Diffs Velox function signatures. Returns a tuple of the delta diff and exit status"""

    delta = DeepDiff(
        base_signatures,
        contender_signatures,
        ignore_order=True,
        report_repetition=True,
        view="tree",
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

    if exit_status:
        print(
            f""" 
            {bcolors.BOLD}Incompatible changes in function signatures have been detected.
            This means your changes have modified function signatures and possibly broken backwards compatibility.  
        """
        )

    return delta, exit_status


def diff(args):
    """Diffs Velox function signatures."""
    base_signatures = json.load(args.base)
    contender_signatures = json.load(args.contender)
    return diff_signatures(base_signatures, contender_signatures)[1]


def bias(args):
    base_signatures = json.load(args.base)
    contender_signatures = json.load(args.contender)
    tickets = args.ticket_value
    bias_output, status = bias_signatures(
        base_signatures, contender_signatures, tickets
    )
    if status:
        return status

    if bias_output:
        with open(args.output_path, "w") as f:
            print(f"{bias_output}", file=f, end="")

    return 0


def bias_signatures(base_signatures, contender_signatures, tickets):
    """Returns newly added functions as string and a status flag.
    Newly added functions are biased like so `fn_name1=<ticket_count>,fn_name2=<ticket_count>`.
    If it detects incompatible changes returns 1 in the status and empty string.
    """
    delta, status = diff_signatures(base_signatures, contender_signatures)

    # Return if the signature check call flags incompatible changes.
    if status:
        return "", status

    if not delta:
        print(f"{bcolors.BOLD} No changes detected: Nothing to do!")
        return "", 0

    function_set = set()
    for items in delta.values():
        for item in items:
            function_set.add(item.get_root_key())

    print(f"{bcolors.BOLD}Functions to be biased: {function_set}")

    if function_set:
        return f"{f'={tickets},'.join(sorted(function_set)) + f'={tickets}'}", 0

    return "", 0


def get_tickets(val):
    tickets = int(val)
    if tickets < 0:
        raise argparse.ArgumentTypeError("Cant have negative values!")
    return tickets


def parse_args(args):
    global parser

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="""Velox Function Signature Utility""",
    )

    command = parser.add_subparsers(dest="command")
    export_command_parser = command.add_parser("export")
    export_command_parser.add_argument("--spark", action="store_true")
    export_command_parser.add_argument("--presto", action="store_true")
    export_command_parser.add_argument("output_file", type=argparse.FileType("w"))

    diff_command_parser = command.add_parser("diff")
    diff_command_parser.add_argument("base", type=argparse.FileType("r"))
    diff_command_parser.add_argument("contender", type=argparse.FileType("r"))

    bias_command_parser = command.add_parser("bias")
    bias_command_parser.add_argument("base", type=argparse.FileType("r"))
    bias_command_parser.add_argument("contender", type=argparse.FileType("r"))
    bias_command_parser.add_argument("output_path")
    bias_command_parser.add_argument(
        "ticket_value", type=get_tickets, default=10, nargs="?"
    )
    parser.set_defaults(command="help")

    return parser.parse_args(args)


def main():
    args = parse_args(sys.argv[1:])
    return globals()[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
