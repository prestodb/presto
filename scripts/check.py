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
#

import argparse
from collections import OrderedDict
import os
import regex
import subprocess
import sys

from util import attrdict
import util

EXTENSIONS = "cpp,h,inc,prolog"
SCRIPTS = util.script_path()


def get_diff(file, formatted):
    if not formatted.endswith("\n"):
        formatted = formatted + "\n"

    status, stdout, stderr = util.run(
        f"diff -u {file} --label {file} --label {file} -", input=formatted
    )
    if stdout != "":
        stdout = f"diff a/{file} b/{file}\n" + stdout

    return status, stdout, stderr


class CppFormatter(str):
    def diff(self, commit):
        if commit == "":
            return get_diff(self, util.run(f"clang-format --style=file {self}")[1])
        else:
            return util.run(
                f"{SCRIPTS}/git-clang-format -q --extensions='{EXTENSIONS}' --diff --style=file {commit} {self}"
            )

    def fix(self, commit):
        if commit == "":
            return util.run(f"clang-format -i --style=file {self}")[0] == 0
        else:
            return (
                util.run(
                    f"{SCRIPTS}/git-clang-format -q --extensions='{EXTENSIONS}' --style=file {commit} {self}"
                )[0]
                == 0
            )


class CMakeFormatter(str):
    def diff(self, commit):
        return get_diff(
            self, util.run(f"cmake-format --first-comment-is-literal True {self}")[1]
        )

    def fix(self, commit):
        return (
            util.run(f"cmake-format --first-comment-is-literal True -i {self}")[0] == 0
        )


class PythonFormatter(str):
    def diff(self, commit):
        return util.run(f"black -q --diff {self}")

    def fix(self, commit):
        return util.run(f"black -q {self}")[0] == 0


format_file_types = OrderedDict(
    {
        "CMakeLists.txt": attrdict({"formatter": CMakeFormatter}),
        "*.cpp": attrdict({"formatter": CppFormatter}),
        "*.h": attrdict({"formatter": CppFormatter}),
        "*.inc": attrdict({"formatter": CppFormatter}),
        "*.prolog": attrdict({"formatter": CppFormatter}),
        "*.py": attrdict({"formatter": PythonFormatter}),
    }
)


def get_formatter(filename):
    if filename in format_file_types:
        return format_file_types[filename]

    return format_file_types.get("*" + util.get_fileextn(filename), None)


def format_command(commit, files, fix):
    ok = 0
    for filepath in files:
        filename = util.get_filename(filepath)
        filetype = get_formatter(filename)

        if filetype is None:
            print("Skip : " + filepath, file=sys.stderr)
            continue

        file = filetype.formatter(filepath)

        if fix == "show":
            status, diff, stderr = file.diff(commit)

            if stderr != "":
                ok = 1
                print(f"Error: {file}", file=sys.stderr)
                continue

            if diff != "" and diff != "no modified files to format":
                ok = 1
                print(f"Fix  : {file}", file=sys.stderr)
                print(diff)
            else:
                print(f"Ok   : {file}", file=sys.stderr)

        else:
            print(f"Fix  : {file}", file=sys.stderr)
            if not file.fix(commit):
                ok = 1
                print(f"Error: {file}", file=sys.stderr)

    return ok


def header_command(commit, files, fix):
    options = "-vk" if fix == "show" else "-i"

    status, stdout, stderr = util.run(
        f"{SCRIPTS}/license-header.py {options} -", input=files
    )

    if stdout != "":
        print(stdout)

    return status


def tidy_command(commit, files, fix):
    files = [file for file in files if regex.match(r".*\.cpp$", file)]

    if not files:
        return 0

    commit = f"--commit {commit}" if commit != "" else ""
    fix = "--fix" if fix == "fix" else ""

    status, stdout, stderr = util.run(
        f"{SCRIPTS}/run-clang-tidy.py {commit} {fix} -", input=files
    )

    if stdout != "":
        print(stdout)

    return status


def get_commit(files):
    if files == "commit":
        return "HEAD^"

    if files == "main" or files == "master":
        return util.run(f"git merge-base origin/{files} HEAD")[1]

    return ""


def get_files(commit, path):
    filelist = []

    if commit != "":
        status, stdout, stderr = util.run(
            f"git diff --name-only --diff-filter='ACM' {commit}"
        )
        filelist = stdout.splitlines()
    else:
        for root, dirs, files in os.walk(path):
            for name in files:
                filelist.append(os.path.join(root, name))

    return [
        file
        for file in filelist
        if "/data/" not in file
        and "velox/external/" not in file
        and "build/fbcode_builder" not in file
        and "build/deps" not in file
        and "cmake-build-debug" not in file
    ]


def help(args):
    parser.print_help()
    return 0


def add_check_options(subparser, name):
    parser = subparser.add_parser(name)
    parser.add_argument("--fix", action="store_const", default="show", const="fix")
    return parser


def add_options(parser):
    files = parser.add_subparsers(dest="files")

    tree_parser = add_check_options(files, "tree")
    tree_parser.add_argument("path", default="")

    branch_parser = add_check_options(files, "main")
    branch_parser = add_check_options(files, "master")
    commit_parser = add_check_options(files, "commit")


def add_check_command(parser, name):
    subparser = parser.add_parser(name)
    add_options(subparser)

    return subparser


def parse_args():
    global parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="""Check format/header/tidy

    check.py {format,header,tidy} {commit,branch} [--fix]
    check.py {format,header,tidy} {tree} [--fix] PATH
""",
    )
    command = parser.add_subparsers(dest="command")
    command.add_parser("help")

    format_command_parser = add_check_command(command, "format")
    header_command_parser = add_check_command(command, "header")
    tidy_command_parser = add_check_command(command, "tidy")

    parser.set_defaults(path="")
    parser.set_defaults(command="help")

    return parser.parse_args()


def run_command(args, command):
    commit = get_commit(args.files)
    files = get_files(commit, args.path)

    return command(commit, files, args.fix)


def format(args):
    return run_command(args, format_command)


def header(args):
    return run_command(args, header_command)


def tidy(args):
    return run_command(args, tidy_command)


def main():
    args = parse_args()
    return globals()[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
