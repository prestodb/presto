#!/usr/bin/env python3
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
from itertools import groupby
import json
import regex
import sys

import util

CODE_CHECKS = """*
    -abseil-*
    -android-*
    -cert-err58-cpp
    -clang-analyzer-osx-*
    -cppcoreguidelines-avoid-c-arrays
    -cppcoreguidelines-avoid-magic-numbers
    -cppcoreguidelines-pro-bounds-array-to-pointer-decay
    -cppcoreguidelines-pro-bounds-pointer-arithmetic
    -cppcoreguidelines-pro-type-reinterpret-cast
    -cppcoreguidelines-pro-type-vararg
    -fuchsia-*
    -google-*
    -hicpp-avoid-c-arrays
    -hicpp-deprecated-headers
    -hicpp-no-array-decay
    -hicpp-use-equals-default
    -hicpp-vararg
    -llvmlibc-*
    -llvm-header-guard
    -llvm-include-order
    -mpi-*
    -misc-non-private-member-variables-in-classes
    -misc-no-recursion
    -misc-unused-parameters
    -modernize-avoid-c-arrays
    -modernize-deprecated-headers
    -modernize-use-nodiscard
    -modernize-use-trailing-return-type
    -objc-*
    -openmp-*
    -readability-avoid-const-params-in-decls
    -readability-convert-member-functions-to-static
    -readability-magic-numbers
    -zircon-*
"""

# Additional opt-outs because googletest macros trip too many things.
#
TEST_CHECKS = (
    CODE_CHECKS
    + """
    -cert-err58-cpp
    -cppcoreguidelines-avoid-goto
    -cppcoreguidelines-avoid-non-const-global-variables
    -cppcoreguidelines-owning-memory
    -cppcoreguidelines-pro-type-vararg
    -cppcoreguidelines-special-member-functions
    -hicpp-avoid-goto
    -hicpp-special-member-functions
    -hicpp-vararg
    -misc-no-recursion
    -readability-implicit-bool-conversion
"""
)


def check_list(check_string):
    return ",".join([check.strip() for check in check_string.strip().splitlines()])


CODE_CHECKS = check_list(CODE_CHECKS)
TEST_CHECKS = check_list(TEST_CHECKS)


class Multimap(dict):
    def __setitem__(self, key, value):
        if key not in self:
            dict.__setitem__(self, key, [value])  # call super method to avoid recursion
        else:
            self[key].append(value)


def git_changed_lines(commit):
    file = ""
    changed_lines = Multimap()

    for line in util.run(f"git diff --text --unified=0 {commit}")[1].splitlines():
        line = line.rstrip("\n")
        fields = line.split()

        match = regex.match(r"^\+\+\+ b/.*", line)
        if match:
            file = ""

        match = regex.match(r"^\+\+\+ b/(.*(\.cpp|\.h))$", line)
        if match:
            file = match.group(1)

        match = regex.match(r"^@@", line)
        if match and file != "":
            lspan = fields[2].split(",")
            if len(lspan) <= 1:
                lspan.append(0)

            changed_lines[file] = [int(lspan[0]), int(lspan[0]) + int(lspan[1])]

    return json.dumps(
        [{"name": key, "lines": value} for key, value in changed_lines.items()]
    )


def checks(args):
    status, stdout, stderr = util.run(
        f"clang-tidy -checks='{CODE_CHECKS}' --list-checks"
    )
    print(stdout)


def check_output(output):
    return regex.match(r"^/.* warning: ", output)


def tidy(args):
    files = util.input_files(args.files)

    groups = Multimap()

    for file in files:
        groups["test" if "/tests/" in file else "main"] = file

    fix = "--fix" if args.fix == "fix" else ""
    lines = (
        ("'--line-filter=" + git_changed_lines(args.commit)) + "'"
        if args.commit is not None
        else ""
    )

    ok = True
    if groups.get("main", None):
        status, stdout, stderr = util.run(
            f"xargs clang-tidy -p=build/release/ --format-style=file -header-filter='.*' --checks='{CODE_CHECKS}' --quiet {fix} {lines}",
            input=groups["main"],
        )
        ok = check_output(stdout) and ok

    if groups.get("test", None):
        status, stdout, stderr = util.run(
            f"xargs clang-tidy -p=build/release/ --format-style=file -header-filter='.*' --checks='{TEST_CHECKS}' --quiet {fix} {lines}",
            input=groups["test"],
        )
        ok = check_output(stdout) and ok

    return 0 if ok else 1


def parse_args():
    global parser
    parser = argparse.ArgumentParser(description="CircliCi Utility")
    parser.add_argument("--commit")
    parser.add_argument("--fix")

    parser.add_argument("files", metavar="FILES", nargs="+", help="files to process")

    return parser.parse_args()


def main():
    return tidy(parse_args())


if __name__ == "__main__":
    sys.exit(main())
