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
from collections import OrderedDict
import fnmatch
import os
import regex
import sys


class attrdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


def parse_args():
    parser = argparse.ArgumentParser(description="Update license headers")
    parser.add_argument("--header", default="license.header", help="header file")
    parser.add_argument(
        "--extra",
        default=80,
        help="extra characters past beginning of file to look for header",
    )
    parser.add_argument(
        "--editdist", default=12, type=int, help="max edit distance between headers"
    )
    parser.add_argument(
        "--remove", default=False, action="store_true", help="remove the header"
    )
    parser.add_argument(
        "--cslash",
        default=False,
        action="store_true",
        help='use C slash "//" style comments',
    )
    parser.add_argument(
        "-v", default=False, action="store_true", dest="verbose", help="verbose output"
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-k", default=False, action="store_true", dest="check", help="check headers"
    )
    group.add_argument(
        "-i",
        default=False,
        action="store_true",
        dest="inplace",
        help="edit file inplace",
    )

    parser.add_argument("files", metavar="FILES", nargs="+", help="files to process")

    return parser.parse_args()


def file_read(filename):
    with open(filename) as file:
        return file.read()


def file_lines(filename):
    return file_read(filename).rstrip().split("\n")


def wrapper(prefix, leader, suffix, header):
    return prefix + "\n".join([leader + line for line in header]) + suffix


def wrapper_chpp(header, args):
    if args.cslash:
        return wrapper("", "//", "\n", header)
    else:
        return wrapper("/*\n", " *", "\n */\n", header)


def wrapper_hash(header, args):
    return wrapper("", "#", "\n", header)


file_types = OrderedDict(
    {
        "CMakeLists.txt": attrdict({"wrapper": wrapper_hash, "hashbang": False}),
        "Makefile": attrdict({"wrapper": wrapper_hash, "hashbang": False}),
        "*.cpp": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.dockfile": attrdict({"wrapper": wrapper_hash, "hashbang": False}),
        "*.h": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.inc": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.java": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.prolog": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.py": attrdict({"wrapper": wrapper_hash, "hashbang": True}),
        "*.sh": attrdict({"wrapper": wrapper_hash, "hashbang": True}),
        "*.thrift": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.txt": attrdict({"wrapper": wrapper_hash, "hashbang": True}),
        "*.yml": attrdict({"wrapper": wrapper_hash, "hashbang": False}),
    }
)

file_pattern = regex.compile(
    "|".join(["^" + fnmatch.translate(type) + "$" for type in file_types.keys()])
)


def get_filename(filename):
    return os.path.basename(filename)


def get_fileextn(filename):
    split = os.path.splitext(filename)
    if len(split) <= 1:
        return ""

    return split[-1]


def get_wrapper(filename):
    if filename in file_types:
        return file_types[filename]

    return file_types["*" + get_fileextn(filename)]


def message(file, string):
    if file:
        print(string, file=file)


def main():
    fail = False
    log_to = None

    args = parse_args()

    if args.verbose:
        log_to = sys.stderr

    if args.check:
        log_to = None

        if args.verbose:
            log_to = sys.stdout

    header_text = file_lines(args.header)

    if len(args.files) == 1 and args.files[0] == "-":
        files = [file.strip() for file in sys.stdin.readlines()]
    else:
        files = args.files

    for filepath in files:
        filename = get_filename(filepath)

        matched = file_pattern.match(filename)

        if not matched:
            message(log_to, "Skip : " + filepath)
            continue

        content = file_read(filepath)
        wrap = get_wrapper(filename)

        header_comment = wrap.wrapper(header_text, args)

        start = 0
        end = 0

        # Look for an exact substr match
        #
        found = content.find(header_comment, 0, len(header_comment) + args.extra)
        if found >= 0:
            if not args.remove:
                message(log_to, "OK   : " + filepath)
                continue

            start = found
            end = found + len(header_comment)
        else:
            # Look for a fuzzy match in the first 60 chars
            #
            found = regex.search(
                "(?be)(%s){e<=%d}" % (regex.escape(header_comment[0:60]), 6),
                content[0 : 80 + args.extra],
            )
            if found:
                fuzzy = regex.compile(
                    "(?be)(%s){e<=%d}" % (regex.escape(header_comment), args.editdist)
                )

                # If the first 80 chars match - try harder for the rest of the header
                #
                found = fuzzy.search(
                    content[0 : len(header_comment) + args.extra], found.start()
                )
                if found:
                    start = found.start()
                    end = found.end()

        if args.remove:
            if start == 0 and end == 0:
                if not args.inplace:
                    print(content, end="")

                message(log_to, "OK   : " + filepath)
                continue

            # If removing the header text, zero it out there.
            header_comment = ""

        message(log_to, "Fix  : " + filepath)

        if args.check:
            fail = True
            continue

        # Remove any partially matching header
        #
        content = content[0:start] + content[end:]

        if wrap.hashbang:
            search = regex.search("^#!.*\n", content)
            if search:
                content = (
                    content[search.start() : search.end()]
                    + header_comment
                    + content[search.end() :]
                )
            else:
                content = header_comment + content
        else:
            content = header_comment + content

        if args.inplace:
            with open(filepath, "w") as file:
                print(content, file=file, end="")
        else:
            print(content, end="")

    if fail:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
