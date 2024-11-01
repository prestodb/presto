#!/usr/bin/env python3
#
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
from string import Template

cpp_template = Template(
    """\
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This file is generated. Do not modify it manually. To re-generate it, run:
//
//  ./velox/type/tz/gen_timezone_links.py -f /tmp/backward \\
//       > velox/type/tz/TimeZoneLinks.cpp
//
// The backward file should be the same one used in JODA,
// The latest is available here :
// https://github.com/JodaOrg/global-tz/blob/global-tz/backward.
// Presto Java currently uses the one found here:
// https://github.com/JodaOrg/global-tz/blob/2024agtz/backward
// To find the current version in Presto Java, check this file at the version of
// JODA Presto Java is using:
// https://github.com/JodaOrg/joda-time/blob/v2.12.7/src/changes/changes.xml
// @generated

#include <string>
#include <unordered_map>

namespace facebook::velox::tz {

const std::unordered_map<std::string, std::string>& getTimeZoneLinks() {
  static auto* tzLinks = new std::unordered_map<std::string, std::string>([] {
    // Work around clang compiler bug causing multi-hour compilation
    // with -fsanitize=fuzzer
    // https://github.com/llvm/llvm-project/issues/75666
    return std::unordered_map<std::string, std::string>{
$entries
    };
  }());
  return *tzLinks;
}

} // namespace facebook::velox::tz\
"""
)

entry_template = Template('        {"$tz_key", "$tz_value"},')


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Reads an input file (specified by -f) containing mappings from "
        "time zone names to the names that should replace them and prints the "
        "corresponding `TimeZoneLinks.cpp` file.",
        epilog="(c) Facebook 2004-present",
    )
    parser.add_argument(
        "-f",
        "--file-input",
        required=True,
        help="Input timezone links file.",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    entries = []

    # Read an input file with links following the format:
    #
    # >  Link	Australia/Sydney	Australia/NSW
    # >  Link	Australia/Darwin	Australia/North
    # >  Link	Australia/Brisbane	Australia/Queensland
    # >  ...
    #
    # Possible containing comments (everything in a line after # is ignored)

    with open(args.file_input) as file_in:
        for line in file_in:
            line = line.strip()

            columns = line.split()
            if len(columns) == 0:
                continue

            if columns[0] != "Link":
                continue

            # Officially the GMT/UTC time zone IDs start with Etc/ so Joda
            # includes this in its link files. Joda ends up removing this
            # prefix at runtime though, so we just skip ahead and do it here.
            tz_value = columns[1]
            if tz_value == "Etc/GMT" or tz_value == "Etc/UTC":
                tz_value = "UTC"

            entries.append(
                entry_template.substitute(tz_key=columns[2], tz_value=tz_value)
            )

    print(cpp_template.substitute(entries="\n".join(entries)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
