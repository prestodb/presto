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
//  ./velox/type/tz/gen_timezone_database.py -f /tmp/zone-index.properties \\
//       > velox/type/tz/TimeZoneDatabase.cpp
//
// The zone-index.properties file should be the same one used in Presto,
// Available here :
// https://github.com/prestodb/presto/blob/master/presto-common/src/main/resources/com/facebook/presto/common/type/zone-index.properties.
// @generated

#include <string>
#include <unordered_map>
#include <vector>

namespace facebook::velox::util {

const std::vector<std::pair<int16_t, std::string>>& getTimeZoneEntries() {
  static auto* tzDB = new std::vector<std::pair<int16_t, std::string>>([] {
    // Work around clang compiler bug causing multi-hour compilation
    // with -fsanitize=fuzzer
    // https://github.com/llvm/llvm-project/issues/75666
    return std::vector<std::pair<int16_t, std::string>>{
$entries
    };
  }());
  return *tzDB;
}

} // namespace facebook::velox::util\
"""
)

entry_template = Template('        {$tz_id, "$tz_name"},')


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Reads an input timezone mapping database file "
        "(specified by -f) and prints the corresponding "
        "`TimeZoneDatabase.cpp` file.",
        epilog="(c) Facebook 2004-present",
    )
    parser.add_argument(
        "-f",
        "--file-input",
        required=True,
        help="Input timezone database file (space separated, one entry per line).",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    entries = []

    # Read an input file following the format:
    #
    # >  1825 America/Los_Angeles
    # >  1826 America/Louisville
    # >  1827 America/Lower_Princes
    # >  ...
    #

    # Explicitly add UTC.
    entries.append(entry_template.substitute(tz_id=0, tz_name="+00:00"))
    with open(args.file_input) as file_in:
        for line in file_in:
            line = line.strip()
            if line[0] == "#":
                continue

            tz_id, tz_name = line.split()
            entries.append(entry_template.substitute(tz_id=tz_id, tz_name=tz_name))
    print(cpp_template.substitute(entries="\n".join(entries)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
