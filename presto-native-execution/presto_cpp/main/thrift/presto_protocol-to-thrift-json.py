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
#

import argparse
import os
import sys

sys.path.insert(0, "../../presto_protocol")
import util


def eprint(text):
    print(text, file=sys.stderr)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate Presto protocol to/from Presto Thrift JSON data"
    )
    parser.add_argument(
        "-c",
        "--config",
        default="presto_protocol-to-thrift-json.yml",
        help="config file",
    )
    parser.add_argument("thrift", help="Thrift spec as JSON")
    parser.add_argument("protocol", help="Presto protocol spec as JSON")

    return parser.parse_args()


def special_file(filename, special, thrift_item, key):
    if os.path.isfile(filename):
        (status, stdout, stderr) = util.run(
            "../../../velox/scripts/license-header.py --header ../../../license.header --remove "
            + filename
        )
        thrift_item[key] = stdout
        return True

    return special


def main():
    args = parse_args()
    config = util.load_yaml(args.config)
    thrift = util.load_yaml(args.thrift)
    protocol = util.load_yaml(args.protocol)

    pmap = {}
    for item in protocol:
        if "class_name" in item:
            pmap[item.class_name] = item

    comment = "// This file is generated DO NOT EDIT @" + "generated"
    result = [{"comment": comment}]

    for thrift_item in thrift:
        config_item = None
        if "class_name" in thrift_item and thrift_item.class_name in pmap:
            protocol_item = pmap[thrift_item.class_name]

            special = False
            if "struct" in thrift_item:
                if thrift_item.class_name in config.StructMap:
                    config_item = config.StructMap[thrift_item.class_name]
                    thrift_item["proto_name"] = config_item.class_name
                    special = True

                for field in thrift_item.fields:
                    if (
                        config_item is not None
                        and field.field_name in config_item.fields
                    ):
                        field["proto_name"] = config_item.fields[
                            field.field_name
                        ].field_name
                    else:
                        field["proto_name"] = field.field_name

                if "struct" in protocol_item:
                    thrift_field_set = {t.proto_name for t in thrift_item.fields}
                    protocol_field_set = {p.field_name for p in protocol_item.fields}
                    valid_fields = thrift_field_set.intersection(protocol_field_set)

                    for field in thrift_item.fields:
                        if field.field_name in valid_fields:
                            field["convert"] = True

                    if len((thrift_field_set - protocol_field_set)) != 0:
                        eprint(
                            "Missing protocol fields: "
                            + thrift_item.class_name
                            + " "
                            + str(thrift_field_set - protocol_field_set)
                        )

                    if len((protocol_field_set - thrift_field_set)) != 0:
                        eprint(
                            "Missing thrift fields: "
                            + thrift_item.class_name
                            + " "
                            + str(protocol_field_set - thrift_field_set)
                        )
                else:
                    hfile = "./special/" + thrift_item.class_name + ".hpp.inc"
                    special = special_file(hfile, special, thrift_item, "hinc")

                    cfile = "./special/" + thrift_item.class_name + ".cpp.inc"
                    special = special_file(cfile, special, thrift_item, "cinc")

                    if not special:
                        eprint(
                            "Thrift struct missing from presto_protocol: "
                            + thrift_item.class_name
                        )
        else:
            eprint("Thrift item missing from presto_protocol: " + item.class_name)

    result.extend(thrift)
    print(util.to_json(result))


if __name__ == "__main__":
    sys.exit(main())
