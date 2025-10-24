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


def special_file(filename, thrift_item, key):
    if os.path.isfile(filename):
        (status, stdout, stderr) = util.run(
            "../../../velox/scripts/checks/license-header.py --header ../../../license.header --remove "
            + filename
        )
        thrift_item[key] = stdout


def verify(thrift_item, protocol_item):
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

def process_fields(thrift_item, config_item):
    for field in thrift_item.fields:
        if (
            config_item is not None 
            and "fields" in config_item
            and field.field_name in config_item.fields
        ):
            field["proto_name"] = config_item.fields[
                field.field_name
            ].field_name
        else:
            field["proto_name"] = field.field_name

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

    # Skip structs that are not used in cpp
    thrift = [item for item in thrift if "class_name" in item and item.class_name not in config.SkipStruct]
    for thrift_item in thrift:
        if "class_name" not in thrift_item:
            continue

        # For structs that are defined in presto_protocol_core.h        
        if thrift_item.class_name in config.StructInProtocolCore:
            thrift_item["core"] = "true"
        
        # For union structs
        if "union" in thrift_item and thrift_item.class_name not in config.Special:
            thrift_item["proto_name"] = thrift_item.class_name.removesuffix("Union")
            if thrift_item.class_name in config.StructMap and "fields" in config.StructMap[thrift_item.class_name]:
                config_item = config.StructMap[thrift_item.class_name].fields
                for field in thrift_item.fields:
                    if field.field_name in config_item:
                        field["proto_field_type"] = config_item[field.field_name].field_type
            continue

        # For structs that have a single field in IDL but defined using type aliases in cpp
        if thrift_item.class_name in config.WrapperStruct:
            thrift_item["wrapper"] = "true"
            del thrift_item["struct"]
            continue

        # For connector structs
        if thrift_item.class_name in config.ConnectorStruct:
            thrift_item["connector"] = "true"
            del thrift_item["struct"]
        
        # For structs that need special implementations
        if thrift_item.class_name in config.Special:
            hfile = "./special/" + thrift_item.class_name + ".hpp.inc"
            special_file(hfile, thrift_item, "hinc")

            cfile = "./special/" + thrift_item.class_name + ".cpp.inc"
            special_file(cfile, thrift_item, "cinc")

        elif (thrift_item.class_name in pmap) or (thrift_item.class_name in config.StructMap and config.StructMap[thrift_item.class_name].class_name in pmap):
            protocol_item = pmap[thrift_item.class_name] if thrift_item.class_name in pmap else pmap[config.StructMap[thrift_item.class_name].class_name]
            thrift_item["proto_name"] = protocol_item.class_name

            config_item = None
            if "struct" in thrift_item:
                # For structs that have different field names in cpp and IDL
                if thrift_item.class_name in config.StructMap:
                    config_item = config.StructMap[thrift_item.class_name]
                    thrift_item["proto_name"] = config_item.class_name

                process_fields(thrift_item, config_item)

                if "struct" in protocol_item:
                    verify(thrift_item, protocol_item)
        else:
            eprint("Thrift item missing from presto_protocol: " + thrift_item.class_name)

    result.extend(thrift)
    print(util.to_json(result))


if __name__ == "__main__":
    sys.exit(main())
