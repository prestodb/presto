#!/Library/Developer/CommandLineTools/usr/bin/python3
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

import json
import sys

from ptsd_jbroll import ast
from ptsd_jbroll.loader import Loader


def eprint(text):
    print(text, file=sys.stderr)


def devnull(string):
    pass


class attrdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


loader = Loader(sys.argv[1], logger=devnull)

typeMap = {
    "byte": "byte",
    "i16": "int16_t",
    "i32": "int32_t",
    "i64": "int64_t",
    "bool": "bool",
    "double": "double",
    "string": "string",
}


def str_type(t):
    if isinstance(
        t, (ast.Byte, ast.I16, ast.I32, ast.I64, ast.Bool, ast.Double, ast.String)
    ):
        return typeMap[str(t)]
    elif isinstance(t, ast.Identifier):
        return t.value
    elif isinstance(t, ast.Map):
        return "std::map<%s, %s>" % (str_type(t.key_type), str_type(t.value_type))
    elif isinstance(t, ast.List):
        return "std::list<%s>" % str_type(t.value_type)
    elif isinstance(t, ast.Set):
        return "std::set<%s>" % str_type(t.value_type)

    return str(t)


def enum(n):
    v = {}
    v["enum"] = True
    v["class_name"] = n.name.value
    v["elements"] = []
    for value in n.values:
        v["elements"].append(dict(name=value.name.value, tag=value.tag))

    return v


def struct(n):
    v = {}
    v["struct"] = True
    v["class_name"] = n.name.value
    v["fields"] = []
    for field in n.fields:
        v["fields"].append(
            dict(
                field_name=field.name.value,
                field_type=str_type(field.type),
                optional=(field.required == "optional"),
                required=(field.required == "required"),
                tag=field.tag,
            )
        )

    return v


def items(module):
    i = []
    for node in module.values():
        if not isinstance(node, ast.Node):
            continue
        if isinstance(node, ast.Enum):
            i.append(enum(node))
        elif isinstance(node, ast.Struct):
            i.append(struct(node))

    return i


everything = [{"comment": "// This file is generated DO NOT EDIT @" + "generated"}]
for module in loader.modules.values():
    everything.extend(items(module))

print(json.dumps(everything))
