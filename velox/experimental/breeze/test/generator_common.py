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

# Copyright (c) 2024 by Rivos Inc.
# Licensed under the Apache License, Version 2.0, see LICENSE for details.
# SPDX-License-Identifier: Apache-2.0

import argparse
import sys
import functools
import re
from datetime import datetime

COPYRIGHT = f"""/*
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

/*
 * Copyright (c) {datetime.now().year} by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */
"""

parser = argparse.ArgumentParser()
parser.add_argument("--template", required=True)
parser.add_argument("-l", "--llvm-root", default=None)
parser.add_argument("--out", required=True)
parser.add_argument(
    "--backend",
    required=True,
    choices=["cuda", "hip", "sycl", "openmp", "opencl", "metal"],
)
args = parser.parse_args()

# If llvm-root not specified rely on the environment to provide libclang.
# This is typically only the case for nvidia-cuda and metal builds.
if args.llvm_root is None:
    libclang_path = None
    libclang_python = None
    clang_format = "clang-format"
else:
    libclang_path = f"{args.llvm_root}/lib"
    libclang_python = f"{args.llvm_root}/lib/python3/site-packages"
    clang_format = f"{args.llvm_root}/bin/clang-format"

# Add libclang python bindings to the library search path
if libclang_python:
    sys.path.append(libclang_python)

import clang.cindex  # noqa E402
from clang.cindex import CursorKind  # noqa E402

if libclang_path:
    clang.cindex.Config.set_library_path(libclang_path)

# The following options are exposed in python already, but we re-expose
# them for consistency
TU_FLAGS_None = 0x0
TU_FLAGS_DetailedPreprocessingRecord = 0x01
TU_FLAGS_Incomplete = 0x02
TU_FLAGS_PrecompiledPreamble = 0x04
TU_FLAGS_CacheCompletionResults = 0x08
TU_FLAGS_SkipFunctionBodies = 0x40
TU_FLAGS_IncludeBriefCommentsInCodeCompletion = 0x80
# Consistency checks, otherwise the newer options may also be suspect...
assert TU_FLAGS_None == clang.cindex.TranslationUnit.PARSE_NONE
assert (
    TU_FLAGS_DetailedPreprocessingRecord
    == clang.cindex.TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD
)
assert TU_FLAGS_Incomplete == clang.cindex.TranslationUnit.PARSE_INCOMPLETE
assert (
    TU_FLAGS_PrecompiledPreamble
    == clang.cindex.TranslationUnit.PARSE_PRECOMPILED_PREAMBLE
)
assert (
    TU_FLAGS_CacheCompletionResults
    == clang.cindex.TranslationUnit.PARSE_CACHE_COMPLETION_RESULTS
)
assert (
    TU_FLAGS_SkipFunctionBodies
    == clang.cindex.TranslationUnit.PARSE_SKIP_FUNCTION_BODIES
)
assert (
    TU_FLAGS_IncludeBriefCommentsInCodeCompletion
    == clang.cindex.TranslationUnit.PARSE_INCLUDE_BRIEF_COMMENTS_IN_CODE_COMPLETION
)
# The following parser options are not exposed in the python bindings
# but *are* supported by libclang.
TU_FLAGS_ForSerialization = 0x10
TU_FLAGS_CXXChainedPCH = 0x20
TU_FLAGS_CreatePreambleOnFirstParse = 0x100
TU_FLAGS_KeepGoing = 0x200
TU_FLAGS_SingleFileParse = 0x400
TU_FLAGS_LimitSkipFunctionBodiesToPreamble = 0x800
TU_FLAGS_IncludeAttributedTypes = 0x1000
TU_FLAGS_VisitImplicitAttributes = 0x2000
TU_FLAGS_IgnoreNonErrorsFromIncludedFiles = 0x4000
TU_FLAGS_RetainExcludedConditionalBlocks = 0x8000


def parse(filename):
    index = clang.cindex.Index.create()
    tu = index.parse(
        filename,
        args=["-xc++", "-std=c++17"],
        options=clang.cindex.TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD
        | clang.cindex.TranslationUnit.PARSE_INCOMPLETE
        | TU_FLAGS_KeepGoing
        | TU_FLAGS_SingleFileParse
        | TU_FLAGS_IncludeAttributedTypes
        | TU_FLAGS_VisitImplicitAttributes
        | TU_FLAGS_IgnoreNonErrorsFromIncludedFiles
        | TU_FLAGS_RetainExcludedConditionalBlocks,
    )
    return index, tu


@functools.lru_cache
def read_cached_file(path):
    f = open(path, "r")
    return f.read()


def read_extent(extent):
    file = extent.start.file
    return read_cached_file(file.name)[extent.start.offset : extent.end.offset]


FUNCTION_CURSOR_KINDS = [
    CursorKind.FUNCTION_DECL,
    CursorKind.CXX_METHOD,
    CursorKind.FUNCTION_TEMPLATE,
]
TEMPLATE_CURSOR_KINDS = [
    CursorKind.TEMPLATE_TYPE_PARAMETER,
    CursorKind.TEMPLATE_NON_TYPE_PARAMETER,
    CursorKind.TEMPLATE_TEMPLATE_PARAMETER,
]


# Helper for extracting commonly needed properties from a cursor
def extract_node(cursor):
    node = {
        "kind": cursor.kind,
        "displayname": cursor.displayname,
        "spelling": cursor.spelling,
        "cursor": cursor,
    }

    if cursor.type.spelling != "":
        node["type_spelling"] = cursor.type.spelling

    if cursor.kind in FUNCTION_CURSOR_KINDS:
        template_params = []
        function_params = []
        raw_attrs = []
        body = None
        for c in cursor.get_children():
            if c.kind == CursorKind.PARM_DECL:
                function_params.append(c)
            elif c.kind in TEMPLATE_CURSOR_KINDS:
                template_params.append(c)
            # Since unknown attributes are dropped during parse we
            # encode them using clang::annotate.
            elif c.kind == CursorKind.ANNOTATE_ATTR:
                raw_attrs.append(c)
            elif c.kind == CursorKind.COMPOUND_STMT:
                if body is not None:
                    raise ValueError("Multiple compound statements unexpected")
                body = c
            else:
                raise ValueError(
                    f"Unhandled method child, \
                        pessimistically exiting: {c.kind} {c.extent}"
                )

        attrs = {}
        for c in raw_attrs:
            a = c.spelling.split("=", 1)
            attrs.setdefault(a[0], []).append(a[1] if len(a) == 2 else True)

        node["fn_props"] = {
            "attrs": attrs,
            "return_type": cursor.result_type,
            "template_params": template_params,
            "function_params": function_params,
            "body": body,
        }

    node["children"] = list(cursor.get_children())

    return node


snake_caser = re.compile(r"(?<!^)([A-Z])")


def to_snake_case(name):
    return snake_caser.sub(r"_\1", name).lower()
