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
import re
import argparse

parser = argparse.ArgumentParser(description="Builds a package descriptor for testing")
parser.add_argument("--var-file", help="Cmake variable")


def f4d_libraries(cmake_vars):
    return f"""
    {{
        "includePath" : ["{cmake_vars['CMAKE_SOURCE_DIR']}"],
        "additionalLinkerObject": []
    }}
    """


def additional_libraries(cmake_vars):
    # libraries with include path and binaries
    library_names = ["PROTOBUF", "ZLIB", "GFLAGS", "FOLLY"]
    library_includes = {
        '"' + cmake_vars[f"{name}_INCLUDE_DIR"] + '"' for name in library_names
    }

    return f"""{{
        "includePath" : [{" ,".join(library_includes)}]

    }}"""


def parse_cmake_vars(file_path):
    reg = re.compile("(?P<name>.*)(:.*)?=(?P<value>.*)")
    matches = [reg.match(line) for line in open(file_path).readlines()]
    variables = {
        match.group("name"): match.group("value") for match in matches if match
    }
    return variables


if __name__ == "__main__":
    args = parser.parse_args()
    cmake_vars = parse_cmake_vars(args.var_file)
    opt_level = "-O3"
    json_string = f"""
    {{"optimizationLevel": "{opt_level}",
    "extraLinkOptions" : ["-Wl,-undefined,dynamic_lookup", "-Wl,-rpath", "." ],
    "extraCompileOption": [
        "-g",
        "-v",
        "-std=c++17",
        "-mavx2",
        "-mfma",
        "-mavx",
        "-mf16c",
        "-march=native",
        "-DUSE_VELOX_COMMON_BASE",
        "-mllvm",
        "-inline-threshold=5000",
        "-DBOOST_ALL_NO_LIB",
        "-DBOOST_CONTEXT_DYN_LINK",
        "-DBOOST_REGEX_DYN_LINK",
        "-fPIC",
        {f'"-isysroot", "{cmake_vars["CMAKE_OSX_SYSROOT"]}"' if 'CMAKE_OSX_SYSROOT' in cmake_vars else ""},
    ],
    "extraLinkOptions" : [{f'"-isysroot", "{cmake_vars["CMAKE_OSX_SYSROOT"]}"' if 'CMAKE_OSX_SYSROOT' in cmake_vars else ""}],
    "defaultLibraries": [{f4d_libraries(cmake_vars)}, {additional_libraries(cmake_vars)}],
    "compilerPath": "{cmake_vars['CMAKE_CXX_COMPILER']}",
    "linker": "{cmake_vars['CMAKE_LINKER']}",
    "tempDirectory": "/tmp"
    }}
    """
    with open("package.json", "w") as f:
        f.write(json_string)
