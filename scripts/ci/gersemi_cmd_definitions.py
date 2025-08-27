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

from copy import deepcopy

from gersemi.builtin_commands import builtin_commands

# Gersemi throws a runtime error if two commands use the same builtin definition
# but changing the canonical name in a deepcopy prevents this
velox_add_library = deepcopy(builtin_commands["add_library"])
velox_base_add_library = deepcopy(builtin_commands["add_library"])
velox_add_library["_canonical_name"] = "velox_add_library"
velox_base_add_library["_canonical_name"] = "velox_base_add_library"

pybind11_add_module = {
    "front_positional_arguments": ["target_name"],
    "options": [
        "MODULE",
        "SHARED",
        "EXCLUDE_FROM_ALL",
        "THIN_LTO",
        "NO_EXTRAS",
        "OPT_SIZE",
    ],
    "back_positional_arguments": ["sources"],
    "_canonical_name": "pybind11_add_module",
}
pyvelox_add_module = deepcopy(pybind11_add_module)
pyvelox_add_module["_canonical_name"] = "pyvelox_add_module"

# Define the argument structure of our custom functions, this influences how they are formatted
command_definitions = {
    "pybind11_add_module": pybind11_add_module,
    "pyvelox_add_module": pyvelox_add_module,
    "velox_add_library": velox_add_library,
    "velox_base_add_library": velox_base_add_library,
    "velox_build_dependency": {
        "front_positional_arguments": ["dependency_name"],
    },
    "velox_compile_definitions": builtin_commands["target_compile_definitions"],
    "velox_get_rpath_origin": {"front_positional_arguments": ["output_variable"]},
    "velox_include_directories": builtin_commands["target_include_directories"],
    "velox_install_library_headers": {"options": ["nothing"]},
    "velox_link_libraries": builtin_commands["target_link_libraries"],
    "velox_resolve_dependency": builtin_commands["find_package"],
    "velox_resolve_dependency_url": {"front_positional_arguments": ["dependency_name"]},
    "velox_set_source": {"front_positional_arguments": ["dependency_name"]},
    "velox_set_with_default": {
        "front_positional_arguments": ["var_name", "envvar_name", "default"]
    },
    "velox_sources": builtin_commands["target_sources"],
}
