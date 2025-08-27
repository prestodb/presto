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

find_package(Python REQUIRED COMPONENTS Interpreter)

execute_process(
  COMMAND
    "${Python_EXECUTABLE}" -c
    "\
import pyarrow
print(pyarrow.get_include())
"
  OUTPUT_VARIABLE _pyarrow_include_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
  COMMAND
    "${Python_EXECUTABLE}" -c
    "\
import pyarrow
pyarrow.create_library_symlinks()
print(pyarrow.get_library_dirs()[0])
"
  OUTPUT_VARIABLE _pyarrow_lib_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

find_library(_libarrow arrow PATHS ${_pyarrow_lib_dir} NO_DEFAULT_PATH)

find_library(_libarrow_py arrow_python PATHS ${_pyarrow_lib_dir} NO_DEFAULT_PATH)

set(pyarrow_LIBARROW ${_libarrow})
set(pyarrow_LIBARROW_PYTHON ${_libarrow_py})
set(pyarrow_INCLUDE_DIR ${_pyarrow_include_dir})

mark_as_advanced(_libarrow _libarrow_py _pyarrow_include_dir _pyarrow_lib_dir)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  pyarrow
  REQUIRED_VARS pyarrow_LIBARROW pyarrow_LIBARROW_PYTHON pyarrow_INCLUDE_DIR
)

if(pyarrow_FOUND)
  if(NOT TARGET pyarrow::dev)
    add_library(pyarrow::dev SHARED IMPORTED)
    set_target_properties(
      pyarrow::dev
      PROPERTIES
        IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
        IMPORTED_LOCATION "${pyarrow_LIBARROW_PYTHON}"
        INTERFACE_INCLUDE_DIRECTORIES "${pyarrow_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES "${pyarrow_LIBARROW}"
    )
  endif()
endif()
