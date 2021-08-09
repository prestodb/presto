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

set(CODEGEN_LINK_PATH "$<TARGET_FILE:velox_all_link>")
get_directory_property(DEFINED_VARIABLE VARIABLES)
foreach(VR IN LISTS DEFINED_VARIABLE)
  set(CMAKE_VARS "${CMAKE_VARS}\n${VR}=${${VR}}")
endforeach()

file(
  GENERATE
  OUTPUT ./cmake_var_file.txt
  CONTENT "${CMAKE_VARS}")

add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/package.json"
  DEPENDS generatePackage.py
  COMMAND python3 "${CMAKE_CURRENT_SOURCE_DIR}/generatePackage.py" "--var-file"
          "${CMAKE_CURRENT_BINARY_DIR}/cmake_var_file.txt")

set(PACKAGE_JSON
    "${CMAKE_CURRENT_BINARY_DIR}/package.json"
    CACHE INTERNAL "package file")
set(BASE_FOLDER
    "${PROJECT_SOURCE_DIR}"
    CACHE INTERNAL "package file")

add_compile_definitions(PACKAGE_JSON="${PACKAGE_JSON}")
add_compile_definitions(BASE_FOLDER="${BASE_FOLDER}")

add_library(velox_codegen_utils_resource_path package.json ResourcePath.cpp)
