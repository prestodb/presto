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

execute_process(
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/proto
  COMMAND
    ${PROTOBUF_PROTOC_EXECUTABLE}
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto/ codegen_proto.proto
    --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}/proto)

add_subdirectory(proto)
add_subdirectory(compiler_utils)
add_subdirectory(library_loader)
add_subdirectory(transform)
add_subdirectory(external_process)
add_subdirectory(code_generator)
add_subdirectory(ast)
add_subdirectory(udf_manager)
add_subdirectory(utils)
add_subdirectory(benchmark)
add_subdirectory(functions)
add_subdirectory(vector_function)

add_library(velox_experimental_codegen Codegen.cpp CodegenStubs.cpp
                                       CodegenLogger.cpp)
target_link_libraries(
  velox_experimental_codegen
  velox_codegen_transform
  velox_codegen_compiler_utils
  velox_codegen_proto
  velox_codegen_library_loader
  velox_codegen_code_generator
  velox_codegen_ast
  velox_codegen_udf_manager
  velox_codegen_external_process
  velox_core
  velox_exec
  velox_expression)
add_subdirectory(tests)
