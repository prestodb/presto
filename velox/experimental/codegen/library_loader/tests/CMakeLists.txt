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

add_subdirectory(resources)
add_compile_definitions(
  velox_codegen_native_loader_test
  GOODLIBPATH="$<TARGET_FILE:velox_codegen_native_loader_resources_good_library>"
)
add_compile_definitions(
  velox_codegen_native_loader_test
  BADLIBPATH="$<TARGET_FILE:velox_codegen_native_loader_resources_bad_library>")
add_compile_definitions(
  velox_codegen_native_loader_test
  TYPEINFO="$<TARGET_FILE:velox_codegen_native_loader_resources_typeinfo_library>"
)
add_executable(velox_codegen_native_loader_test NativeLoaderTest.cpp)
add_test(velox_codegen_native_loader_test velox_codegen_native_loader_test)
add_dependencies(
  velox_codegen_native_loader_test
  velox_codegen_native_loader_resources_good_library
  velox_codegen_native_loader_resources_bad_library
  velox_codegen_native_loader_resources_typeinfo_library)
target_link_libraries(
  velox_codegen_native_loader_test
  gtest
  gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  ${DOUBLE_CONVERSION}
  ${GFLAGS_LIBRARIES}
  glog::glog
  ${FMT})
