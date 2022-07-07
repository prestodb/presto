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

include(ExternalProject)

if(NOT VELOX_DISABLE_GOOGLETEST)
  add_subdirectory(googletest)
endif()

add_subdirectory(xsimd)

if(VELOX_ENABLE_ARROW)
  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep")
  set(ARROW_CMAKE_ARGS
      -DARROW_PARQUET=ON -DARROW_JEMALLOC=OFF -DARROW_SIMD_LEVEL=NONE
      -DARROW_RUNTIME_SIMD_LEVEL=NONE
      -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}/install -DARROW_BUILD_STATIC=ON)
  set(ARROW_LIBDIR ${ARROW_PREFIX}/install/${CMAKE_INSTALL_LIBDIR})
  ExternalProject_Add(
    arrow_ep
    PREFIX ${ARROW_PREFIX}
    URL "https://dlcdn.apache.org/arrow/arrow-8.0.0/apache-arrow-8.0.0.tar.gz"
    SOURCE_SUBDIR cpp
    CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    BUILD_BYPRODUCTS ${ARROW_LIBDIR}/libarrow.a ${ARROW_LIBDIR}/libparquet.a)
  add_library(arrow STATIC IMPORTED GLOBAL)
  add_library(parquet STATIC IMPORTED GLOBAL)
  add_dependencies(arrow arrow_ep)
  add_dependencies(parquet arrow)
  file(MAKE_DIRECTORY ${ARROW_PREFIX}/install/include)
  set_target_properties(
    arrow parquet PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                             ${ARROW_PREFIX}/install/include)
  set_target_properties(arrow PROPERTIES IMPORTED_LOCATION
                                         ${ARROW_LIBDIR}/libarrow.a)
  set_target_properties(parquet PROPERTIES IMPORTED_LOCATION
                                           ${ARROW_LIBDIR}/libparquet.a)
endif()
