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

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
                      "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules/")

include(ExternalProject)

if(NOT VELOX_DISABLE_GOOGLETEST)
  add_subdirectory(googletest)
endif()

add_subdirectory(xsimd)

if(VELOX_ENABLE_ARROW)
  find_package(Thrift)
  if(Thrift_FOUND)
    set(THRIFT_SOURCE "SYSTEM")
  else()
    set(THRIFT_SOURCE "BUNDLED")
  endif()
  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep")
  set(ARROW_CMAKE_ARGS
      -DARROW_PARQUET=ON
      -DARROW_WITH_LZ4=ON
      -DARROW_WITH_SNAPPY=ON
      -DARROW_WITH_ZLIB=ON
      -DARROW_WITH_ZSTD=ON
      -DARROW_JEMALLOC=OFF
      -DARROW_SIMD_LEVEL=NONE
      -DARROW_RUNTIME_SIMD_LEVEL=NONE
      -DARROW_WITH_UTF8PROC=OFF
      -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}/install
      -DARROW_BUILD_STATIC=ON
      -DThrift_SOURCE=${THRIFT_SOURCE})
  set(ARROW_LIBDIR ${ARROW_PREFIX}/install/${CMAKE_INSTALL_LIBDIR})

  add_library(thrift STATIC IMPORTED GLOBAL)
  if(NOT Thrift_FOUND)
    set(THRIFT_ROOT ${ARROW_PREFIX}/src/arrow_ep-build/thrift_ep-install)
    set(THRIFT_LIB ${THRIFT_ROOT}/lib/libthrift.a)
    file(MAKE_DIRECTORY ${THRIFT_ROOT}/include)
    set(THRIFT_INCLUDE_DIR ${THRIFT_ROOT}/include)
  endif()

  set_property(TARGET thrift PROPERTY INTERFACE_INCLUDE_DIRECTORIES
                                      ${THRIFT_INCLUDE_DIR})
  set_property(TARGET thrift PROPERTY IMPORTED_LOCATION ${THRIFT_LIB})

  ExternalProject_Add(
    arrow_ep
    PREFIX ${ARROW_PREFIX}
    URL "https://dlcdn.apache.org/arrow/arrow-8.0.0/apache-arrow-8.0.0.tar.gz"
    SOURCE_SUBDIR cpp
    CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    BUILD_BYPRODUCTS ${ARROW_LIBDIR}/libarrow.a ${ARROW_LIBDIR}/libparquet.a
                     ${THRIFT_LIB})
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
  set_property(TARGET arrow PROPERTY INTERFACE_LINK_LIBRARIES ${RE2} thrift)
  set_target_properties(parquet PROPERTIES IMPORTED_LOCATION
                                           ${ARROW_LIBDIR}/libparquet.a)

endif()
