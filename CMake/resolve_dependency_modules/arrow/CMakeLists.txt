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
project(Arrow)

if(VELOX_ENABLE_ARROW)
  if(Thrift_FOUND)
    set(THRIFT_SOURCE "SYSTEM")
  else()
    set(THRIFT_SOURCE "BUNDLED")
  endif()
  # Apache Arrow 21.0.0 uses archive.apache.org as the default mirror, which
  # tends to timeout in CI. We don't need to explicitly set this alternate
  # mirror after the 22.0.0 release because this will be used by default. See
  # also: https://github.com/apache/arrow/issues/47111
  set(THRIFT_URL
      "https://packages.apache.org/artifactory/arrow/thirdparty/thrift-0.22.0.tar.gz"
  )

  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep")
  set(ARROW_CMAKE_ARGS
      -DARROW_PARQUET=OFF
      -DARROW_DEPENDENCY_SOURCE=AUTO
      -DARROW_WITH_THRIFT=ON
      -DARROW_WITH_LZ4=ON
      -DARROW_WITH_SNAPPY=ON
      -DARROW_WITH_ZLIB=ON
      -DARROW_WITH_ZSTD=ON
      -DARROW_JEMALLOC=OFF
      -DARROW_MIMALLOC=OFF
      -DARROW_SIMD_LEVEL=NONE
      -DARROW_RUNTIME_SIMD_LEVEL=NONE
      -DARROW_WITH_UTF8PROC=OFF
      -DARROW_TESTING=ON
      -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}/install
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
      -DCMAKE_DEBUG_POSTFIX=
      -DARROW_BUILD_STATIC=ON
      -DThrift_SOURCE=${THRIFT_SOURCE}
      -DARROW_THRIFT_BUILD_SHA256_CHECKSUM=${THRIFT_BUILD_SHA256_CHECKSUM}
      -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH})
  set(ARROW_LIBDIR ${ARROW_PREFIX}/install/${CMAKE_INSTALL_LIBDIR})

  add_library(thrift STATIC IMPORTED GLOBAL)
  if(NOT Thrift_FOUND)
    set(THRIFT_SOURCE_ROOT ${ARROW_PREFIX}/src/arrow_ep-build/_deps/thrift-src)
    set(THRIFT_BUILD_ROOT ${ARROW_PREFIX}/src/arrow_ep-build/_deps/thrift-build)
    set(THRIFT_INCLUDE_DIR ${THRIFT_BUILD_ROOT}
                           ${THRIFT_SOURCE_ROOT}/lib/cpp/src)
    foreach(dir ${THRIFT_INCLUDE_DIR})
      file(MAKE_DIRECTORY ${dir})
    endforeach()
    set(THRIFT_LIB ${ARROW_PREFIX}/src/arrow_ep-build/lib/libthrift.a)
  endif()
  target_include_directories(thrift INTERFACE ${THRIFT_INCLUDE_DIR})
  set_property(TARGET thrift PROPERTY IMPORTED_LOCATION ${THRIFT_LIB})

  set(VELOX_ARROW_BUILD_VERSION 21.0.0)
  set(VELOX_ARROW_BUILD_SHA256_CHECKSUM
      5d3f8db7e72fb9f65f4785b7a1634522e8d8e9657a445af53d4a34a3849857b5)
  set(VELOX_ARROW_SOURCE_URL
      "https://github.com/apache/arrow/releases/download/apache-arrow-${VELOX_ARROW_BUILD_VERSION}/apache-arrow-${VELOX_ARROW_BUILD_VERSION}.tar.gz"
  )

  velox_resolve_dependency_url(ARROW)

  ExternalProject_Add(
    arrow_ep
    PREFIX ${ARROW_PREFIX}
    URL ${VELOX_ARROW_SOURCE_URL}
    URL_HASH ${VELOX_ARROW_BUILD_SHA256_CHECKSUM}
    SOURCE_SUBDIR cpp
    # We can't change ARROW_THRIFT_URL by CMake variable. We need to use
    # environment variable. So we use "cmake -E env ARROW_THRIFT_URL=..." here.
    # We can remove this when we use Apache Arrow C++ 22.0.0 or later. See also
    # the above comment THRIFT_URL.
    CMAKE_COMMAND ${CMAKE_COMMAND}
    -E
    env
    ARROW_THRIFT_URL=${THRIFT_URL}
    --
    ${CMAKE_COMMAND}
    CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    BUILD_BYPRODUCTS ${ARROW_LIBDIR}/libarrow.a
                     ${ARROW_LIBDIR}/libarrow_testing.a ${THRIFT_LIB})

  add_library(arrow STATIC IMPORTED GLOBAL)
  add_library(arrow_testing STATIC IMPORTED GLOBAL)
  add_dependencies(arrow arrow_ep)
  add_dependencies(arrow_testing arrow)
  file(MAKE_DIRECTORY ${ARROW_PREFIX}/install/include)
  set_target_properties(
    arrow arrow_testing PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                   ${ARROW_PREFIX}/install/include)
  set_target_properties(arrow PROPERTIES IMPORTED_LOCATION
                                         ${ARROW_LIBDIR}/libarrow.a)
  set_property(TARGET arrow PROPERTY INTERFACE_LINK_LIBRARIES ${RE2} thrift)
  set_target_properties(
    arrow_testing PROPERTIES IMPORTED_LOCATION
                             ${ARROW_LIBDIR}/libarrow_testing.a)
endif()
