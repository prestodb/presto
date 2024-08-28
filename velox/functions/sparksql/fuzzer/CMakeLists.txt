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

# Download Spark source code and copy the Spark connect proto files to Velox
# directory.
set(SPARK_VERSION 3.5.1)
set(SPARK_SHA256_CHECKSUM
    SHA256=8eb939a637cc9d4406467620cfef8e8985bf370d766907967fc85db3e9a120cb)
set(SPARK_SOURCE_URL
    "https://github.com/apache/spark/archive/refs/tags/v${SPARK_VERSION}.tar.gz"
)

FetchContent_Declare(
  Spark
  URL ${SPARK_SOURCE_URL}
  URL_HASH ${SPARK_SHA256_CHECKSUM})
FetchContent_MakeAvailable(Spark)

set(SPARK_CONNECT_PROTO_BASE_DIR
    "${spark_SOURCE_DIR}/connector/connect/common/src/main/protobuf")
set(SPARK_CONNECT_PROTO_DIR "${SPARK_CONNECT_PROTO_BASE_DIR}/spark/connect")

# Set up Spark connect.
file(
  GLOB PROTO_FILES
  RELATIVE ${PROJECT_SOURCE_DIR}
  CONFIGURE_DEPENDS ${SPARK_CONNECT_PROTO_DIR}/*.proto)
foreach(PROTO ${PROTO_FILES})
  get_filename_component(PROTO_NAME ${PROTO} NAME_WE)
  list(APPEND PROTO_SRCS
       "${PROJECT_BINARY_DIR}/spark/connect/${PROTO_NAME}.pb.cc")
  list(APPEND PROTO_HDRS
       "${PROJECT_BINARY_DIR}/spark/connect/${PROTO_NAME}.pb.h")
  list(APPEND GRPC_SRCS
       "${PROJECT_BINARY_DIR}/spark/connect/${PROTO_NAME}.grpc.pb.cc")
  list(APPEND GRPC_HDRS
       "${PROJECT_BINARY_DIR}/spark/connect/${PROTO_NAME}.grpc.pb.h")
  list(APPEND PROTO_FILES_FULL "${SPARK_CONNECT_PROTO_DIR}/${PROTO_NAME}.proto")
endforeach()

set(PROTO_OUTPUT_FILES ${PROTO_HDRS} ${PROTO_SRCS})
set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

set(GRPC_OUTPUT_FILES ${GRPC_HDRS} ${GRPC_SRCS})
set_source_files_properties(${GRPC_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

# Ensure that the option --proto_path is not given an empty argument.
foreach(PROTO_PATH ${SPARK_CONNECT_PROTO_BASE_DIR} ${Protobuf_INCLUDE_DIRS})
  list(APPEND PROTO_PATH_ARGS --proto_path=${PROTO_PATH})
endforeach()

# Generate Spark connect hearders and sources.
add_custom_command(
  OUTPUT ${PROTO_OUTPUT_FILES}
  COMMAND protobuf::protoc ${PROTO_PATH_ARGS} --cpp_out ${PROJECT_BINARY_DIR}
          ${PROTO_FILES_FULL}
  DEPENDS protobuf::protoc
  COMMENT "Running PROTO compiler"
  VERBATIM)

# Generate grpc headers and sources.
add_custom_command(
  OUTPUT ${GRPC_OUTPUT_FILES}
  COMMAND
    protobuf::protoc ${PROTO_PATH_ARGS} --grpc_out=${PROJECT_BINARY_DIR}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    ${PROTO_FILES_FULL}
  DEPENDS protobuf::protoc
  COMMENT "Running gRPC C++ protocol buffer compiler"
  VERBATIM)

add_library(velox_spark_query_runner ${PROTO_SRCS} ${GRPC_SRCS}
                                     SparkQueryRunner.cpp)

target_include_directories(velox_spark_query_runner
                           PUBLIC ${PROJECT_BINARY_DIR}/spark/connect/)

target_link_libraries(
  velox_spark_query_runner
  velox_dwio_arrow_parquet_writer
  velox_fuzzer_util
  velox_exec_test_lib
  velox_arrow_bridge
  gRPC::grpc++
  gRPC::grpc
  protobuf::libprotobuf
  arrow)

add_executable(spark_aggregation_fuzzer_test SparkAggregationFuzzerTest.cpp)

target_link_libraries(
  spark_aggregation_fuzzer_test
  velox_aggregation_fuzzer
  velox_aggregation_fuzzer_base
  velox_functions_spark_aggregates
  velox_spark_query_runner
  velox_fuzzer_util
  velox_window
  velox_vector_test_lib
  GTest::gtest
  GTest::gtest_main)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
