#!/bin/bash
# shellcheck disable=SC2034
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

# The versions of the dependencies are also listed in the
# README.md velox/CMake/resolve_dependency_modules/README.md
# The versions should match the declared versions in this file.

# Build dependencies versions.
FB_OS_VERSION="v2025.04.28.00"
FMT_VERSION="10.1.1"
BOOST_VERSION="boost-1.84.0"
ARROW_VERSION="15.0.0"
DUCKDB_VERSION="v0.8.1"
PROTOBUF_VERSION="21.8"
XSIMD_VERSION="10.0.0"
SIMDJSON_VERSION="3.9.3"
CPR_VERSION="1.10.5"
DOUBLE_CONVERSION_VERSION="v3.1.5"
RANGE_V3_VERSION="0.12.0"
RE2_VERSION="2024-07-02"
GFLAGS_VERSION="v2.2.2"
GLOG_VERSION="v0.6.0"
LZO_VERSION="2.10"
SNAPPY_VERSION="1.1.8"
THRIFT_VERSION="${THRIFT_VERSION:-v0.16.0}"
STEMMER_VERSION="2.2.0"
GEOS_VERSION="3.10.7"
# shellcheck disable=SC2034
FAISS_VERSION="1.11.0"
FAST_FLOAT_VERSION="v8.0.2"
CCACHE_VERSION="4.11.3"

# Adapter related versions.
ABSEIL_VERSION="20240116.2"
GRPC_VERSION="v1.48.1"
CRC32_VERSION="1.1.2"
NLOHMAN_JSON_VERSION="v3.11.3"
GOOGLE_CLOUD_CPP_VERSION="v2.22.0"
HADOOP_VERSION="3.3.0"
AZURE_SDK_VERSION="12.8.0"
MINIO_VERSION="2022-05-26T05-48-41Z"
MINIO_BINARY_NAME="minio-2022-05-26"
AWS_SDK_VERSION="1.11.321"
