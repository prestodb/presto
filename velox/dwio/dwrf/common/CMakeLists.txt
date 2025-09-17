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

add_subdirectory(wrap)

velox_add_library(
  velox_dwio_dwrf_common
  ByteRLE.cpp
  Common.cpp
  Config.cpp
  Decryption.cpp
  Encryption.cpp
  EncryptionSpecification.cpp
  FileMetadata.cpp
  IntEncoder.cpp
  RLEv1.cpp
  RLEv2.cpp
  Statistics.cpp
  wrap/orc-proto-wrapper.cpp
  wrap/dwrf-proto-wrapper.cpp
)

velox_link_libraries(
  velox_dwio_dwrf_common
  velox_common_base
  velox_common_compression
  velox_common_config
  velox_dwio_common
  velox_dwio_common_compression
  velox_caching
  Snappy::snappy
  zstd::zstd
  protobuf::libprotobuf
)

# required for the wrapped protobuf headers/sources
velox_include_directories(
  velox_dwio_dwrf_common
  PUBLIC "$<BUILD_LOCAL_INTERFACE:${PROJECT_BINARY_DIR}>"
)

if(NOT VELOX_MONO_LIBRARY)
  # trigger generation of pb files
  add_dependencies(velox_dwio_dwrf_common dwio_proto)
endif()
