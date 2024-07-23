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

velox_add_library(
  velox_dwio_arrow_parquet_writer_util_lib
  Compression.cpp
  CompressionSnappy.cpp
  CompressionZstd.cpp
  CompressionZlib.cpp
  CompressionLZ4.cpp
  Hashing.cpp
  Crc32.cpp)

velox_link_libraries(
  velox_dwio_arrow_parquet_writer_util_lib
  arrow
  Snappy::snappy
  zstd::zstd
  ZLIB::ZLIB
  lz4::lz4)
