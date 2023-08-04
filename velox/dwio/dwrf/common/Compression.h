/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/common/compression/Compression.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/compression/Compression.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/common/Decryption.h"
#include "velox/dwio/dwrf/common/Encryption.h"

namespace facebook::velox::dwrf {

constexpr uint8_t PAGE_HEADER_SIZE = 3;

/**
 * Create a compressor for the given compression kind.
 * @param kind the compression type to implement
 * @param bufferPool pool for compression buffer
 * @param bufferHolder buffer holder that handles buffer allocation and
 * collection
 * @param level compression level
 */
static std::unique_ptr<dwio::common::BufferedOutputStream> createCompressor(
    common::CompressionKind kind,
    dwio::common::compression::CompressionBufferPool& bufferPool,
    dwio::common::DataBufferHolder& bufferHolder,
    const Config& config,
    const dwio::common::encryption::Encrypter* encrypter = nullptr) {
  return dwio::common::compression::createCompressor(
      kind,
      bufferPool,
      bufferHolder,
      config.get(Config::COMPRESSION_THRESHOLD),
      config.get(Config::ZLIB_COMPRESSION_LEVEL),
      config.get(Config::ZSTD_COMPRESSION_LEVEL),
      PAGE_HEADER_SIZE,
      encrypter);
}

} // namespace facebook::velox::dwrf
