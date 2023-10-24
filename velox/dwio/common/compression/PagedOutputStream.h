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

#include "velox/dwio/common/OutputStream.h"
#include "velox/dwio/common/compression/Compression.h"
#include "velox/dwio/common/compression/CompressionBufferPool.h"

namespace facebook::velox::dwio::common::compression {

class PagedOutputStream : public BufferedOutputStream {
 public:
  PagedOutputStream(
      CompressionBufferPool& pool,
      DataBufferHolder& bufferHolder,
      uint32_t compressionThreshold,
      uint8_t pageHeaderSize,
      std::unique_ptr<Compressor> compressor,
      const dwio::common::encryption::Encrypter* encryptor)
      : BufferedOutputStream(bufferHolder),
        pool_{&pool},
        compressor_{std::move(compressor)},
        encryptor_{encryptor},
        threshold_{compressionThreshold},
        pageHeaderSize_{pageHeaderSize} {
    VELOX_CHECK(
        compressor_ || encryptor_,
        "Neither compressor or encryptor is set for paged output stream");
  }

  bool Next(void** data, int32_t* size, uint64_t increment) override;

  uint64_t flush() override;

  uint64_t size() const override {
    // only care about compressed size
    return bufferHolder_.size();
  }

  void BackUp(int32_t count) override;

  std::string getName() const override {
    return "paged output stream";
  }

  void recordPosition(
      PositionRecorder& recorder,
      int32_t bufferLength,
      int32_t bufferOffset,
      int32_t strideIndex = -1) const override;

 private:
  // create page using compressor and encryptor
  std::vector<folly::StringPiece> createPage();

  void writeHeader(char* buffer, size_t compressedSize, bool original);

  void updateSize(char* buffer, size_t compressedSize);

  void resetBuffers();

  CompressionBufferPool* const pool_;

  const std::unique_ptr<Compressor> compressor_;

  // Encryption provider
  const dwio::common::encryption::Encrypter* const encryptor_;

  // threshold below which, we skip compression
  const uint32_t threshold_;

  const uint8_t pageHeaderSize_;

  // Buffer to hold compressed data
  std::unique_ptr<dwio::common::DataBuffer<char>> compressionBuffer_{nullptr};

  // buffer that holds encrypted data
  std::unique_ptr<folly::IOBuf> encryptionBuffer_{nullptr};
};

} // namespace facebook::velox::dwio::common::compression
