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

#include "velox/dwio/common/compression/Compression.h"

namespace facebook::velox::dwio::common::compression {

class PagedOutputStream : public BufferedOutputStream {
 public:
  PagedOutputStream(
      CompressionBufferPool& pool,
      DataBufferHolder& bufferHolder,
      uint32_t compressionThreshold,
      uint8_t pageHeaderSize,
      std::unique_ptr<Compressor> compressor,
      const dwio::common::encryption::Encrypter* encrypter)
      : BufferedOutputStream(bufferHolder),
        pool_{pool},
        compressor_{std::move(compressor)},
        encrypter_{encrypter},
        threshold_{compressionThreshold},
        pageHeaderSize_{pageHeaderSize} {
    DWIO_ENSURE(compressor_ || encrypter_, "invalid paged output stream");
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
      int32_t strideOffset = -1) const override;

 private:
  // create page using compressor and encrypter
  std::vector<folly::StringPiece> createPage();

  void writeHeader(char* buffer, size_t compressedSize, bool original);

  void updateSize(char* buffer, size_t compressedSize);

  void resetBuffers();

  CompressionBufferPool& pool_;

  std::unique_ptr<Compressor> compressor_;

  // Buffer to hold compressed data
  std::unique_ptr<dwio::common::DataBuffer<char>> compressionBuffer_{nullptr};

  // buffer that holds encrypted data
  std::unique_ptr<folly::IOBuf> encryptionBuffer_{nullptr};

  // Encryption provider
  const dwio::common::encryption::Encrypter* encrypter_;

  // threshold below which, we skip compression
  uint32_t threshold_;

  uint8_t pageHeaderSize_;
};

} // namespace facebook::velox::dwio::common::compression
