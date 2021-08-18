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

#include "velox/dwio/dwrf/common/PagedOutputStream.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwrf {

std::vector<folly::StringPiece> PagedOutputStream::createPage() {
  auto origSize = buffer_.size();
  DWIO_ENSURE_GT(origSize, PAGE_HEADER_SIZE);
  origSize -= PAGE_HEADER_SIZE;

  auto compressedSize = origSize;
  // apply compressoin if there is compressor and original data size exceeds
  // threshold
  if (compressor_ && origSize >= threshold_) {
    compressionBuffer_ = pool_.getBuffer(buffer_.size());
    compressedSize = compressor_->compress(
        buffer_.data() + PAGE_HEADER_SIZE,
        compressionBuffer_->data() + PAGE_HEADER_SIZE,
        origSize);
  }

  folly::StringPiece compressed;
  if (compressedSize >= origSize) {
    // write orig
    writeHeader(buffer_.data(), origSize, true);
    compressed =
        folly::StringPiece(buffer_.data(), origSize + PAGE_HEADER_SIZE);
  } else {
    // write compressed
    writeHeader(compressionBuffer_->data(), compressedSize, false);
    compressed = folly::StringPiece(
        compressionBuffer_->data(), compressedSize + PAGE_HEADER_SIZE);
  }

  if (!encrypter_) {
    return {compressed};
  }

  encryptionBuffer_ = encrypter_->encrypt(folly::StringPiece(
      compressed.begin() + PAGE_HEADER_SIZE, compressed.end()));
  updateSize(
      const_cast<char*>(compressed.begin()), encryptionBuffer_->length());
  return {
      folly::StringPiece(compressed.begin(), PAGE_HEADER_SIZE),
      folly::StringPiece(
          reinterpret_cast<const char*>(encryptionBuffer_->data()),
          encryptionBuffer_->length())};
}

void PagedOutputStream::writeHeader(
    char* buffer,
    size_t compressedSize,
    bool original) {
  DWIO_ENSURE_LT(compressedSize, 1 << 23);
  buffer[0] = static_cast<char>((compressedSize << 1) + (original ? 1 : 0));
  buffer[1] = static_cast<char>(compressedSize >> 7);
  buffer[2] = static_cast<char>(compressedSize >> 15);
}

void PagedOutputStream::updateSize(char* buffer, size_t compressedSize) {
  DWIO_ENSURE_LT(compressedSize, 1 << 23);
  buffer[0] = ((buffer[0] & 0x01) | static_cast<char>(compressedSize << 1));
  buffer[1] = static_cast<char>(compressedSize >> 7);
  buffer[2] = static_cast<char>(compressedSize >> 15);
}

void PagedOutputStream::resetBuffers() {
  // reset compression buffer size and return
  if (compressionBuffer_) {
    pool_.returnBuffer(std::move(compressionBuffer_));
  }
  encryptionBuffer_ = nullptr;
}

uint64_t PagedOutputStream::flush() {
  auto size = buffer_.size();
  auto originalSize = bufferHolder_.size();
  if (size > PAGE_HEADER_SIZE) {
    bufferHolder_.take(createPage());
    resetBuffers();
    // reset input buffer
    buffer_.resize(PAGE_HEADER_SIZE);
  }
  return bufferHolder_.size() - originalSize;
}

void PagedOutputStream::BackUp(int32_t count) {
  if (count > 0) {
    DWIO_ENSURE_GE(buffer_.size(), count + PAGE_HEADER_SIZE);
    BufferedOutputStream::BackUp(count);
  }
}

bool PagedOutputStream::Next(void** data, int32_t* size, uint64_t increment) {
  if (!tryResize(data, size, PAGE_HEADER_SIZE, increment)) {
    flushAndReset(data, size, PAGE_HEADER_SIZE, createPage());
    resetBuffers();
  }
  return true;
}

void PagedOutputStream::recordPosition(
    PositionRecorder& recorder,
    int32_t bufferLength,
    int32_t bufferOffset,
    int32_t strideOffset) const {
  // add compressed size, then uncompressed
  recorder.add(bufferHolder_.size(), strideOffset);
  auto size = buffer_.size();
  if (size) {
    size -= (PAGE_HEADER_SIZE + bufferLength - bufferOffset);
  }
  recorder.add(size, strideOffset);
}

} // namespace facebook::velox::dwrf
