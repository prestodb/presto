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

#include "velox/dwio/common/OutputStream.h"

namespace facebook::velox::dwio::common {

bool BufferedOutputStream::Next(
    void** buffer,
    int32_t* size,
    uint64_t increment) {
  // Try resize the buffer, if failed, flush it to make room
  if (!tryResize(buffer, size, 0, increment)) {
    flushAndReset(
        buffer, size, 0, {folly::Range(buffer_.data(), buffer_.size())});
  }
  return true;
}

void BufferedOutputStream::BackUp(int32_t count) {
  if (count > 0) {
    uint64_t unsignedCount = static_cast<uint64_t>(count);
    VELOX_CHECK_LE(unsignedCount, buffer_.size(), "Can't backup that much!");
    buffer_.resize(buffer_.size() - unsignedCount);
  }
}

uint64_t BufferedOutputStream::flush() {
  const uint64_t dataSize = buffer_.size();
  if (dataSize > 0) {
    bufferHolder_.take(folly::Range(buffer_.data(), buffer_.size()));
    // resize to 0 is needed to make sure size() returns correct value
    buffer_.resize(0);
  }
  return dataSize;
}

void AppendOnlyBufferedStream::write(const char* data, size_t size) {
  while (size > 0) {
    if (FOLLY_UNLIKELY(bufferOffset_ == bufferLength_)) {
      VELOX_CHECK(
          outStream_->Next(
              reinterpret_cast<void**>(&buffer_), &bufferLength_, size),
          "Failed to allocate buffer.");
      bufferOffset_ = 0;
    }
    const size_t len =
        std::min(static_cast<size_t>(bufferLength_ - bufferOffset_), size);
    ::memcpy(buffer_ + bufferOffset_, data, len);
    bufferOffset_ += static_cast<int32_t>(len);
    data += len;
    size -= len;
  }
}

uint64_t AppendOnlyBufferedStream::flush() {
  outStream_->BackUp(bufferLength_ - bufferOffset_);
  bufferOffset_ = 0;
  bufferLength_ = 0;
  buffer_ = nullptr;
  return outStream_->flush();
}

} // namespace facebook::velox::dwio::common
