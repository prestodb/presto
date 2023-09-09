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

#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/FileSink.h"

namespace facebook::velox::dwio::common {

constexpr float DEFAULT_PAGE_GROW_RATIO = 2.0f;
constexpr float MIN_PAGE_GROW_RATIO = 1.2f;

// Class that handles buffering and allocation of data buffers for
// BufferedOutputStream. When output stream is specified, content of data buffer
// is immediately written to it, instead of being buffered.
class DataBufferHolder {
 public:
  DataBufferHolder(
      memory::MemoryPool& pool,
      uint64_t maxSize,
      uint64_t initialSize = 0,
      float growRatio = DEFAULT_PAGE_GROW_RATIO,
      dwio::common::FileSink* sink = nullptr)
      : pool_{&pool},
        sink_{sink},
        maxSize_{maxSize},
        initialSize_{(initialSize > 0) ? initialSize : maxSize},
        growRatio_{growRatio} {
    VELOX_CHECK_GT(initialSize_, 0);
    VELOX_CHECK_LE(initialSize_, maxSize_);
    VELOX_CHECK_GE(growRatio_, MIN_PAGE_GROW_RATIO);
  }

  /// Takes content of the incoming data buffer. It is the caller's
  /// responsibility to resize the buffer (if required).
  void take(const std::vector<folly::StringPiece>& buffers);

  void take(folly::StringPiece buffer) {
    take(std::vector<folly::StringPiece>{buffer});
  }

  void take(const dwio::common::DataBuffer<char>& buffer) {
    take(folly::StringPiece{buffer.data(), buffer.size()});
  }

  std::vector<dwio::common::DataBuffer<char>>& getBuffers() {
    return buffers_;
  }

  void truncate(size_t newSize) {
    VELOX_CHECK_LE(newSize, size_);
    VELOX_CHECK_NULL(sink_, "Only non sink buffers can be truncated");

    size_t newCount = 0;
    size_t sizeRemaining = newSize;
    for (auto& buf : buffers_) {
      ++newCount;
      if (sizeRemaining > buf.size()) {
        sizeRemaining -= buf.size();
      } else {
        buf.resize(sizeRemaining);
        sizeRemaining = 0;
        break;
      }
    }

    while (newCount < buffers_.size()) {
      buffers_.pop_back();
    }

    size_ = newSize;
  }

  /// Spill buffered data to another data buffer
  void spill(dwio::common::DataBuffer<char>& out) const {
    VELOX_CHECK_NULL(sink_);
    out.resize(size_);
    size_t offset = 0;
    auto* const data = out.data();
    for (const auto& buf : buffers_) {
      ::memcpy(data + offset, buf.data(), buf.size());
      offset += buf.size();
    }
  }
  void reset() {
    buffers_.clear();
    size_ = 0;
    unsuppress();
  }

  bool isSuppressed() const {
    return suppressed_;
  }

  void suppress() {
    suppressed_ = true;
  }

  uint64_t size() const {
    return size_;
  }

  /// Tries to resize the buffer. Returned buffer follows below rules
  /// - size() == capacity()
  /// - size() >= min buffer size + header size
  /// - size() <= max buffer size + header size
  /// - size() increases by grow ratio until increment fits or exceeds max size
  /// Returns true when buffer size is increased
  bool tryResize(
      dwio::common::DataBuffer<char>& buffer,
      uint64_t headerSize = 0,
      uint64_t increment = 1) const;

  memory::MemoryPool& getMemoryPool() {
    return *pool_;
  }

 private:
  void unsuppress() {
    suppressed_ = false;
  }

  memory::MemoryPool* const pool_;
  dwio::common::FileSink* const sink_;
  const uint64_t maxSize_;

  // members used for controlling allocated buffer size
  const uint64_t initialSize_;
  const float growRatio_;

  // state
  uint64_t size_{0};
  std::vector<dwio::common::DataBuffer<char>> buffers_;
  bool suppressed_{false};
};

} // namespace facebook::velox::dwio::common
