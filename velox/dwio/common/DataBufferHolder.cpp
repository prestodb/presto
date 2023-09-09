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

#include "velox/dwio/common/DataBufferHolder.h"
#include "velox/common/testutil/TestValue.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::dwio::common {

void DataBufferHolder::take(const std::vector<folly::StringPiece>& buffers) {
  // compute size
  uint64_t totalSize = 0;
  for (auto& buf : buffers) {
    totalSize += buf.size();
  }
  if (totalSize == 0) {
    return;
  }

  TestValue::adjust(
      "facebook::velox::dwio::common::DataBufferHolder::take", pool_);

  dwio::common::DataBuffer<char> buf(*pool_, totalSize);
  auto* data = buf.data();
  for (auto& buffer : buffers) {
    const auto size = buffer.size();
    ::memcpy(data, buffer.begin(), size);
    data += size;
  }
  // If possibly, write content of the data to output immediately. Otherwise,
  // make a copy and add it to buffer list
  if (sink_ != nullptr) {
    sink_->write(std::move(buf));
  } else {
    buffers_.push_back(std::move(buf));
  }
  size_ += totalSize;
}

bool DataBufferHolder::tryResize(
    dwio::common::DataBuffer<char>& buffer,
    uint64_t headerSize,
    uint64_t increment) const {
  auto size = buffer.size();
  // Makes sure size is at least header size
  if (FOLLY_LIKELY(size >= headerSize)) {
    size -= headerSize;
  } else {
    VELOX_CHECK_EQ(size, 0);
  }

  VELOX_CHECK_LE(size, maxSize_);
  // If already at max size, return.
  if (size == maxSize_) {
    return false;
  }

  // make sure size is at least same as initial size
  auto targetSize = size + increment;
  size = std::max(initialSize_, size);
  while (size < targetSize) {
    size = static_cast<uint64_t>(size * growRatio_);
    if (size >= maxSize_) {
      size = maxSize_;
      break;
    }
  }

  // make sure size is at least same as capacity
  size = std::max(size + headerSize, buffer.capacity());
  buffer.resize(size);
  return true;
}
} // namespace facebook::velox::dwio::common
