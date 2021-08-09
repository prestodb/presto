/*
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

#include "velox/dwio/dwrf/common/DataBufferHolder.h"

namespace facebook::velox::dwrf {

bool DataBufferHolder::tryResize(
    dwio::common::DataBuffer<char>& buffer,
    uint64_t headerSize,
    uint64_t increment) const {
  auto size = buffer.size();
  // make sure size is at least header size
  if (LIKELY(size >= headerSize)) {
    size -= headerSize;
  } else {
    DWIO_ENSURE_EQ(size, 0);
  }

  DWIO_ENSURE_LE(size, maxSize_);
  // if already at max size, return
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

} // namespace facebook::velox::dwrf
