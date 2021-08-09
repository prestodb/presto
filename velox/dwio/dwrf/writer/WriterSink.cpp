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

#include "velox/dwio/dwrf/writer/WriterSink.h"

namespace facebook::velox::dwrf {

void WriterSink::addBuffer(dwio::common::DataBuffer<char> buffer) {
  auto length = buffer.size();
  if (length > 0) {
    if (shouldChecksum()) {
      checksum_->update(buffer.data(), length);
    }
    if (shouldCache()) {
      if (getCurrentCacheSize() + length > maxCacheSize_) {
        exceedsLimit_ = true;
        truncateCache();
      } else {
        // capture input to the cache
        auto len = length;
        auto src = buffer.data();
        auto capacity = cacheBuffer_.capacity();
        auto size = cacheBuffer_.size();
        // resize the buffer assuming we need to use all of it
        cacheBuffer_.resize(capacity);
        while (len > 0) {
          DWIO_ENSURE_LT(size, capacity);
          auto toWrite = std::min(capacity - size, len);
          std::memcpy(cacheBuffer_.data() + size, src, toWrite);
          size += toWrite;
          if (size == capacity) {
            cacheHolder_.take(cacheBuffer_);
            size = 0;
          }
          len -= toWrite;
          src += toWrite;
        }
        cacheBuffer_.resize(size);
      }
    }
  }
  if (shouldBuffer_) {
    buffers_.push_back(std::move(buffer));
    size_ += length;
  } else {
    sink_.write(std::move(buffer));
  }
}

} // namespace facebook::velox::dwrf
