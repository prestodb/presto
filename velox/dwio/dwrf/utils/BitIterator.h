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

#pragma once

#include <vector>

namespace facebook::velox::dwrf::utils {
// Assumptions:
// 1) all raw byte buffers are added at the beginning.
//    If a raw byte buffer is added mid flight, we'll start iterating
//    at the same bit offset as all others.
// 2) caller is responsible for passing in byte buffers that are
//    big enough. This class doesn't do any boundary checking.
template <typename T>
class BulkBitIterator {
  static constexpr uint8_t kWidth = sizeof(T) * 8;

 public:
  explicit BulkBitIterator() : mod_{0}, index_{0} {}

  void addRawByteBuffer(T* buf) {
    rawByteBuffers_.push_back(buf);
    byteCaches_.push_back(0);
  }

  // Caller needs to always call loadNext() before accessing
  // value.
  void loadNext() {
    if (mod_ == 0) {
      // Refresh the byte caches.
      for (size_t i = 0; i < rawByteBuffers_.size(); ++i) {
        byteCaches_[i] = rawByteBuffers_[i][index_];
      }
      ++index_;
    }
    mask_ = static_cast<T>(1) << mod_;
    // This is actually mod_ for the next value. We do this
    // so that loadNext() would not try to load something out
    // of range.
    mod_ = (mod_ + 1) % kWidth;
  }

  bool hasValueAt(size_t index) {
    return byteCaches_[index] & mask_;
  }

 private:
  uint8_t mod_;
  int32_t index_;
  std::vector<T*> rawByteBuffers_;
  std::vector<T> byteCaches_;
  T mask_;
};

} // namespace facebook::velox::dwrf::utils
