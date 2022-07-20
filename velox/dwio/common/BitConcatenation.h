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

#include "velox/buffer/Buffer.h"

namespace facebook::velox::dwio::common {

/// Concatenates multiple bit vectors and exposes the result in a
/// single Buffer. Creates and resizes the result Buffer as needed. If
/// only one bits were added, sets the output buffer to nullptr.
class BitConcatenation {
 public:
  explicit BitConcatenation(memory::MemoryPool& pool) : pool_(pool) {}

  /// Prepares to concatenate bits given to append() or appendOnes()
  /// into 'buffer'. 'buffer' is allocated and resized as needed. If
  /// 'buffer' is initially nullptr and only ones are appended, buffer
  /// may stay nullptr. The size() of 'buffer' is set to the next byte,
  /// so the caller must  use numBits() to get the bit count.
  void reset(BufferPtr& outBuffer) {
    buffer_ = &outBuffer;
    numBits_ = 0;
    hasZeros_ = false;
  }

  /// Appends  'bits' between bit offset 'begin' and 'end' to the result.
  /// A nullptr 'bits' is treated as a bit range with all bits set.
  void append(const uint64_t* FOLLY_NULLABLE bits, int32_t begin, int32_t end);

  /// Appends 'numOnes' ones.
  void appendOnes(int32_t numOnes);

  // Returns 'buffer_' or nullptr if only ones have been appended.
  BufferPtr buffer() const {
    return hasZeros_ ? *buffer_ : nullptr;
  }

  int32_t numBits() const {
    return numBits_;
  }

 private:
  // Allocates or reallocates '*buffer' to have space for 'numBits_ + newBits'
  // bits. Retuns a pointer to the first word of 'buffer_'.
  uint64_t* FOLLY_NONNULL ensureSpace(int32_t newBits);

  void setSize() {
    if (*buffer_) {
      (*buffer_)->setSize(bits::roundUp(numBits_, 8) / 8);
    }
  }

  memory::MemoryPool& pool_;
  BufferPtr* FOLLY_NULLABLE buffer_{nullptr};
  int32_t numBits_{0};
  bool hasZeros_{false};
};

} // namespace facebook::velox::dwio::common
