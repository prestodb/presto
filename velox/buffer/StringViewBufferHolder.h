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

#include "velox/buffer/Buffer.h"
#include "velox/type/StringView.h"

namespace facebook::velox {

/// This class allows converting StringViews into memory-owned StringViews such
/// that either this StringViewBufferHolder holds the necessary memory after
/// copying, or the StringView is self contained. The main purpose is provide
/// the caller a safe way of storing StringViews from an external source while
/// maintaining appropriate ownership to memory.
class StringViewBufferHolder {
 public:
  explicit StringViewBufferHolder(velox::memory::MemoryPool* pool)
      : pool_(pool) {}

  /// Return a copy of the StringView where the StringView is copied to this
  /// StringViewBufferHolder if the StringView is not inlined. std::string and
  /// folly::StringPiece are also copied to the internal buffers (see the
  /// specializations below).
  ///
  /// NOTE: Out of convenience, we allow different types to be passed in, but
  /// just don't store them, so that the client does need to have this check.
  template <typename T>
  T getOwnedValue(T value) {
    if constexpr (std::is_same_v<T, StringView>) {
      return getOwnedStringView(std::move(value));
    } else {
      return std::move(value);
    }
  }

  /// Specialization for std::string type.
  StringView getOwnedValue(const std::string& value) {
    return getOwnedStringView(value.data(), value.size());
  }

  /// Specialization for folly::StringPiece type.
  StringView getOwnedValue(folly::StringPiece value) {
    return getOwnedStringView(value.data(), value.size());
  }

  /// Get all the buffers used for storing memory used by StringViews returned
  /// by getOwnedValue.
  std::vector<BufferPtr> moveBuffers() {
    auto result = std::move(stringBuffers_);
    stringBuffers_.clear();
    return result;
  }

  /// Return a copy of the vector of buffer pointers used for storing memory
  /// used in StringViews returned by getOwnedValue.  The underlying buffer
  /// pointers remain shared between this holder and the resulting collection.
  std::vector<BufferPtr> buffers() const {
    return stringBuffers_;
  }

 private:
  StringView getOwnedStringView(StringView stringView);
  StringView getOwnedStringView(const char* data, int32_t size);

  static constexpr int32_t kInitialStringReservation{1024 * 8};

  std::vector<BufferPtr> stringBuffers_;
  velox::memory::MemoryPool* pool_;
};

} // namespace facebook::velox
