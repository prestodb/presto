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

#include <assert.h>
#include <stdint.h>
#include "velox/experimental/wave/common/CompilerDefines.h"

namespace facebook::velox::wave {

WAVE_DEVICE_HOST inline int
stringview_memcmp(const void* lhs, const void* rhs, size_t n) {
  auto* a = reinterpret_cast<const unsigned char*>(lhs);
  auto* b = reinterpret_cast<const unsigned char*>(rhs);
  for (size_t i = 0; i < n; ++i) {
    if (int c = (int)a[i] - (int)b[i]) {
      return c;
    }
  }
  return 0;
}

class StringView {
 public:
  WAVE_DEVICE_HOST void init(const char* data, int32_t len) {
    data_ = len;
    if (len == 0) {
      return;
    }
    assert(len > 0);
    if (len <= kInlineSize) {
      memcpy(inlineData(), data, len);
    } else {
      assert(len <= kMaxSize);
      assert(!((uintptr_t)data >> (64 - kSizeBits)));
      data_ |= reinterpret_cast<uintptr_t>(data) << kSizeBits;
    }
  }

  WAVE_DEVICE_HOST uint16_t size() const {
    return data_ & kMaxSize;
  }

  WAVE_DEVICE_HOST bool isInline() const {
    return size() <= kInlineSize;
  }

  WAVE_DEVICE_HOST const char* data() const {
    if (isInline()) {
      return const_cast<StringView*>(this)->inlineData();
    }
    return reinterpret_cast<const char*>(data_ >> kSizeBits);
  }

  WAVE_DEVICE_HOST bool operator==(StringView other) const {
    if (isInline()) {
      return data_ == other.data_;
    }
    auto len = size();
    return len == other.size() &&
        stringview_memcmp(data(), other.data(), len) == 0;
  }

  WAVE_DEVICE_HOST bool operator!=(StringView other) const {
    return !(*this == other);
  }

#ifdef __NVCC__
  __device__ StringView cas(StringView compare, StringView val);
#endif
#if !defined(__NVCC__) && !defined(__CUDACC_RTC__)
  operator std::string_view() const {
    return {data(), size()};
  }
#endif

 private:
  WAVE_DEVICE_HOST char* inlineData() {
    return reinterpret_cast<char*>(&data_) + kSizeBits / 8;
  }

  static constexpr int kSizeBits = 16;
  static constexpr uint64_t kMaxSize = (1ull << kSizeBits) - 1;
  static constexpr int kInlineSize = 8 - kSizeBits / 8;

  unsigned long long data_;
};

// Non-trivial class does not play well in device code.
#ifndef __CUDACC_RTC__
static_assert(std::is_trivial_v<StringView>);
#endif
// Ensure StringView is 64 bits so we can do atomic operations on it.
static_assert(sizeof(StringView) == 8);

} // namespace facebook::velox::wave
