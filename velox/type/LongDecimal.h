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
#include <folly/dynamic.h>
#include <sstream>
#include <string>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"

#pragma once

namespace facebook::velox {

using int128_t = __int128_t;

constexpr int128_t buildInt128(uint64_t hi, uint64_t lo) {
  // GCC does not allow left shift negative value.
  return (static_cast<__uint128_t>(hi) << 64) | lo;
}

struct LongDecimal {
 public:
  // Default required for creating vector with NULL values.
  LongDecimal() = default;
  constexpr explicit LongDecimal(int128_t value) : unscaledValue_(value) {}

  int128_t unscaledValue() const {
    return unscaledValue_;
  }

  void setUnscaledValue(const int128_t& unscaledValue) {
    unscaledValue_ = unscaledValue;
  }

  bool operator==(const LongDecimal& other) const {
    return unscaledValue_ == other.unscaledValue_;
  }

  bool operator!=(const LongDecimal& other) const {
    return unscaledValue_ != other.unscaledValue_;
  }

  bool operator<(const LongDecimal& other) const {
    return unscaledValue_ < other.unscaledValue_;
  }

  bool operator<=(const LongDecimal& other) const {
    return unscaledValue_ <= other.unscaledValue_;
  }

 private:
  int128_t unscaledValue_;
}; // struct LongDecimal
} // namespace facebook::velox

namespace folly {
template <>
struct hasher<::facebook::velox::LongDecimal> {
  size_t operator()(const ::facebook::velox::LongDecimal& value) const {
    auto upperHash = folly::hasher<uint64_t>{}(
        static_cast<uint64_t>(value.unscaledValue() >> 64));
    auto lowerHash =
        folly::hasher<uint64_t>{}(static_cast<uint64_t>(value.unscaledValue()));
    return facebook::velox::bits::hashMix(upperHash, lowerHash);
  }
};
} // namespace folly

namespace std {
string to_string(facebook::velox::int128_t x);
} // namespace std
