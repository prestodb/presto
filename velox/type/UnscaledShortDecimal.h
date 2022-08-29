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
#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"

#pragma once

namespace facebook::velox {

struct UnscaledShortDecimal {
 public:
  // Default required for creating vector with NULL values.
  UnscaledShortDecimal() = default;

  constexpr explicit UnscaledShortDecimal(int64_t value)
      : unscaledValue_(value) {}

  int64_t unscaledValue() const {
    return unscaledValue_;
  }

  void setUnscaledValue(const int64_t unscaledValue) {
    unscaledValue_ = unscaledValue;
  }

  bool operator==(const UnscaledShortDecimal& other) const {
    return unscaledValue_ == other.unscaledValue_;
  }

  bool operator!=(const UnscaledShortDecimal& other) const {
    return unscaledValue_ != other.unscaledValue_;
  }

  bool operator<(const UnscaledShortDecimal& other) const {
    return unscaledValue_ < other.unscaledValue_;
  }

  bool operator<=(const UnscaledShortDecimal& other) const {
    return unscaledValue_ <= other.unscaledValue_;
  }

  bool operator>(const UnscaledShortDecimal& other) const {
    return unscaledValue_ > other.unscaledValue_;
  }

  bool operator>=(const UnscaledShortDecimal& other) const {
    return unscaledValue_ >= other.unscaledValue_;
  }

  UnscaledShortDecimal operator=(int value) const {
    return UnscaledShortDecimal(static_cast<int64_t>(value));
  }

 private:
  int64_t unscaledValue_;
};

static inline UnscaledShortDecimal operator/(
    const UnscaledShortDecimal& a,
    int b) {
  VELOX_CHECK_NE(b, 0, "Divide by zero is not supported");
  return UnscaledShortDecimal(a.unscaledValue() / b);
}
} // namespace facebook::velox

namespace folly {
template <>
struct hasher<::facebook::velox::UnscaledShortDecimal> {
  size_t operator()(
      const ::facebook::velox::UnscaledShortDecimal& value) const {
    return std::hash<int64_t>{}(value.unscaledValue());
  }
};
} // namespace folly

namespace std {

// Required for STL containers like unordered_map.
template <>
struct hash<facebook::velox::UnscaledShortDecimal> {
  size_t operator()(const facebook::velox::UnscaledShortDecimal& val) const {
    return hash<int64_t>()(val.unscaledValue());
  }
};

template <>
class numeric_limits<facebook::velox::UnscaledShortDecimal> {
 public:
  static facebook::velox::UnscaledShortDecimal min() {
    return facebook::velox::UnscaledShortDecimal(
        std::numeric_limits<int64_t>::min());
  }
  static facebook::velox::UnscaledShortDecimal max() {
    return facebook::velox::UnscaledShortDecimal(
        std::numeric_limits<int64_t>::max());
  }
};
} // namespace std
