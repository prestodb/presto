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

struct ShortDecimal {
 public:
  // Default required for creating vector with NULL values.
  ShortDecimal() = default;
  constexpr explicit ShortDecimal(int64_t value) : unscaledValue_(value) {}

  int64_t unscaledValue() const {
    return unscaledValue_;
  }

  void setUnscaledValue(const int64_t unscaledValue) {
    unscaledValue_ = unscaledValue;
  }

  bool operator==(const ShortDecimal& other) const {
    return unscaledValue_ == other.unscaledValue_;
  }

  bool operator!=(const ShortDecimal& other) const {
    return unscaledValue_ != other.unscaledValue_;
  }

  bool operator<(const ShortDecimal& other) const {
    return unscaledValue_ < other.unscaledValue_;
  }

  bool operator<=(const ShortDecimal& other) const {
    return unscaledValue_ <= other.unscaledValue_;
  }

  bool operator>(const ShortDecimal& other) const {
    return unscaledValue_ > other.unscaledValue_;
  }

  bool operator>=(const ShortDecimal& other) const {
    return unscaledValue_ >= other.unscaledValue_;
  }

 private:
  int64_t unscaledValue_;
};
} // namespace facebook::velox

namespace folly {
template <>
struct hasher<::facebook::velox::ShortDecimal> {
  size_t operator()(const ::facebook::velox::ShortDecimal& value) const {
    return std::hash<int64_t>{}(value.unscaledValue());
  }
};
} // namespace folly
