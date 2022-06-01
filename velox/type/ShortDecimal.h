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
  explicit ShortDecimal(int64_t value) : unscaledValue_(value) {}

  int64_t unscaledValue() const {
    return unscaledValue_;
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

  // Needed for serialization of FlatVector<ShortDecimal>
  operator StringView() const {
    VELOX_NYI();
  }

  std::string toString() const;

  operator std::string() const {
    return toString();
  }

  operator folly::dynamic() const {
    return folly::dynamic(unscaledValue_);
  }

 private:
  const int64_t unscaledValue_;
};
} // namespace facebook::velox

namespace std {
template <>
struct hash<::facebook::velox::ShortDecimal> {
  size_t operator()(const ::facebook::velox::ShortDecimal& value) const {
    return std::hash<int64_t>{}(value.unscaledValue());
  }
};

std::string to_string(const ::facebook::velox::ShortDecimal& ts);

} // namespace std

namespace folly {
template <>
struct hasher<::facebook::velox::ShortDecimal> {
  size_t operator()(const ::facebook::velox::ShortDecimal& value) const {
    return std::hash<int64_t>{}(value.unscaledValue());
  }
};
} // namespace folly
