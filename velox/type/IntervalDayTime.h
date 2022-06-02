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

#include <folly/dynamic.h>
#include <iomanip>
#include <sstream>
#include <string>
#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"

namespace facebook::velox {

constexpr long kMillisInSecond = 1000;
constexpr long kMillisInMinute = 60 * kMillisInSecond;
constexpr long kMillisInHour = 60 * kMillisInMinute;
constexpr long kMillisInDay = 24 * kMillisInHour;

struct IntervalDayTime {
 public:
  constexpr IntervalDayTime() : milliseconds_(0) {}
  explicit constexpr IntervalDayTime(int64_t milliseconds)
      : milliseconds_(milliseconds) {}

  int64_t milliseconds() const {
    return milliseconds_;
  }

  /// Returns number of whole days in the interval.
  int64_t days() const;

  /// Returns true if contains whole days only.
  bool hasWholeDays() const;

  bool operator==(const IntervalDayTime& other) const {
    return milliseconds_ == other.milliseconds_;
  }

  bool operator!=(const IntervalDayTime& other) const {
    return milliseconds_ != other.milliseconds_;
  }

  bool operator<(const IntervalDayTime& other) const {
    return milliseconds_ < other.milliseconds_;
  }

  bool operator<=(const IntervalDayTime& other) const {
    return milliseconds_ <= other.milliseconds_;
  }

  bool operator>(const IntervalDayTime& other) const {
    return milliseconds_ > other.milliseconds_;
  }

  bool operator>=(const IntervalDayTime& other) const {
    return milliseconds_ >= other.milliseconds_;
  }

  /// Needed for serialization of FlatVector<IntervalDayTime>
  operator StringView() const {VELOX_NYI()}

  std::string toString() const;

  operator std::string() const {
    return toString();
  }

  operator folly::dynamic() const {
    return folly::dynamic(milliseconds_);
  }

 private:
  /// Number of milliseconds in the interval.
  int64_t milliseconds_;
};

void parseTo(folly::StringPiece in, ::facebook::velox::IntervalDayTime& out);

template <typename T>
void toAppend(const ::facebook::velox::IntervalDayTime& value, T* result) {
  result->append(value.toString());
}

} // namespace facebook::velox

namespace std {
template <>
struct hash<::facebook::velox::IntervalDayTime> {
  size_t operator()(const ::facebook::velox::IntervalDayTime& value) const {
    return std::hash<int64_t>{}(value.milliseconds());
  }
};

std::string to_string(const ::facebook::velox::IntervalDayTime& ts);

} // namespace std

namespace folly {
template <>
struct hasher<::facebook::velox::IntervalDayTime> {
  size_t operator()(const ::facebook::velox::IntervalDayTime& value) const {
    return std::hash<int64_t>{}(value.milliseconds());
  }
};

} // namespace folly
