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

#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"

#include <iomanip>
#include <sstream>
#include <string>

#include <folly/dynamic.h>

namespace facebook::velox {

struct Date {
 public:
  constexpr Date() : days_(0) {}
  constexpr Date(int32_t days) : days_(days) {}

  int32_t days() const {
    return days_;
  }

  void addDays(int32_t days) {
    days_ += days;
  }

  bool operator==(const Date& other) const {
    return days_ == other.days_;
  }

  bool operator!=(const Date& other) const {
    return days_ != other.days_;
  }

  bool operator<(const Date& other) const {
    return days_ < other.days_;
  }

  bool operator<=(const Date& other) const {
    return days_ <= other.days_;
  }

  bool operator>(const Date& other) const {
    return days_ > other.days_;
  }

  bool operator>=(const Date& other) const {
    return days_ >= other.days_;
  }

  // Needed for serialization of FlatVector<Date>
  operator StringView() const {VELOX_NYI()}

  std::string toString() const;

  operator std::string() const {
    return toString();
  }

  operator folly::dynamic() const {
    return folly::dynamic(days_);
  }

 private:
  // Number of days since the epoch ( 1970-01-01).
  int32_t days_;
};

void parseTo(folly::StringPiece in, ::facebook::velox::Date& out);

template <typename T>
void toAppend(const ::facebook::velox::Date& value, T* result) {
  result->append(value.toString());
}

} // namespace facebook::velox

namespace std {
template <>
struct hash<::facebook::velox::Date> {
  size_t operator()(const ::facebook::velox::Date& value) const {
    return std::hash<int32_t>{}(value.days());
  }
};

std::string to_string(const ::facebook::velox::Date& ts);

} // namespace std

template <>
struct fmt::formatter<facebook::velox::Date> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const facebook::velox::Date& d, FormatContext& ctx) {
    return fmt::format_to(ctx.out(), "{}", std::to_string(d));
  }
};

namespace folly {
template <>
struct hasher<::facebook::velox::Date> {
  size_t operator()(const ::facebook::velox::Date& value) const {
    return std::hash<int32_t>{}(value.days());
  }
};

} // namespace folly
