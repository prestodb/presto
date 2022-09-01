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

#include "velox/type/StringView.h"

#include <iomanip>
#include <sstream>
#include <string>

#include <folly/dynamic.h>

namespace date {
class time_zone;
}

namespace facebook::velox {

struct Timestamp {
 public:
  constexpr Timestamp() : seconds_(0), nanos_(0) {}
  constexpr Timestamp(int64_t seconds, uint64_t nanos)
      : seconds_(seconds), nanos_(nanos) {}

  int64_t getSeconds() const {
    return seconds_;
  }

  uint64_t getNanos() const {
    return nanos_;
  }

  int64_t toNanos() const {
    // int64 can store around 292 years in nanos ~ till 2262-04-12
    return seconds_ * 1'000'000'000 + nanos_;
  }

  int64_t toMillis() const {
    return seconds_ * 1'000 + nanos_ / 1'000'000;
  }

  int64_t toMicros() const {
    return seconds_ * 1'000'000 + nanos_ / 1'000;
  }

  static Timestamp fromMillis(int64_t millis) {
    if (millis >= 0 || millis % 1'000 == 0) {
      return Timestamp(millis / 1'000, (millis % 1'000) * 1'000'000);
    }
    auto second = millis / 1'000 - 1;
    auto nano = ((millis - second * 1'000) % 1'000) * 1'000'000;
    return Timestamp(second, nano);
  }

  static Timestamp fromMicros(int64_t micros) {
    if (micros >= 0 || micros % 1'000'000 == 0) {
      return Timestamp(micros / 1'000'000, (micros % 1'000'000) * 1'000);
    }
    auto second = micros / 1'000'000 - 1;
    auto nano = ((micros - second * 1'000'000) % 1'000'000) * 1'000;
    return Timestamp(second, nano);
  }

  static Timestamp fromNanos(int64_t nanos) {
    if (nanos >= 0 || nanos % 1'000'000'000 == 0) {
      return Timestamp(nanos / 1'000'000'000, nanos % 1'000'000'000);
    }
    auto second = nanos / 1'000'000'000 - 1;
    auto nano = (nanos - second * 1'000'000'000) % 1'000'000'000;
    return Timestamp(second, nano);
  }

  // Assuming the timestamp represents a time at zone, converts it to the GMT
  // time at the same moment.
  // Example: Timestamp ts{0, 0};
  // ts.Timezone("America/Los_Angeles");
  // ts.toString() returns January 1, 1970 08:00:00
  void toGMT(const date::time_zone& zone);

  // Same as above, but accepts PrestoDB time zone ID.
  void toGMT(int16_t tzID);

  // Assuming the timestamp represents a GMT time, converts it to the time at
  // the same moment at zone.
  // Example: Timestamp ts{0, 0};
  // ts.Timezone("America/Los_Angeles");
  // ts.toString() returns December 31, 1969 16:00:00
  void toTimezone(const date::time_zone& zone);

  // Same as above, but accepts PrestoDB time zone ID.
  void toTimezone(int16_t tzID);

  bool operator==(const Timestamp& b) const {
    return seconds_ == b.seconds_ && nanos_ == b.nanos_;
  }

  bool operator!=(const Timestamp& b) const {
    return seconds_ != b.seconds_ || nanos_ != b.nanos_;
  }

  bool operator<(const Timestamp& b) const {
    return seconds_ < b.seconds_ ||
        (seconds_ == b.seconds_ && nanos_ < b.nanos_);
  }

  bool operator<=(const Timestamp& b) const {
    return seconds_ < b.seconds_ ||
        (seconds_ == b.seconds_ && nanos_ <= b.nanos_);
  }

  bool operator>(const Timestamp& b) const {
    return seconds_ > b.seconds_ ||
        (seconds_ == b.seconds_ && nanos_ > b.nanos_);
  }

  bool operator>=(const Timestamp& b) const {
    return seconds_ > b.seconds_ ||
        (seconds_ == b.seconds_ && nanos_ >= b.nanos_);
  }

  // Needed for serialization of FlatVector<Timestamp>
  operator StringView() const {
    return StringView("TODO: Implement");
  };

  std::string toString() const {
    // mbasmanova: error: no matching function for call to 'gmtime_r'
    // mbasmanova: time_t is long not long long
    // struct tm tmValue;
    // auto bt = gmtime_r(&seconds_, &tmValue);
    auto bt = gmtime((const time_t*)&seconds_);
    if (!bt) {
      const auto& error_message = folly::to<std::string>(
          "Can't convert Seconds to time: ", folly::to<std::string>(seconds_));
      throw std::runtime_error{error_message};
    }

    // return ISO 8601 time format.
    // %F - equivalent to "%Y-%m-%d" (the ISO 8601 date format)
    // T - literal T
    // %T - equivalent to "%H:%M:%S" (the ISO 8601 time format)
    // so this return time in the format
    // %Y-%m-%dT%H:%M:%S.nnnnnnnnn
    // Note there is no Z suffix, which denotes UTC timestamp.
    std::ostringstream oss;
    oss << std::put_time(bt, "%FT%T");
    oss << '.' << std::setfill('0') << std::setw(9) << nanos_;

    return oss.str();
  }

  operator std::string() const {
    return toString();
  }

  operator folly::dynamic() const {
    return folly::dynamic(seconds_);
  }

 private:
  int64_t seconds_;
  uint64_t nanos_;
};

void parseTo(folly::StringPiece in, ::facebook::velox::Timestamp& out);

template <typename T>
void toAppend(const ::facebook::velox::Timestamp& value, T* result) {
  // TODO Implement
}

} // namespace facebook::velox

namespace std {
template <>
struct hash<::facebook::velox::Timestamp> {
  size_t operator()(const ::facebook::velox::Timestamp value) const {
    return facebook::velox::bits::hashMix(value.getSeconds(), value.getNanos());
  }
};

std::string to_string(const ::facebook::velox::Timestamp& ts);

} // namespace std

namespace folly {
template <>
struct hasher<::facebook::velox::Timestamp> {
  size_t operator()(const ::facebook::velox::Timestamp value) const {
    return facebook::velox::bits::hashMix(value.getSeconds(), value.getNanos());
  }
};

} // namespace folly
