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

#include <iomanip>
#include <sstream>
#include <string>

#include <folly/dynamic.h>

#include "velox/common/base/CheckedArithmetic.h"
#include "velox/type/StringView.h"

namespace facebook::velox {

namespace date {
class time_zone;
}

struct TimestampToStringOptions {
  enum class Precision : int8_t {
    kMilliseconds = 3, // 10^3 milliseconds are equal to one second.
    kMicroseconds = 6, // 10^6 microseconds are equal to one second.
    kNanoseconds = 9, // 10^9 nanoseconds are equal to one second.
  };

  Precision precision = Precision::kNanoseconds;

  // Whether to add a leading '+' when year is greater than 9999.
  bool leadingPositiveSign = false;

  /// Whether to skip trailing zeros of fractional part. E.g. when true,
  /// '2000-01-01 12:21:56.129000' becomes '2000-01-01 12:21:56.129'.
  bool skipTrailingZeros = false;

  /// Whether padding zeros are added when the digits of year is less than 4.
  /// E.g. when true, '1-01-01 05:17:32.000' becomes '0001-01-01 05:17:32.000',
  /// '-03-24 13:20:00.000' becomes '0000-03-24 13:20:00.000', and '-1-11-29
  /// 19:33:20.000' becomes '-0001-11-29 19:33:20.000'.
  bool zeroPaddingYear = false;

  // The separator of date and time.
  char dateTimeSeparator = 'T';

  enum class Mode : int8_t {
    /// ISO 8601 timestamp format: %Y-%m-%dT%H:%M:%S.nnnnnnnnn for nanoseconds
    /// precision; %Y-%m-%dT%H:%M:%S.nnn for milliseconds precision.
    kFull,
    /// ISO 8601 date format: %Y-%m-%d.
    kDateOnly,
    /// ISO 8601 time format: %H:%M:%S.nnnnnnnnn for nanoseconds precision,
    /// or %H:%M:%S.nnn for milliseconds precision.
    kTimeOnly,
  };

  Mode mode = Mode::kFull;
};

struct Timestamp {
 public:
  static constexpr int64_t kMillisecondsInSecond = 1'000;
  static constexpr int64_t kMicrosecondsInMillisecond = 1'000;
  static constexpr int64_t kNanosecondsInMicrosecond = 1'000;
  static constexpr int64_t kNanosecondsInMillisecond = 1'000'000;

  // Limit the range of seconds to avoid some problems. Seconds should be
  // in the range [INT64_MIN/1000 - 1, INT64_MAX/1000].
  // Presto's Timestamp is stored in one 64-bit signed integer for
  // milliseconds, this range ensures that Timestamp's range in Velox will not
  // be smaller than Presto, and can make Timestamp::toString work correctly.
  static constexpr int64_t kMaxSeconds =
      std::numeric_limits<int64_t>::max() / kMillisecondsInSecond;
  static constexpr int64_t kMinSeconds =
      std::numeric_limits<int64_t>::min() / kMillisecondsInSecond - 1;

  // Nanoseconds should be less than 1 second.
  static constexpr uint64_t kMaxNanos = 999'999'999;

  constexpr Timestamp() : seconds_(0), nanos_(0) {}

  Timestamp(int64_t seconds, uint64_t nanos)
      : seconds_(seconds), nanos_(nanos) {
    VELOX_USER_DCHECK_GE(
        seconds, kMinSeconds, "Timestamp seconds out of range");
    VELOX_USER_DCHECK_LE(
        seconds, kMaxSeconds, "Timestamp seconds out of range");
    VELOX_USER_DCHECK_LE(nanos, kMaxNanos, "Timestamp nanos out of range");
  }

  // Returns the current unix timestamp (ms precision).
  static Timestamp now();

  static Timestamp create(const folly::dynamic& obj) {
    auto seconds = obj["seconds"].asInt();
    auto nanos = obj["nanos"].asInt();
    return Timestamp(seconds, nanos);
  }

  int64_t getSeconds() const {
    return seconds_;
  }

  uint64_t getNanos() const {
    return nanos_;
  }

  int64_t toNanos() const {
    // int64 can store around 292 years in nanos ~ till 2262-04-12.
    // When an integer overflow occurs in the calculation,
    // an exception will be thrown.
    try {
      return checkedPlus(
          checkedMultiply(seconds_, (int64_t)1'000'000'000), (int64_t)nanos_);
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(
          "Could not convert Timestamp({}, {}) to nanoseconds, {}",
          seconds_,
          nanos_,
          e.what());
    }
  }

  int64_t toMillis() const {
    // We use int128_t to make sure the computation does not overflows since
    // there are cases such that seconds*1000 does not fit in int64_t,
    // but seconds*1000 + nanos does, an example is TimeStamp::minMillis().

    // If the final result does not fit in int64_tw we throw.
    __int128_t result =
        (__int128_t)seconds_ * 1'000 + (int64_t)(nanos_ / 1'000'000);
    if (result < std::numeric_limits<int64_t>::min() ||
        result > std::numeric_limits<int64_t>::max()) {
      VELOX_USER_FAIL(
          "Could not convert Timestamp({}, {}) to milliseconds",
          seconds_,
          nanos_);
    }
    return result;
  }

  int64_t toMicros() const {
    // When an integer overflow occurs in the calculation,
    // an exception will be thrown.
    try {
      return checkedPlus(
          checkedMultiply(seconds_, (int64_t)1'000'000),
          (int64_t)(nanos_ / 1'000));
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(
          "Could not convert Timestamp({}, {}) to microseconds, {}",
          seconds_,
          nanos_,
          e.what());
    }
  }

  /// Due to the limit of std::chrono, throws if timestamp is outside of
  /// [-32767-01-01, 32767-12-31] range.
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
  toTimePoint() const;

  static Timestamp fromMillis(int64_t millis) {
    if (millis >= 0 || millis % 1'000 == 0) {
      return Timestamp(millis / 1'000, (millis % 1'000) * 1'000'000);
    }
    auto second = millis / 1'000 - 1;
    auto nano = ((millis - second * 1'000) % 1'000) * 1'000'000;
    return Timestamp(second, nano);
  }

  static Timestamp fromMillisNoError(int64_t millis)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
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

  static const Timestamp minMillis() {
    // The minimum Timestamp that toMillis() method will not overflow.
    // Used to calculate the minimum value of the Presto timestamp.
    constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
    return Timestamp(
        kMinSeconds,
        (kMin % kMillisecondsInSecond + kMillisecondsInSecond) *
            kNanosecondsInMillisecond);
  }

  static const Timestamp maxMillis() {
    // The maximum Timestamp that toMillis() method will not overflow.
    // Used to calculate the maximum value of the Presto timestamp.
    constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
    return Timestamp(
        kMaxSeconds, kMax % kMillisecondsInSecond * kNanosecondsInMillisecond);
  }

  static const Timestamp min() {
    return Timestamp(kMinSeconds, 0);
  }

  static const Timestamp max() {
    return Timestamp(kMaxSeconds, kMaxNanos);
  }

  /// Our own version of gmtime_r to avoid expensive calls to __tz_convert.
  /// This might not be very significant in micro benchmark, but is causing
  /// significant context switching cost in real world queries with higher
  /// concurrency (71% of time is on __tz_convert for some queries).
  ///
  /// Return whether the epoch second can be converted to a valid std::tm.
  static bool epochToUtc(int64_t seconds, std::tm& out);

  /// Converts a std::tm to a time/date/timestamp string in ISO 8601 format
  /// according to TimestampToStringOptions.
  static std::string tmToString(
      const std::tm&,
      uint64_t nanos,
      const TimestampToStringOptions& options);

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

  /// A default time zone that is same across the process.
  static const date::time_zone& defaultTimezone();

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

  void operator++() {
    if (nanos_ < kMaxNanos) {
      nanos_++;
      return;
    }
    if (seconds_ < kMaxSeconds) {
      seconds_++;
      nanos_ = 0;
      return;
    }
    VELOX_USER_FAIL("Timestamp nanos out of range");
  }

  void operator--() {
    if (nanos_ > 0) {
      nanos_--;
      return;
    }
    if (seconds_ > kMinSeconds) {
      seconds_--;
      nanos_ = kMaxNanos;
      return;
    }
    VELOX_USER_FAIL("Timestamp nanos out of range");
  }

  // Needed for serialization of FlatVector<Timestamp>
  operator StringView() const {
    return StringView("TODO: Implement");
  }

  std::string toString(const TimestampToStringOptions& options = {}) const {
    std::tm tm;
    VELOX_USER_CHECK(
        epochToUtc(seconds_, tm),
        "Can't convert seconds to time: {}",
        seconds_);
    return tmToString(tm, nanos_, options);
  }

  operator std::string() const {
    return toString();
  }

  operator folly::dynamic() const {
    return folly::dynamic(seconds_);
  }

  folly::dynamic serialize() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["seconds"] = seconds_;
    obj["nanos"] = nanos_;
    return obj;
  }

 private:
  int64_t seconds_;
  uint64_t nanos_;
};

void parseTo(folly::StringPiece in, ::facebook::velox::Timestamp& out);

template <typename T>
void toAppend(const ::facebook::velox::Timestamp& value, T* result) {
  result->append(value.toString());
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

template <>
class numeric_limits<facebook::velox::Timestamp> {
 public:
  static facebook::velox::Timestamp min() {
    return facebook::velox::Timestamp::min();
  }
  static facebook::velox::Timestamp max() {
    return facebook::velox::Timestamp::max();
  }

  static facebook::velox::Timestamp lowest() {
    return facebook::velox::Timestamp::min();
  }
};

} // namespace std

namespace folly {
template <>
struct hasher<::facebook::velox::Timestamp> {
  size_t operator()(const ::facebook::velox::Timestamp value) const {
    return facebook::velox::bits::hashMix(value.getSeconds(), value.getNanos());
  }
};

} // namespace folly
