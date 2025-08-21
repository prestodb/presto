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

#include "velox/common/base/Doubles.h"
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/lib/DateTimeUtil.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Timestamp.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {

FOLLY_ALWAYS_INLINE double toUnixtime(const Timestamp& timestamp) {
  double result = timestamp.getSeconds();
  result +=
      static_cast<double>(timestamp.getNanos()) / Timestamp::kNanosInSecond;
  return result;
}

FOLLY_ALWAYS_INLINE Timestamp fromUnixtime(double unixtime) {
  if (FOLLY_UNLIKELY(std::isnan(unixtime))) {
    return Timestamp(0, 0);
  }

  static const int64_t kMin = std::numeric_limits<int64_t>::min();

  if (FOLLY_UNLIKELY(unixtime >= kMinDoubleAboveInt64Max)) {
    return Timestamp::maxMillis();
  }

  if (FOLLY_UNLIKELY(unixtime <= kMin)) {
    return Timestamp::minMillis();
  }

  if (FOLLY_UNLIKELY(std::isinf(unixtime))) {
    return unixtime < 0 ? Timestamp::minMillis() : Timestamp::maxMillis();
  }

  auto seconds = std::floor(unixtime);
  auto milliseconds = std::llround((unixtime - seconds) * kMillisInSecond);
  if (FOLLY_UNLIKELY(milliseconds == kMillisInSecond)) {
    ++seconds;
    milliseconds = 0;
  }
  return Timestamp(
      seconds, milliseconds * Timestamp::kNanosecondsInMillisecond);
}

FOLLY_ALWAYS_INLINE boost::int64_t fromUnixtime(
    double unixtime,
    int16_t timeZoneId) {
  if (FOLLY_UNLIKELY(std::isnan(unixtime))) {
    return pack(0, timeZoneId);
  }

  static const int64_t kMin = std::numeric_limits<int64_t>::min();

  if (FOLLY_UNLIKELY(unixtime >= kMinDoubleAboveInt64Max)) {
    return pack(std::numeric_limits<int64_t>::max(), timeZoneId);
  }

  if (FOLLY_UNLIKELY(unixtime <= kMin)) {
    return pack(std::numeric_limits<int64_t>::min(), timeZoneId);
  }

  if (FOLLY_UNLIKELY(std::isinf(unixtime))) {
    return unixtime < 0 ? pack(std::numeric_limits<int64_t>::min(), timeZoneId)
                        : pack(std::numeric_limits<int64_t>::max(), timeZoneId);
  }

  return pack(
      std::llround(unixtime * Timestamp::kMillisecondsInSecond), timeZoneId);
}

// If time zone is provided, use it for the arithmetic operation (convert to it,
// apply operation, then convert back to UTC).
FOLLY_ALWAYS_INLINE Timestamp addToTimestamp(
    const Timestamp& timestamp,
    const DateTimeUnit unit,
    const int32_t value,
    const tz::TimeZone* timeZone) {
  if (timeZone == nullptr) {
    return addToTimestamp(timestamp, unit, value);
  } else {
    Timestamp zonedTimestamp = timestamp;
    zonedTimestamp.toTimezone(*timeZone);
    auto resultTimestamp = addToTimestamp(zonedTimestamp, unit, value);
    resultTimestamp.toGMT(*timeZone);
    return resultTimestamp;
  }
}

FOLLY_ALWAYS_INLINE int64_t addToTimestampWithTimezone(
    int64_t timestampWithTimezone,
    const DateTimeUnit unit,
    const int32_t value) {
  {
    int64_t finalSysMs;
    if (unit < DateTimeUnit::kDay) {
      auto originalTimestamp = unpackTimestampUtc(timestampWithTimezone);
      finalSysMs =
          addToTimestamp(originalTimestamp, unit, (int32_t)value).toMillis();
    } else {
      // Use local time to handle crossing daylight savings time boundaries.
      // E.g. the "day" when the clock moves back an hour is 25 hours long, and
      // the day it moves forward is 23 hours long. Daylight savings time
      // doesn't affect time units less than a day, and will produce incorrect
      // results if we use local time.
      const tz::TimeZone* timeZone =
          tz::locateZone(unpackZoneKeyId(timestampWithTimezone));
      auto originalTimestamp =
          Timestamp::fromMillis(timeZone
                                    ->to_local(std::chrono::milliseconds(
                                        unpackMillisUtc(timestampWithTimezone)))
                                    .count());
      auto updatedTimeStamp =
          addToTimestamp(originalTimestamp, unit, (int32_t)value);
      updatedTimeStamp = Timestamp(
          timeZone
              ->correct_nonexistent_time(
                  std::chrono::seconds(updatedTimeStamp.getSeconds()))
              .count(),
          updatedTimeStamp.getNanos());
      finalSysMs =
          timeZone
              ->to_sys(
                  std::chrono::milliseconds(updatedTimeStamp.toMillis()),
                  tz::TimeZone::TChoose::kEarliest)
              .count();
    }

    return pack(finalSysMs, unpackZoneKeyId(timestampWithTimezone));
  }
}

FOLLY_ALWAYS_INLINE int64_t diffTimestampWithTimeZone(
    const DateTimeUnit unit,
    const int64_t fromTimestampWithTimeZone,
    const int64_t toTimestampWithTimeZone) {
  auto fromTimeZoneId = unpackZoneKeyId(fromTimestampWithTimeZone);
  auto toTimeZoneId = unpackZoneKeyId(toTimestampWithTimeZone);
  VELOX_CHECK_EQ(
      fromTimeZoneId,
      toTimeZoneId,
      "diffTimestampWithTimeZone must receive timestamps in the same time zone.");

  Timestamp fromTimestamp;
  Timestamp toTimestamp;

  if (unit < DateTimeUnit::kDay) {
    fromTimestamp = unpackTimestampUtc(fromTimestampWithTimeZone);
    toTimestamp = unpackTimestampUtc(toTimestampWithTimeZone);
  } else {
    // Use local time to handle crossing daylight savings time boundaries.
    // E.g. the "day" when the clock moves back an hour is 25 hours long, and
    // the day it moves forward is 23 hours long. Daylight savings time
    // doesn't affect time units less than a day, and will produce incorrect
    // results if we use local time.
    const tz::TimeZone* timeZone = tz::locateZone(fromTimeZoneId);
    fromTimestamp = Timestamp::fromMillis(
        timeZone
            ->to_local(std::chrono::milliseconds(
                unpackMillisUtc(fromTimestampWithTimeZone)))
            .count());
    toTimestamp =
        Timestamp::fromMillis(timeZone
                                  ->to_local(std::chrono::milliseconds(
                                      unpackMillisUtc(toTimestampWithTimeZone)))
                                  .count());
  }

  return diffTimestamp(unit, fromTimestamp, toTimestamp);
}

FOLLY_ALWAYS_INLINE
int64_t diffDate(
    const DateTimeUnit unit,
    const int32_t fromDate,
    const int32_t toDate) {
  if (fromDate == toDate) {
    return 0;
  }
  return diffTimestamp(
      unit,
      // prevent overflow
      Timestamp((int64_t)fromDate * util::kSecsPerDay, 0),
      Timestamp((int64_t)toDate * util::kSecsPerDay, 0));
}

FOLLY_ALWAYS_INLINE int64_t
valueOfTimeUnitToMillis(const double value, std::string_view unit) {
  double convertedValue = value;
  if (unit == "ns") {
    convertedValue = convertedValue * std::milli::den / std::nano::den;
  } else if (unit == "us") {
    convertedValue = convertedValue * std::milli::den / std::micro::den;
  } else if (unit == "ms") {
  } else if (unit == "s") {
    convertedValue = convertedValue * std::milli::den;
  } else if (unit == "m") {
    convertedValue = convertedValue * 60 * std::milli::den;
  } else if (unit == "h") {
    convertedValue = convertedValue * 3600 * std::milli::den;
  } else if (unit == "d") {
    convertedValue = convertedValue * 86400 * std::milli::den;
  } else {
    VELOX_USER_FAIL("Unknown time unit: {}", unit);
  }

  auto result = folly::tryTo<int64_t>(std::round(convertedValue));
  if (result.hasValue()) {
    return result.value();
  }
  VELOX_USER_FAIL(
      "Value in {} unit is too large to be represented in ms unit as an int64_t",
      unit);
}

} // namespace facebook::velox::functions
