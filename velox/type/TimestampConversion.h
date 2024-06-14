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

#include <cstdint>
#include "velox/common/base/Status.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox::util {

constexpr const int32_t kHoursPerDay{24};
constexpr const int32_t kMinsPerHour{60};
constexpr const int32_t kSecsPerMinute{60};
constexpr const int64_t kMsecsPerSec{1000};

constexpr const int64_t kMicrosPerMsec{1000};
constexpr const int64_t kMicrosPerSec{kMicrosPerMsec * kMsecsPerSec};
constexpr const int64_t kMicrosPerMinute{kMicrosPerSec * kSecsPerMinute};
constexpr const int64_t kMicrosPerHour{kMicrosPerMinute * kMinsPerHour};

constexpr const int64_t kNanosPerMicro{1000};

constexpr const int32_t kSecsPerHour{kSecsPerMinute * kMinsPerHour};
constexpr const int32_t kSecsPerDay{kSecsPerHour * kHoursPerDay};

// Max and min year correspond to Joda datetime min and max
constexpr const int32_t kMinYear{-292275055};
constexpr const int32_t kMaxYear{292278994};

constexpr const int32_t kYearInterval{400};
constexpr const int32_t kDaysPerYearInterval{146097};

/// Enum to dictate parsing modes for date strings.
enum class ParseMode {
  // For date string conversion, align with DuckDB's implementation.
  kStrict,

  // For timestamp string conversion, align with DuckDB's implementation.
  kNonStrict,

  // Accepts complete ISO 8601 format, i.e. [+-](YYYY-MM-DD). Allows leading and
  // trailing spaces.
  // Aligned with Presto casting conventions.
  kPrestoCast,

  // ISO-8601 format with optional leading or trailing spaces. Optional trailing
  // 'T' is also allowed.
  // Aligned with Spark SQL casting conventions.
  //
  // [+-][Y]Y*
  // [+-][Y]Y*-[M]M
  // [+-][Y]Y*-[M]M*-[D]D
  // [+-][Y]Y*-[M]M*-[D]D *
  // [+-][Y]Y*-[M]M*-[D]DT*
  kSparkCast,

  // ISO-8601 format. No leading or trailing spaces allowed.
  //
  // [+-][Y]Y*
  // [+-][Y]Y*-[M]M
  // [+-][Y]Y*-[M]M*-[D]D
  kIso8601
};

// Returns true if leap year, false otherwise
bool isLeapYear(int32_t year);

// Returns true if year, month, day corresponds to valid date, false otherwise
bool isValidDate(int32_t year, int32_t month, int32_t day);

// Returns true if yday of year is valid for given year
bool isValidDayOfYear(int32_t year, int32_t dayOfYear);

// Returns max day of month for inputted month of inputted year
int32_t getMaxDayOfMonth(int32_t year, int32_t month);

/// Computes the last day of month since unix epoch (1970-01-01).
/// Returns UserError status if the date is invalid.
Status lastDayOfMonthSinceEpochFromDate(const std::tm& dateTime, int64_t& out);

/// Date conversions.

/// Computes the (signed) number of days since unix epoch (1970-01-01).
/// Returns UserError status if the date is invalid.
Status
daysSinceEpochFromDate(int32_t year, int32_t month, int32_t day, int64_t& out);

/// Computes the (signed) number of days since unix epoch (1970-01-01).
/// Returns UserError status if the date is invalid.
Status daysSinceEpochFromWeekDate(
    int32_t weekYear,
    int32_t weekOfYear,
    int32_t dayOfWeek,
    int64_t& out);

/// Computes the (signed) number of days since unix epoch (1970-01-01).
/// Returns UserError status if the date is invalid.
Status
daysSinceEpochFromDayOfYear(int32_t year, int32_t dayOfYear, int64_t& out);

/// Cast string to date. Supported date formats vary, depending on input
/// ParseMode. Refer to ParseMode enum for further info.
///
/// Returns Unexpected with UserError status if the format or date is invalid.
Expected<int32_t> fromDateString(const char* buf, size_t len, ParseMode mode);

inline Expected<int32_t> fromDateString(const StringView& str, ParseMode mode) {
  return fromDateString(str.data(), str.size(), mode);
}

// Extracts the day of the week from the number of days since epoch
int32_t extractISODayOfTheWeek(int32_t daysSinceEpoch);

/// Time conversions.

/// Returns the cumulative number of microseconds.
/// Does not perform any sanity checks.
int64_t
fromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds);

// Timestamp conversion

enum class TimestampParseMode {
  /// Accepted syntax:
  // clang-format off
  ///   datetime          = time | date-opt-time
  ///   time              = 'T' time-element [offset]
  ///   date-opt-time     = date-element ['T' [time-element] [offset]]
  ///   date-element      = yyyy ['-' MM ['-' dd]]
  ///   time-element      = HH [minute-element] | [fraction]
  ///   minute-element    = ':' mm [second-element] | [fraction]
  ///   second-element    = ':' ss [fraction]
  ///   fraction          = ('.' | ',') digit+
  ///   offset            = 'Z' | (('+' | '-') HH [':' mm [':' ss [('.' | ',') SSS]]])
  // clang-format on
  kIso8601,

  /// Accepted syntax:
  // clang-format off
  ///   date-opt-time     = date-element [' ' [time-element] [[' '] [offset]]]
  ///   date-element      = yyyy ['-' MM ['-' dd]]
  ///   time-element      = HH [minute-element] | [fraction]
  ///   minute-element    = ':' mm [second-element] | [fraction]
  ///   second-element    = ':' ss [fraction]
  ///   fraction          = '.' digit+
  ///   offset            = 'Z' | ZZZ
  // clang-format on
  // Allows leading and trailing spaces.
  kPrestoCast,

  // Same as kPrestoCast, but allows 'T' separator between date and time.
  kLegacyCast,

  /// A Spark-compatible timestamp string. A mix of the above. Accepts T and
  /// space as separator between date and time. Allows leading and trailing
  /// spaces.
  kSparkCast,
};

/// Parses a timestamp string using specified TimestampParseMode.
///
/// This function does not accept any timezone information in the string (e.g.
/// UTC, Z, or a timezone offsets). This is because the returned timestamp does
/// not contain timezone information; therefore, it would either be required for
/// this function to convert the parsed timestamp (but we don't know the
/// original timezone), or ignore the timezone information, which would be
/// incorecct.
///
/// For a timezone-aware version of this function, check
/// `fromTimestampWithTimezoneString()` below.
Expected<Timestamp>
fromTimestampString(const char* buf, size_t len, TimestampParseMode parseMode);

inline Expected<Timestamp> fromTimestampString(
    const StringView& str,
    TimestampParseMode parseMode) {
  return fromTimestampString(str.data(), str.size(), parseMode);
}

/// Parses a timestamp string using specified TimestampParseMode.
///
/// This is a timezone-aware version of the function above
/// `fromTimestampString()` which returns both the parsed timestamp and the
/// timezone ID. It is up to the client to do the expected conversion based on
/// these two values.
///
/// The timezone information at the end of the string may contain a timezone
/// name (as defined in velox/type/tz/*), such as "UTC" or
/// "America/Los_Angeles", or a timezone offset, like "+06:00" or "-09:30". The
/// white space between the hour definition and timestamp is optional.
///
/// -1 means no timezone information was found. Returns Unexpected with
/// UserError status in case of parsing errors.
Expected<std::pair<Timestamp, int16_t>> fromTimestampWithTimezoneString(
    const char* buf,
    size_t len,
    TimestampParseMode parseMode);

inline Expected<std::pair<Timestamp, int16_t>> fromTimestampWithTimezoneString(
    const StringView& str,
    TimestampParseMode parseMode) {
  return fromTimestampWithTimezoneString(str.data(), str.size(), parseMode);
}

Timestamp fromDatetime(int64_t daysSinceEpoch, int64_t microsSinceMidnight);

/// Returns the number of days since epoch for a given timestamp and optional
/// time zone.
int32_t toDate(const Timestamp& timestamp, const date::time_zone* timeZone_);

} // namespace facebook::velox::util
