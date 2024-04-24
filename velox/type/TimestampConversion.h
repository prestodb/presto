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

  // Strictly processes dates only in complete ISO 8601 format,
  // e.g. [+-](YYYY-MM-DD).
  // Align with Presto casting conventions.
  kStandardCast,

  // Like kStandardCast but permits years less than four digits, missing
  // day/month, and allows trailing 'T' or spaces.
  // Align with Spark SQL casting conventions.
  // Supported formats:
  // `[+-][Y]Y*`
  // `[+-][Y]Y*-[M]M`
  // `[+-][Y]Y*-[M]M*-[D]D`
  // `[+-][Y]Y*-[M]M*-[D]D *`
  // `[+-][Y]Y*-[M]M*-[D]DT*`
  kNonStandardCast,

  // Like kNonStandardCast but does not permit inclusion of timestamp.
  // Supported formats:
  // `[+-][Y]Y*`
  // `[+-][Y]Y*-[M]M`
  // `[+-][Y]Y*-[M]M*-[D]D`
  // `[+-][Y]Y*-[M]M*-[D]D *`
  kNonStandardNoTimeCast
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

/// Returns the (signed) number of days since unix epoch (1970-01-01), following
/// the "YYYY-MM-DD" format (ISO 8601). ' ', '/' and '\' are also acceptable
/// separators. Negative years and a trailing "(BC)" are also supported.
///
/// Throws VeloxUserError if the format or date is invalid.
int64_t fromDateString(const char* buf, size_t len);

inline int64_t fromDateString(const StringView& str) {
  return fromDateString(str.data(), str.size());
}

/// Cast string to date. Supported date formats vary, depending on input
/// ParseMode. Refer to ParseMode enum for further info.
///
/// Throws VeloxUserError if the format or date is invalid.
int32_t castFromDateString(const char* buf, size_t len, ParseMode mode);

inline int32_t castFromDateString(const StringView& str, ParseMode mode) {
  return castFromDateString(str.data(), str.size(), mode);
}

// Extracts the day of the week from the number of days since epoch
int32_t extractISODayOfTheWeek(int32_t daysSinceEpoch);

/// Time conversions.

/// Returns the cumulative number of microseconds.
/// Does not perform any sanity checks.
int64_t
fromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds);

/// Parses the input string and returns the number of cumulative microseconds,
/// following the "HH:MM[:SS[.MS]]" format (ISO 8601).
//
/// Throws VeloxUserError if the format or time is invalid.
int64_t fromTimeString(const char* buf, size_t len);

inline int64_t fromTimeString(const StringView& str) {
  return fromTimeString(str.data(), str.size());
}

// Timestamp conversion

/// Parses a full ISO 8601 timestamp string, following the format:
///
///  "YYYY-MM-DD HH:MM[:SS[.MS]]"
///
/// Seconds and milliseconds are optional.
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
Timestamp fromTimestampString(const char* buf, size_t len);

inline Timestamp fromTimestampString(const StringView& str) {
  return fromTimestampString(str.data(), str.size());
}

/// Parses a full ISO 8601 timestamp string, following the format:
///
///  "YYYY-MM-DD HH:MM[:SS[.MS]] +00:00"
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
/// -1 means no timezone information was found. Throws VeloxUserError in case of
/// parsing errors.
std::pair<Timestamp, int64_t> fromTimestampWithTimezoneString(
    const char* buf,
    size_t len);

inline auto fromTimestampWithTimezoneString(const StringView& str) {
  return fromTimestampWithTimezoneString(str.data(), str.size());
}

Timestamp fromDatetime(int64_t daysSinceEpoch, int64_t microsSinceMidnight);

} // namespace facebook::velox::util
