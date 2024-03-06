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

/// Cast string to date.
/// When isIso8601 = true, only support "[+-]YYYY-MM-DD" format (ISO 8601).
/// When isIso8601 = false, supported date formats include:
///
/// `[+-]YYYY*`
/// `[+-]YYYY*-[M]M`
/// `[+-]YYYY*-[M]M-[D]D`
/// `[+-]YYYY*-[M]M-[D]D `
/// `[+-]YYYY*-[M]M-[D]D *`
/// `[+-]YYYY*-[M]M-[D]DT*`
///
/// Throws VeloxUserError if the format or date is invalid.
int32_t castFromDateString(const char* buf, size_t len, bool isIso8601);

inline int32_t castFromDateString(const StringView& str, bool isIso8601) {
  return castFromDateString(str.data(), str.size(), isIso8601);
}

// Extracts the day of the week from the number of days since epoch
int32_t extractISODayOfTheWeek(int32_t daysSinceEpoch);

/// Time conversions.

/// Returns the cumulative number of microseconds.
/// Does not perform any sanity checks.
int64_t
fromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds);

/// Parses the input string and returns the number of cumulative microseconds,
/// following the "HH:MM:SS[.MS]" format (ISO 8601).
//
/// Throws VeloxUserError if the format or time is invalid.
int64_t fromTimeString(const char* buf, size_t len);

inline int64_t fromTimeString(const StringView& str) {
  return fromTimeString(str.data(), str.size());
}

// Timestamp conversion

/// Parses a full ISO 8601 timestamp string, following the format
/// "YYYY-MM-DD HH:MM:SS[.MS] +00:00"
Timestamp fromTimestampString(const char* buf, size_t len);

inline Timestamp fromTimestampString(const StringView& str) {
  return fromTimestampString(str.data(), str.size());
}

Timestamp fromDatetime(int64_t daysSinceEpoch, int64_t microsSinceMidnight);

} // namespace facebook::velox::util
