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

#include "velox/type/Timestamp.h"

namespace facebook::velox::util {

/// Date conversions.

/// Returns the (signed) number of days since unix epoch (1970-01-01).
/// Throws VeloxUserError if the date is invalid.
int32_t fromDate(int32_t year, int32_t month, int32_t day);

/// Returns the (signed) number of days since unix epoch (1970-01-01), following
/// the "YYYY-MM-DD" format (ISO 8601). ' ', '/' and '\' are also acceptable
/// separators. Negative years and a trailing "(BC)" are also supported.
///
/// Throws VeloxUserError if the format or date is invalid.
int32_t fromDateString(const char* buf, size_t len);

inline int32_t fromDateString(const StringView& str) {
  return fromDateString(str.data(), str.size());
}

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

Timestamp fromDatetime(int32_t daysSinceEpoch, int64_t microsSinceMidnight);

} // namespace facebook::velox::util
