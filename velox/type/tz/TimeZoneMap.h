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

#include <string>

namespace facebook::velox::date {
class time_zone;
}

namespace facebook::velox::tz {

/// This library provides time zone lookup and mapping utilities, in addition to
/// functions to enable timestamp conversions across time zones. It leverages
/// the velox/external/date underneath to perform conversions.
///
/// This library provides a thin layer of functionality on top of
/// velox/external/date for timezone lookup and conversions, so don't use the
/// external library directly.

/// TimeZone is the object that allows conversions across timezones using the
/// .to_sys() and .to_local() methods, as documented in:
///
///   https://howardhinnant.github.io/date/tz.html
///
using TimeZone = date::time_zone;

/// TimeZone pointers can be found using `locateZone()`.
///
/// This function in mostly implemented by velox/external/date, and performs a
/// binary search in the internal time zone database. On the first call,
/// velox/external/date will initialize a static list of timezone, read from the
/// local tzdata database.
const TimeZone* locateZone(std::string_view timeZone);

/// Returns the timezone name associated with timeZoneID.
std::string getTimeZoneName(int64_t timeZoneID);

/// Returns the timeZoneID for the timezone name.
/// If failOnError = true, throws an exception for unrecognized timezone.
/// Otherwise, returns -1.
int16_t getTimeZoneID(std::string_view timeZone, bool failOnError = true);

/// Returns the timeZoneID for a given offset in minutes. The offset must be in
/// [-14:00, +14:00] range.
int16_t getTimeZoneID(int32_t offsetMinutes);

} // namespace facebook::velox::tz

#ifdef VELOX_ENABLE_BACKWARD_COMPATIBILITY
namespace facebook::velox::util {

inline std::string getTimeZoneName(int64_t timeZoneID) {
  return tz::getTimeZoneName(timeZoneID);
}

} // namespace facebook::velox::util
#endif
