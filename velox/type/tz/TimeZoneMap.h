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

#include <chrono>
#include <string>
#include <vector>

namespace facebook::velox::tzdb {
class time_zone;
}

namespace facebook::velox::tz {

/// This library provides time zone management primitives. It maintains an
/// internal static database which is contructed lazily based on the first
/// access, based on TimeZoneDatabase.cpp and the local tzdata installed in your
/// system (through velox/external/tzdata).
///
/// It provides functions for one to lookup TimeZone pointers based on time zone
/// name or ID, and to performance timestamp conversion across time zones.
///
/// This library provides a layer of functionality on top of
/// velox/external/tzdata, so do not use the external library directly for
/// time zone routines.

class TimeZone;

/// Returns a TimeZone pointer based on a time zone name. This makes an hash
/// map access, and will construct the index on the first access. `failOnError`
/// controls whether to throw or return nullptr in case the time zone was not
/// found.
const TimeZone* locateZone(std::string_view timeZone, bool failOnError = true);

/// Returns a TimeZone pointer based on a time zone ID. This makes a simple
/// vector access, and will construct the index on the first access.
/// `failOnError` controls whether to throw or return nullptr in case the time
/// zone ID was not valid.
const TimeZone* locateZone(int16_t timeZoneID, bool failOnError = true);

/// Returns the timezone name associated with timeZoneID.
std::string getTimeZoneName(int64_t timeZoneID);

/// Returns the timeZoneID for the timezone name.
/// If failOnError = true, throws an exception for unrecognized timezone.
/// Otherwise, returns -1.
int16_t getTimeZoneID(std::string_view timeZone, bool failOnError = true);

/// Returns the timeZoneID for a given offset in minutes. The offset must be in
/// [-14:00, +14:00] range.
int16_t getTimeZoneID(int32_t offsetMinutes);

/// Returns all valid time zone IDs.
std::vector<int16_t> getTimeZoneIDs();

// Validates that the time point can be safely used by the external date
// library.
template <typename T>
using time_point = std::chrono::time_point<std::chrono::system_clock, T>;

void validateRange(time_point<std::chrono::seconds> timePoint);
void validateRange(time_point<std::chrono::milliseconds> timePoint);

/// TimeZone is the proxy object for time zone management. It provides access to
/// time zone names, their IDs (as defined in TimeZoneDatabase.cpp and
/// consistent with Presto), and utilities for timestamp conversion across
/// timezones by leveraging the .to_sys() and .to_local() methods as documented
/// in:
///
///   https://howardhinnant.github.io/date/tz.html
///
/// Do not create your own objects; rather, look up a pointer by using one of
/// the methods above.
class TimeZone {
 public:
  // Constructor for regular time zones with a name and a pointer to
  // external/date time zone database (from tzdata).
  TimeZone(
      std::string_view timeZoneName,
      int16_t timeZoneID,
      const tzdb::time_zone* tz)
      : tz_(tz),
        offset_(0),
        timeZoneName_(timeZoneName),
        timeZoneID_(timeZoneID) {}

  // Constructor for time zone offsets ("+00:00").
  TimeZone(
      std::string_view timeZoneName,
      int16_t timeZoneID,
      std::chrono::minutes offset)
      : tz_(nullptr),
        offset_(offset),
        timeZoneName_(timeZoneName),
        timeZoneID_(timeZoneID) {}

  // Do not copy it.
  TimeZone(const TimeZone&) = delete;
  TimeZone& operator=(const TimeZone&) = delete;

  friend std::ostream& operator<<(std::ostream& os, const TimeZone& timezone) {
    return os << timezone.name();
  }

  using seconds = std::chrono::seconds;
  using milliseconds = std::chrono::milliseconds;

  /// Converts a local time (the time as perceived in the user time zone
  /// represented by this object) to a system time (the corresponding time in
  /// GMT at the same instant).
  ///
  /// Conversions from local time to GMT are non-linear and may be ambiguous
  /// during day light savings transitions, or non existent. By default (kFail),
  /// `to_sys()` will throw `tzdb::ambiguous_local_time` and
  /// `tzdb::nonexistent_local_time` in these cases.
  ///
  /// You can overwrite the behavior in ambiguous conversions by setting the
  /// TChoose flag, but it will still throws in case of nonexistent conversions.
  enum class TChoose {
    kFail = 0,
    kEarliest = 1,
    kLatest = 2,
  };

  seconds to_sys(seconds timestamp, TChoose choose = TChoose::kFail) const;
  milliseconds to_sys(milliseconds timestamp, TChoose choose = TChoose::kFail)
      const;

  /// Do the opposite conversion. Taking a system time (the time as perceived in
  /// GMT), convert to the same instant in time as observed in the user local
  /// time represented by this object). Note that this conversion is not
  /// susceptible to the error above.
  seconds to_local(seconds timestamp) const;
  milliseconds to_local(milliseconds timestamp) const;

  /// If a local time is nonexistent, i.e. refers to a time that exists in the
  /// gap during a time zone conversion, this returns the time adjusted by
  /// the difference between the two time zones, so that it lies in the later
  /// time zone.
  ///
  /// If the local time exists then the same time is returned.
  seconds correct_nonexistent_time(seconds timestamp) const;

  const std::string& name() const {
    return timeZoneName_;
  }

  int16_t id() const {
    return timeZoneID_;
  }

  const tzdb::time_zone* tz() const {
    return tz_;
  }

  /// Returns the short name (abbreviation) of the time zone for the given
  /// timestamp. Note that the timestamp is needed for time zones that support
  /// daylight savings time as the short name will change depending on the date
  /// (e.g. PST/PDT).
  std::string getShortName(
      milliseconds timestamp,
      TChoose choose = TChoose::kFail) const;

  /// Returns the long name of the time zone for the given timestamp, e.g.
  /// Pacific Standard Time.  Note that the timestamp is needed for time zones
  /// that support daylight savings time as the long name will change depending
  /// on the date (e.g. Pacific Standard Time vs Pacific Daylight Time).
  std::string getLongName(
      milliseconds timestamp,
      TChoose choose = TChoose::kFail) const;

 private:
  const tzdb::time_zone* tz_{nullptr};
  const std::chrono::minutes offset_{0};
  const std::string timeZoneName_;
  const int16_t timeZoneID_;
};

} // namespace facebook::velox::tz
