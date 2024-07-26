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

#include "velox/type/tz/TimeZoneMap.h"

#include <boost/algorithm/string.hpp>
#include <fmt/core.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include "velox/common/base/Exceptions.h"
#include "velox/external/date/tz.h"

namespace facebook::velox::tz {

using TTimeZoneDatabase = std::vector<std::unique_ptr<TimeZone>>;
using TTimeZoneIndex = folly::F14FastMap<std::string, const TimeZone*>;

// Defined in TimeZoneDatabase.cpp
extern const std::vector<std::pair<int16_t, std::string>>& getTimeZoneEntries();

namespace {

// Returns the offset in minutes for a specific time zone offset in the
// database. Do not call for tzID 0 (UTC / "+00:00").
inline std::chrono::minutes getTimeZoneOffset(int16_t tzID) {
  return std::chrono::minutes{(tzID <= 840) ? (tzID - 841) : (tzID - 840)};
}

// Flattens the input vector of pairs into a vector, assuming that the
// timezoneIDs are (mostly) sequential. Note that since they are "mostly"
// senquential, the vector can have holes. But it is still more efficient than
// looking up on a map.
TTimeZoneDatabase buildTimeZoneDatabase(
    const std::vector<std::pair<int16_t, std::string>>& dbInput) {
  TTimeZoneDatabase tzDatabase;
  tzDatabase.resize(dbInput.back().first + 1);

  for (const auto& entry : dbInput) {
    std::unique_ptr<TimeZone> timeZonePtr;

    if (entry.first == 0) {
      timeZonePtr = std::make_unique<TimeZone>(
          "UTC", entry.first, date::locate_zone("UTC"));
    } else if (entry.first <= 1680) {
      std::chrono::minutes offset = getTimeZoneOffset(entry.first);
      timeZonePtr =
          std::make_unique<TimeZone>(entry.second, entry.first, offset);
    }
    // Every single other time zone entry (outside of offsets) needs to be
    // available in external/date or this will throw.
    else {
      timeZonePtr = std::make_unique<TimeZone>(
          entry.second, entry.first, date::locate_zone(entry.second));
    }
    tzDatabase[entry.first] = std::move(timeZonePtr);
  }
  return tzDatabase;
}

const TTimeZoneDatabase& getTimeZoneDatabase() {
  static TTimeZoneDatabase timeZoneDatabase =
      buildTimeZoneDatabase(getTimeZoneEntries());
  return timeZoneDatabase;
}

// Reverses the vector of pairs into a map key'ed by the timezone name for
// reverse look ups.
TTimeZoneIndex buildTimeZoneIndex(const TTimeZoneDatabase& tzDatabase) {
  TTimeZoneIndex reversed;
  reversed.reserve(tzDatabase.size() + 2);

  for (int16_t i = 0; i < tzDatabase.size(); ++i) {
    if (tzDatabase[i] != nullptr) {
      reversed.emplace(
          boost::algorithm::to_lower_copy(tzDatabase[i]->name()),
          tzDatabase[i].get());
    }
  }

  // Add aliases to UTC.
  reversed.emplace("+00:00", tzDatabase.front().get());
  reversed.emplace("-00:00", tzDatabase.front().get());
  return reversed;
}

const TTimeZoneIndex& getTimeZoneIndex() {
  static TTimeZoneIndex timeZoneIndex =
      buildTimeZoneIndex(getTimeZoneDatabase());
  return timeZoneIndex;
}

inline bool isDigit(char c) {
  return c >= '0' && c <= '9';
}

inline bool startsWith(std::string_view str, const char* prefix) {
  return str.rfind(prefix, 0) == 0;
}

// The timezone parsing logic follows what is defined here:
//   https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
inline bool isUtcEquivalentName(std::string_view zone) {
  static folly::F14FastSet<std::string> utcSet = {
      "utc", "uct", "gmt", "gmt0", "greenwich", "universal", "zulu", "z"};
  return utcSet.find(zone) != utcSet.end();
}

std::string normalizeTimeZone(const std::string& originalZoneId) {
  std::string_view zoneId = originalZoneId;
  const bool startsWithEtc = startsWith(zoneId, "etc/");

  if (startsWithEtc) {
    zoneId = zoneId.substr(4);
  }

  // ETC/GMT, ETC/GREENWICH, and others are all valid and link to GMT.
  if (isUtcEquivalentName(zoneId)) {
    return "utc";
  }

  // Check for Etc/GMT(+/-)H[H] pattern.
  if (startsWithEtc) {
    if (zoneId.size() > 4 && startsWith(zoneId, "gmt")) {
      zoneId = zoneId.substr(3);
      char signChar = zoneId[0];

      if (signChar == '+' || signChar == '-') {
        // ETC flips the sign.
        signChar = (signChar == '-') ? '+' : '-';

        // Extract the tens and ones characters for the hour.
        char hourTens;
        char hourOnes;

        if (zoneId.size() == 2) {
          hourTens = '0';
          hourOnes = zoneId[1];
        } else {
          hourTens = zoneId[1];
          hourOnes = zoneId[2];
        }

        // Prevent it from returning -00:00, which is just utc.
        if (hourTens == '0' && hourOnes == '0') {
          return "utc";
        }

        if (isDigit(hourTens) && isDigit(hourOnes)) {
          return std::string() + signChar + hourTens + hourOnes + ":00";
        }
      }
    }
  }
  return originalZoneId;
}

} // namespace

std::string getTimeZoneName(int64_t timeZoneID) {
  const auto& timeZoneDatabase = getTimeZoneDatabase();

  VELOX_CHECK_LT(
      timeZoneID,
      timeZoneDatabase.size(),
      "Unable to resolve timeZoneID '{}'",
      timeZoneID);

  // Check if timeZoneID is not one of the "holes".
  VELOX_CHECK_NOT_NULL(
      timeZoneDatabase[timeZoneID],
      "Unable to resolve timeZoneID '{}'",
      timeZoneID);
  return timeZoneDatabase[timeZoneID]->name();
}

const TimeZone* locateZone(std::string_view timeZone, bool failOnError) {
  const auto& timeZoneIndex = getTimeZoneIndex();

  std::string timeZoneLowered;
  boost::algorithm::to_lower_copy(
      std::back_inserter(timeZoneLowered), timeZone);

  auto it = timeZoneIndex.find(timeZoneLowered);
  if (it != timeZoneIndex.end()) {
    return it->second;
  }

  // If an exact match wasn't found, try to normalize the timezone name.
  it = timeZoneIndex.find(normalizeTimeZone(timeZoneLowered));
  if (it != timeZoneIndex.end()) {
    return it->second;
  }

  if (failOnError) {
    VELOX_USER_FAIL("Unknown time zone: '{}'", timeZone);
  }
  return nullptr;
}

int16_t getTimeZoneID(std::string_view timeZone, bool failOnError) {
  const TimeZone* tz = locateZone(timeZone, failOnError);
  return tz == nullptr ? -1 : tz->id();
}

int16_t getTimeZoneID(int32_t offsetMinutes) {
  static constexpr int32_t kMinOffset = -14 * 60;
  static constexpr int32_t kMaxOffset = 14 * 60;

  if (offsetMinutes == 0) {
    return 0;
  }

  VELOX_USER_CHECK_LE(
      kMinOffset,
      offsetMinutes,
      "Invalid timezone offset minutes: {}",
      offsetMinutes);
  VELOX_USER_CHECK_LE(
      offsetMinutes,
      kMaxOffset,
      "Invalid timezone offset minutes: {}",
      offsetMinutes);

  if (offsetMinutes < 0) {
    return 1 + (offsetMinutes - kMinOffset);
  } else {
    return offsetMinutes - kMinOffset;
  }
}

TimeZone::seconds TimeZone::to_sys(
    TimeZone::seconds timestamp,
    TimeZone::TChoose choose) const {
  date::local_seconds timePoint{timestamp};

  if (tz_ == nullptr) {
    // We can ignore `choose` as time offset conversions are always linear.
    return (timePoint - offset_).time_since_epoch();
  }

  if (choose == TimeZone::TChoose::kFail) {
    // By default, throws.
    return date::zoned_time{tz_, timePoint}.get_sys_time().time_since_epoch();
  }

  auto dateChoose = (choose == TimeZone::TChoose::kEarliest)
      ? date::choose::earliest
      : date::choose::latest;
  return date::zoned_time{tz_, timePoint, dateChoose}
      .get_sys_time()
      .time_since_epoch();
}

TimeZone::seconds TimeZone::to_local(TimeZone::seconds timestamp) const {
  date::sys_seconds timePoint{timestamp};

  // If this is an offset time zone.
  if (tz_ == nullptr) {
    return (timePoint + offset_).time_since_epoch();
  }
  return date::zoned_time{tz_, timePoint}.get_local_time().time_since_epoch();
}

} // namespace facebook::velox::tz
