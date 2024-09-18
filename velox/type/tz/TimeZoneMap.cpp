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
#include "velox/common/testutil/TestValue.h"
#include "velox/external/date/tz.h"

using facebook::velox::common::testutil::TestValue;

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

const date::time_zone* locateZoneImpl(std::string_view tz_name) {
  TestValue::adjust("facebook::velox::tz::locateZoneImpl", &tz_name);
  const date::time_zone* zone = date::locate_zone(tz_name);
  return zone;
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
      timeZonePtr =
          std::make_unique<TimeZone>("UTC", entry.first, locateZoneImpl("UTC"));
    } else if (entry.first <= 1680) {
      std::chrono::minutes offset = getTimeZoneOffset(entry.first);
      timeZonePtr =
          std::make_unique<TimeZone>(entry.second, entry.first, offset);
    }
    // Every single other time zone entry (outside of offsets) needs to be
    // available in external/date or this will throw.
    else {
      const date::time_zone* zone;
      try {
        zone = locateZoneImpl(entry.second);
      } catch (date::invalid_timezone& err) {
        // When this exception is thrown, it typically means the time zone name
        // we are trying to locate cannot be found from OS's time zone database.
        // Thus, we just command a "continue;" to skip the creation of the
        // corresponding TimeZone object.
        //
        // Then, once this time zone is requested at runtime, a runtime error
        // will be thrown and caller is expected to handle that error in code.
        LOG(WARNING) << "Unable to load [" << entry.second
                     << "] from local timezone database: " << err.what();
        continue;
      }
      timeZonePtr = std::make_unique<TimeZone>(entry.second, entry.first, zone);
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

inline bool isTimeZoneOffset(std::string_view str) {
  return str.size() >= 3 && (str[0] == '+' || str[0] == '-');
}

// The timezone parsing logic follows what is defined here:
//   https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
inline bool isUtcEquivalentName(std::string_view zone) {
  static folly::F14FastSet<std::string> utcSet = {
      "utc", "uct", "gmt", "gmt0", "greenwich", "universal", "zulu", "z"};
  return utcSet.find(zone) != utcSet.end();
}

// This function tries to apply two normalization rules to time zone offsets:
//
// 1. If the offset only defines the hours portion, assume minutes are zeroed
// out (e.g. "+00" -> "+00:00")
//
// 2. Check if the ':' in between in missing; if so, correct the offset string
// (e.g. "+0000" -> "+00:00").
//
// This function assumes the first character is either '+' or '-'.
std::string normalizeTimeZoneOffset(const std::string& zoneOffset) {
  if (zoneOffset.size() == 3 && isDigit(zoneOffset[1]) &&
      isDigit(zoneOffset[2])) {
    return zoneOffset + ":00";
  } else if (
      zoneOffset.size() == 5 && isDigit(zoneOffset[1]) &&
      isDigit(zoneOffset[2]) && isDigit(zoneOffset[3]) &&
      isDigit(zoneOffset[4])) {
    return zoneOffset.substr(0, 3) + ':' + zoneOffset.substr(3, 2);
  }
  return zoneOffset;
}

std::string normalizeTimeZone(const std::string& originalZoneId) {
  // If this is an offset that hasn't matched, check if this is an incomplete
  // offset.
  if (isTimeZoneOffset(originalZoneId)) {
    return normalizeTimeZoneOffset(originalZoneId);
  }

  // Otherwise, try other time zone name normalizations.
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

template <typename TDuration>
void validateRangeImpl(time_point<TDuration> timePoint) {
  using namespace velox::date;
  static constexpr auto kMinYear = date::year::min();
  static constexpr auto kMaxYear = date::year::max();

  auto year = year_month_day(floor<days>(timePoint)).year();

  if (year < kMinYear || year > kMaxYear) {
    // This is a special case where we intentionally throw
    // VeloxRuntimeError to avoid it being suppressed by TRY().
    VELOX_FAIL_UNSUPPORTED_INPUT_UNCATCHABLE(
        "Timepoint is outside of supported year range: [{}, {}], got {}",
        (int)kMinYear,
        (int)kMaxYear,
        (int)year);
  }
}

} // namespace

void validateRange(time_point<std::chrono::seconds> timePoint) {
  validateRangeImpl(timePoint);
}

void validateRange(time_point<std::chrono::milliseconds> timePoint) {
  validateRangeImpl(timePoint);
}

std::string getTimeZoneName(int64_t timeZoneID) {
  return locateZone(timeZoneID, true)->name();
}

const TimeZone* locateZone(int16_t timeZoneID, bool failOnError) {
  const auto& timeZoneDatabase = getTimeZoneDatabase();

  // Check if timeZoneID does not exceed the vector size or is one of the
  // "holes".
  if (timeZoneID > timeZoneDatabase.size() ||
      timeZoneDatabase[timeZoneID] == nullptr) {
    if (failOnError) {
      VELOX_FAIL("Unable to resolve timeZoneID '{}'", timeZoneID);
    }
    return nullptr;
  }
  return timeZoneDatabase[timeZoneID].get();
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
  validateRange(date::sys_seconds{timestamp});

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
  validateRange(timePoint);

  // If this is an offset time zone.
  if (tz_ == nullptr) {
    return (timePoint + offset_).time_since_epoch();
  }
  return date::zoned_time{tz_, timePoint}.get_local_time().time_since_epoch();
}

} // namespace facebook::velox::tz
