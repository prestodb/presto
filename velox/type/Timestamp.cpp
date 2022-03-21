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
#include "velox/type/Timestamp.h"
#include <chrono>
#include "velox/common/base/Exceptions.h"
#include "velox/external/date/tz.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox {
namespace {

inline int64_t deltaWithTimezone(const date::time_zone& zone, int64_t seconds) {
  auto tp = std::chrono::time_point<std::chrono::system_clock>(
      std::chrono::seconds(seconds));
  auto epoch = zone.to_local(tp).time_since_epoch();
  int64_t delta =
      seconds - std::chrono::duration_cast<std::chrono::seconds>(epoch).count();
  return delta;
}

// Assuming tzID is in [1, 1680] range.
// tzID - PrestoDB time zone ID.
inline int64_t getPrestoTZOffsetInSeconds(int16_t tzID) {
  // TODO(spershin): Maybe we need something better if we can (we could use
  //  precomputed vector for PrestoDB timezones, for instance).

  // PrestoDb time zone ids require some custom code.
  // Mapping is 1-based and covers [-14:00, +14:00] range without 00:00.
  return ((tzID <= 840) ? (tzID - 841) : (tzID - 840)) * 60;
}

} // namespace

void Timestamp::toGMT(const date::time_zone& zone) {
  // Magic number -2^39 + 24*3600. This number and any number lower than that
  // will cause time_zone::to_sys() to SIGABRT. We don't want that to happen.
  if (seconds_ <= (-1096193779200l + 86400l)) {
    VELOX_UNSUPPORTED(
        "Timestamp out of bound for time zone adjustment {} seconds", seconds_);
  }
  date::local_time<std::chrono::seconds> localTime{
      std::chrono::seconds(seconds_)};
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>
      sysTime = zone.to_sys(localTime, date::choose::latest);
  seconds_ = sysTime.time_since_epoch().count();
}

void Timestamp::toGMT(int16_t tzID) {
  if (tzID == 0) {
    // No conversion required for time zone id 0, as it is '+00:00'.
  } else if (tzID <= 1680) {
    seconds_ -= getPrestoTZOffsetInSeconds(tzID);
  } else {
    // Other ids go this path.
    toGMT(*date::locate_zone(util::getTimeZoneName(tzID)));
  }
}

void Timestamp::toTimezone(const date::time_zone& zone) {
  seconds_ -= deltaWithTimezone(zone, seconds_);
}

void Timestamp::toTimezone(int16_t tzID) {
  if (tzID == 0) {
    // No conversion required for time zone id 0, as it is '+00:00'.
  } else if (tzID <= 1680) {
    seconds_ += getPrestoTZOffsetInSeconds(tzID);
  } else {
    // Other ids go this path.
    toTimezone(*date::locate_zone(util::getTimeZoneName(tzID)));
  }
}

void parseTo(folly::StringPiece in, ::facebook::velox::Timestamp& out) {
  // TODO Implement
}

} // namespace facebook::velox

namespace std {
std::string to_string(const ::facebook::velox::Timestamp& ts) {
  return ts.toString();
}

} // namespace std
