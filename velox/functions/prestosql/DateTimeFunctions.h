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
#include "velox/external/date/tz.h"
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/DateTimeImpl.h"

namespace facebook::velox::functions {

VELOX_UDF_BEGIN(to_unixtime)
FOLLY_ALWAYS_INLINE bool call(
    double& result,
    const arg_type<Timestamp>& timestamp) {
  result = toUnixtime(timestamp);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(from_unixtime)
FOLLY_ALWAYS_INLINE bool call(
    Timestamp& result,
    const arg_type<double>& unixtime) {
  auto resultOptional = fromUnixtime(unixtime);
  if (LIKELY(resultOptional.has_value())) {
    result = resultOptional.value();
    return true;
  }
  return false;
}
VELOX_UDF_END();

namespace {
FOLLY_ALWAYS_INLINE const date::time_zone* getTimeZoneFromConfig(
    const core::QueryConfig& config) {
  if (config.adjustTimestampToTimezone()) {
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      return date::locate_zone(sessionTzName);
    }
  }
  return nullptr;
}

FOLLY_ALWAYS_INLINE int64_t
getSeconds(Timestamp timestamp, const date::time_zone* timeZone) {
  if (timeZone != nullptr) {
    timestamp.toTimezoneUTC(*timeZone);
    return timestamp.getSeconds();
  } else {
    return timestamp.getSeconds();
  }
}
} // namespace

VELOX_UDF_BEGIN(year)
const date::time_zone* timeZone_ = nullptr;

FOLLY_ALWAYS_INLINE void initialize(const core::QueryConfig& config) {
  timeZone_ = getTimeZoneFromConfig(config);
}

FOLLY_ALWAYS_INLINE bool call(
    int64_t& result,
    const arg_type<Timestamp>& timestamp) {
  int64_t seconds = getSeconds(timestamp, timeZone_);
  std::tm dateTime;
  gmtime_r((const time_t*)&seconds, &dateTime);
  result = 1900 + dateTime.tm_year;
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(month)
const date::time_zone* timeZone_ = nullptr;

FOLLY_ALWAYS_INLINE void initialize(const core::QueryConfig& config) {
  timeZone_ = getTimeZoneFromConfig(config);
}

FOLLY_ALWAYS_INLINE bool call(
    int64_t& result,
    const arg_type<Timestamp>& timestamp) {
  int64_t seconds = getSeconds(timestamp, timeZone_);
  std::tm dateTime;
  gmtime_r((const time_t*)&seconds, &dateTime);
  result = 1 + dateTime.tm_mon;
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(day)
const date::time_zone* timeZone_ = nullptr;

FOLLY_ALWAYS_INLINE void initialize(const core::QueryConfig& config) {
  timeZone_ = getTimeZoneFromConfig(config);
}

FOLLY_ALWAYS_INLINE bool call(
    int64_t& result,
    const arg_type<Timestamp>& timestamp) {
  int64_t seconds = getSeconds(timestamp, timeZone_);
  std::tm dateTime;
  gmtime_r((const time_t*)&seconds, &dateTime);
  result = dateTime.tm_mday;
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(hour)
const date::time_zone* timeZone_ = nullptr;

FOLLY_ALWAYS_INLINE void initialize(const core::QueryConfig& config) {
  timeZone_ = getTimeZoneFromConfig(config);
}

FOLLY_ALWAYS_INLINE bool call(
    int64_t& result,
    const arg_type<Timestamp>& timestamp) {
  int64_t seconds = getSeconds(timestamp, timeZone_);
  std::tm dateTime;
  gmtime_r((const time_t*)&seconds, &dateTime);
  result = dateTime.tm_hour;
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(minute)
const date::time_zone* timeZone_ = nullptr;

FOLLY_ALWAYS_INLINE void initialize(const core::QueryConfig& config) {
  timeZone_ = getTimeZoneFromConfig(config);
}

FOLLY_ALWAYS_INLINE bool call(
    int64_t& result,
    const arg_type<Timestamp>& timestamp) {
  int64_t seconds = getSeconds(timestamp, timeZone_);
  std::tm dateTime;
  gmtime_r((const time_t*)&seconds, &dateTime);
  result = dateTime.tm_min;
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(second)
FOLLY_ALWAYS_INLINE bool call(
    int64_t& result,
    const arg_type<Timestamp>& timestamp) {
  int64_t seconds = timestamp.getSeconds();
  std::tm dateTime;
  gmtime_r((const time_t*)&seconds, &dateTime);
  result = dateTime.tm_sec;
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(millisecond)
FOLLY_ALWAYS_INLINE bool call(
    int64_t& result,
    const arg_type<Timestamp>& timestamp) {
  result = timestamp.getNanos() / kNanosecondsInMilliseconds;
  return true;
}
VELOX_UDF_END();
} // namespace facebook::velox::functions
