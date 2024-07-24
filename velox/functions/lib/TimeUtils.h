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

#include <velox/type/Timestamp.h>
#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {

inline constexpr int64_t kSecondsInDay = 86'400;
inline constexpr int64_t kDaysInWeek = 7;
extern const folly::F14FastMap<std::string, int8_t> kDayOfWeekNames;

FOLLY_ALWAYS_INLINE const tz::TimeZone* getTimeZoneFromConfig(
    const core::QueryConfig& config) {
  if (config.adjustTimestampToTimezone()) {
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      return tz::locateZone(sessionTzName);
    }
  }
  return nullptr;
}

FOLLY_ALWAYS_INLINE int64_t
getSeconds(Timestamp timestamp, const tz::TimeZone* timeZone) {
  if (timeZone != nullptr) {
    timestamp.toTimezone(*timeZone);
    return timestamp.getSeconds();
  } else {
    return timestamp.getSeconds();
  }
}

FOLLY_ALWAYS_INLINE
std::tm getDateTime(Timestamp timestamp, const tz::TimeZone* timeZone) {
  int64_t seconds = getSeconds(timestamp, timeZone);
  std::tm dateTime;
  VELOX_USER_CHECK(
      Timestamp::epochToCalendarUtc(seconds, dateTime),
      "Timestamp is too large: {} seconds since epoch",
      seconds);
  return dateTime;
}

// days is the number of days since Epoch.
FOLLY_ALWAYS_INLINE
std::tm getDateTime(int32_t days) {
  int64_t seconds = days * kSecondsInDay;
  std::tm dateTime;
  VELOX_USER_CHECK(
      Timestamp::epochToCalendarUtc(seconds, dateTime),
      "Date is too large: {} days",
      days);
  return dateTime;
}

FOLLY_ALWAYS_INLINE int getYear(const std::tm& time) {
  // tm_year: years since 1900.
  return 1900 + time.tm_year;
}

FOLLY_ALWAYS_INLINE int getMonth(const std::tm& time) {
  // tm_mon: months since January – [0, 11].
  return 1 + time.tm_mon;
}

FOLLY_ALWAYS_INLINE int getDay(const std::tm& time) {
  return time.tm_mday;
}

FOLLY_ALWAYS_INLINE int32_t getQuarter(const std::tm& time) {
  return time.tm_mon / 3 + 1;
}

FOLLY_ALWAYS_INLINE int32_t getDayOfYear(const std::tm& time) {
  return time.tm_yday + 1;
}

template <typename T>
struct InitSessionTimezone {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  const tz::TimeZone* timeZone_{nullptr};

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/) {
    timeZone_ = getTimeZoneFromConfig(config);
  }
};
} // namespace facebook::velox::functions
