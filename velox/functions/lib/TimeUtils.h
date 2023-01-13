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
#include "velox/external/date/tz.h"
#include "velox/functions/Macros.h"
#include "velox/type/Date.h"

namespace facebook::velox::functions {
namespace {
inline constexpr int64_t kSecondsInDay = 86'400;
inline constexpr int64_t kDaysInWeek = 7;

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
    timestamp.toTimezone(*timeZone);
    return timestamp.getSeconds();
  } else {
    return timestamp.getSeconds();
  }
}
} // namespace

FOLLY_ALWAYS_INLINE
std::tm getDateTime(Timestamp timestamp, const date::time_zone* timeZone) {
  int64_t seconds = getSeconds(timestamp, timeZone);
  std::tm dateTime;
  VELOX_USER_CHECK_NOT_NULL(
      gmtime_r((const time_t*)&seconds, &dateTime),
      "Timestamp is too large: {} seconds since epoch",
      seconds);
  return dateTime;
}

FOLLY_ALWAYS_INLINE
std::tm getDateTime(Date date) {
  int64_t seconds = date.days() * kSecondsInDay;
  std::tm dateTime;
  VELOX_USER_CHECK_NOT_NULL(
      gmtime_r((const time_t*)&seconds, &dateTime),
      "Date is too large: {} days",
      date.days());
  return dateTime;
}

template <typename T>
struct InitSessionTimezone {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  const date::time_zone* timeZone_{nullptr};

  FOLLY_ALWAYS_INLINE void initialize(
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/) {
    timeZone_ = getTimeZoneFromConfig(config);
  }
};
} // namespace facebook::velox::functions
