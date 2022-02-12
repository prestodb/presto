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
#include <optional>
#include "velox/external/date/date.h"
#include "velox/type/Date.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox::functions {
namespace {
constexpr double kNanosecondsInSecond = 1'000'000'000;
constexpr int64_t kNanosecondsInMillisecond = 1'000'000;
constexpr int64_t kMillisecondsInSecond = 1'000;
} // namespace

FOLLY_ALWAYS_INLINE double toUnixtime(const Timestamp& timestamp) {
  double result = timestamp.getSeconds();
  result += static_cast<double>(timestamp.getNanos()) / kNanosecondsInSecond;
  return result;
}

FOLLY_ALWAYS_INLINE std::optional<Timestamp> fromUnixtime(double unixtime) {
  if (UNLIKELY(std::isnan(unixtime))) {
    return Timestamp(0, 0);
  }

  static const int64_t kMax = std::numeric_limits<int64_t>::max();
  static const int64_t kMin = std::numeric_limits<int64_t>::min();

  static const Timestamp kMaxTimestamp(
      kMax / 1000, kMax % 1000 * kNanosecondsInMillisecond);
  static const Timestamp kMinTimestamp(
      kMin / 1000 - 1, (kMin % 1000 + 1000) * kNanosecondsInMillisecond);

  if (UNLIKELY(unixtime >= kMax)) {
    return kMaxTimestamp;
  }

  if (UNLIKELY(unixtime <= kMin)) {
    return kMinTimestamp;
  }

  if (UNLIKELY(std::isinf(unixtime))) {
    return unixtime < 0 ? kMinTimestamp : kMaxTimestamp;
  }

  auto seconds = std::floor(unixtime);
  auto nanos = unixtime - seconds;
  return Timestamp(seconds, nanos * kNanosecondsInSecond);
}

namespace {
enum class DateTimeUnit {
  kMillisecond,
  kSecond,
  kMinute,
  kHour,
  kDay,
  kMonth,
  kQuarter,
  kYear
};
} // namespace

// Year, quarter or month are not uniformly incremented. Months have different
// total days, and leap years have more days than the rest. If the new year,
// quarter or month has less total days than the given one, it will be coerced
// to use the valid last day of the new month. This could result in weird
// arithmetic behavior. For example,
//
// 2022-01-30 + (1 month) = 2022-02-28
// 2022-02-28 - (1 month) = 2022-01-28
//
// 2022-08-31 + (1 quarter) = 2022-11-30
// 2022-11-30 - (1 quarter) = 2022-08-30
//
// 2020-02-29 + (1 year) = 2021-02-28
// 2021-02-28 - (1 year) = 2020-02-28
FOLLY_ALWAYS_INLINE
Date addToDate(
    const Date& date,
    const DateTimeUnit& unit,
    const int32_t value) {
  // TODO(gaoge): Handle overflow and underflow with 64-bit representation
  if (value == 0) {
    return date;
  }

  const std::chrono::time_point<std::chrono::system_clock, date::days> inDate(
      date::days(date.days()));
  std::chrono::time_point<std::chrono::system_clock, date::days> outDate;

  if (unit == DateTimeUnit::kDay) {
    outDate = inDate + date::days(value);
  } else {
    const date::year_month_day inCalDate(inDate);
    date::year_month_day outCalDate;

    if (unit == DateTimeUnit::kMonth) {
      outCalDate = inCalDate + date::months(value);
    } else if (unit == DateTimeUnit::kQuarter) {
      outCalDate = inCalDate + date::months(3 * value);
    } else if (unit == DateTimeUnit::kYear) {
      outCalDate = inCalDate + date::years(value);
    } else {
      VELOX_UNREACHABLE();
    }

    if (!outCalDate.ok()) {
      outCalDate = outCalDate.year() / outCalDate.month() / date::last;
    }
    outDate = date::sys_days{outCalDate};
  }

  return Date(outDate.time_since_epoch().count());
}

FOLLY_ALWAYS_INLINE Timestamp addToTimestamp(
    const Timestamp& timestamp,
    const DateTimeUnit& unit,
    const int32_t value) {
  // TODO(gaoge): Handle overflow and underflow with 64-bit representation
  if (value == 0) {
    return timestamp;
  }

  const std::chrono::
      time_point<std::chrono::system_clock, std::chrono::milliseconds>
          inTimestamp(std::chrono::milliseconds(timestamp.toMillis()));
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      outTimestamp;

  switch (unit) {
    // Year, quarter or month are not uniformly incremented in terms of number
    // of days. So we treat them differently.
    case DateTimeUnit::kYear:
    case DateTimeUnit::kQuarter:
    case DateTimeUnit::kMonth:
    case DateTimeUnit::kDay: {
      const Date inDate(
          std::chrono::duration_cast<date::days>(inTimestamp.time_since_epoch())
              .count());
      const Date outDate = addToDate(inDate, unit, value);

      outTimestamp = inTimestamp + date::days(outDate.days() - inDate.days());
      break;
    }
    case DateTimeUnit::kHour: {
      outTimestamp = inTimestamp + std::chrono::hours(value);
      break;
    }
    case DateTimeUnit::kMinute: {
      outTimestamp = inTimestamp + std::chrono::minutes(value);
      break;
    }
    case DateTimeUnit::kSecond: {
      outTimestamp = inTimestamp + std::chrono::seconds(value);
      break;
    }
    case DateTimeUnit::kMillisecond: {
      outTimestamp = inTimestamp + std::chrono::milliseconds(value);
      break;
    }
    default:
      VELOX_UNREACHABLE();
  }

  Timestamp milliTimestamp =
      Timestamp::fromMillis(outTimestamp.time_since_epoch().count());
  return Timestamp(
      milliTimestamp.getSeconds(),
      milliTimestamp.getNanos() +
          timestamp.getNanos() % kNanosecondsInMillisecond);
}
} // namespace facebook::velox::functions
