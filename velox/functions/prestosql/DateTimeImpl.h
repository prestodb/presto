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
#include "velox/type/TimestampConversion.h"

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

  // On some compilers if we cast kMax to a double, we can get a number larger
  // than 'kMax'. This will allow 'unixtime' values > 'kMax'. The workaround
  // here is to use uint64_t to represent ('kMax' + 1), which can be represented
  // exactly as double. We then check if the difference with 'unixtime' <= 1.
  if (UNLIKELY((static_cast<uint64_t>(kMax) + 1) - unixtime <= 1)) {
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
Date addToDate(const Date& date, const DateTimeUnit unit, const int32_t value) {
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
    const DateTimeUnit unit,
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

FOLLY_ALWAYS_INLINE int64_t diffTimestamp(
    const DateTimeUnit unit,
    const Timestamp& fromTimestamp,
    const Timestamp& toTimestamp) {
  // TODO(gaoge): Handle overflow and underflow with 64-bit representation
  if (fromTimestamp == toTimestamp) {
    return 0;
  }

  int8_t sign = fromTimestamp < toTimestamp ? 1 : -1;

  // fromTimepoint is less than or equal to toTimepoint
  const std::chrono::
      time_point<std::chrono::system_clock, std::chrono::milliseconds>
          fromTimepoint(std::chrono::milliseconds(
              std::min(fromTimestamp, toTimestamp).toMillis()));
  const std::chrono::
      time_point<std::chrono::system_clock, std::chrono::milliseconds>
          toTimepoint(std::chrono::milliseconds(
              std::max(fromTimestamp, toTimestamp).toMillis()));

  // Millisecond, second, minute, hour and day have fixed conversion ratio
  switch (unit) {
    case DateTimeUnit::kMillisecond: {
      return sign *
          std::chrono::duration_cast<std::chrono::milliseconds>(
              toTimepoint - fromTimepoint)
              .count();
    }
    case DateTimeUnit::kSecond: {
      return sign *
          std::chrono::duration_cast<std::chrono::seconds>(
              toTimepoint - fromTimepoint)
              .count();
    }
    case DateTimeUnit::kMinute: {
      return sign *
          std::chrono::duration_cast<std::chrono::minutes>(
              toTimepoint - fromTimepoint)
              .count();
    }
    case DateTimeUnit::kHour: {
      return sign *
          std::chrono::duration_cast<std::chrono::hours>(
              toTimepoint - fromTimepoint)
              .count();
    }
    case DateTimeUnit::kDay: {
      return sign *
          std::chrono::duration_cast<date::days>(toTimepoint - fromTimepoint)
              .count();
    }
    default:
      break;
  }

  // Month, quarter and year do not have fixed conversion ratio. Ex. a month can
  // have 28, 29, 30 or 31 days. A year can have 365 or 366 days.
  const std::chrono::time_point<std::chrono::system_clock, date::days>
      fromDaysTimepoint = std::chrono::floor<date::days>(fromTimepoint);
  const std::chrono::time_point<std::chrono::system_clock, date::days>
      toDaysTimepoint = std::chrono::floor<date::days>(toTimepoint);
  const date::year_month_day fromCalDate(fromDaysTimepoint);
  const date::year_month_day toCalDate(toDaysTimepoint);
  const uint64_t fromTimeInstantOfDay =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          fromTimepoint - fromDaysTimepoint)
          .count();
  const uint64_t toTimeInstantOfDay =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          toTimepoint - toDaysTimepoint)
          .count();
  const uint8_t fromDay = static_cast<unsigned>(fromCalDate.day()),
                fromMonth = static_cast<unsigned>(fromCalDate.month());
  const uint8_t toDay = static_cast<unsigned>(toCalDate.day()),
                toMonth = static_cast<unsigned>(toCalDate.month());
  const date::year_month_day toCalLastYearMonthDay(
      toCalDate.year() / toCalDate.month() / date::last);
  const uint8_t toLastYearMonthDay =
      static_cast<unsigned>(toCalLastYearMonthDay.day());

  int64_t diff;
  if (unit == DateTimeUnit::kMonth || unit == DateTimeUnit::kQuarter) {
    diff = (int(toCalDate.year()) - int(fromCalDate.year())) * 12 +
        int(toMonth) - int(fromMonth);

    if ((toDay != toLastYearMonthDay && fromDay > toDay) ||
        (fromDay == toDay && fromTimeInstantOfDay > toTimeInstantOfDay)) {
      diff--;
    }

    diff = (unit == DateTimeUnit::kMonth) ? diff : diff / 3;
    return sign * diff;
  }

  if (unit == DateTimeUnit::kYear) {
    diff = (toCalDate.year() - fromCalDate.year()).count();

    if (fromMonth > toMonth ||
        (fromMonth == toMonth && fromDay > toDay &&
         toDay != toLastYearMonthDay) ||
        (fromMonth == toMonth && fromDay == toDay &&
         fromTimeInstantOfDay > toTimeInstantOfDay)) {
      diff--;
    }
    return sign * diff;
  }

  VELOX_UNREACHABLE();
}

FOLLY_ALWAYS_INLINE
int64_t
diffDate(const DateTimeUnit unit, const Date& fromDate, const Date& toDate) {
  if (fromDate == toDate) {
    return 0;
  }
  return diffTimestamp(
      unit,
      Timestamp(fromDate.days() * util::kSecsPerDay, 0),
      Timestamp(toDate.days() * util::kSecsPerDay, 0));
}
} // namespace facebook::velox::functions
