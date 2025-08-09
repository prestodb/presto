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

#include "velox/external/date/date.h"
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/type/Timestamp.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {

/// Returns toTimestamp - fromTimestamp expressed in terms of unit.
/// @param respectLastDay If true, the last day of a year-month is respected.
/// For example, the difference between '2020-01-31' and '2020-02-01' is 1 day.
/// If false, the last day of a year-month is not respected. For example, the
/// difference between '2020-01-31' and '2020-02-01' is 0 day.
/// This is useful for Spark compatibility, as Spark does not respect the last
/// day of a year-month.
FOLLY_ALWAYS_INLINE int64_t diffTimestamp(
    DateTimeUnit unit,
    const Timestamp& fromTimestamp,
    const Timestamp& toTimestamp,
    bool respectLastDay = true) {
  // TODO: Handle overflow and underflow with 64-bit representation.
  if (fromTimestamp == toTimestamp) {
    return 0;
  }

  const int8_t sign = fromTimestamp < toTimestamp ? 1 : -1;

  // The microsecond unit is handled independently to prevent overflow while
  // converting all timestamps to microseconds for each unit.
  if (unit == DateTimeUnit::kMicrosecond) {
    const std::chrono::time_point<std::chrono::system_clock>
        fromMicrosecondpoint(std::chrono::microseconds(
            std::min(fromTimestamp, toTimestamp).toMicros()));
    const std::chrono::time_point<std::chrono::system_clock> toMicrosecondpoint(
        std::chrono::microseconds(
            std::max(fromTimestamp, toTimestamp).toMicros()));
    return sign *
        std::chrono::duration_cast<std::chrono::microseconds>(
            toMicrosecondpoint - fromMicrosecondpoint)
            .count();
  }

  // fromTimepoint is less than or equal to toTimepoint.
  const std::chrono::
      time_point<std::chrono::system_clock, std::chrono::milliseconds>
          fromTimepoint(std::chrono::milliseconds(
              std::min(fromTimestamp, toTimestamp).toMillis()));
  const std::chrono::
      time_point<std::chrono::system_clock, std::chrono::milliseconds>
          toTimepoint(std::chrono::milliseconds(
              std::max(fromTimestamp, toTimestamp).toMillis()));

  // Millisecond, second, minute, hour and day have fixed conversion ratio.
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
    case DateTimeUnit::kWeek: {
      return sign *
          std::chrono::duration_cast<date::days>(toTimepoint - fromTimepoint)
              .count() /
          7;
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

  uint64_t toTimeInstantOfDay = 0;
  uint64_t toTimePointMillis = toTimepoint.time_since_epoch().count();
  uint64_t toDaysTimepointMillis =
      std::chrono::
          time_point<std::chrono::system_clock, std::chrono::milliseconds>(
              toDaysTimepoint)
              .time_since_epoch()
              .count();
  bool overflow = __builtin_sub_overflow(
      toTimePointMillis, toDaysTimepointMillis, &toTimeInstantOfDay);
  VELOX_USER_CHECK_EQ(
      overflow,
      false,
      "{} - {} Causes arithmetic overflow: {} - {}",
      fromTimestamp.toString(),
      toTimestamp.toString(),
      toTimePointMillis,
      toDaysTimepointMillis);
  const uint8_t fromDay = static_cast<unsigned>(fromCalDate.day()),
                fromMonth = static_cast<unsigned>(fromCalDate.month());
  const uint8_t toDay = static_cast<unsigned>(toCalDate.day()),
                toMonth = static_cast<unsigned>(toCalDate.month());
  const date::year_month_day toCalLastYearMonthDay(
      toCalDate.year() / toCalDate.month() / date::last);
  const uint8_t toLastYearMonthDay =
      static_cast<unsigned>(toCalLastYearMonthDay.day());

  if (unit == DateTimeUnit::kMonth || unit == DateTimeUnit::kQuarter) {
    int64_t diff =
        (int64_t(toCalDate.year()) - int64_t(fromCalDate.year())) * 12 +
        int(toMonth) - int(fromMonth);

    if (((!respectLastDay || toDay != toLastYearMonthDay) && fromDay > toDay) ||
        (fromDay == toDay && fromTimeInstantOfDay > toTimeInstantOfDay)) {
      diff--;
    }

    diff = (unit == DateTimeUnit::kMonth) ? diff : diff / 3;
    return sign * diff;
  }

  if (unit == DateTimeUnit::kYear) {
    int64_t diff = (toCalDate.year() - fromCalDate.year()).count();

    if (fromMonth > toMonth ||
        (fromMonth == toMonth && fromDay > toDay &&
         (!respectLastDay || toDay != toLastYearMonthDay)) ||
        (fromMonth == toMonth && fromDay == toDay &&
         fromTimeInstantOfDay > toTimeInstantOfDay)) {
      diff--;
    }
    return sign * diff;
  }

  VELOX_UNREACHABLE();
}

FOLLY_ALWAYS_INLINE int64_t diffTimestamp(
    DateTimeUnit unit,
    const Timestamp& fromTimestamp,
    const Timestamp& toTimestamp,
    const tz::TimeZone* timeZone,
    bool respectLastDay = true) {
  if (LIKELY(timeZone != nullptr)) {
    // sessionTimeZone not null means that the config
    // adjust_timestamp_to_timezone is on.
    Timestamp fromZonedTimestamp = fromTimestamp;
    fromZonedTimestamp.toTimezone(*timeZone);

    Timestamp toZonedTimestamp = toTimestamp;
    if (isTimeUnit(unit)) {
      const int64_t offset =
          static_cast<Timestamp>(fromTimestamp).getSeconds() -
          fromZonedTimestamp.getSeconds();
      toZonedTimestamp = Timestamp(
          toZonedTimestamp.getSeconds() - offset, toZonedTimestamp.getNanos());
    } else {
      toZonedTimestamp.toTimezone(*timeZone);
    }
    return diffTimestamp(
        unit, fromZonedTimestamp, toZonedTimestamp, respectLastDay);
  }
  return diffTimestamp(unit, fromTimestamp, toTimestamp, respectLastDay);
}

/// Year, quarter or month are not uniformly incremented. Months have different
/// total days, and leap years have more days than the rest. If the new year,
/// quarter or month has less total days than the given one, it will be coerced
/// to use the valid last day of the new month. This could result in weird
/// arithmetic behavior. For example,
///
/// 2022-01-30 + (1 month) = 2022-02-28
/// 2022-02-28 - (1 month) = 2022-01-28
///
/// 2022-08-31 + (1 quarter) = 2022-11-30
/// 2022-11-30 - (1 quarter) = 2022-08-30
///
/// 2020-02-29 + (1 year) = 2021-02-28
/// 2021-02-28 - (1 year) = 2020-02-28
FOLLY_ALWAYS_INLINE
int32_t addToDate(int32_t input, DateTimeUnit unit, int32_t value) {
  // TODO: Handle overflow and underflow with 64-bit representation
  if (value == 0) {
    return input;
  }

  const std::chrono::time_point<std::chrono::system_clock, date::days> inDate{
      date::days(input)};
  std::chrono::time_point<std::chrono::system_clock, date::days> outDate;

  if (unit == DateTimeUnit::kDay) {
    outDate = inDate + date::days(value);
  } else if (unit == DateTimeUnit::kWeek) {
    outDate = inDate + date::days(value * 7);
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

  return outDate.time_since_epoch().count();
}

FOLLY_ALWAYS_INLINE Timestamp
addToTimestamp(const Timestamp& timestamp, DateTimeUnit unit, int32_t value) {
  // TODO: Handle overflow and underflow with 64-bit representation.
  if (value == 0) {
    return timestamp;
  }

  // The microsecond unit is handled independently to prevent overflow while
  // converting all timestamps to microseconds for each unit.
  if (unit == DateTimeUnit::kMicrosecond) {
    const std::chrono::time_point<std::chrono::system_clock> inMicroTimestamp(
        std::chrono::microseconds(timestamp.toMicros()));
    const std::chrono::time_point<std::chrono::system_clock> outMicroTimestamp =
        inMicroTimestamp + std::chrono::microseconds(value);
    const Timestamp microTimestamp = Timestamp::fromMicros(
        std::chrono::duration_cast<std::chrono::microseconds>(
            outMicroTimestamp.time_since_epoch())
            .count());
    return Timestamp(
        microTimestamp.getSeconds(),
        microTimestamp.getNanos() +
            timestamp.getNanos() % Timestamp::kNanosecondsInMicrosecond);
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
      const int32_t inDate =
          std::chrono::duration_cast<date::days>(inTimestamp.time_since_epoch())
              .count();
      const int32_t outDate = addToDate(inDate, unit, value);
      outTimestamp =
          inTimestamp + date::days(checkedMinus<int32_t>(outDate, inDate));
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
    case DateTimeUnit::kWeek: {
      const int32_t inDate =
          std::chrono::duration_cast<date::days>(inTimestamp.time_since_epoch())
              .count();
      const int32_t outDate = addToDate(inDate, DateTimeUnit::kDay, 7 * value);

      outTimestamp = inTimestamp + date::days(outDate - inDate);
      break;
    }
    default:
      VELOX_UNREACHABLE("Unsupported datetime unit");
  }

  Timestamp milliTimestamp =
      Timestamp::fromMillis(outTimestamp.time_since_epoch().count());
  return Timestamp(
      milliTimestamp.getSeconds(),
      milliTimestamp.getNanos() +
          timestamp.getNanos() % Timestamp::kNanosecondsInMillisecond);
}

FOLLY_ALWAYS_INLINE Timestamp addToTimestamp(
    DateTimeUnit unit,
    int32_t value,
    const Timestamp& timestamp,
    const tz::TimeZone* timeZone) {
  Timestamp result;
  if (LIKELY(timeZone != nullptr)) {
    // timeZone not null means that the config
    // adjust_timestamp_to_timezone is on.
    Timestamp zonedTimestamp = timestamp;
    zonedTimestamp.toTimezone(*timeZone);

    Timestamp resultTimestamp = addToTimestamp(zonedTimestamp, unit, value);

    if (isTimeUnit(unit)) {
      const int64_t offset =
          timestamp.getSeconds() - zonedTimestamp.getSeconds();
      result = Timestamp(
          resultTimestamp.getSeconds() + offset, resultTimestamp.getNanos());
    } else {
      result = Timestamp(
          timeZone
              ->correct_nonexistent_time(
                  std::chrono::seconds(resultTimestamp.getSeconds()))
              .count(),
          resultTimestamp.getNanos());
      result.toGMT(*timeZone);
    }
  } else {
    result = addToTimestamp(timestamp, unit, value);
  }
  return result;
}

} // namespace facebook::velox::functions
