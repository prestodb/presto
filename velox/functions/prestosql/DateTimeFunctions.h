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

#include <string_view>
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/functions/prestosql/DateTimeImpl.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {

template <typename T>
struct ToUnixtimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      double& result,
      const arg_type<Timestamp>& timestamp) {
    result = toUnixtime(timestamp);
  }

  FOLLY_ALWAYS_INLINE void call(
      double& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    const auto milliseconds = unpackMillisUtc(timestampWithTimezone);
    result = (double)milliseconds / kMillisecondsInSecond;
  }
};

template <typename T>
struct FromUnixtimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

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
};

namespace {

template <typename T>
struct TimestampWithTimezoneSupport {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Convert timestampWithTimezone to a timestamp representing the moment at the
  // zone in timestampWithTimezone. If `asGMT` is set to true, return the GMT
  // time at the same moment.
  FOLLY_ALWAYS_INLINE
  Timestamp toTimestamp(
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      bool asGMT = false) {
    auto timestamp = unpackTimestampUtc(timestampWithTimezone);
    if (!asGMT) {
      timestamp.toTimezone(unpackZoneKeyId(timestampWithTimezone));
    }

    return timestamp;
  }

  // Get offset in seconds with GMT from timestampWithTimezone.
  FOLLY_ALWAYS_INLINE
  int64_t getGMTOffsetSec(
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    Timestamp inputTimeStamp = this->toTimestamp(timestampWithTimezone);
    // Create a copy of inputTimeStamp and convert it to GMT
    auto gmtTimeStamp = inputTimeStamp;
    gmtTimeStamp.toGMT(unpackZoneKeyId(timestampWithTimezone));

    // Get offset in seconds with GMT and convert to hour
    return (inputTimeStamp.getSeconds() - gmtTimeStamp.getSeconds());
  }
};

} // namespace

template <typename T>
struct DateFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* date) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* timestamp) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<TimestampWithTimezone>* timestampWithTimezone) {
    // Do nothing. Session timezone doesn't affect the result.
  }

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Date>& result, const arg_type<Varchar>& date) {
    auto days = util::fromDateString(date, util::ParseMode::kPrestoCast);
    if (days.hasError()) {
      return days.error();
    }

    result = days.value();
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Timestamp>& timestamp) {
    result = util::toDate(timestamp, timeZone_);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    result = util::toDate(this->toTimestamp(timestampWithTimezone), nullptr);
  }

 private:
  const date::time_zone* timeZone_ = nullptr;
};

template <typename T>
struct WeekFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getWeek(const std::tm& time) {
    // The computation of ISO week from date follows the algorithm here:
    // https://en.wikipedia.org/wiki/ISO_week_date
    int64_t week = floor(
                       10 + (time.tm_yday + 1) -
                       (time.tm_wday ? time.tm_wday : kDaysInWeek)) /
        kDaysInWeek;

    if (week == 0) {
      // Distance in days between the first day of the current year and the
      // Monday of the current week.
      auto mondayOfWeek =
          time.tm_yday + 1 - (time.tm_wday + kDaysInWeek - 1) % kDaysInWeek;
      // Distance in days between the first day and the first Monday of the
      // current year.
      auto firstMondayOfYear =
          1 + (mondayOfWeek + kDaysInWeek - 1) % kDaysInWeek;

      if ((util::isLeapYear(time.tm_year + 1900 - 1) &&
           firstMondayOfYear == 2) ||
          firstMondayOfYear == 3 || firstMondayOfYear == 4) {
        week = 53;
      } else {
        week = 52;
      }
    } else if (week == 53) {
      // Distance in days between the first day of the current year and the
      // Monday of the current week.
      auto mondayOfWeek =
          time.tm_yday + 1 - (time.tm_wday + kDaysInWeek - 1) % kDaysInWeek;
      auto daysInYear = util::isLeapYear(time.tm_year + 1900) ? 366 : 365;
      if (daysInYear - mondayOfWeek < 3) {
        week = 1;
      }
    }

    return week;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getWeek(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getWeek(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct YearFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getYear(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getYear(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getYear(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct QuarterFunction : public InitSessionTimezone<T>,
                         public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getQuarter(const std::tm& time) {
    return time.tm_mon / 3 + 1;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getQuarter(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getQuarter(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getQuarter(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct MonthFunction : public InitSessionTimezone<T>,
                       public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getMonth(const std::tm& time) {
    return 1 + time.tm_mon;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getMonth(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getMonth(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getMonth(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct DayFunction : public InitSessionTimezone<T>,
                     public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_mday;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_mday;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_mday;
  }
};

template <typename T>
struct DayFromIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<IntervalDayTime>& interval) {
    result = interval / kMillisInDay;
  }
};

template <typename T>
struct LastDayOfMonthFunction : public InitSessionTimezone<T>,
                                public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Timestamp>& timestamp) {
    auto dt = getDateTime(timestamp, this->timeZone_);
    int64_t daysSinceEpochFromDate;
    auto status =
        util::lastDayOfMonthSinceEpochFromDate(dt, daysSinceEpochFromDate);
    if (!status.ok()) {
      VELOX_DCHECK(status.isUserError());
      VELOX_USER_FAIL(status.message());
    }
    result = daysSinceEpochFromDate;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date) {
    auto dt = getDateTime(date);
    int64_t daysSinceEpochFromDate;
    auto status =
        util::lastDayOfMonthSinceEpochFromDate(dt, daysSinceEpochFromDate);
    if (!status.ok()) {
      VELOX_DCHECK(status.isUserError());
      VELOX_USER_FAIL(status.message());
    }
    result = daysSinceEpochFromDate;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    auto dt = getDateTime(timestamp, nullptr);
    int64_t daysSinceEpochFromDate;
    auto status =
        util::lastDayOfMonthSinceEpochFromDate(dt, daysSinceEpochFromDate);
    if (!status.ok()) {
      VELOX_DCHECK(status.isUserError());
      VELOX_USER_FAIL(status.message());
    }
    result = daysSinceEpochFromDate;
  }
};

namespace {

bool isIntervalWholeDays(int64_t milliseconds) {
  return (milliseconds % kMillisInDay) == 0;
}

int64_t intervalDays(int64_t milliseconds) {
  return milliseconds / kMillisInDay;
}

} // namespace

template <typename T>
struct DateMinusInterval {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    VELOX_USER_CHECK(
        isIntervalWholeDays(interval),
        "Cannot subtract hours, minutes, seconds or milliseconds from a date");
    result = addToDate(date, DateTimeUnit::kDay, -intervalDays(interval));
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToDate(date, DateTimeUnit::kMonth, -interval);
  }
};

template <typename T>
struct DatePlusInterval {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    VELOX_USER_CHECK(
        isIntervalWholeDays(interval),
        "Cannot add hours, minutes, seconds or milliseconds to a date");
    result = addToDate(date, DateTimeUnit::kDay, intervalDays(interval));
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToDate(date, DateTimeUnit::kMonth, interval);
  }
};

template <typename T>
struct TimestampMinusFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<IntervalDayTime>& result,
      const arg_type<Timestamp>& a,
      const arg_type<Timestamp>& b) {
    result = a.toMillis() - b.toMillis();
  }
};

template <typename T>
struct TimestampPlusInterval {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = Timestamp::fromMillisNoError(a.toMillis() + b);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToTimestamp(timestamp, DateTimeUnit::kMonth, interval);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<IntervalDayTime>& interval) {
    result = addToTimestampWithTimezone(
        timestampWithTimezone, DateTimeUnit::kMillisecond, interval);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToTimestampWithTimezone(
        timestampWithTimezone, DateTimeUnit::kMonth, interval);
  }
};

template <typename T>
struct IntervalPlusTimestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<Timestamp>& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = Timestamp::fromMillisNoError(a + b.toMillis());
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<IntervalYearMonth>& interval,
      const arg_type<Timestamp>& timestamp) {
    result = addToTimestamp(timestamp, DateTimeUnit::kMonth, interval);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<IntervalDayTime>& interval,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    result = addToTimestampWithTimezone(
        timestampWithTimezone, DateTimeUnit::kMillisecond, interval);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<IntervalYearMonth>& interval,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    result = addToTimestampWithTimezone(
        timestampWithTimezone, DateTimeUnit::kMonth, interval);
  }
};

template <typename T>
struct TimestampMinusInterval {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = Timestamp::fromMillisNoError(a.toMillis() - b);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToTimestamp(timestamp, DateTimeUnit::kMonth, -interval);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<IntervalDayTime>& interval) {
    result = addToTimestampWithTimezone(
        timestampWithTimezone, DateTimeUnit::kMillisecond, -interval);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToTimestampWithTimezone(
        timestampWithTimezone, DateTimeUnit::kMonth, -interval);
  }
};

template <typename T>
struct DayOfWeekFunction : public InitSessionTimezone<T>,
                           public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getDayOfWeek(const std::tm& time) {
    return time.tm_wday == 0 ? 7 : time.tm_wday;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDayOfWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDayOfWeek(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDayOfWeek(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct DayOfYearFunction : public InitSessionTimezone<T>,
                           public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getDayOfYear(const std::tm& time) {
    return time.tm_yday + 1;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDayOfYear(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDayOfYear(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDayOfYear(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct YearOfWeekFunction : public InitSessionTimezone<T>,
                            public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t computeYearOfWeek(const std::tm& dateTime) {
    int isoWeekDay = dateTime.tm_wday == 0 ? 7 : dateTime.tm_wday;
    // The last few days in December may belong to the next year if they are
    // in the same week as the next January 1 and this January 1 is a Thursday
    // or before.
    if (UNLIKELY(
            dateTime.tm_mon == 11 && dateTime.tm_mday >= 29 &&
            dateTime.tm_mday - isoWeekDay >= 31 - 3)) {
      return 1900 + dateTime.tm_year + 1;
    }
    // The first few days in January may belong to the last year if they are
    // in the same week as January 1 and January 1 is a Friday or after.
    else if (UNLIKELY(
                 dateTime.tm_mon == 0 && dateTime.tm_mday <= 3 &&
                 isoWeekDay - (dateTime.tm_mday - 1) >= 5)) {
      return 1900 + dateTime.tm_year - 1;
    } else {
      return 1900 + dateTime.tm_year;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = computeYearOfWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = computeYearOfWeek(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = computeYearOfWeek(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct HourFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_hour;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_hour;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_hour;
  }
};

template <typename T>
struct MinuteFunction : public InitSessionTimezone<T>,
                        public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_min;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_min;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_min;
  }
};

template <typename T>
struct SecondFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, nullptr).tm_sec;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_sec;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_sec;
  }
};

template <typename T>
struct MillisecondFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = timestamp.getNanos() / kNanosecondsInMillisecond;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Date>& /*date*/) {
    // Dates do not have millisecond granularity.
    result = 0;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = timestamp.getNanos() / kNanosecondsInMillisecond;
  }
};

namespace {
inline std::optional<DateTimeUnit> fromDateTimeUnitString(
    const StringView& unitString,
    bool throwIfInvalid) {
  static const StringView kMillisecond("millisecond");
  static const StringView kSecond("second");
  static const StringView kMinute("minute");
  static const StringView kHour("hour");
  static const StringView kDay("day");
  static const StringView kWeek("week");
  static const StringView kMonth("month");
  static const StringView kQuarter("quarter");
  static const StringView kYear("year");

  const auto unit = boost::algorithm::to_lower_copy(unitString.str());

  if (unit == kMillisecond) {
    return DateTimeUnit::kMillisecond;
  }
  if (unit == kSecond) {
    return DateTimeUnit::kSecond;
  }
  if (unit == kMinute) {
    return DateTimeUnit::kMinute;
  }
  if (unit == kHour) {
    return DateTimeUnit::kHour;
  }
  if (unit == kDay) {
    return DateTimeUnit::kDay;
  }
  if (unit == kWeek) {
    return DateTimeUnit::kWeek;
  }
  if (unit == kMonth) {
    return DateTimeUnit::kMonth;
  }
  if (unit == kQuarter) {
    return DateTimeUnit::kQuarter;
  }
  if (unit == kYear) {
    return DateTimeUnit::kYear;
  }
  if (throwIfInvalid) {
    VELOX_UNSUPPORTED("Unsupported datetime unit: {}", unitString);
  }
  return std::nullopt;
}

inline bool isTimeUnit(const DateTimeUnit unit) {
  return unit == DateTimeUnit::kMillisecond || unit == DateTimeUnit::kSecond ||
      unit == DateTimeUnit::kMinute || unit == DateTimeUnit::kHour;
}

inline bool isDateUnit(const DateTimeUnit unit) {
  return unit == DateTimeUnit::kDay || unit == DateTimeUnit::kMonth ||
      unit == DateTimeUnit::kQuarter || unit == DateTimeUnit::kYear ||
      unit == DateTimeUnit::kWeek;
}

inline std::optional<DateTimeUnit> getDateUnit(
    const StringView& unitString,
    bool throwIfInvalid) {
  std::optional<DateTimeUnit> unit =
      fromDateTimeUnitString(unitString, throwIfInvalid);
  if (unit.has_value() && !isDateUnit(unit.value())) {
    if (throwIfInvalid) {
      VELOX_USER_FAIL("{} is not a valid DATE field", unitString);
    }
    return std::nullopt;
  }
  return unit;
}

inline std::optional<DateTimeUnit> getTimestampUnit(
    const StringView& unitString) {
  std::optional<DateTimeUnit> unit =
      fromDateTimeUnitString(unitString, false /*throwIfInvalid*/);
  VELOX_USER_CHECK(
      !(unit.has_value() && unit.value() == DateTimeUnit::kMillisecond),
      "{} is not a valid TIMESTAMP field",
      unitString);

  return unit;
}

} // namespace

template <typename T>
struct DateTruncFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  const date::time_zone* timeZone_ = nullptr;
  std::optional<DateTimeUnit> unit_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const arg_type<Timestamp>* /*timestamp*/) {
    timeZone_ = getTimeZoneFromConfig(config);

    if (unitString != nullptr) {
      unit_ = getTimestampUnit(*unitString);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Date>* /*date*/) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<TimestampWithTimezone>* /*timestamp*/) {
    if (unitString != nullptr) {
      unit_ = getTimestampUnit(*unitString);
    }
  }

  FOLLY_ALWAYS_INLINE void adjustDateTime(
      std::tm& dateTime,
      const DateTimeUnit& unit) {
    switch (unit) {
      case DateTimeUnit::kYear:
        dateTime.tm_mon = 0;
        dateTime.tm_yday = 0;
        FMT_FALLTHROUGH;
      case DateTimeUnit::kQuarter:
        dateTime.tm_mon = dateTime.tm_mon / 3 * 3;
        FMT_FALLTHROUGH;
      case DateTimeUnit::kMonth:
        dateTime.tm_mday = 1;
        dateTime.tm_hour = 0;
        dateTime.tm_min = 0;
        dateTime.tm_sec = 0;
        break;
      case DateTimeUnit::kWeek:
        // Subtract the truncation
        dateTime.tm_mday -= dateTime.tm_wday == 0 ? 6 : dateTime.tm_wday - 1;
        // Setting the day of the week to Monday
        dateTime.tm_wday = 1;

        // If the adjusted day of the month falls in the previous month
        // Move to the previous month
        if (dateTime.tm_mday < 1) {
          dateTime.tm_mon -= 1;

          // If the adjusted month falls in the previous year
          // Set to December and Move to the previous year
          if (dateTime.tm_mon < 0) {
            dateTime.tm_mon = 11;
            dateTime.tm_year -= 1;
          }

          // Calculate the correct day of the month based on the number of days
          // in the adjusted month
          static const int daysInMonth[] = {
              31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
          int daysInPrevMonth = daysInMonth[dateTime.tm_mon];

          // Adjust for leap year if February
          if (dateTime.tm_mon == 1 && (dateTime.tm_year + 1900) % 4 == 0 &&
              ((dateTime.tm_year + 1900) % 100 != 0 ||
               (dateTime.tm_year + 1900) % 400 == 0)) {
            daysInPrevMonth = 29;
          }
          // Set to the correct day in the previous month
          dateTime.tm_mday += daysInPrevMonth;
        }
        dateTime.tm_hour = 0;
        dateTime.tm_min = 0;
        dateTime.tm_sec = 0;
        break;
      case DateTimeUnit::kDay:
        dateTime.tm_hour = 0;
        FMT_FALLTHROUGH;
      case DateTimeUnit::kHour:
        dateTime.tm_min = 0;
        FMT_FALLTHROUGH;
      case DateTimeUnit::kMinute:
        dateTime.tm_sec = 0;
        break;
      default:
        VELOX_UNREACHABLE();
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Timestamp>& timestamp) {
    DateTimeUnit unit;
    if (unit_.has_value()) {
      unit = unit_.value();
    } else {
      unit = getTimestampUnit(unitString).value();
    }

    switch (unit) {
      case DateTimeUnit::kSecond:
        result = Timestamp(timestamp.getSeconds(), 0);
        return;
      case DateTimeUnit::kMinute:
        result = adjustEpoch(getSeconds(timestamp, timeZone_), 60);
        break;
      case DateTimeUnit::kHour:
        result = adjustEpoch(getSeconds(timestamp, timeZone_), 60 * 60);
        break;
      case DateTimeUnit::kDay:
        result = adjustEpoch(getSeconds(timestamp, timeZone_), 24 * 60 * 60);
        break;
      default:
        auto dateTime = getDateTime(timestamp, timeZone_);
        adjustDateTime(dateTime, unit);
        result = Timestamp(Timestamp::calendarUtcToEpoch(dateTime), 0);
    }

    if (timeZone_ != nullptr) {
      result.toGMT(*timeZone_);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Date>& date) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();

    if (unit == DateTimeUnit::kDay) {
      result = date;
      return;
    }

    auto dateTime = getDateTime(date);
    adjustDateTime(dateTime, unit);

    result = Timestamp::calendarUtcToEpoch(dateTime) / kSecondsInDay;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& unitString,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    DateTimeUnit unit;
    if (unit_.has_value()) {
      unit = unit_.value();
    } else {
      unit = getTimestampUnit(unitString).value();
    }

    if (unit == DateTimeUnit::kSecond) {
      auto utcTimestamp = unpackTimestampUtc(timestampWithTimezone);
      result = pack(
          utcTimestamp.getSeconds() * 1000,
          unpackZoneKeyId(timestampWithTimezone));
      return;
    }

    auto timestamp = this->toTimestamp(timestampWithTimezone);
    auto dateTime = getDateTime(timestamp, nullptr);
    adjustDateTime(dateTime, unit);
    timestamp =
        Timestamp::fromMillis(Timestamp::calendarUtcToEpoch(dateTime) * 1000);
    timestamp.toGMT(unpackZoneKeyId(timestampWithTimezone));

    result = pack(timestamp.toMillis(), unpackZoneKeyId(timestampWithTimezone));
  }

 private:
  /// For fixed interval like second, minute, hour, day and week
  /// we can truncate date by a simple arithmetic expression:
  /// floor(seconds / intervalSeconds) * intervalSeconds.
  FOLLY_ALWAYS_INLINE Timestamp
  adjustEpoch(int64_t seconds, int64_t intervalSeconds) {
    int64_t s = seconds / intervalSeconds;
    if (seconds < 0 && seconds % intervalSeconds) {
      s = s - 1;
    }
    int64_t truncedSeconds = s * intervalSeconds;
    return Timestamp(truncedSeconds, 0);
  }
};

template <typename T>
struct DateAddFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  const date::time_zone* sessionTimeZone_ = nullptr;
  std::optional<DateTimeUnit> unit_ = std::nullopt;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const int64_t* /*value*/,
      const arg_type<Timestamp>* /*timestamp*/) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, false /*throwIfInvalid*/);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const int64_t* /*value*/,
      const arg_type<Date>* /*date*/) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, false);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& unitString,
      const int64_t value,
      const arg_type<Timestamp>& timestamp) {
    const auto unit = unit_.has_value()
        ? unit_.value()
        : fromDateTimeUnitString(unitString, true /*throwIfInvalid*/).value();

    if (value != (int32_t)value) {
      VELOX_UNSUPPORTED("integer overflow");
    }

    if (LIKELY(sessionTimeZone_ != nullptr)) {
      // sessionTimeZone not null means that the config
      // adjust_timestamp_to_timezone is on.
      Timestamp zonedTimestamp = timestamp;
      zonedTimestamp.toTimezone(*sessionTimeZone_);

      Timestamp resultTimestamp =
          addToTimestamp(zonedTimestamp, unit, (int32_t)value);

      if (isTimeUnit(unit)) {
        const int64_t offset = static_cast<Timestamp>(timestamp).getSeconds() -
            zonedTimestamp.getSeconds();
        result = Timestamp(
            resultTimestamp.getSeconds() + offset, resultTimestamp.getNanos());
      } else {
        resultTimestamp.toGMT(*sessionTimeZone_);
        result = resultTimestamp;
      }
    } else {
      result = addToTimestamp(timestamp, unit, (int32_t)value);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& unitString,
      const int64_t value,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    const auto unit = unit_.has_value()
        ? unit_.value()
        : fromDateTimeUnitString(unitString, true /*throwIfInvalid*/).value();

    if (value != (int32_t)value) {
      VELOX_UNSUPPORTED("integer overflow");
    }

    result =
        addToTimestampWithTimezone(timestampWithTimezone, unit, (int32_t)value);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Varchar>& unitString,
      const int64_t value,
      const arg_type<Date>& date) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();

    if (value != (int32_t)value) {
      VELOX_UNSUPPORTED("integer overflow");
    }

    result = addToDate(date, unit, (int32_t)value);
  }
};

template <typename T>
struct DateDiffFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  const date::time_zone* sessionTimeZone_ = nullptr;
  std::optional<DateTimeUnit> unit_ = std::nullopt;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const arg_type<Timestamp>* /*timestamp1*/,
      const arg_type<Timestamp>* /*timestamp2*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, false /*throwIfInvalid*/);
    }

    sessionTimeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Date>* /*date1*/,
      const arg_type<Date>* /*date2*/) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const arg_type<TimestampWithTimezone>* /*timestampWithTimezone1*/,
      const arg_type<TimestampWithTimezone>* /*timestampWithTimezone2*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, false /*throwIfInvalid*/);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Timestamp>& timestamp1,
      const arg_type<Timestamp>& timestamp2) {
    const auto unit = unit_.has_value()
        ? unit_.value()
        : fromDateTimeUnitString(unitString, true /*throwIfInvalid*/).value();

    if (LIKELY(sessionTimeZone_ != nullptr)) {
      // sessionTimeZone not null means that the config
      // adjust_timestamp_to_timezone is on.
      Timestamp fromZonedTimestamp = timestamp1;
      fromZonedTimestamp.toTimezone(*sessionTimeZone_);

      Timestamp toZonedTimestamp = timestamp2;
      if (isTimeUnit(unit)) {
        const int64_t offset = static_cast<Timestamp>(timestamp1).getSeconds() -
            fromZonedTimestamp.getSeconds();
        toZonedTimestamp = Timestamp(
            toZonedTimestamp.getSeconds() - offset,
            toZonedTimestamp.getNanos());
      } else {
        toZonedTimestamp.toTimezone(*sessionTimeZone_);
      }
      result = diffTimestamp(unit, fromZonedTimestamp, toZonedTimestamp);
    } else {
      result = diffTimestamp(unit, timestamp1, timestamp2);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Date>& date1,
      const arg_type<Date>& date2) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();

    result = diffDate(unit, date1, date2);
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<TimestampWithTimezone>& timestamp1,
      const arg_type<TimestampWithTimezone>& timestamp2) {
    call(
        result,
        unitString,
        this->toTimestamp(timestamp1, true),
        this->toTimestamp(timestamp2, true));
  }
};

template <typename T>
struct DateFormatFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<TimestampWithTimezone>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      setFormatter(formatString);
    }

    result.reserve(maxResultSize_);
    const auto resultSize = mysqlDateTime_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data());
    result.resize(resultSize);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<Varchar>& formatString) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    call(result, timestamp, formatString);
  }

 private:
  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar> formatString) {
    mysqlDateTime_ = buildMysqlDateTimeFormatter(
        std::string_view(formatString.data(), formatString.size()));
    maxResultSize_ = mysqlDateTime_->maxResultSize(sessionTimeZone_);
  }

  const date::time_zone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> mysqlDateTime_;
  uint32_t maxResultSize_;
  bool isConstFormat_ = false;
};

template <typename T>
struct FromIso8601Date {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Date>& result, const arg_type<Varchar>& input) {
    const auto castResult = util::fromDateString(
        input.data(), input.size(), util::ParseMode::kIso8601);
    if (castResult.hasError()) {
      return castResult.error();
    }

    result = castResult.value();
    return Status::OK();
  }
};

template <typename T>
struct FromIso8601Timestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/) {
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTzID_ = util::getTimeZoneID(sessionTzName);
    }
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& input) {
    const auto castResult = util::fromTimestampWithTimezoneString(
        input.data(), input.size(), util::TimestampParseMode::kIso8601);
    if (castResult.hasError()) {
      return castResult.error();
    }

    auto [ts, tzID] = castResult.value();
    // Input string may not contain a timezone - if so, it is interpreted in
    // session timezone.
    if (tzID == -1) {
      tzID = sessionTzID_;
    }
    ts.toGMT(tzID);
    result = pack(ts.toMillis(), tzID);
    return Status::OK();
  }

 private:
  int16_t sessionTzID_{0};
};

template <typename T>
struct DateParseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  std::shared_ptr<DateTimeFormatter> format_;
  std::optional<int64_t> sessionTzID_;
  bool isConstFormat_ = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* formatString) {
    if (formatString != nullptr) {
      format_ = buildMysqlDateTimeFormatter(
          std::string_view(formatString->data(), formatString->size()));
      isConstFormat_ = true;
    }

    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTzID_ = util::getTimeZoneID(sessionTzName);
    }
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstFormat_) {
      format_ = buildMysqlDateTimeFormatter(
          std::string_view(format.data(), format.size()));
    }

    auto dateTimeResult = format_->parse((std::string_view)(input));
    if (!dateTimeResult.has_value()) {
      return Status::UserError("Invalid date format: '{}'", input);
    }

    // Since MySql format has no timezone specifier, simply check if session
    // timezone was provided. If not, fallback to 0 (GMT).
    int16_t timezoneId = sessionTzID_.value_or(0);
    dateTimeResult->timestamp.toGMT(timezoneId);
    result = dateTimeResult->timestamp;
    return Status::OK();
  }
};

template <typename T>
struct FormatDateTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& formatString) {
    ensureFormatter(formatString);

    format(timestamp, sessionTimeZone_, maxResultSize_, result);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<Varchar>& formatString) {
    ensureFormatter(formatString);

    const auto timestamp = unpackTimestampUtc(timestampWithTimezone);
    const auto timeZoneId = unpackZoneKeyId(timestampWithTimezone);
    auto* timezonePtr = date::locate_zone(util::getTimeZoneName(timeZoneId));

    const auto maxResultSize = jodaDateTime_->maxResultSize(timezonePtr);
    format(timestamp, timezonePtr, maxResultSize, result);
  }

 private:
  FOLLY_ALWAYS_INLINE void ensureFormatter(
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      setFormatter(formatString);
    }
  }

  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar>& formatString) {
    jodaDateTime_ = buildJodaDateTimeFormatter(
        std::string_view(formatString.data(), formatString.size()));
    maxResultSize_ = jodaDateTime_->maxResultSize(sessionTimeZone_);
  }

  void format(
      const Timestamp& timestamp,
      const date::time_zone* timeZone,
      uint32_t maxResultSize,
      out_type<Varchar>& result) const {
    result.reserve(maxResultSize);
    const auto resultSize = jodaDateTime_->format(
        timestamp, timeZone, maxResultSize, result.data());
    result.resize(resultSize);
  }

  const date::time_zone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> jodaDateTime_;
  uint32_t maxResultSize_;
  bool isConstFormat_ = false;
};

template <typename T>
struct ParseDateTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  std::shared_ptr<DateTimeFormatter> format_;
  std::optional<int64_t> sessionTzID_;
  bool isConstFormat_ = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    if (format != nullptr) {
      format_ = buildJodaDateTimeFormatter(
          std::string_view(format->data(), format->size()));
      isConstFormat_ = true;
    }

    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTzID_ = util::getTimeZoneID(sessionTzName);
    }
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstFormat_) {
      format_ = buildJodaDateTimeFormatter(
          std::string_view(format.data(), format.size()));
    }
    auto dateTimeResult =
        format_->parse(std::string_view(input.data(), input.size()), false);
    if (!dateTimeResult.has_value()) {
      return Status::UserError("Invalid date format: '{}'", input);
    }

    // If timezone was not parsed, fallback to the session timezone. If there's
    // no session timezone, fallback to 0 (GMT).
    int16_t timezoneId = dateTimeResult->timezoneId != -1
        ? dateTimeResult->timezoneId
        : sessionTzID_.value_or(0);
    dateTimeResult->timestamp.toGMT(timezoneId);
    result = pack(dateTimeResult->timestamp.toMillis(), timezoneId);
    return Status::OK();
  }
};

template <typename T>
struct CurrentDateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  const date::time_zone* timeZone_ = nullptr;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Date>& result) {
    auto now = Timestamp::now();
    if (timeZone_ != nullptr) {
      now.toTimezone(*timeZone_);
    }
    const std::chrono::
        time_point<std::chrono::system_clock, std::chrono::milliseconds>
            localTimepoint(std::chrono::milliseconds(now.toMillis()));
    result = std::chrono::floor<date::days>((localTimepoint).time_since_epoch())
                 .count();
  }
};

template <typename T>
struct TimeZoneHourFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& input) {
    // Get offset in seconds with GMT and convert to hour
    auto offset = this->getGMTOffsetSec(input);
    result = offset / 3600;
  }
};

template <typename T>
struct TimeZoneMinuteFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& input) {
    // Get offset in seconds with GMT and convert to minute
    auto offset = this->getGMTOffsetSec(input);
    result = (offset / 60) % 60;
  }
};

template <typename T>
struct ToISO8601Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*input*/) {
    if (inputTypes[0]->isTimestamp()) {
      auto sessionTzName = config.sessionTimezone();
      if (!sessionTzName.empty()) {
        timeZone_ = date::locate_zone(sessionTzName);
      } else {
        timeZone_ = date::locate_zone("UTC");
      }
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Date>& date) {
    result = DateType::toIso8601(date);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp) {
    // TODO DateTimeFormatter requires timestamp in UTC. It then converts it to
    // the specified timezone. We can avoid extra conversion if we change
    // DateTimeFormatter to accept non-UTC timestamps.
    auto utcTimestamp = timestamp;
    utcTimestamp.toGMT(*timeZone_);

    toIso8601(utcTimestamp, timeZone_, result);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    const auto timestamp = unpackTimestampUtc(timestampWithTimezone);
    const auto timeZoneId = unpackZoneKeyId(timestampWithTimezone);
    const auto* timeZone = date::locate_zone(util::getTimeZoneName(timeZoneId));

    toIso8601(timestamp, timeZone, result);
  }

 private:
  void toIso8601(
      const Timestamp& timestamp,
      const date::time_zone* timeZone,
      out_type<Varchar>& result) const {
    const auto maxResultSize = formatter_->maxResultSize(timeZone);
    result.reserve(maxResultSize);
    const auto resultSize =
        formatter_->format(timestamp, timeZone, maxResultSize, result.data());
    result.resize(resultSize);
  }

  const date::time_zone* timeZone_{nullptr};
  std::shared_ptr<DateTimeFormatter> formatter_ =
      functions::buildJodaDateTimeFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
};

template <typename T>
struct AtTimezoneFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  std::optional<int64_t> targetTimezoneID_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<TimestampWithTimezone>* /*tsWithTz*/,
      const arg_type<Varchar>* timezone) {
    if (timezone != nullptr) {
      targetTimezoneID_ = util::getTimeZoneID(
          std::string_view(timezone->data(), timezone->size()));
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<TimestampWithTimezone>& tsWithTz,
      const arg_type<Varchar>& timezone) {
    const auto inputMs = unpackMillisUtc(tsWithTz);
    const auto targetTimezoneID = targetTimezoneID_.has_value()
        ? targetTimezoneID_.value()
        : util::getTimeZoneID(
              std::string_view(timezone.data(), timezone.size()));

    // Input and output TimestampWithTimezones should not contain
    // different timestamp values - solely timezone ID should differ between the
    // two, as timestamp is stored as a UTC offset. The timestamp is then
    // resolved to the respective timezone at the time of display.
    result = pack(inputMs, targetTimezoneID);
  }
};

} // namespace facebook::velox::functions
