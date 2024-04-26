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

#include <boost/algorithm/string.hpp>

#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct YearFunction : public InitSessionTimezone<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int32_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getYear(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getYear(getDateTime(date));
  }
};

template <typename T>
struct WeekFunction : public InitSessionTimezone<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int32_t getWeek(const std::tm& time) {
    // The computation of ISO week from date follows the algorithm here:
    // https://en.wikipedia.org/wiki/ISO_week_date
    int32_t week = floor(
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
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getWeek(getDateTime(date));
  }
};

template <typename T>
struct YearOfWeekFunction : public InitSessionTimezone<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    const auto dateTime = getDateTime(date);

    int isoWeekDay = dateTime.tm_wday == 0 ? 7 : dateTime.tm_wday;
    // The last few days in December may belong to the next year if they are
    // in the same week as the next January 1 and this January 1 is a Thursday
    // or before.
    if (UNLIKELY(
            dateTime.tm_mon == 11 && dateTime.tm_mday >= 29 &&
            dateTime.tm_mday - isoWeekDay >= 31 - 3)) {
      result = 1900 + dateTime.tm_year + 1;
      return;
    }
    // The first few days in January may belong to the last year if they are
    // in the same week as January 1 and January 1 is a Friday or after.
    if (UNLIKELY(
            dateTime.tm_mon == 0 && dateTime.tm_mday <= 3 &&
            isoWeekDay - (dateTime.tm_mday - 1) >= 5)) {
      result = 1900 + dateTime.tm_year - 1;
      return;
    }
    result = 1900 + dateTime.tm_year;
  }
};

template <typename T>
struct UnixDateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = date;
  }
};

template <typename T>
struct UnixTimestampFunction {
  // unix_timestamp();
  // If no parameters, return the current unix timestamp without adjusting
  // timezones.
  FOLLY_ALWAYS_INLINE void call(int64_t& result) {
    result = Timestamp::now().getSeconds();
  }
};

template <typename T>
struct UnixTimestampParseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // unix_timestamp(input);
  // If format is not specified, assume kDefaultFormat.
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/) {
    format_ = buildJodaDateTimeFormatter(kDefaultFormat_);
    setTimezone(config);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input) {
    auto dateTimeResult =
        format_->parse(std::string_view(input.data(), input.size()));
    // Return null if could not parse.
    if (!dateTimeResult.has_value()) {
      return false;
    }
    (*dateTimeResult).timestamp.toGMT(getTimezoneId(*dateTimeResult));
    result = (*dateTimeResult).timestamp.getSeconds();
    return true;
  }

 protected:
  void setTimezone(const core::QueryConfig& config) {
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTzID_ = util::getTimeZoneID(sessionTzName);
    }
  }

  int16_t getTimezoneId(const DateTimeResult& result) {
    // If timezone was not parsed, fallback to the session timezone. If there's
    // no session timezone, fallback to 0 (GMT).
    return result.timezoneId != -1 ? result.timezoneId
                                   : sessionTzID_.value_or(0);
  }

  // Default if format is not specified, as per Spark documentation.
  constexpr static std::string_view kDefaultFormat_{"yyyy-MM-dd HH:mm:ss"};
  std::shared_ptr<DateTimeFormatter> format_;
  std::optional<int64_t> sessionTzID_;
};

template <typename T>
struct UnixTimestampParseWithFormatFunction
    : public UnixTimestampParseFunction<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // unix_timestamp(input, format):
  // If format is constant, compile it just once per batch.
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    if (format != nullptr) {
      try {
        this->format_ = buildJodaDateTimeFormatter(
            std::string_view(format->data(), format->size()));
      } catch (const VeloxUserError&) {
        invalidFormat_ = true;
      }
      isConstFormat_ = true;
    }
    this->setTimezone(config);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (invalidFormat_) {
      return false;
    }

    // Format error returns null.
    try {
      if (!isConstFormat_) {
        this->format_ = buildJodaDateTimeFormatter(
            std::string_view(format.data(), format.size()));
      }
    } catch (const VeloxUserError&) {
      return false;
    }
    auto dateTimeResult =
        this->format_->parse(std::string_view(input.data(), input.size()));
    // parsing error returns null
    if (!dateTimeResult.has_value()) {
      return false;
    }
    (*dateTimeResult).timestamp.toGMT(this->getTimezoneId(*dateTimeResult));
    result = (*dateTimeResult).timestamp.getSeconds();
    return true;
  }

 private:
  bool isConstFormat_{false};
  bool invalidFormat_{false};
};

// Parses unix time in seconds to a formatted string.
template <typename T>
struct FromUnixtimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/,
      const arg_type<Varchar>* format) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (format != nullptr) {
      setFormatter(*format);
      isConstantTimeFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& second,
      const arg_type<Varchar>& format) {
    if (!isConstantTimeFormat_) {
      setFormatter(format);
    }
    const Timestamp timestamp{second, 0};
    result.reserve(maxResultSize_);
    int32_t resultSize;
    resultSize = formatter_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data(), true);
    result.resize(resultSize);
  }

 private:
  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar>& format) {
    formatter_ = buildJodaDateTimeFormatter(
        std::string_view(format.data(), format.size()));
    maxResultSize_ = formatter_->maxResultSize(sessionTimeZone_);
  }

  const date::time_zone* sessionTimeZone_{nullptr};
  std::shared_ptr<DateTimeFormatter> formatter_;
  uint32_t maxResultSize_;
  bool isConstantTimeFormat_{false};
};

template <typename T>
struct ToUtcTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* timezone) {
    if (timezone) {
      timezone_ = date::locate_zone(
          std::string_view((*timezone).data(), (*timezone).size()));
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& timezone) {
    result = timestamp;
    auto fromTimezone = timezone_
        ? timezone_
        : date::locate_zone(std::string_view(timezone.data(), timezone.size()));
    result.toGMT(*fromTimezone);
  }

 private:
  const date::time_zone* timezone_{nullptr};
};

template <typename T>
struct FromUtcTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* timezone) {
    if (timezone) {
      timezone_ = date::locate_zone(
          std::string_view((*timezone).data(), (*timezone).size()));
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& timezone) {
    result = timestamp;
    auto toTimezone = timezone_
        ? timezone_
        : date::locate_zone(std::string_view(timezone.data(), timezone.size()));
    result.toTimezone(*toTimezone);
  }

 private:
  const date::time_zone* timezone_{nullptr};
};

/// Converts date string to Timestmap type.
template <typename T>
struct GetTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    auto sessionTimezoneName = config.sessionTimezone();
    if (!sessionTimezoneName.empty()) {
      sessionTimezoneId_ = util::getTimeZoneID(sessionTimezoneName);
    }
    if (format != nullptr) {
      formatter_ = buildJodaDateTimeFormatter(
          std::string_view(format->data(), format->size()));
      isConstantTimeFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstantTimeFormat_) {
      formatter_ = buildJodaDateTimeFormatter(
          std::string_view(format.data(), format.size()));
    }
    auto dateTimeResult =
        formatter_->parse(std::string_view(input.data(), input.size()));
    // Null as result for parsing error.
    if (!dateTimeResult.has_value()) {
      return false;
    }
    (*dateTimeResult).timestamp.toGMT(getTimezoneId(*dateTimeResult));
    result = (*dateTimeResult).timestamp;
    return true;
  }

 private:
  int16_t getTimezoneId(const DateTimeResult& result) const {
    // If timezone was not parsed, fallback to the session timezone. If there's
    // no session timezone, fallback to 0 (GMT).
    return result.timezoneId != -1 ? result.timezoneId
                                   : sessionTimezoneId_.value_or(0);
  }

  std::shared_ptr<DateTimeFormatter> formatter_{nullptr};
  bool isConstantTimeFormat_{false};
  std::optional<int64_t> sessionTimezoneId_;
};

template <typename T>
struct MakeDateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const int32_t year,
      const int32_t month,
      const int32_t day) {
    int64_t daysSinceEpoch;
    auto status =
        util::daysSinceEpochFromDate(year, month, day, daysSinceEpoch);
    if (!status.ok()) {
      VELOX_DCHECK(status.isUserError());
      VELOX_USER_FAIL(status.message());
    }
    VELOX_USER_CHECK_EQ(
        daysSinceEpoch,
        (int32_t)daysSinceEpoch,
        "Integer overflow in make_date({}, {}, {})",
        year,
        month,
        day);
    result = daysSinceEpoch;
  }
};

template <typename T>
struct LastDayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  FOLLY_ALWAYS_INLINE int64_t getMonth(const std::tm& time) {
    return 1 + time.tm_mon;
  }

  FOLLY_ALWAYS_INLINE int64_t getDay(const std::tm& time) {
    return time.tm_mday;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date) {
    auto dateTime = getDateTime(date);
    int32_t year = getYear(dateTime);
    int32_t month = getMonth(dateTime);
    int32_t day = getMonth(dateTime);
    auto lastDay = util::getMaxDayOfMonth(year, month);
    int64_t daysSinceEpoch;
    auto status =
        util::daysSinceEpochFromDate(year, month, lastDay, daysSinceEpoch);
    if (!status.ok()) {
      VELOX_DCHECK(status.isUserError());
      VELOX_USER_FAIL(status.message());
    }
    VELOX_USER_CHECK_EQ(
        daysSinceEpoch,
        (int32_t)daysSinceEpoch,
        "Integer overflow in last_day({}-{}-{})",
        year,
        month,
        day);
    result = daysSinceEpoch;
  }
};

template <typename T>
struct DateFromUnixDateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Date>& result, const int32_t& value) {
    result = value;
  }
};

template <typename T>
struct DateAddFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const TInput& value) {
    __builtin_add_overflow(date, value, &result);
  }
};

template <typename T>
struct DateSubFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const TInput& value) {
    __builtin_sub_overflow(date, value, &result);
  }
};

template <typename T>
struct DayOfWeekFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // 1 = Sunday, 2 = Monday, ..., 7 = Saturday
  FOLLY_ALWAYS_INLINE int32_t getDayOfWeek(const std::tm& time) {
    return time.tm_wday + 1;
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getDayOfWeek(getDateTime(date));
  }
};

template <typename T>
struct DateDiffFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Date>& endDate,
      const arg_type<Date>& startDate)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = endDate - startDate;
  }
};

template <typename T>
struct AddMonthsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(out_type<Date>& result, const arg_type<Date>& date, int32_t numMonths) {
    const auto dateTime = getDateTime(date);
    const auto year = getYear(dateTime);
    const auto month = getMonth(dateTime);
    const auto day = getDay(dateTime);

    // Similar to handling number in base 12. Here, month - 1 makes it in
    // [0, 11] range.
    int64_t monthAdded = (int64_t)month - 1 + numMonths;
    // Used to adjust month/year when monthAdded is not in [0, 11] range.
    int64_t yearOffset = (monthAdded >= 0 ? monthAdded : monthAdded - 11) / 12;
    // Adjusts monthAdded to natural month number in [1, 12] range.
    auto monthResult = static_cast<int32_t>(monthAdded - yearOffset * 12 + 1);
    // Adjusts year.
    auto yearResult = year + yearOffset;

    auto lastDayOfMonth = util::getMaxDayOfMonth(yearResult, monthResult);
    // Adjusts day to valid one.
    auto dayResult = lastDayOfMonth < day ? lastDayOfMonth : day;

    int64_t daysSinceEpoch;
    auto status = util::daysSinceEpochFromDate(
        yearResult, monthResult, dayResult, daysSinceEpoch);
    if (!status.ok()) {
      VELOX_DCHECK(status.isUserError());
      VELOX_USER_FAIL(status.message());
    }
    VELOX_USER_CHECK_EQ(
        daysSinceEpoch,
        (int32_t)daysSinceEpoch,
        "Integer overflow in add_months({}, {})",
        DATE()->toString(date),
        numMonths);
    result = daysSinceEpoch;
  }
};

template <typename T>
struct MonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getMonth(getDateTime(date));
  }
};

template <typename T>
struct QuarterFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getQuarter(getDateTime(date));
  }
};

template <typename T>
struct DayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_mday;
  }
};

template <typename T>
struct DayOfYearFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getDayOfYear(getDateTime(date));
  }
};

template <typename T>
struct WeekdayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // 0 = Monday, 1 = Tuesday, ..., 6 = Sunday
  FOLLY_ALWAYS_INLINE int32_t getWeekday(const std::tm& time) {
    return (time.tm_wday + 6) % 7;
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getWeekday(getDateTime(date));
  }
};

template <typename T>
struct NextDayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Date>* /*startDate*/,
      const arg_type<Varchar>* dayOfWeek) {
    if (dayOfWeek != nullptr) {
      weekDay_ = getDayOfWeekFromString(*dayOfWeek);
      if (!weekDay_.has_value()) {
        invalidFormat_ = true;
      }
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& startDate,
      const arg_type<Varchar>& dayOfWeek) {
    if (invalidFormat_) {
      return false;
    }
    auto weekDay = weekDay_.has_value() ? weekDay_.value()
                                        : getDayOfWeekFromString(dayOfWeek);
    if (!weekDay.has_value()) {
      return false;
    }
    auto nextDay = getNextDate(startDate, weekDay.value());
    if (nextDay != (int32_t)nextDay) {
      return false;
    }
    result = nextDay;
    return true;
  }

 private:
  static FOLLY_ALWAYS_INLINE std::optional<int8_t> getDayOfWeekFromString(
      const StringView& dayOfWeek) {
    std::string lowerDayOfWeek =
        boost::algorithm::to_lower_copy(dayOfWeek.str());
    auto it = kDayOfWeekNames.find(lowerDayOfWeek);
    if (it != kDayOfWeekNames.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  static FOLLY_ALWAYS_INLINE int64_t
  getNextDate(int64_t startDay, int8_t dayOfWeek) {
    return startDay + 1 + ((dayOfWeek - 1 - startDay) % 7 + 7) % 7;
  }

  std::optional<int8_t> weekDay_;
  bool invalidFormat_{false};
};

template <typename T>
struct HourFunction : public InitSessionTimezone<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_hour;
  }
};

template <typename T>
struct MinuteFunction : public InitSessionTimezone<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_min;
  }
};

template <typename T>
struct SecondFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, nullptr).tm_sec;
  }
};

template <typename T>
struct MakeYMIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<IntervalYearMonth>& result) {
    result = 0;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<IntervalYearMonth>& result,
      const int32_t year) {
    VELOX_USER_CHECK(
        !__builtin_mul_overflow(year, kMonthInYear, &result),
        "Integer overflow in make_ym_interval({})",
        year);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<IntervalYearMonth>& result,
      const int32_t year,
      const int32_t month) {
    auto totalMonths = (int64_t)year * kMonthInYear + month;
    VELOX_USER_CHECK_EQ(
        totalMonths,
        (int32_t)totalMonths,
        "Integer overflow in make_ym_interval({}, {})",
        year,
        month);
    result = totalMonths;
  }
};
} // namespace facebook::velox::functions::sparksql
