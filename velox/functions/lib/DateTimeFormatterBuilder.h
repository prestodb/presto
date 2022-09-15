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

#include <memory>
#include <string>
#include <vector>
#include "velox/functions/lib/DateTimeFormatter.h"

namespace facebook::velox::functions {

class DateTimeFormatter;
enum class DateTimeFormatterType;
struct DateTimeToken;

/// Builder class that builds DateTimeFormatter object. For example, let's
/// assume MySQL date time standard is used. To build a formatter of the form
/// '%Y-%m-%d' you do:
/// DateTimeFormatterBuilder(8)     // Length of format
///     .appendYear(4)              // Year of 4 digits
///     .appendLiteral("-")
///     .appendMonthOfYear(2)       // Month of 2 digits
///     .appendLiteral("-")
///     .appendDayOfMonth(2)        // Day of 2 digits
///     .build();
class DateTimeFormatterBuilder {
 public:
  explicit DateTimeFormatterBuilder(size_t literalBufSize);

  /// Appends era to formatter builder, e.g: "AD"
  DateTimeFormatterBuilder& appendEra();

  /// Appends century of era (>=0) to formatter builder, e.g: 20
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent century of era. The format by default is going use
  /// as few digits as possible but greater than or equal to minDigits to
  /// represent century of era. e.g. year 999, with min digit being 1 the
  /// formatted result will be 9 , with min digit being 2 the formatted result
  /// will be 09
  DateTimeFormatterBuilder& appendCenturyOfEra(size_t minDigits);

  /// Appends year of era (>=0) to formatter builder, e.g: 1999
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent year of era. The format by default is going use as
  /// few digits as possible greater than or equal to minDigits to represent
  /// year of era. e.g. year 1999, with min digit being 1 the formatted result
  /// will be 99 (year of century), with min digit being 2 the formatted result
  /// will be 99 (year of century), with min digit being 3 the formatted result
  /// will be 1999, with min digit being 4 the formatted result will be 1999,
  /// and with min digit being 5 the formatted result will be 01999
  DateTimeFormatterBuilder& appendYearOfEra(size_t minDigits);

  /// Appends week year to formatter builder, e.g: 1999
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent week year. The format by default is going use as few
  /// digits as possible greater than or equal to minDigits to represent
  /// week year. e.g. 1999-01-01, with min digit being 1 the formatted result
  /// will be 98 (year of century), with min digit being 2 the formatted result
  /// will be 98 (year of century), with min digit being 3 the formatted result
  /// will be 1998, with min digit being 4 the formatted result will be 1998,
  /// and with min digit being 5 the formatted result will be 01998
  DateTimeFormatterBuilder& appendWeekYear(size_t minDigits);

  /// Appends week of week year to formatter builder, e.g: 2
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent week of week year. The format by default is going
  /// use as few digits as possible greater than or equal to minDigits to
  /// represent week of week year. e.g. 1999-01-08, with min digit being 1 the
  /// formatted result will be 1, with min digit being 2 the formatted result
  /// will be 01 (year of century), with min digit being 3 the formatted result
  /// will be 001
  DateTimeFormatterBuilder& appendWeekOfWeekYear(size_t minDigits);

  /// Appends day of week to formatter builder. The number is 0 based with 0 ~ 6
  /// representing Sunday to Saturday respectively
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent day of week. The format by default is going use as
  /// few digits as possible greater than or equal to minDigits to represent day
  /// of week. e.g. 1999-01-01, with min digit being 1 the formatted result will
  /// be 5, with min digit being 2 the formatted result will be 05
  DateTimeFormatterBuilder& appendDayOfWeek0Based(size_t minDigits);

  /// Appends day of week to formatter builder. The number is 1 based with 1 ~ 7
  /// representing Monday to Sunday respectively
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent day of week. The format by default is going use as
  /// few digits as possible greater than or equal to minDigits to represent day
  /// of week. e.g. 1999-01-01, with min digit being 1 the formatted result will
  /// be 5, with min digit being 2 the formatted result will be 05
  DateTimeFormatterBuilder& appendDayOfWeek1Based(size_t minDigits);

  /// Appends day of week text to formatter builder, e.g: 'Tue' or 'Tuesday'
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent day of week in text. The format by default is going
  /// use as few digits as possible greater than or equal to minDigits to
  /// represent day of week. e.g. 1999-01-01, with min digit being 1 the
  /// formatted result will be 'Fri', with min digit being 4 the formatted
  /// result will be 'Friday'
  DateTimeFormatterBuilder& appendDayOfWeekText(size_t minDigits);

  /// Appends year to formatter builder, e.g: 1999
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent year. The format by default is going use as
  /// few digits as possible greater than or equal to minDigits to represent
  /// year. e.g. year 1999, with min digit being 1 the formatted result will be
  /// 99 (year of century), with min digit being 2 the formatted result will be
  /// 99 (year of century), with min digit being 3 the formatted result will be
  /// 1999, with min digit being 4 the formatted result will be 1999, and with
  /// min digit being 5 the formatted result will be 01999
  DateTimeFormatterBuilder& appendYear(size_t minDigits);

  /// Appends day of year to formatter builder, e.g: 365
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent day of year. The format by default is going use as
  /// few digits as possible greater than or equal to minDigits to represent day
  /// of year. e.g. 1999-01-01, with min digit being 1 the formatted result will
  /// be 1, with min digit being 4 the formatted result will be 0001
  DateTimeFormatterBuilder& appendDayOfYear(size_t minDigits);

  /// Appends month of year to formatter builder, e.g: 12
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent month of year. The format by default is going use as
  /// few digits as possible greater than or equal to minDigits to represent
  /// month of year. e.g. 1999-01-01, with min digit being 1 the formatted
  /// result will be 1, with min digit being 4 the formatted result will be 0001
  DateTimeFormatterBuilder& appendMonthOfYear(size_t minDigits);

  /// Appends month of year in text to formatter builder, e.g: Jan, Feb
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent month of year in text. The format by default is
  /// going use as few digits as possible greater than or equal to minDigits to
  /// represent month of year. e.g. 1999-01-01, with min digit being 1 the
  /// formatted result will be Jan, with min digit being 4 the formatted result
  /// will be January
  DateTimeFormatterBuilder& appendMonthOfYearText(size_t minDigits);

  /// Appends day of month to formatter builder, e.g: 30
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent day of month. The format by default is going use as
  /// few digits as possible greater than or equal to minDigits to represent day
  /// of month. e.g. 1999-01-01, with min digit being 1 the formatted result
  /// will be 1, with min digit being 4 the formatted result will be 0001
  DateTimeFormatterBuilder& appendDayOfMonth(size_t minDigits);

  /// Appends half day of day to formatter builder, e.g: 'AM' or 'PM'
  DateTimeFormatterBuilder& appendHalfDayOfDay();

  /// Appends hour of half day to formatter builder, e.g: 11
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent hour of half day. The format by default is going use
  /// as few digits as possible greater than or equal to minDigits to represent
  /// hour of half day. e.g. 1999-01-01 01:00:00, with min digit being 1 the
  /// formatted result will be 1, with min digit being 4 the formatted result
  /// will be 0001
  DateTimeFormatterBuilder& appendHourOfHalfDay(size_t minDigits);

  /// Appends clock hour of half day to formatter builder, e.g: 12
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent clock hour of half day. The format by default is
  /// going use as few digits as possible greater than or equal to minDigits to
  /// represent clock hour of half day. e.g. 1999-01-01 00:00:00, with min digit
  /// being 1 the formatted result will be 12, with min digit being 4 the
  /// formatted result will be 0012
  DateTimeFormatterBuilder& appendClockHourOfHalfDay(size_t minDigits);

  /// Appends hour of day to formatter builder, e.g: 23
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent hour of day. The format by default is going use
  /// as few digits as possible greater than or equal to minDigits to represent
  /// hour of day. e.g. 1999-01-01 01:00:00, with min digit being 1 the
  /// formatted result will be 1, with min digit being 4 the formatted result
  /// will be 0001
  DateTimeFormatterBuilder& appendHourOfDay(size_t minDigits);

  /// Appends clock hour of day to formatter builder, e.g: 24
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent clock hour of day. The format by default is going
  /// use as few digits as possible greater than or equal to minDigits to
  /// represent clock hour of day. e.g. 1999-01-01 01:00:00, with min digit
  /// being 1 the formatted result will be 1, with min digit being 4 the
  /// formatted result will be 0001
  DateTimeFormatterBuilder& appendClockHourOfDay(size_t minDigits);

  /// Appends minute of hour to formatter builder, e.g: 59
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent minute of hour. The format by default is going
  /// use as few digits as possible greater than or equal to minDigits to
  /// represent minute of hour. e.g. 1999-01-01 01:59:00, with min digit
  /// being 1 the formatted result will be 59, with min digit being 4 the
  /// formatted result will be 0059
  DateTimeFormatterBuilder& appendMinuteOfHour(size_t minDigits);

  /// Appends second of minute to formatter builder, e.g: 59
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent second of minute. The format by default is going
  /// use as few digits as possible greater than or equal to minDigits to
  /// represent second of minute. e.g. 1999-01-01 01:59:00, with min digit
  /// being 1 the formatted result will be 59, with min digit being 4 the
  /// formatted result will be 0059
  DateTimeFormatterBuilder& appendSecondOfMinute(size_t minDigits);

  /// Appends fraction of second to formatter builder, e.g: 998
  ///
  /// \param digits describes the number of digits this format uses to represent
  /// fraction of second. e.g. 1999-01-01 01:59:00.987, with digit being 1 the
  /// formatted result will be 9, with digit being 4 the formatted result will
  /// be 9870, with digit being 6 the formatted result will be 987000
  DateTimeFormatterBuilder& appendFractionOfSecond(size_t digits);

  /// Appends time zone to formatter builder, e.g: 'Pacific Standard Time' or
  /// 'PST'
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent time zone. The format by default is going
  /// use as few digits as possible greater than or equal to minDigits to
  /// represent time zone.
  DateTimeFormatterBuilder& appendTimeZone(size_t minDigits);

  /// Appends time zone offset id to formatter builder, e.g: '-0800' or '-08:00'
  /// or 'America/Los_Angeles'
  ///
  /// \param minDigits describes the minimum number of digits this format is
  /// required to represent time zone offset id. The format by default is going
  /// use as few digits as possible greater than or equal to minDigits to
  /// represent time zone offset id.
  DateTimeFormatterBuilder& appendTimeZoneOffsetId(size_t minDigits);

  DateTimeFormatterBuilder& appendLiteral(const std::string_view& literal);

  DateTimeFormatterBuilder& appendLiteral(
      const char* literalStart,
      size_t literalSize);

  DateTimeFormatterBuilder& setType(DateTimeFormatterType type);

  std::shared_ptr<DateTimeFormatter> build();

 private:
  std::unique_ptr<char[]> literalBuf_;
  size_t bufEnd_{0};
  std::vector<DateTimeToken> tokens_;
  DateTimeFormatterType type_{DateTimeFormatterType::UNKNOWN};
};

} // namespace facebook::velox::functions
