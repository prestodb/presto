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
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/DateTimeFunctions.h"
#include "velox/functions/sparksql/DateTimeFunctions.h"

namespace facebook::velox::functions::sparksql {

void registerDatetimeFunctions(const std::string& prefix) {
  registerFunction<YearFunction, int32_t, Timestamp>({prefix + "year"});
  registerFunction<YearFunction, int32_t, Date>({prefix + "year"});
  registerFunction<WeekFunction, int32_t, Date>({prefix + "week_of_year"});
  registerFunction<YearOfWeekFunction, int32_t, Date>(
      {prefix + "year_of_week"});
  registerFunction<ToUtcTimestampFunction, Timestamp, Timestamp, Varchar>(
      {prefix + "to_utc_timestamp"});
  registerFunction<FromUtcTimestampFunction, Timestamp, Timestamp, Varchar>(
      {prefix + "from_utc_timestamp"});
  registerFunction<UnixDateFunction, int32_t, Date>({prefix + "unix_date"});
  registerFunction<UnixSecondsFunction, int64_t, Timestamp>(
      {prefix + "unix_seconds"});
  registerFunction<UnixTimestampFunction, int64_t>({prefix + "unix_timestamp"});
  registerFunction<UnixTimestampParseFunction, int64_t, Varchar>(
      {prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      UnixTimestampParseWithFormatFunction,
      int64_t,
      Varchar,
      Varchar>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<UnixTimestampParseWithFormatFunction, int64_t, Timestamp>(
      {prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<UnixTimestampParseWithFormatFunction, int64_t, Date>(
      {prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<FromUnixtimeFunction, Varchar, int64_t, Varchar>(
      {prefix + "from_unixtime"});
  registerFunction<MakeDateFunction, Date, int32_t, int32_t, int32_t>(
      {prefix + "make_date"});
  registerFunction<DateDiffFunction, int32_t, Date, Date>(
      {prefix + "datediff"});
  registerFunction<LastDayFunction, Date, Date>({prefix + "last_day"});
  registerFunction<AddMonthsFunction, Date, Date, int32_t>(
      {prefix + "add_months"});
  registerFunction<DateAddFunction, Date, Date, int8_t>({prefix + "date_add"});
  registerFunction<DateAddFunction, Date, Date, int16_t>({prefix + "date_add"});
  registerFunction<DateAddFunction, Date, Date, int32_t>({prefix + "date_add"});
  registerFunction<FormatDateTimeFunction, Varchar, Timestamp, Varchar>(
      {prefix + "date_format"});
  registerFunction<DateFromUnixDateFunction, Date, int32_t>(
      {prefix + "date_from_unix_date"});
  registerFunction<DateSubFunction, Date, Date, int8_t>({prefix + "date_sub"});
  registerFunction<DateSubFunction, Date, Date, int16_t>({prefix + "date_sub"});
  registerFunction<DateSubFunction, Date, Date, int32_t>({prefix + "date_sub"});
  registerFunction<DayFunction, int32_t, Date>(
      {prefix + "day", prefix + "dayofmonth"});
  registerFunction<DayOfYearFunction, int32_t, Date>(
      {prefix + "doy", prefix + "dayofyear"});
  registerFunction<DayOfWeekFunction, int32_t, Date>({prefix + "dayofweek"});
  registerFunction<WeekdayFunction, int32_t, Date>({prefix + "weekday"});
  registerFunction<QuarterFunction, int32_t, Date>({prefix + "quarter"});
  registerFunction<MonthFunction, int32_t, Date>({prefix + "month"});
  registerFunction<NextDayFunction, Date, Date, Varchar>({prefix + "next_day"});
  registerFunction<GetTimestampFunction, Timestamp, Varchar, Varchar>(
      {prefix + "get_timestamp"});
  registerFunction<HourFunction, int32_t, Timestamp>({prefix + "hour"});
  registerFunction<MinuteFunction, int32_t, Timestamp>({prefix + "minute"});
  registerFunction<SecondFunction, int32_t, Timestamp>({prefix + "second"});
  registerFunction<MakeYMIntervalFunction, IntervalYearMonth>(
      {prefix + "make_ym_interval"});
  registerFunction<MakeYMIntervalFunction, IntervalYearMonth, int32_t>(
      {prefix + "make_ym_interval"});
  registerFunction<MakeYMIntervalFunction, IntervalYearMonth, int32_t, int32_t>(
      {prefix + "make_ym_interval"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_make_timestamp, prefix + "make_timestamp");
  registerFunction<TimestampToMicrosFunction, int64_t, Timestamp>(
      {prefix + "unix_micros"});
  registerUnaryIntegralWithTReturn<MicrosToTimestampFunction, Timestamp>(
      {prefix + "timestamp_micros"});
  registerFunction<TimestampToMillisFunction, int64_t, Timestamp>(
      {prefix + "unix_millis"});
  registerUnaryIntegralWithTReturn<MillisToTimestampFunction, Timestamp>(
      {prefix + "timestamp_millis"});
  registerFunction<DateTruncFunction, Timestamp, Varchar, Timestamp>(
      {prefix + "date_trunc"});
}

} // namespace facebook::velox::functions::sparksql
