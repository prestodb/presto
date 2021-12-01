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
#include "velox/functions/prestosql/SimpleFunctions.h"

#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/ArrayFunctions.h"
#include "velox/functions/prestosql/DateTimeFunctions.h"
#include "velox/functions/prestosql/Hash.h"
#include "velox/functions/prestosql/HyperLogLogFunctions.h"
#include "velox/functions/prestosql/JsonExtractScalar.h"
#include "velox/functions/prestosql/Rand.h"
#include "velox/functions/prestosql/RegisterArithmetic.h"
#include "velox/functions/prestosql/RegisterCheckedArithmetic.h"
#include "velox/functions/prestosql/RegisterComparisons.h"
#include "velox/functions/prestosql/SplitPart.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/URLFunctions.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace facebook::velox::functions {

void registerFunctions() {
  // Register functions here.
  registerFunction<JsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {"json_extract_scalar"});

  // Register string functions.
  registerFunction<ChrFunction, Varchar, int64_t>({"chr"});
  registerFunction<CodePointFunction, int32_t, Varchar>({"codepoint"});
  registerFunction<LengthFunction, int64_t, Varchar>({"length"});

  registerFunction<SubstrFunction, Varchar, Varchar, int64_t>({"substr"});
  registerFunction<SubstrFunction, Varchar, Varchar, int64_t, int64_t>(
      {"substr"});
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t>({"substr"});
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t, int32_t>(
      {"substr"});

  registerFunction<SplitPart, Varchar, Varchar, Varchar, int64_t>(
      {"split_part"});

  registerFunction<TrimFunction, Varchar, Varchar>({"trim"});
  registerFunction<LTrimFunction, Varchar, Varchar>({"ltrim"});
  registerFunction<RTrimFunction, Varchar, Varchar>({"rtrim"});

  // Register hash functions.
  registerFunction<XxHash64Function, Varbinary, Varbinary>({"xxhash64"});
  registerFunction<Md5Function, Varbinary, Varbinary>({"md5"});

  registerFunction<ToHexFunction, Varchar, Varbinary>({"to_hex"});
  registerFunction<FromHexFunction, Varbinary, Varchar>({"from_hex"});
  registerFunction<ToBase64Function, Varchar, Varbinary>({"to_base64"});
  registerFunction<FromBase64Function, Varbinary, Varchar>({"from_base64"});
  registerFunction<UrlEncodeFunction, Varchar, Varchar>({"url_encode"});
  registerFunction<UrlDecodeFunction, Varchar, Varchar>({"url_decode"});

  registerFunction<RandFunction, double>({"rand"});

  registerFunction<udf_pad<true>, Varchar, Varchar, int64_t, Varchar>({"lpad"});
  registerFunction<udf_pad<false>, Varchar, Varchar, int64_t, Varchar>(
      {"rpad"});

  // Date time functions.
  registerFunction<ToUnixtimeFunction, double, Timestamp>(
      {"to_unixtime", "to_unix_timestamp"});
  registerFunction<FromUnixtimeFunction, Timestamp, double>({"from_unixtime"});

  registerFunction<YearFunction, int64_t, Timestamp>({"year"});
  registerFunction<YearFunction, int64_t, Date>({"year"});
  registerFunction<QuarterFunction, int64_t, Timestamp>({"quarter"});
  registerFunction<QuarterFunction, int64_t, Date>({"quarter"});
  registerFunction<MonthFunction, int64_t, Timestamp>({"month"});
  registerFunction<MonthFunction, int64_t, Date>({"month"});
  registerFunction<DayFunction, int64_t, Timestamp>({"day", "day_of_month"});
  registerFunction<DayFunction, int64_t, Date>({"day", "day_of_month"});
  registerFunction<DayOfWeekFunction, int64_t, Timestamp>(
      {"dow", "day_of_week"});
  registerFunction<DayOfWeekFunction, int64_t, Date>({"dow", "day_of_week"});
  registerFunction<DayOfYearFunction, int64_t, Timestamp>(
      {"doy", "day_of_year"});
  registerFunction<DayOfYearFunction, int64_t, Date>({"doy", "day_of_year"});
  registerFunction<YearOfWeekFunction, int64_t, Timestamp>(
      {"yow", "year_of_week"});
  registerFunction<YearOfWeekFunction, int64_t, Date>({"yow", "year_of_week"});
  registerFunction<HourFunction, int64_t, Timestamp>({"hour"});
  registerFunction<HourFunction, int64_t, Date>({"hour"});
  registerFunction<MinuteFunction, int64_t, Timestamp>({"minute"});
  registerFunction<MinuteFunction, int64_t, Date>({"minute"});
  registerFunction<SecondFunction, int64_t, Timestamp>({"second"});
  registerFunction<SecondFunction, int64_t, Date>({"second"});
  registerFunction<MillisecondFunction, int64_t, Timestamp>({"millisecond"});
  registerFunction<MillisecondFunction, int64_t, Date>({"millisecond"});
  registerFunction<DateTruncFunction, Timestamp, Varchar, Timestamp>(
      {"date_trunc"});
  registerFunction<DateTruncFunction, Date, Varchar, Date>({"date_trunc"});
  registerFunction<
      ParseDateTimeFunction,
      TimestampWithTimezone,
      Varchar,
      Varchar>({"parse_datetime"});

  registerFunction<CardinalityFunction, int64_t, HyperLogLog>({"cardinality"});
  registerFunction<EmptyApproxSetFunction, HyperLogLog>({"empty_approx_set"});
  registerFunction<EmptyApproxSetWithMaxErrorFunction, HyperLogLog, double>(
      {"empty_approx_set"});

  // Url Functions.
  registerFunction<UrlExtractHostFunction, Varchar, Varchar>(
      {"url_extract_host"});
  registerFunction<UrlExtractFragmentFunction, Varchar, Varchar>(
      {"url_extract_fragment"});
  registerFunction<UrlExtractPathFunction, Varchar, Varchar>(
      {"url_extract_path"});
  registerFunction<UrlExtractParameterFunction, Varchar, Varchar, Varchar>(
      {"url_extract_parameter"});
  registerFunction<UrlExtractProtocolFunction, Varchar, Varchar>(
      {"url_extract_protocol"});
  registerFunction<UrlExtractPortFunction, int64_t, Varchar>(
      {"url_extract_port"});
  registerFunction<UrlExtractQueryFunction, Varchar, Varchar>(
      {"url_extract_query"});

  registerArithmeticFunctions();
  registerCheckedArithmeticFunctions();
  registerComparisonFunctions();

  registerArrayMinMaxFunctions<int8_t>();
  registerArrayMinMaxFunctions<int16_t>();
  registerArrayMinMaxFunctions<int32_t>();
  registerArrayMinMaxFunctions<int64_t>();
  registerArrayMinMaxFunctions<float>();
  registerArrayMinMaxFunctions<double>();
  registerArrayMinMaxFunctions<bool>();
  registerArrayMinMaxFunctions<Varchar>();
  registerArrayMinMaxFunctions<Timestamp>();
  registerArrayMinMaxFunctions<Date>();
}

} // namespace facebook::velox::functions
