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

#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/DateTimeFunctions.h"
#include "velox/functions/prestosql/Hash.h"
#include "velox/functions/prestosql/JsonExtractScalar.h"
#include "velox/functions/prestosql/Rand.h"
#include "velox/functions/prestosql/RegisterArithmetic.h"
#include "velox/functions/prestosql/RegisterCheckedArithmetic.h"
#include "velox/functions/prestosql/RegisterComparisons.h"
#include "velox/functions/prestosql/StringFunctions.h"

namespace facebook::velox::functions {

void registerFunctions() {
  // Register functions here.
  static const std::vector<std::string> EMPTY{};

  registerFunction<udf_rand, double>(EMPTY);

  registerFunction<udf_json_extract_scalar, Varchar, Varchar, Varchar>();

  // Register string functions.
  registerFunction<udf_chr, Varchar, int64_t>();
  registerFunction<udf_codepoint, int32_t, Varchar>();

  registerFunction<udf_substr<int64_t>, Varchar, Varchar, int64_t>();
  registerFunction<udf_substr<int64_t>, Varchar, Varchar, int64_t, int64_t>();
  registerFunction<udf_substr<int32_t>, Varchar, Varchar, int32_t>();
  registerFunction<udf_substr<int32_t>, Varchar, Varchar, int32_t, int32_t>();

  registerFunction<udf_trim<true, true>, Varchar, Varchar>({"trim"});
  registerFunction<udf_trim<true, false>, Varchar, Varchar>({"ltrim"});
  registerFunction<udf_trim<false, true>, Varchar, Varchar>({"rtrim"});

  // Register hash functions.
  registerFunction<udf_xxhash64, Varbinary, Varbinary>({"xxhash64"});
  registerFunction<udf_md5<Varbinary, Varbinary>, Varbinary, Varbinary>(
      {"md5"});

  registerFunction<udf_to_hex, Varchar, Varbinary>();
  registerFunction<udf_from_hex, Varbinary, Varchar>();
  registerFunction<udf_to_base64, Varchar, Varbinary>();
  registerFunction<udf_from_base64, Varbinary, Varchar>();
  registerFunction<udf_url_encode, Varchar, Varchar>();
  registerFunction<udf_url_decode, Varchar, Varchar>();

  // Date time functions.
  registerFunction<ToUnixtimeFunction, double, Timestamp>(
      {"to_unixtime", "to_unix_timestamp"});
  registerFunction<FromUnixtimeFunction, Timestamp, double>({"from_unixtime"});
  registerFunction<YearFunction, int64_t, Timestamp>({"year"});
  registerFunction<MonthFunction, int64_t, Timestamp>({"month"});
  registerFunction<DayFunction, int64_t, Timestamp>({"day", "day_of_month"});
  registerFunction<DayOfWeekFunction, int64_t, Timestamp>(
      {"dow", "day_of_week"});
  registerFunction<DayOfYearFunction, int64_t, Timestamp>(
      {"doy", "day_of_year"});
  registerFunction<HourFunction, int64_t, Timestamp>({"hour"});
  registerFunction<MinuteFunction, int64_t, Timestamp>({"minute"});
  registerFunction<SecondFunction, int64_t, Timestamp>({"second"});
  registerFunction<MillisecondFunction, int64_t, Timestamp>({"millisecond"});
  registerFunction<DateTruncFunction, Timestamp, Varchar, Timestamp>(
      {"date_trunc"});

  registerArithmeticFunctions();
  registerCheckedArithmeticFunctions();
  registerComparisonFunctions();
}

} // namespace facebook::velox::functions
