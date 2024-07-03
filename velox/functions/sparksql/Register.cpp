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
#include "velox/functions/sparksql/Register.h"

#include "velox/expression/RegisterSpecialForm.h"
#include "velox/expression/RowConstructor.h"
#include "velox/expression/SpecialFormRegistry.h"
#include "velox/functions/lib/ArrayShuffle.h"
#include "velox/functions/lib/IsNull.h"
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/Repeat.h"
#include "velox/functions/prestosql/ArrayFunctions.h"
#include "velox/functions/prestosql/BinaryFunctions.h"
#include "velox/functions/prestosql/DateTimeFunctions.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/URLFunctions.h"
#include "velox/functions/sparksql/ArrayFlattenFunction.h"
#include "velox/functions/sparksql/ArrayMinMaxFunction.h"
#include "velox/functions/sparksql/ArraySort.h"
#include "velox/functions/sparksql/Bitwise.h"
#include "velox/functions/sparksql/DateTimeFunctions.h"
#include "velox/functions/sparksql/Hash.h"
#include "velox/functions/sparksql/In.h"
#include "velox/functions/sparksql/LeastGreatest.h"
#include "velox/functions/sparksql/MightContain.h"
#include "velox/functions/sparksql/MonotonicallyIncreasingId.h"
#include "velox/functions/sparksql/RaiseError.h"
#include "velox/functions/sparksql/RegexFunctions.h"
#include "velox/functions/sparksql/RegisterArithmetic.h"
#include "velox/functions/sparksql/RegisterCompare.h"
#include "velox/functions/sparksql/Size.h"
#include "velox/functions/sparksql/SparkPartitionId.h"
#include "velox/functions/sparksql/String.h"
#include "velox/functions/sparksql/StringToMap.h"
#include "velox/functions/sparksql/UnscaledValueFunction.h"
#include "velox/functions/sparksql/Uuid.h"
#include "velox/functions/sparksql/specialforms/DecimalRound.h"
#include "velox/functions/sparksql/specialforms/MakeDecimal.h"
#include "velox/functions/sparksql/specialforms/SparkCastExpr.h"

namespace facebook::velox::functions {
extern void registerElementAtFunction(
    const std::string& name,
    bool enableCaching);

template <typename T>
inline void registerArrayRemoveFunctions(const std::string& prefix) {
  registerFunction<ArrayRemoveFunction, Array<T>, Array<T>, T>(
      {prefix + "array_remove"});
}

inline void registerArrayRemoveFunctions(const std::string& prefix) {
  registerArrayRemoveFunctions<int8_t>(prefix);
  registerArrayRemoveFunctions<int16_t>(prefix);
  registerArrayRemoveFunctions<int32_t>(prefix);
  registerArrayRemoveFunctions<int64_t>(prefix);
  registerArrayRemoveFunctions<int128_t>(prefix);
  registerArrayRemoveFunctions<float>(prefix);
  registerArrayRemoveFunctions<double>(prefix);
  registerArrayRemoveFunctions<bool>(prefix);
  registerArrayRemoveFunctions<Timestamp>(prefix);
  registerArrayRemoveFunctions<Date>(prefix);
  registerArrayRemoveFunctions<Varbinary>(prefix);
  registerArrayRemoveFunctions<Generic<T1>>(prefix);
  registerFunction<
      ArrayRemoveFunctionString,
      Array<Varchar>,
      Array<Varchar>,
      Varchar>({prefix + "array_remove"});
}

static void workAroundRegistrationMacro(const std::string& prefix) {
  // VELOX_REGISTER_VECTOR_FUNCTION must be invoked in the same namespace as the
  // vector function definition.
  // Higher order functions.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_transform, prefix + "transform");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reduce, prefix + "aggregate");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_filter, prefix + "filter");
  // Spark and Presto map_filter function has the same definition:
  //   function expression corresponds to body, arguments to signature
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_filter, prefix + "map_filter");
  // Complex types.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_constructor, prefix + "array");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_contains, prefix + "array_contains");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_array_intersect, prefix + "array_intersect");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_distinct, prefix + "array_distinct");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_except, prefix + "array_except");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_position, prefix + "array_position");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_zip_with, prefix + "zip_with");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_all_match, prefix + "forall");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_any_match, prefix + "exists");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_zip, prefix + "arrays_zip");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_entries, prefix + "map_entries");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_keys, prefix + "map_keys");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_values, prefix + "map_values");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_zip_with, prefix + "map_zip_with");

  // This is the semantics of spark.sql.ansi.enabled = false.
  registerElementAtFunction(prefix + "element_at", true);

  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_map_allow_duplicates, prefix + "map_from_arrays");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_concat_row, exec::RowConstructorCallToSpecialForm::kRowConstructor);
  // String functions.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat, prefix + "concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_lower, prefix + "lower");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_upper, prefix + "upper");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reverse, prefix + "reverse");
  // Logical.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, prefix + "not");
  registerIsNullFunction(prefix + "isnull");
  registerIsNotNullFunction(prefix + "isnotnull");
  registerArrayRemoveFunctions(prefix);
}

namespace sparksql {

void registerAllSpecialFormGeneralFunctions() {
  exec::registerFunctionCallToSpecialForms();
  exec::registerFunctionCallToSpecialForm(
      MakeDecimalCallToSpecialForm::kMakeDecimal,
      std::make_unique<MakeDecimalCallToSpecialForm>());
  exec::registerFunctionCallToSpecialForm(
      DecimalRoundCallToSpecialForm::kRoundDecimal,
      std::make_unique<DecimalRoundCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      "cast", std::make_unique<SparkCastCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      "try_cast", std::make_unique<SparkTryCastCallToSpecialForm>());
}

namespace {
template <typename T>
inline void registerArrayMinMaxFunctions(const std::string& prefix) {
  registerFunction<ArrayMinFunction, T, Array<T>>({prefix + "array_min"});
  registerFunction<ArrayMaxFunction, T, Array<T>>({prefix + "array_max"});
}

inline void registerArrayMinMaxFunctions(const std::string& prefix) {
  registerArrayMinMaxFunctions<int8_t>(prefix);
  registerArrayMinMaxFunctions<int16_t>(prefix);
  registerArrayMinMaxFunctions<int32_t>(prefix);
  registerArrayMinMaxFunctions<int64_t>(prefix);
  registerArrayMinMaxFunctions<int128_t>(prefix);
  registerArrayMinMaxFunctions<float>(prefix);
  registerArrayMinMaxFunctions<double>(prefix);
  registerArrayMinMaxFunctions<bool>(prefix);
  registerArrayMinMaxFunctions<Varchar>(prefix);
  registerArrayMinMaxFunctions<Timestamp>(prefix);
  registerArrayMinMaxFunctions<Date>(prefix);
}
} // namespace

void registerFunctions(const std::string& prefix) {
  registerAllSpecialFormGeneralFunctions();

  // Register size functions
  registerSize(prefix + "size");

  registerRegexpReplace(prefix);

  // Register string functions.
  registerFunction<sparksql::ChrFunction, Varchar, int64_t>({prefix + "chr"});
  registerFunction<AsciiFunction, int32_t, Varchar>({prefix + "ascii"});
  registerFunction<sparksql::LPadFunction, Varchar, Varchar, int32_t, Varchar>(
      {prefix + "lpad"});
  registerFunction<sparksql::RPadFunction, Varchar, Varchar, int32_t, Varchar>(
      {prefix + "rpad"});
  registerFunction<sparksql::LPadFunction, Varchar, Varchar, int32_t>(
      {prefix + "lpad"});
  registerFunction<sparksql::RPadFunction, Varchar, Varchar, int32_t>(
      {prefix + "rpad"});
  registerFunction<sparksql::SubstrFunction, Varchar, Varchar, int32_t>(
      {prefix + "substring"});
  registerFunction<
      sparksql::SubstrFunction,
      Varchar,
      Varchar,
      int32_t,
      int32_t>({prefix + "substring"});
  registerFunction<
      sparksql::OverlayVarcharFunction,
      Varchar,
      Varchar,
      Varchar,
      int32_t,
      int32_t>({prefix + "overlay"});
  registerFunction<
      sparksql::OverlayVarbinaryFunction,
      Varbinary,
      Varbinary,
      Varbinary,
      int32_t,
      int32_t>({prefix + "overlay"});

  registerFunction<
      sparksql::StringToMapFunction,
      Map<Varchar, Varchar>,
      Varchar,
      Varchar,
      Varchar>({prefix + "str_to_map"});

  registerFunction<sparksql::LeftFunction, Varchar, Varchar, int32_t>(
      {prefix + "left"});

  registerFunction<sparksql::BitLengthFunction, int32_t, Varchar>(
      {prefix + "bit_length"});
  registerFunction<sparksql::BitLengthFunction, int32_t, Varbinary>(
      {prefix + "bit_length"});

  exec::registerStatefulVectorFunction(
      prefix + "instr", instrSignatures(), makeInstr);
  exec::registerStatefulVectorFunction(
      prefix + "length", lengthSignatures(), makeLength);
  registerFunction<SubstringIndexFunction, Varchar, Varchar, Varchar, int32_t>(
      {prefix + "substring_index"});

  registerFunction<Md5Function, Varchar, Varbinary>({prefix + "md5"});
  registerFunction<Sha1HexStringFunction, Varchar, Varbinary>(
      {prefix + "sha1"});
  registerFunction<Sha2HexStringFunction, Varchar, Varbinary, int32_t>(
      {prefix + "sha2"});
  registerFunction<CRC32Function, int64_t, Varbinary>({prefix + "crc32"});

  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract", re2ExtractSignatures(), makeRegexExtract);
  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract_all",
      re2ExtractAllSignatures(),
      makeRe2ExtractAll);
  exec::registerStatefulVectorFunction(
      prefix + "rlike", re2SearchSignatures(), makeRLike);
  exec::registerStatefulVectorFunction(
      prefix + "like", likeSignatures(), makeLike);
  VELOX_REGISTER_VECTOR_FUNCTION(udf_regexp_split, prefix + "split");

  exec::registerStatefulVectorFunction(
      prefix + "least",
      leastSignatures(),
      makeLeast,
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());
  exec::registerStatefulVectorFunction(
      prefix + "greatest",
      greatestSignatures(),
      makeGreatest,
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());
  exec::registerStatefulVectorFunction(
      prefix + "hash", hashSignatures(), makeHash, hashMetadata());
  exec::registerStatefulVectorFunction(
      prefix + "hash_with_seed",
      hashWithSeedSignatures(),
      makeHashWithSeed,
      hashMetadata());
  exec::registerStatefulVectorFunction(
      prefix + "xxhash64", xxhash64Signatures(), makeXxHash64, hashMetadata());
  exec::registerStatefulVectorFunction(
      prefix + "xxhash64_with_seed",
      xxhash64WithSeedSignatures(),
      makeXxHash64WithSeed,
      hashMetadata());
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map, prefix + "map");

  // Register 'in' functions.
  registerIn(prefix);

  // These vector functions are only accessible via the
  // VELOX_REGISTER_VECTOR_FUNCTION macro, which must be invoked in the same
  // namespace as the function definition.
  workAroundRegistrationMacro(prefix);

  // These groups of functions involve instantiating many templates. They're
  // broken out into a separate compilation unit to improve build latency.
  registerArithmeticFunctions(prefix);
  registerCompareFunctions(prefix);
  registerBitwiseFunctions(prefix);

  // String sreach function
  registerFunction<StartsWithFunction, bool, Varchar, Varchar>(
      {prefix + "startswith"});
  registerFunction<EndsWithFunction, bool, Varchar, Varchar>(
      {prefix + "endswith"});
  registerFunction<ContainsFunction, bool, Varchar, Varchar>(
      {prefix + "contains"});

  registerFunction<TrimSpaceFunction, Varchar, Varchar>({prefix + "trim"});
  registerFunction<TrimFunction, Varchar, Varchar, Varchar>({prefix + "trim"});
  registerFunction<LTrimSpaceFunction, Varchar, Varchar>({prefix + "ltrim"});
  registerFunction<LTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "ltrim"});
  registerFunction<RTrimSpaceFunction, Varchar, Varchar>({prefix + "rtrim"});
  registerFunction<RTrimFunction, Varchar, Varchar, Varchar>(
      {prefix + "rtrim"});

  registerFunction<TranslateFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "translate"});

  registerFunction<ConvFunction, Varchar, Varchar, int32_t, int32_t>(
      {prefix + "conv"});

  registerFunction<ReplaceFunction, Varchar, Varchar, Varchar>(
      {prefix + "replace"});
  registerFunction<ReplaceFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "replace"});

  registerFunction<FindInSetFunction, int32_t, Varchar, Varchar>(
      {prefix + "find_in_set"});

  registerFunction<UrlEncodeFunction, Varchar, Varchar>(
      {prefix + "url_encode"});
  registerFunction<UrlDecodeFunction, Varchar, Varchar>(
      {prefix + "url_decode"});

  // Register array sort functions.
  exec::registerStatefulVectorFunction(
      prefix + "array_sort", arraySortSignatures(), makeArraySort);
  exec::registerStatefulVectorFunction(
      prefix + "sort_array", sortArraySignatures(), makeSortArray);

  exec::registerStatefulVectorFunction(
      prefix + "array_repeat",
      repeatSignatures(),
      makeRepeatAllowNegativeCount,
      repeatMetadata());

  exec::registerStatefulVectorFunction(
      prefix + "shuffle",
      arrayShuffleWithCustomSeedSignatures(),
      makeArrayShuffleWithCustomSeed,
      getMetadataForArrayShuffle());

  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_get, prefix + "get");

  // Register date functions.
  registerFunction<YearFunction, int32_t, Timestamp>({prefix + "year"});
  registerFunction<YearFunction, int32_t, Date>({prefix + "year"});
  registerFunction<WeekFunction, int32_t, Timestamp>({prefix + "week_of_year"});
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

  // Register bloom filter function
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, int64_t>(
      {prefix + "might_contain"});

  registerArrayMinMaxFunctions(prefix);

  // Register decimal vector functions.
  exec::registerVectorFunction(
      prefix + "unscaled_value",
      unscaledValueSignatures(),
      makeUnscaledValue());

  registerFunction<SparkPartitionIdFunction, int32_t>(
      {prefix + "spark_partition_id"});

  registerFunction<MonotonicallyIncreasingIdFunction, int64_t>(
      {prefix + "monotonically_increasing_id"});

  registerFunction<UuidFunction, Varchar, Constant<int64_t>>({prefix + "uuid"});

  registerFunction<
      ArrayFlattenFunction,
      Array<Generic<T1>>,
      Array<Array<Generic<T1>>>>({prefix + "flatten"});

  registerFunction<SoundexFunction, Varchar, Varchar>({prefix + "soundex"});

  registerFunction<RaiseErrorFunction, UnknownValue, Varchar>(
      {prefix + "raise_error"});
}

} // namespace sparksql
} // namespace facebook::velox::functions
