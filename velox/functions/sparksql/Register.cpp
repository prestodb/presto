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

#include "velox/functions/lib/IsNull.h"
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/JsonFunctions.h"
#include "velox/functions/prestosql/Rand.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/sparksql/ArraySort.h"
#include "velox/functions/sparksql/Bitwise.h"
#include "velox/functions/sparksql/CompareFunctionsNullSafe.h"
#include "velox/functions/sparksql/DateTimeFunctions.h"
#include "velox/functions/sparksql/Hash.h"
#include "velox/functions/sparksql/In.h"
#include "velox/functions/sparksql/LeastGreatest.h"
#include "velox/functions/sparksql/MightContain.h"
#include "velox/functions/sparksql/RegexFunctions.h"
#include "velox/functions/sparksql/RegisterArithmetic.h"
#include "velox/functions/sparksql/RegisterCompare.h"
#include "velox/functions/sparksql/Size.h"
#include "velox/functions/sparksql/String.h"

namespace facebook::velox::functions {

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
  // This is the semantics of spark.sql.ansi.enabled = false.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_element_at, prefix + "element_at");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_map_allow_duplicates, prefix + "map_from_arrays");
  // String functions.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat, prefix + "concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_lower, prefix + "lower");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_replace, prefix + "replace");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_upper, prefix + "upper");
  // Logical.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, prefix + "not");
  registerIsNullFunction(prefix + "isnull");
  registerIsNotNullFunction(prefix + "isnotnull");
}

namespace sparksql {

void registerFunctions(const std::string& prefix) {
  registerFunction<RandFunction, double>({"rand"});

  // Register size functions
  registerSize(prefix + "size");

  registerFunction<JsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {prefix + "get_json_object"});

  // Register string functions.
  registerFunction<sparksql::ChrFunction, Varchar, int64_t>({prefix + "chr"});
  registerFunction<AsciiFunction, int32_t, Varchar>({prefix + "ascii"});

  registerFunction<sparksql::SubstrFunction, Varchar, Varchar, int32_t>(
      {prefix + "substring"});
  registerFunction<
      sparksql::SubstrFunction,
      Varchar,
      Varchar,
      int32_t,
      int32_t>({prefix + "substring"});
  exec::registerStatefulVectorFunction("instr", instrSignatures(), makeInstr);
  exec::registerStatefulVectorFunction(
      "length", lengthSignatures(), makeLength);

  registerFunction<Md5Function, Varchar, Varbinary>({prefix + "md5"});
  registerFunction<Sha1HexStringFunction, Varchar, Varbinary>(
      {prefix + "sha1"});
  registerFunction<Sha2HexStringFunction, Varchar, Varbinary, int32_t>(
      {prefix + "sha2"});

  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract", re2ExtractSignatures(), makeRegexExtract);
  exec::registerStatefulVectorFunction(
      prefix + "rlike", re2SearchSignatures(), makeRLike);
  VELOX_REGISTER_VECTOR_FUNCTION(udf_regexp_split, prefix + "split");

  exec::registerStatefulVectorFunction(
      prefix + "least", leastSignatures(), makeLeast);
  exec::registerStatefulVectorFunction(
      prefix + "greatest", greatestSignatures(), makeGreatest);
  exec::registerStatefulVectorFunction(
      prefix + "hash", hashSignatures(), makeHash);
  exec::registerStatefulVectorFunction(
      prefix + "xxhash64", xxhash64Signatures(), makeXxHash64);
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map, prefix + "map");

  // Register 'in' functions.
  registerIn(prefix);

  // Compare nullsafe functions
  exec::registerStatefulVectorFunction(
      prefix + "equalnullsafe", equalNullSafeSignatures(), makeEqualNullSafe);

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

  // Register array sort functions.
  exec::registerStatefulVectorFunction(
      prefix + "array_sort", arraySortSignatures(), makeArraySort);
  exec::registerStatefulVectorFunction(
      prefix + "sort_array", sortArraySignatures(), makeSortArray);

  // Register date functions.
  registerFunction<YearFunction, int32_t, Timestamp>({prefix + "year"});
  registerFunction<YearFunction, int32_t, Date>({prefix + "year"});

  registerFunction<UnixTimestampFunction, int64_t>({prefix + "unix_timestamp"});

  registerFunction<UnixTimestampParseFunction, int64_t, Varchar>(
      {prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      UnixTimestampParseWithFormatFunction,
      int64_t,
      Varchar,
      Varchar>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});

  // Register bloom filter function
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, int64_t>(
      {prefix + "might_contain"});
}

} // namespace sparksql
} // namespace facebook::velox::functions
