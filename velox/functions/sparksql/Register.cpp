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

#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/DateTimeFunctions.h"
#include "velox/functions/prestosql/JsonExtractScalar.h"
#include "velox/functions/prestosql/Rand.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/sparksql/ArraySort.h"
#include "velox/functions/sparksql/CompareFunctionsNullSafe.h"
#include "velox/functions/sparksql/Hash.h"
#include "velox/functions/sparksql/In.h"
#include "velox/functions/sparksql/IsEmptySimple.h"
#include "velox/functions/sparksql/LeastGreatest.h"
#include "velox/functions/sparksql/RegexFunctions.h"
#include "velox/functions/sparksql/RegisterArithmetic.h"
#include "velox/functions/sparksql/RegisterCompare.h"
#include "velox/functions/sparksql/String.h"

namespace facebook::velox::functions {

static void workAroundRegistrationMacro(const std::string& prefix) {
  // VELOX_REGISTER_VECTOR_FUNCTION must be invoked in the same namespace as the
  // vector function definition.
  // Higher order functions.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_transform, prefix + "transform");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reduce, prefix + "aggregate");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_filter, prefix + "filter");
  // Complex types.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_constructor, prefix + "array");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_contains, prefix + "array_contains");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_array_intersect, prefix + "array_intersect");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_element_at, prefix + "element_at");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat_row, prefix + "named_struct");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_map_allow_duplicates, prefix + "map_from_arrays");
  // String functions.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat, prefix + "concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_lower, prefix + "lower");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_replace, prefix + "replace");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_upper, prefix + "upper");
  // Logical.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_is_null, prefix + "isnull");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_is_not_null, prefix + "isnotnull");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, prefix + "not");
}

namespace sparksql {

void registerFunctions(const std::string& prefix) {
  registerFunction<RandFunction, double>({"rand"});

  registerFunction<JsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {prefix + "get_json_object"});

  // Register string functions.
  registerFunction<sparksql::ChrFunction, Varchar, int64_t>({prefix + "chr"});
  registerFunction<AsciiFunction, int32_t, Varchar>({prefix + "ascii"});

  registerFunction<SubstrFunction, Varchar, Varchar, int32_t>(
      {prefix + "substring"});
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t, int32_t>(
      {prefix + "substring"});
  registerFunction<IsEmpty, bool, int8_t>({prefix + "is_empty"});
  registerFunction<IsEmpty, bool, int16_t>({prefix + "is_empty"});
  registerFunction<IsEmpty, bool, int32_t>({prefix + "is_empty"});

  registerFunction<IsEmpty, bool, int64_t>({prefix + "is_empty"});
  registerFunction<IsEmpty, bool, bool>({prefix + "is_empty"});
  registerFunction<IsEmpty, bool, float>({prefix + "is_empty"});
  registerFunction<IsEmpty, bool, double>({prefix + "is_empty"});
  registerFunction<IsEmpty, bool, Varchar>({prefix + "is_empty"});
  registerFunction<IsEmpty, bool, Varbinary>({prefix + "is_empty"});
  registerFunction<IsEmpty, bool, Array<Generic<>>>({prefix + "is_empty"});
  registerFunction<IsEmpty, bool, Map<Generic<>, Generic<>>>(
      {prefix + "is_empty"});
  registerFunction<IsEmpty, bool, Generic<>>({prefix + "is_empty"});

  exec::registerStatefulVectorFunction("instr", instrSignatures(), makeInstr);
  exec::registerStatefulVectorFunction(
      "length", lengthSignatures(), makeLength);

  registerFunction<Md5Function, Varchar, Varbinary>({prefix + "md5"});

  exec::registerStatefulVectorFunction(
      prefix + "regexp_extract", re2ExtractSignatures(), makeRegexExtract);
  exec::registerStatefulVectorFunction(
      prefix + "rlike", re2SearchSignatures(), makeRLike);
  VELOX_REGISTER_VECTOR_FUNCTION(udf_regexp_split, prefix + "split");

  // Subscript operators. See ExtractValue in complexTypeExtractors.scala.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_subscript, prefix + "getarrayitem");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_subscript, prefix + "getmapvalue");

  exec::registerStatefulVectorFunction(
      prefix + "least", leastSignatures(), makeLeast);
  exec::registerStatefulVectorFunction(
      prefix + "greatest", greatestSignatures(), makeGreatest);
  exec::registerStatefulVectorFunction(
      prefix + "hash", hashSignatures(), makeHash);
  exec::registerStatefulVectorFunction(
      prefix + "murmur3hash", hashSignatures(), makeHash);

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

  // String sreach function
  registerFunction<StartsWithFunction, bool, Varchar, Varchar>(
      {prefix + "startswith"});
  registerFunction<EndsWithFunction, bool, Varchar, Varchar>(
      {prefix + "endswith"});
  registerFunction<ContainsFunction, bool, Varchar, Varchar>(
      {prefix + "contains"});

  // Register array sort functions.
  exec::registerStatefulVectorFunction(
      prefix + "array_sort", arraySortSignatures(), makeArraySort);
  exec::registerStatefulVectorFunction(
      prefix + "sort_array", sortArraySignatures(), makeSortArray);
}

} // namespace sparksql
} // namespace facebook::velox::functions
