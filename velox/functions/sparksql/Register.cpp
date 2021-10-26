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
#include "velox/functions/sparksql/Hash.h"
#include "velox/functions/sparksql/In.h"
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
  VELOX_REGISTER_VECTOR_FUNCTION(udf_filter, prefix + "filter");
  // Complex types.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_constructor, prefix + "array");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_contains, prefix + "array_contains");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_element_at, prefix + "element_at");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat_row, prefix + "named_struct");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map, prefix + "map_from_arrays");
  // String functions.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat, prefix + "concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_lower, prefix + "lower");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_replace, prefix + "replace");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_upper, prefix + "upper");
  // Logical.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_coalesce, prefix + "coalesce");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_is_null, prefix + "isnull");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, prefix + "not");
}

namespace sparksql {

void registerFunctions(const std::string& prefix) {
  registerFunction<udf_rand, double>({"rand"});

  registerFunction<udf_json_extract_scalar, Varchar, Varchar, Varchar>(
      {prefix + "get_json_object"});

  // Register string functions.
  registerFunction<udf_chr, Varchar, int64_t>();
  registerFunction<udf_ascii, int32_t, Varchar>();

  registerFunction<udf_substr<int32_t>, Varchar, Varchar, int32_t>(
      {prefix + "substring"});
  registerFunction<udf_substr<int32_t>, Varchar, Varchar, int32_t, int32_t>(
      {prefix + "substring"});

  exec::registerStatefulVectorFunction("instr", instrSignatures(), makeInstr);
  exec::registerStatefulVectorFunction(
      "length", lengthSignatures(), makeLength);

  registerFunction<udf_md5<Varchar, Varbinary>, Varchar, Varbinary>(
      {prefix + "md5"});

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
  exec::registerStatefulVectorFunction(prefix + "in", inSignatures(), makeIn);

  // These vector functions are only accessible via the
  // VELOX_REGISTER_VECTOR_FUNCTION macro, which must be invoked in the same
  // namespace as the function definition.
  workAroundRegistrationMacro(prefix);

  // These groups of functions involve instantiating many templates. They're
  // broken out into a separate compilation unit to improve build latency.
  registerArithmeticFunctions(prefix);
  registerCompareFunctions(prefix);

  // String sreach function
  registerFunction<udf_starts_with, bool, Varchar, Varchar>(
      {prefix + "startswith"});
  registerFunction<udf_ends_with, bool, Varchar, Varchar>(
      {prefix + "endswith"});
  registerFunction<udf_contains, bool, Varchar, Varchar>({prefix + "contains"});
}

} // namespace sparksql
} // namespace facebook::velox::functions
