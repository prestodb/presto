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
#include "velox/functions/sparksql/RegisterCompare.h"

#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/sparksql/Comparisons.h"

namespace facebook::velox::functions::sparksql {

void registerCompareFunctions(const std::string& prefix) {
  exec::registerStatefulVectorFunction(
      prefix + "equalto", comparisonSignatures(), makeEqualTo);
  registerFunction<EqualToFunction, bool, Generic<T1>, Generic<T1>>(
      {prefix + "equalto"});
  exec::registerStatefulVectorFunction(
      prefix + "lessthan", comparisonSignatures(), makeLessThan);
  exec::registerStatefulVectorFunction(
      prefix + "greaterthan", comparisonSignatures(), makeGreaterThan);
  exec::registerStatefulVectorFunction(
      prefix + "lessthanorequal", comparisonSignatures(), makeLessThanOrEqual);
  exec::registerStatefulVectorFunction(
      prefix + "greaterthanorequal",
      comparisonSignatures(),
      makeGreaterThanOrEqual);
  // Compare nullsafe functions.
  exec::registerStatefulVectorFunction(
      prefix + "equalnullsafe",
      comparisonSignatures(),
      makeEqualToNullSafe,
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());
  registerFunction<EqualNullSafeFunction, bool, Generic<T1>, Generic<T1>>(
      {prefix + "equalnullsafe"});
  registerFunction<BetweenFunction, bool, int8_t, int8_t, int8_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int16_t, int16_t, int16_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int32_t, int32_t, int32_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int64_t, int64_t, int64_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int128_t, int128_t, int128_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, float, float, float>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, double, double, double>(
      {prefix + "between"});
  // Decimal comapre functions.
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_decimal_gt, prefix + "decimal_greaterthan");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_decimal_gte, prefix + "decimal_greaterthanorequal");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_lt, prefix + "decimal_lessthan");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_decimal_lte, prefix + "decimal_lessthanorequal");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_eq, prefix + "decimal_equalto");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_decimal_neq, prefix + "decimal_notequalto");
}

} // namespace facebook::velox::functions::sparksql
