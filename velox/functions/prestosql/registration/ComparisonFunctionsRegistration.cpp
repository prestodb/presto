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
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions {

void registerComparisonFunctions() {
  registerNonSimdizableScalar<EqFunction, bool>({"eq"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_eq, "eq");
  registerFunction<EqFunction, bool, Generic<T1>, Generic<T1>>({"eq"});

  registerNonSimdizableScalar<NeqFunction, bool>({"neq"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_neq, "neq");

  registerNonSimdizableScalar<LtFunction, bool>({"lt"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_lt, "lt");

  registerNonSimdizableScalar<GtFunction, bool>({"gt"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_gt, "gt");

  registerNonSimdizableScalar<LteFunction, bool>({"lte"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_lte, "lte");

  registerNonSimdizableScalar<GteFunction, bool>({"gte"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_gte, "gte");

  registerBinaryScalar<DistinctFromFunction, bool>({"distinct_from"});

  registerFunction<BetweenFunction, bool, int8_t, int8_t, int8_t>({"between"});
  registerFunction<BetweenFunction, bool, int16_t, int16_t, int16_t>(
      {"between"});
  registerFunction<BetweenFunction, bool, int32_t, int32_t, int32_t>(
      {"between"});
  registerFunction<BetweenFunction, bool, int64_t, int64_t, int64_t>(
      {"between"});
  registerFunction<BetweenFunction, bool, double, double, double>({"between"});
  registerFunction<BetweenFunction, bool, float, float, float>({"between"});
  registerFunction<BetweenFunction, bool, Varchar, Varchar, Varchar>(
      {"between"});
  registerFunction<BetweenFunction, bool, Date, Date, Date>({"between"});
  registerFunction<
      BetweenFunction,
      bool,
      UnscaledShortDecimal,
      UnscaledShortDecimal,
      UnscaledShortDecimal>({"between"});
  registerFunction<
      BetweenFunction,
      bool,
      UnscaledLongDecimal,
      UnscaledLongDecimal,
      UnscaledLongDecimal>({"between"});
  registerFunction<
      GtFunction,
      bool,
      UnscaledShortDecimal,
      UnscaledShortDecimal>({"gt"});
  registerFunction<GtFunction, bool, UnscaledLongDecimal, UnscaledLongDecimal>(
      {"gt"});
  registerFunction<
      LtFunction,
      bool,
      UnscaledShortDecimal,
      UnscaledShortDecimal>({"lt"});
  registerFunction<LtFunction, bool, UnscaledLongDecimal, UnscaledLongDecimal>(
      {"lt"});

  registerFunction<
      GteFunction,
      bool,
      UnscaledShortDecimal,
      UnscaledShortDecimal>({"gte"});
  registerFunction<GteFunction, bool, UnscaledLongDecimal, UnscaledLongDecimal>(
      {"gte"});
  registerFunction<
      LteFunction,
      bool,
      UnscaledShortDecimal,
      UnscaledShortDecimal>({"lte"});
  registerFunction<LteFunction, bool, UnscaledLongDecimal, UnscaledLongDecimal>(
      {"lte"});
}

} // namespace facebook::velox::functions
