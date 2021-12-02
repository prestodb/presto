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
#include "velox/functions/sparksql/RegisterArithmetic.h"

#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/CheckedArithmetic.h"
#include "velox/functions/sparksql/Arithmetic.h"

namespace facebook::velox::functions::sparksql {

void registerArithmeticFunctions(const std::string& prefix) {
  // Operators.
  registerBinaryNumeric<PlusFunction>({prefix + "add"});
  registerBinaryNumeric<MinusFunction>({prefix + "subtract"});
  registerBinaryNumeric<MultiplyFunction>({prefix + "multiply"});
  registerFunction<udf_divide, double, double, double>({prefix + "divide"});
  registerBinaryIntegral<RemainderFunction>({prefix + "remainder"});
  registerUnaryNumeric<udf_unaryminus>({prefix + "unaryminus"});
  // Math functions.
  registerUnaryNumeric<udf_abs>({prefix + "abs"});
  registerFunction<udf_exp, double, double>({prefix + "exp"});
  registerBinaryIntegral<PModFunction>({prefix + "pmod"});
  registerFunction<udf_power<double>, double, double, double>(
      {prefix + "power"});
  registerUnaryNumeric<udf_round>({prefix + "round"});
  registerFunction<udf_round<int8_t>, int8_t, int8_t, int32_t>(
      {prefix + "round"});
  registerFunction<udf_round<int16_t>, int16_t, int16_t, int32_t>(
      {prefix + "round"});
  registerFunction<udf_round<int32_t>, int32_t, int32_t, int32_t>(
      {prefix + "round"});
  registerFunction<udf_round<int64_t>, int64_t, int64_t, int32_t>(
      {prefix + "round"});
  registerFunction<udf_round<double>, double, double, int32_t>(
      {prefix + "round"});
  registerFunction<udf_round<float>, float, float, int32_t>({prefix + "round"});
  // In Spark only long, double, and decimal have ceil/floor
  registerFunction<udf_ceil<int64_t>, int64_t, int64_t>({prefix + "ceil"});
  registerFunction<udf_ceil<double>, int64_t, double>({prefix + "ceil"});
  registerFunction<udf_floor<int64_t>, int64_t, int64_t>({prefix + "floor"});
  registerFunction<udf_floor<double>, int64_t, double>({prefix + "floor"});
}

} // namespace facebook::velox::functions::sparksql
