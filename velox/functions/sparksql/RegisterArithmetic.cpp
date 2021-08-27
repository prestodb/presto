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

#include "velox/functions/common/Arithmetic.h"
#include "velox/functions/common/CheckedArithmetic.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/sparksql/Arithmetic.h"

namespace facebook::velox::functions::sparksql {

void registerArithmeticFunctions(const std::string& prefix) {
  // Operators.
  registerBinaryNumeric<udf_plus>({prefix + "add"});
  registerBinaryNumeric<udf_minus>({prefix + "subtract"});
  registerBinaryNumeric<udf_multiply>({prefix + "multiply"});
  registerFunction<udf_divide, double, double, double>({prefix + "divide"});
  registerBinaryIntegral<udf_remainder>({prefix + "remainder"});
  registerUnaryNumeric<udf_unaryminus>({prefix + "unaryminus"});
  // Math functions.
  registerUnaryNumeric<udf_abs>({prefix + "abs"});
  registerUnaryNumeric<udf_ceil>({prefix + "ceil"});
  registerFunction<udf_exp, double, double>({prefix + "exp"});
  registerUnaryNumeric<udf_floor>({prefix + "floor"});
  registerBinaryIntegral<udf_pmod>({prefix + "pmod"});
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
}

} // namespace facebook::velox::functions::sparksql
