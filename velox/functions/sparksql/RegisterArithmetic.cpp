/*
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
  registerBinaryFloatingPoint<udf_plus>({prefix + "add"});
  registerBinaryFloatingPoint<udf_minus>({prefix + "minus"});
  registerBinaryFloatingPoint<udf_multiply>({prefix + "multiply"});
  registerBinaryFloatingPoint<udf_divide>({prefix + "divide"});
  registerUnaryNumeric<udf_ceil>({prefix + "ceil", prefix + "ceiling"});
  registerUnaryNumeric<udf_floor>({prefix + "floor"});
  registerUnaryNumeric<udf_abs>({prefix + "abs"});
  registerUnaryNumeric<udf_unaryminus>({prefix + "unaryminus"});
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
  registerFunction<udf_power<double>, double, double, double>(
      {prefix + "power", prefix + "pow"});
  registerFunction<udf_power<int64_t>, double, int64_t, int64_t>(
      {prefix + "power", prefix + "pow"});
  registerFunction<udf_exp, double, double>({prefix + "exp"});
  registerFunction<udf_clamp<int8_t>, int8_t, int8_t, int8_t, int8_t>(
      {prefix + "clamp"});
  registerFunction<udf_clamp<int16_t>, int16_t, int16_t, int16_t, int16_t>(
      {prefix + "clamp"});
  registerFunction<udf_clamp<int32_t>, int32_t, int32_t, int32_t, int32_t>(
      {prefix + "clamp"});
  registerFunction<udf_clamp<int64_t>, int64_t, int64_t, int64_t, int64_t>(
      {prefix + "clamp"});
  registerFunction<udf_clamp<double>, double, double, double, double>(
      {prefix + "clamp"});
  registerFunction<udf_clamp<float>, float, float, float, float>(
      {prefix + "clamp"});

  // only integrals are accepted for mod operator in C++
  registerBinaryIntegral<udf_pmod>({prefix + "pmod"});
  // Decimal is not supported.
  registerBinaryIntegral<udf_remainder>({prefix + "remainder"});
}

} // namespace facebook::velox::functions::sparksql
