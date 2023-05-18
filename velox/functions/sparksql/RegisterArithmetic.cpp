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
  registerFunction<DivideFunction, double, double, double>({prefix + "divide"});
  registerBinaryIntegral<RemainderFunction>({prefix + "remainder"});
  registerUnaryNumeric<UnaryMinusFunction>({prefix + "unaryminus"});
  // Math functions.
  registerUnaryNumeric<AbsFunction>({prefix + "abs"});
  registerFunction<AcoshFunction, double, double>({prefix + "acosh"});
  registerFunction<AsinhFunction, double, double>({prefix + "asinh"});
  registerFunction<AtanhFunction, double, double>({prefix + "atanh"});
  registerFunction<SecFunction, double, double>({prefix + "sec"});
  registerFunction<CscFunction, double, double>({prefix + "csc"});
  registerFunction<ExpFunction, double, double>({prefix + "exp"});
  registerBinaryIntegral<PModFunction>({prefix + "pmod"});
  registerFunction<PowerFunction, double, double, double>({prefix + "power"});
  registerUnaryNumeric<RoundFunction>({prefix + "round"});
  registerFunction<RoundFunction, int8_t, int8_t, int32_t>({prefix + "round"});
  registerFunction<RoundFunction, int16_t, int16_t, int32_t>(
      {prefix + "round"});
  registerFunction<RoundFunction, int32_t, int32_t, int32_t>(
      {prefix + "round"});
  registerFunction<RoundFunction, int64_t, int64_t, int32_t>(
      {prefix + "round"});
  registerFunction<RoundFunction, double, double, int32_t>({prefix + "round"});
  registerFunction<RoundFunction, float, float, int32_t>({prefix + "round"});
  // In Spark only long, double, and decimal have ceil/floor
  registerFunction<sparksql::CeilFunction, int64_t, int64_t>({prefix + "ceil"});
  registerFunction<sparksql::CeilFunction, int64_t, double>({prefix + "ceil"});
  registerFunction<sparksql::FloorFunction, int64_t, int64_t>(
      {prefix + "floor"});
  registerFunction<sparksql::FloorFunction, int64_t, double>(
      {prefix + "floor"});
}

} // namespace facebook::velox::functions::sparksql
