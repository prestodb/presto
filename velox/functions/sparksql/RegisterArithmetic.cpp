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
#include "velox/functions/lib/CheckedArithmetic.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/sparksql/Arithmetic.h"
#include "velox/functions/sparksql/Rand.h"

namespace facebook::velox::functions::sparksql {

void registerRandFunctions(const std::string& prefix) {
  registerFunction<RandFunction, double>({prefix + "rand", prefix + "random"});
  registerFunction<RandFunction, double, Constant<int32_t>>(
      {prefix + "rand", prefix + "random"});
  registerFunction<RandFunction, double, Constant<int64_t>>(
      {prefix + "rand", prefix + "random"});
}

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
  registerFunction<
      DecimalAbsFunction,
      LongDecimal<P1, S1>,
      LongDecimal<P1, S1>>({prefix + "abs"});
  registerFunction<
      DecimalAbsFunction,
      ShortDecimal<P1, S1>,
      ShortDecimal<P1, S1>>({prefix + "abs"});
  registerFunction<AcosFunction, double, double>({prefix + "acos"});
  registerFunction<AsinFunction, double, double>({prefix + "asin"});
  registerFunction<AcoshFunction, double, double>({prefix + "acosh"});
  registerFunction<AsinhFunction, double, double>({prefix + "asinh"});
  registerFunction<AtanFunction, double, double>({prefix + "atan"});
  registerFunction<AtanhFunction, double, double>({prefix + "atanh"});
  registerFunction<SecFunction, double, double>({prefix + "sec"});
  registerFunction<CscFunction, double, double>({prefix + "csc"});
  registerFunction<SinhFunction, double, double>({prefix + "sinh"});
  registerFunction<CosFunction, double, double>({prefix + "cos"});
  registerFunction<CoshFunction, double, double>({prefix + "cosh"});
  registerFunction<CotFunction, double, double>({prefix + "cot"});
  registerFunction<DegreesFunction, double, double>({prefix + "degrees"});
  registerFunction<Atan2Function, double, double, double>({prefix + "atan2"});
  registerFunction<Log1pFunction, double, double>({prefix + "log1p"});
  registerFunction<ToBinaryStringFunction, Varchar, int64_t>({prefix + "bin"});
  registerFunction<ToHexBigintFunction, Varchar, int64_t>({prefix + "hex"});
  registerFunction<ToHexVarcharFunction, Varchar, Varchar>({prefix + "hex"});
  registerFunction<ToHexVarbinaryFunction, Varchar, Varbinary>(
      {prefix + "hex"});
  registerFunction<ExpFunction, double, double>({prefix + "exp"});
  registerFunction<Expm1Function, double, double>({prefix + "expm1"});
  registerBinaryIntegral<PModIntFunction>({prefix + "pmod"});
  registerBinaryFloatingPoint<PModFloatFunction>({prefix + "pmod"});
  registerFunction<PowerFunction, double, double, double>({prefix + "power"});
  registerFunction<RIntFunction, double, double>({prefix + "rint"});
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
  registerFunction<UnHexFunction, Varbinary, Varchar>({prefix + "unhex"});
  // In Spark only long, double, and decimal have ceil/floor
  registerFunction<sparksql::CeilFunction, int64_t, int64_t>({prefix + "ceil"});
  registerFunction<sparksql::CeilFunction, int64_t, double>({prefix + "ceil"});
  registerFunction<sparksql::FloorFunction, int64_t, int64_t>(
      {prefix + "floor"});
  registerFunction<sparksql::FloorFunction, int64_t, double>(
      {prefix + "floor"});
  registerFunction<HypotFunction, double, double, double>({prefix + "hypot"});
  registerFunction<sparksql::Log2Function, double, double>({prefix + "log2"});
  registerFunction<sparksql::Log10Function, double, double>({prefix + "log10"});
  registerFunction<
      WidthBucketFunction,
      int64_t,
      double,
      double,
      double,
      int64_t>({prefix + "width_bucket"});
  registerRandFunctions(prefix);

  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_add, prefix + "add");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_sub, prefix + "subtract");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_mul, prefix + "multiply");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_div, prefix + "divide");
  registerFunction<sparksql::IsNanFunction, bool, float>({prefix + "isnan"});
  registerFunction<sparksql::IsNanFunction, bool, double>({prefix + "isnan"});
}

} // namespace facebook::velox::functions::sparksql
