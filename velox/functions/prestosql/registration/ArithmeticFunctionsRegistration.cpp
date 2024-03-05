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
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/Bitwise.h"
#include "velox/functions/prestosql/Probability.h"
#include "velox/functions/prestosql/Rand.h"

namespace facebook::velox::functions {

namespace {
void registerSimpleFunctions(const std::string& prefix) {
  registerBinaryFloatingPoint<PlusFunction>({prefix + "plus"});
  registerFunction<
      PlusFunction,
      IntervalDayTime,
      IntervalDayTime,
      IntervalDayTime>({prefix + "plus"});
  registerBinaryFloatingPoint<MinusFunction>({prefix + "minus"});
  registerFunction<
      MinusFunction,
      IntervalDayTime,
      IntervalDayTime,
      IntervalDayTime>({prefix + "minus"});
  registerBinaryFloatingPoint<MultiplyFunction>({prefix + "multiply"});
  registerFunction<MultiplyFunction, IntervalDayTime, IntervalDayTime, int64_t>(
      {prefix + "multiply"});
  registerFunction<MultiplyFunction, IntervalDayTime, int64_t, IntervalDayTime>(
      {prefix + "multiply"});
  registerFunction<
      IntervalMultiplyFunction,
      IntervalDayTime,
      IntervalDayTime,
      double>({prefix + "multiply"});
  registerFunction<
      IntervalMultiplyFunction,
      IntervalDayTime,
      double,
      IntervalDayTime>({prefix + "multiply"});
  registerBinaryFloatingPoint<DivideFunction>({prefix + "divide"});
  registerFunction<
      IntervalDivideFunction,
      IntervalDayTime,
      IntervalDayTime,
      double>({prefix + "divide"});
  registerBinaryFloatingPoint<ModulusFunction>({prefix + "mod"});
  registerUnaryNumeric<CeilFunction>({prefix + "ceil", prefix + "ceiling"});
  registerUnaryNumeric<FloorFunction>({prefix + "floor"});
  registerUnaryNumeric<AbsFunction>({prefix + "abs"});
  registerUnaryFloatingPoint<NegateFunction>({prefix + "negate"});
  registerFunction<RadiansFunction, double, double>({prefix + "radians"});
  registerFunction<DegreesFunction, double, double>({prefix + "degrees"});
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
  registerFunction<PowerFunction, double, double, double>(
      {prefix + "power", prefix + "pow"});
  registerFunction<PowerFunction, double, int64_t, int64_t>(
      {prefix + "power", prefix + "pow"});
  registerFunction<ExpFunction, double, double>({prefix + "exp"});
  registerFunction<ClampFunction, int8_t, int8_t, int8_t, int8_t>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, int16_t, int16_t, int16_t, int16_t>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, int32_t, int32_t, int32_t, int32_t>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, int64_t, int64_t, int64_t, int64_t>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, double, double, double, double>(
      {prefix + "clamp"});
  registerFunction<ClampFunction, float, float, float, float>(
      {prefix + "clamp"});
  registerFunction<LnFunction, double, double>({prefix + "ln"});
  registerFunction<Log2Function, double, double>({prefix + "log2"});
  registerFunction<Log10Function, double, double>({prefix + "log10"});
  registerFunction<CosFunction, double, double>({prefix + "cos"});
  registerFunction<CoshFunction, double, double>({prefix + "cosh"});
  registerFunction<AcosFunction, double, double>({prefix + "acos"});
  registerFunction<SinFunction, double, double>({prefix + "sin"});
  registerFunction<AsinFunction, double, double>({prefix + "asin"});
  registerFunction<TanFunction, double, double>({prefix + "tan"});
  registerFunction<TanhFunction, double, double>({prefix + "tanh"});
  registerFunction<AtanFunction, double, double>({prefix + "atan"});
  registerFunction<Atan2Function, double, double, double>({prefix + "atan2"});
  registerFunction<SqrtFunction, double, double>({prefix + "sqrt"});
  registerFunction<CbrtFunction, double, double>({prefix + "cbrt"});
  registerFunction<
      WidthBucketFunction,
      int64_t,
      double,
      double,
      double,
      int64_t>({prefix + "width_bucket"});

  registerUnaryNumeric<SignFunction>({prefix + "sign"});
  registerFunction<InfinityFunction, double>({prefix + "infinity"});
  registerFunction<IsFiniteFunction, bool, double>({prefix + "is_finite"});
  registerFunction<IsInfiniteFunction, bool, double>({prefix + "is_infinite"});
  registerFunction<IsNanFunction, bool, double>({prefix + "is_nan"});
  registerFunction<NanFunction, double>({prefix + "nan"});
  registerFunction<RandFunction, double>({prefix + "rand", prefix + "random"});
  registerUnaryIntegral<RandFunction>({prefix + "rand", prefix + "random"});
  registerFunction<FromBaseFunction, int64_t, Varchar, int64_t>(
      {prefix + "from_base"});
  registerFunction<ToBaseFunction, Varchar, int64_t, int64_t>(
      {prefix + "to_base"});
  registerFunction<PiFunction, double>({prefix + "pi"});
  registerFunction<EulerConstantFunction, double>({prefix + "e"});
  registerFunction<TruncateFunction, double, double>({prefix + "truncate"});
  registerFunction<TruncateFunction, double, double, int32_t>(
      {prefix + "truncate"});
  registerFunction<BetaCDFFunction, double, double, double, double>(
      {prefix + "beta_cdf"});
  registerFunction<NormalCDFFunction, double, double, double, double>(
      {prefix + "normal_cdf"});
  registerFunction<BinomialCDFFunction, double, int64_t, double, int64_t>(
      {prefix + "binomial_cdf"});
  registerFunction<BinomialCDFFunction, double, int32_t, double, int32_t>(
      {prefix + "binomial_cdf"});
  registerFunction<CauchyCDFFunction, double, double, double, double>(
      {prefix + "cauchy_cdf"});
  registerFunction<ChiSquaredCDFFunction, double, double, double>(
      {prefix + "chi_squared_cdf"});
  registerFunction<FCDFFunction, double, double, double, double>(
      {prefix + "f_cdf"});
  registerFunction<InverseBetaCDFFunction, double, double, double, double>(
      {prefix + "inverse_beta_cdf"});
  registerFunction<PoissonCDFFunction, double, double, int64_t>(
      {prefix + "poisson_cdf"});
  registerFunction<PoissonCDFFunction, double, double, int32_t>(
      {prefix + "poisson_cdf"});
  registerFunction<GammaCDFFunction, double, double, double, double>(
      {prefix + "gamma_cdf"});
  registerFunction<LaplaceCDFFunction, double, double, double, double>(
      {prefix + "laplace_cdf"});
  registerFunction<
      WilsonIntervalUpperFunction,
      double,
      int64_t,
      int64_t,
      double>({prefix + "wilson_interval_upper"});
  registerFunction<
      WilsonIntervalLowerFunction,
      double,
      int64_t,
      int64_t,
      double>({prefix + "wilson_interval_lower"});
  registerFunction<
      CosineSimilarityFunction,
      double,
      Map<Varchar, double>,
      Map<Varchar, double>>({prefix + "cosine_similarity"});
  registerFunction<WeibullCDFFunction, double, double, double, double>(
      {prefix + "weibull_cdf"});
}

} // namespace

void registerArithmeticFunctions(const std::string& prefix = "") {
  registerSimpleFunctions(prefix);
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, prefix + "not");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_add, prefix + "plus");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_sub, prefix + "minus");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_mul, prefix + "multiply");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_div, prefix + "divide");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_round, prefix + "round");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_floor, prefix + "floor");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_abs, prefix + "abs");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_negate, prefix + "negate");
}

} // namespace facebook::velox::functions
