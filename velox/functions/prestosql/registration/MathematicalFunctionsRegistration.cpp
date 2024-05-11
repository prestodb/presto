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
#include "velox/functions/prestosql/DecimalFunctions.h"
#include "velox/functions/prestosql/Rand.h"

namespace facebook::velox::functions {

namespace {
void registerMathFunctions(const std::string& prefix) {
  registerUnaryNumeric<CeilFunction>({prefix + "ceil", prefix + "ceiling"});
  registerUnaryNumeric<FloorFunction>({prefix + "floor"});

  registerUnaryNumeric<AbsFunction>({prefix + "abs"});
  registerFunction<
      DecimalAbsFunction,
      LongDecimal<P1, S1>,
      LongDecimal<P1, S1>>({prefix + "abs"});
  registerFunction<
      DecimalAbsFunction,
      ShortDecimal<P1, S1>,
      ShortDecimal<P1, S1>>({prefix + "abs"});

  registerUnaryFloatingPoint<NegateFunction>({prefix + "negate"});
  registerFunction<NegateFunction, LongDecimal<P1, S1>, LongDecimal<P1, S1>>(
      {prefix + "negate"});
  registerFunction<NegateFunction, ShortDecimal<P1, S1>, ShortDecimal<P1, S1>>(
      {prefix + "negate"});

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
  registerFunction<
      CosineSimilarityFunction,
      double,
      Map<Varchar, double>,
      Map<Varchar, double>>({prefix + "cosine_similarity"});
}

} // namespace

void registerMathematicalFunctions(const std::string& prefix = "") {
  registerMathFunctions(prefix);
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, prefix + "not");

  registerDecimalFloor(prefix);
  registerDecimalRound(prefix);
}

} // namespace facebook::velox::functions
