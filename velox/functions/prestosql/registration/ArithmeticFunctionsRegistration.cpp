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
#include "velox/functions/prestosql/Rand.h"

namespace facebook::velox::functions {

namespace {
void registerSimpleFunctions() {
  registerBinaryFloatingPoint<PlusFunction>({"plus"});
  registerBinaryFloatingPoint<MinusFunction>({"minus"});
  registerBinaryFloatingPoint<MultiplyFunction>({"multiply"});
  registerBinaryFloatingPoint<DivideFunction>({"divide"});
  registerBinaryFloatingPoint<ModulusFunction>({"mod"});
  registerUnaryNumeric<CeilFunction>({"ceil", "ceiling"});
  registerUnaryNumeric<FloorFunction>({"floor"});
  registerUnaryNumeric<AbsFunction>({"abs"});
  registerUnaryFloatingPoint<NegateFunction>({"negate"});
  registerFunction<RadiansFunction, double, double>({"radians"});
  registerFunction<DegreesFunction, double, double>({"degrees"});
  registerUnaryNumeric<RoundFunction>({"round"});
  registerFunction<RoundFunction, int8_t, int8_t, int32_t>({"round"});
  registerFunction<RoundFunction, int16_t, int16_t, int32_t>({"round"});
  registerFunction<RoundFunction, int32_t, int32_t, int32_t>({"round"});
  registerFunction<RoundFunction, int64_t, int64_t, int32_t>({"round"});
  registerFunction<RoundFunction, double, double, int32_t>({"round"});
  registerFunction<RoundFunction, float, float, int32_t>({"round"});
  registerFunction<PowerFunction, double, double, double>({"power", "pow"});
  registerFunction<PowerFunction, double, int64_t, int64_t>({"power", "pow"});
  registerFunction<ExpFunction, double, double>({"exp"});
  registerFunction<ClampFunction, int8_t, int8_t, int8_t, int8_t>({"clamp"});
  registerFunction<ClampFunction, int16_t, int16_t, int16_t, int16_t>(
      {"clamp"});
  registerFunction<ClampFunction, int32_t, int32_t, int32_t, int32_t>(
      {"clamp"});
  registerFunction<ClampFunction, int64_t, int64_t, int64_t, int64_t>(
      {"clamp"});
  registerFunction<ClampFunction, double, double, double, double>({"clamp"});
  registerFunction<ClampFunction, float, float, float, float>({"clamp"});
  registerFunction<LnFunction, double, double>({"ln"});
  registerFunction<Log2Function, double, double>({"log2"});
  registerFunction<Log10Function, double, double>({"log10"});
  registerFunction<CosFunction, double, double>({"cos"});
  registerFunction<CoshFunction, double, double>({"cosh"});
  registerFunction<AcosFunction, double, double>({"acos"});
  registerFunction<SinFunction, double, double>({"sin"});
  registerFunction<AsinFunction, double, double>({"asin"});
  registerFunction<TanFunction, double, double>({"tan"});
  registerFunction<TanhFunction, double, double>({"tanh"});
  registerFunction<AtanFunction, double, double>({"atan"});
  registerFunction<Atan2Function, double, double, double>({"atan2"});
  registerFunction<SqrtFunction, double, double>({"sqrt"});
  registerFunction<CbrtFunction, double, double>({"cbrt"});
  registerFunction<
      WidthBucketFunction,
      int64_t,
      double,
      double,
      double,
      int64_t>({"width_bucket"});

  registerUnaryNumeric<SignFunction>({"sign"});
  registerFunction<InfinityFunction, double>({"infinity"});
  registerFunction<IsFiniteFunction, bool, double>({"is_finite"});
  registerFunction<IsInfiniteFunction, bool, double>({"is_infinite"});
  registerFunction<IsNanFunction, bool, double>({"is_nan"});
  registerFunction<NanFunction, double>({"nan"});
  registerFunction<RandFunction, double>({"rand", "random"});
  registerFunction<FromBaseFunction, int64_t, Varchar, int64_t>({"from_base"});
  registerFunction<ToBaseFunction, Varchar, int64_t, int64_t>({"to_base"});
  registerFunction<PiFunction, double>({"pi"});
  registerFunction<EulerConstantFunction, double>({"e"});
}

} // namespace

void registerArithmeticFunctions() {
  registerSimpleFunctions();
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, "not");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_decimal_add, "plus");
}

} // namespace facebook::velox::functions
