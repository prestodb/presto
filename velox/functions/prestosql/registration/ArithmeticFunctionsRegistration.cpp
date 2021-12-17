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
  registerFunction<udf_radians, double, double>({"radians"});
  registerUnaryNumeric<RoundFunction>({"round"});
  registerFunction<RoundFunction, int8_t, int8_t, int32_t>({"round"});
  registerFunction<RoundFunction, int16_t, int16_t, int32_t>({"round"});
  registerFunction<RoundFunction, int32_t, int32_t, int32_t>({"round"});
  registerFunction<RoundFunction, int64_t, int64_t, int32_t>({"round"});
  registerFunction<RoundFunction, double, double, int32_t>({"round"});
  registerFunction<RoundFunction, float, float, int32_t>({"round"});
  registerFunction<udf_power<double>, double, double, double>({"power", "pow"});
  registerFunction<udf_power<int64_t>, double, int64_t, int64_t>(
      {"power", "pow"});
  registerFunction<udf_exp, double, double>({"exp"});
  registerFunction<udf_clamp<int8_t>, int8_t, int8_t, int8_t, int8_t>(
      {"clamp"});
  registerFunction<udf_clamp<int16_t>, int16_t, int16_t, int16_t, int16_t>(
      {"clamp"});
  registerFunction<udf_clamp<int32_t>, int32_t, int32_t, int32_t, int32_t>(
      {"clamp"});
  registerFunction<udf_clamp<int64_t>, int64_t, int64_t, int64_t, int64_t>(
      {"clamp"});
  registerFunction<udf_clamp<double>, double, double, double, double>(
      {"clamp"});
  registerFunction<udf_clamp<float>, float, float, float, float>({"clamp"});
  registerFunction<udf_ln, double, double>({"ln"});
  registerFunction<udf_log2, double, double>({"log2"});
  registerFunction<udf_log10, double, double>({"log10"});
  registerFunction<udf_cos, double, double>({"cos"});
  registerFunction<udf_cosh, double, double>({"cosh"});
  registerFunction<udf_acos, double, double>({"acos"});
  registerFunction<udf_sin, double, double>({"sin"});
  registerFunction<udf_asin, double, double>({"asin"});
  registerFunction<udf_tan, double, double>({"tan"});
  registerFunction<udf_tanh, double, double>({"tanh"});
  registerFunction<udf_atan, double, double>({"atan"});
  registerFunction<udf_atan2, double, double, double>({"atan2"});
  registerFunction<udf_sqrt, double, double>({"sqrt"});
  registerFunction<udf_cbrt, double, double>({"cbrt"});
  registerFunction<udf_width_bucket, int64_t, double, double, double, int64_t>(
      {"width_bucket"});

  registerUnaryNumeric<SignFunction>({"sign"});
  registerFunction<udf_infinity, double>({});
  registerFunction<udf_is_finite, bool, double>({});
  registerFunction<udf_is_infinite, bool, double>({});
  registerFunction<udf_is_nan, bool, double>({});
  registerFunction<udf_nan, double>({});
  registerFunction<RandFunction, double>({"rand", "random"});
}

} // namespace

void registerArithmeticFunctions() {
  registerSimpleFunctions();
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, "not");
}

} // namespace facebook::velox::functions
