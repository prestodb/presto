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
#include "velox/functions/prestosql/RegisterArithmetic.h"

#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/Bitwise.h"

namespace facebook::velox::functions {
namespace {
template <template <class> class T>
void registerBitwiseBinaryIntegral(const std::vector<std::string>& aliases) {
  registerFunction<T<int8_t>, int64_t, int8_t, int8_t>(aliases);
  registerFunction<T<int16_t>, int64_t, int16_t, int16_t>(aliases);
  registerFunction<T<int32_t>, int64_t, int32_t, int32_t>(aliases);
  registerFunction<T<int64_t>, int64_t, int64_t, int64_t>(aliases);
}

template <template <class> class T>
void registerBitwiseUnaryIntegral(const std::vector<std::string>& aliases) {
  registerFunction<T<int8_t>, int64_t, int8_t>(aliases);
  registerFunction<T<int16_t>, int64_t, int16_t>(aliases);
  registerFunction<T<int32_t>, int64_t, int32_t>(aliases);
  registerFunction<T<int64_t>, int64_t, int64_t>(aliases);
}

} // namespace

void registerArithmeticFunctions() {
  registerBinaryFloatingPoint<PlusFunction>({"plus"});
  registerBinaryFloatingPoint<MinusFunction>({"minus"});
  registerBinaryFloatingPoint<MultiplyFunction>({"multiply"});
  registerBinaryFloatingPoint<DivideFunction>({"divide"});
  registerBinaryFloatingPoint<ModulusFunction>({"modulus"});
  registerUnaryNumeric<udf_ceil>({"ceil", "ceiling"});
  registerUnaryNumeric<udf_floor>({});
  registerUnaryNumeric<udf_abs>({});
  registerUnaryFloatingPoint<udf_negate>({});
  registerFunction<udf_radians, double, double>({"radians"});
  registerUnaryNumeric<udf_round>({"round"});
  registerFunction<udf_round<int8_t>, int8_t, int8_t, int32_t>({"round"});
  registerFunction<udf_round<int16_t>, int16_t, int16_t, int32_t>({"round"});
  registerFunction<udf_round<int32_t>, int32_t, int32_t, int32_t>({"round"});
  registerFunction<udf_round<int64_t>, int64_t, int64_t, int32_t>({"round"});
  registerFunction<udf_round<double>, double, double, int32_t>({"round"});
  registerFunction<udf_round<float>, float, float, int32_t>({"round"});
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
  registerBitwiseBinaryIntegral<udf_bitwise_and>({});
  registerBitwiseUnaryIntegral<udf_bitwise_not>({});
  registerBitwiseBinaryIntegral<udf_bitwise_or>({});
  registerBitwiseBinaryIntegral<udf_bitwise_xor>({});
  registerBitwiseBinaryIntegral<udf_bitwise_arithmetic_shift_right>({});
  registerBitwiseBinaryIntegral<udf_bitwise_left_shift>({});
  registerBitwiseBinaryIntegral<udf_bitwise_right_shift>({});
  registerBitwiseBinaryIntegral<udf_bitwise_right_shift_arithmetic>({});
  registerFunction<
      udf_bitwise_logical_shift_right,
      int64_t,
      int64_t,
      int64_t,
      int64_t>({});
  registerFunction<udf_bitwise_shift_left, int64_t, int64_t, int64_t, int64_t>(
      {});
  registerUnaryNumeric<udf_sign>({});
}

} // namespace facebook::velox::functions
