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
  registerBinaryFloatingPoint<udf_plus>({});
  registerBinaryFloatingPoint<udf_minus>({});
  registerBinaryFloatingPoint<udf_multiply>({});
  registerBinaryFloatingPoint<udf_divide>({});
  registerUnaryNumeric<udf_ceil>({"ceil", "ceiling"});
  registerUnaryNumeric<udf_floor>({});
  registerUnaryNumeric<udf_abs>({});
  registerUnaryFloatingPoint<udf_negate>({});
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
  registerFunction<udf_sqrt, double, double>({"sqrt"});
  registerFunction<udf_cbrt, double, double>({"cbrt"});
  registerFunction<udf_width_bucket, int64_t, double, double, double, int64_t>(
      {"width_bucket"});
  registerBitwiseBinaryIntegral<udf_bitwise_and>({});
  registerBitwiseUnaryIntegral<udf_bitwise_not>({});
  registerBitwiseBinaryIntegral<udf_bitwise_or>({});
  registerBitwiseBinaryIntegral<udf_bitwise_xor>({});
}

} // namespace facebook::velox::functions
