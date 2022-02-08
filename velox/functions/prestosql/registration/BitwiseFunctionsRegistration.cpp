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
#include "velox/functions/prestosql/Bitwise.h"

namespace facebook::velox::functions {
namespace {
template <template <class> class T>
void registerBitwiseBinaryIntegral(const std::vector<std::string>& aliases) {
  registerFunction<T, int64_t, int8_t, int8_t>(aliases);
  registerFunction<T, int64_t, int16_t, int16_t>(aliases);
  registerFunction<T, int64_t, int32_t, int32_t>(aliases);
  registerFunction<T, int64_t, int64_t, int64_t>(aliases);
}

template <template <class> class T>
void registerBitwiseUnaryIntegral(const std::vector<std::string>& aliases) {
  registerFunction<T, int64_t, int8_t>(aliases);
  registerFunction<T, int64_t, int16_t>(aliases);
  registerFunction<T, int64_t, int32_t>(aliases);
  registerFunction<T, int64_t, int64_t>(aliases);
}
} // namespace

void registerBitwiseFunctions() {
  registerBitwiseBinaryIntegral<BitwiseAndFunction>({"bitwise_and"});
  registerBitwiseUnaryIntegral<BitwiseNotFunction>({"bitwise_not"});
  registerBitwiseBinaryIntegral<BitwiseOrFunction>({"bitwise_or"});
  registerBitwiseBinaryIntegral<BitwiseXorFunction>({"bitwise_xor"});
  registerBitwiseBinaryIntegral<BitCountFunction>({"bit_count"});
  registerBitwiseBinaryIntegral<BitwiseArithmeticShiftRightFunction>(
      {"bitwise_arithmetic_shift_right"});
  registerBitwiseBinaryIntegral<BitwiseLeftShiftFunction>(
      {"bitwise_left_shift"});
  registerBitwiseBinaryIntegral<BitwiseRightShiftFunction>(
      {"bitwise_right_shift"});
  registerBitwiseBinaryIntegral<BitwiseRightShiftArithmeticFunction>(
      {"bitwise_right_shift_arithmetic"});
  registerFunction<
      BitwiseLogicalShiftRightFunction,
      int64_t,
      int64_t,
      int64_t,
      int64_t>({"bitwise_logical_shift_right"});
  registerFunction<
      BitwiseShiftLeftFunction,
      int64_t,
      int64_t,
      int64_t,
      int64_t>({"bitwise_shift_left"});
}

} // namespace facebook::velox::functions
