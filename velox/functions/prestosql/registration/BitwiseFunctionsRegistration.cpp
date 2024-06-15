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

template <template <class> class T>
void registerShift(const std::vector<std::string>& aliases) {
  registerFunction<T, int8_t, int8_t, int32_t>(aliases);
  registerFunction<T, int16_t, int16_t, int32_t>(aliases);
  registerFunction<T, int32_t, int32_t, int32_t>(aliases);
  registerFunction<T, int64_t, int64_t, int32_t>(aliases);
}
} // namespace

void registerBitwiseFunctions(const std::string& prefix) {
  registerBitwiseBinaryIntegral<BitwiseAndFunction>({prefix + "bitwise_and"});
  registerBitwiseUnaryIntegral<BitwiseNotFunction>({prefix + "bitwise_not"});
  registerBitwiseBinaryIntegral<BitwiseOrFunction>({prefix + "bitwise_or"});
  registerBitwiseBinaryIntegral<BitwiseXorFunction>({prefix + "bitwise_xor"});
  registerBitwiseBinaryIntegral<BitCountFunction>({prefix + "bit_count"});
  registerFunction<
      BitwiseArithmeticShiftRightFunction,
      int64_t,
      int64_t,
      int64_t>({prefix + "bitwise_arithmetic_shift_right"});
  registerShift<BitwiseLeftShiftFunction>({prefix + "bitwise_left_shift"});
  registerShift<BitwiseRightShiftFunction>({prefix + "bitwise_right_shift"});
  registerShift<BitwiseRightShiftArithmeticFunction>(
      {prefix + "bitwise_right_shift_arithmetic"});
  registerFunction<
      BitwiseLogicalShiftRightFunction,
      int64_t,
      int64_t,
      int64_t,
      int64_t>({prefix + "bitwise_logical_shift_right"});
  registerFunction<
      BitwiseShiftLeftFunction,
      int64_t,
      int64_t,
      int64_t,
      int64_t>({prefix + "bitwise_shift_left"});
}

} // namespace facebook::velox::functions
