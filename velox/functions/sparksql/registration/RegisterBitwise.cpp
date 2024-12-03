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
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/sparksql/Bitwise.h"

namespace facebook::velox::functions::sparksql {

void registerBitwiseFunctions(const std::string& prefix) {
  registerBinaryIntegral<BitwiseAndFunction>({prefix + "bitwise_and"});
  registerBinaryIntegral<BitwiseOrFunction>({prefix + "bitwise_or"});
  registerBinaryIntegral<BitwiseXorFunction>({prefix + "bitwise_xor"});

  registerUnaryIntegral<BitwiseNotFunction>({prefix + "bitwise_not"});

  registerFunction<BitCountFunction, int32_t, bool>({prefix + "bit_count"});
  registerFunction<BitCountFunction, int32_t, int8_t>({prefix + "bit_count"});
  registerFunction<BitCountFunction, int32_t, int16_t>({prefix + "bit_count"});
  registerFunction<BitCountFunction, int32_t, int32_t>({prefix + "bit_count"});
  registerFunction<BitCountFunction, int32_t, int64_t>({prefix + "bit_count"});

  registerFunction<BitGetFunction, int8_t, int8_t, int32_t>(
      {prefix + "bit_get"});
  registerFunction<BitGetFunction, int8_t, int16_t, int32_t>(
      {prefix + "bit_get"});
  registerFunction<BitGetFunction, int8_t, int32_t, int32_t>(
      {prefix + "bit_get"});
  registerFunction<BitGetFunction, int8_t, int64_t, int32_t>(
      {prefix + "bit_get"});

  registerFunction<ShiftLeftFunction, int32_t, int32_t, int32_t>(
      {prefix + "shiftleft"});
  registerFunction<ShiftLeftFunction, int64_t, int64_t, int32_t>(
      {prefix + "shiftleft"});

  registerFunction<ShiftRightFunction, int32_t, int32_t, int32_t>(
      {prefix + "shiftright"});
  registerFunction<ShiftRightFunction, int64_t, int64_t, int32_t>(
      {prefix + "shiftright"});
}

} // namespace facebook::velox::functions::sparksql
