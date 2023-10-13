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

#include "velox/functions/lib/CheckedArithmetic.h"
#include "velox/functions/lib/RegistrationHelpers.h"

namespace facebook::velox::functions {

void registerCheckedArithmeticFunctions(const std::string& prefix) {
  registerBinaryIntegral<CheckedPlusFunction>({prefix + "plus"});
  registerBinaryIntegral<CheckedMinusFunction>({prefix + "minus"});
  registerBinaryIntegral<CheckedMultiplyFunction>({prefix + "multiply"});
  registerBinaryIntegral<CheckedModulusFunction>({prefix + "mod"});
  registerBinaryIntegral<CheckedDivideFunction>({prefix + "divide"});
  registerUnaryIntegral<CheckedNegateFunction>({prefix + "negate"});
}

} // namespace facebook::velox::functions
