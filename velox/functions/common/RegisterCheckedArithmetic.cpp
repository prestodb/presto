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
#include "velox/functions/common/RegisterCheckedArithmetic.h"

#include "velox/functions/common/CheckedArithmetic.h"
#include "velox/functions/lib/RegistrationHelpers.h"

namespace facebook::velox::functions {

void registerCheckedArithmeticFunctions() {
  registerBinaryIntegral<udf_checked_plus>({"plus"});
  registerBinaryIntegral<udf_checked_minus>({"minus"});
  registerBinaryIntegral<udf_checked_multiply>({"multiply"});
  registerBinaryIntegral<udf_checked_modulus>({"modulus"});
  registerBinaryIntegral<udf_checked_divide>({"divide"});
  registerUnaryIntegral<udf_checked_negate>({"negate"});
}

} // namespace facebook::velox::functions
