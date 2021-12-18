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
#include "velox/functions/sparksql/RegisterCompare.h"

#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/Comparisons.h"

namespace facebook::velox::functions::sparksql {

void registerCompareFunctions(const std::string& prefix) {
  registerBinaryScalar<EqFunction, bool>({prefix + "equalto"});
  registerBinaryScalar<NeqFunction, bool>({prefix + "notequalto"});
  registerBinaryScalar<LtFunction, bool>({prefix + "lessthan"});
  registerBinaryScalar<GtFunction, bool>({prefix + "greaterthan"});
  registerBinaryScalar<LteFunction, bool>({prefix + "lessthanorequal"});
  registerBinaryScalar<GteFunction, bool>({prefix + "greaterthanorequal"});

  registerFunction<BetweenFunction, bool, int8_t, int8_t, int8_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int16_t, int16_t, int16_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int32_t, int32_t, int32_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int64_t, int64_t, int64_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, double, double, double>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, float, float, float>(
      {prefix + "between"});
}

} // namespace facebook::velox::functions::sparksql
