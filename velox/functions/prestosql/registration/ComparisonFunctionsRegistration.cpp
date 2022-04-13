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
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions {

void registerComparisonFunctions() {
  registerBinaryScalar<EqFunction, bool>({"eq"});
  registerFunction<EqFunction, bool, Generic<T1>, Generic<T1>>({"eq"});

  registerBinaryScalar<NeqFunction, bool>({"neq"});
  registerBinaryScalar<LtFunction, bool>({"lt"});
  registerBinaryScalar<GtFunction, bool>({"gt"});
  registerBinaryScalar<LteFunction, bool>({"lte"});
  registerBinaryScalar<GteFunction, bool>({"gte"});

  registerFunction<BetweenFunction, bool, int8_t, int8_t, int8_t>({"between"});
  registerFunction<BetweenFunction, bool, int16_t, int16_t, int16_t>(
      {"between"});
  registerFunction<BetweenFunction, bool, int32_t, int32_t, int32_t>(
      {"between"});
  registerFunction<BetweenFunction, bool, int64_t, int64_t, int64_t>(
      {"between"});
  registerFunction<BetweenFunction, bool, double, double, double>({"between"});
  registerFunction<BetweenFunction, bool, float, float, float>({"between"});
  registerFunction<BetweenFunction, bool, Varchar, Varchar, Varchar>(
      {"between"});
  registerFunction<BetweenFunction, bool, Date, Date, Date>({"between"});
}

} // namespace facebook::velox::functions
