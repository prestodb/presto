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
  registerBinaryScalar<udf_eq, bool>({prefix + "equalto"});
  registerBinaryScalar<udf_neq, bool>({prefix + "notequalto"});
  registerBinaryScalar<udf_lt, bool>({prefix + "lessthan"});
  registerBinaryScalar<udf_gt, bool>({prefix + "lessthanorequal"});
  registerBinaryScalar<udf_lte, bool>({prefix + "greaterthan"});
  registerBinaryScalar<udf_gte, bool>({prefix + "greaterthanorequal"});

  registerFunction<udf_between<int8_t>, bool, int8_t, int8_t, int8_t>(
      {prefix + "between"});
  registerFunction<udf_between<int16_t>, bool, int16_t, int16_t, int16_t>(
      {prefix + "between"});
  registerFunction<udf_between<int32_t>, bool, int32_t, int32_t, int32_t>(
      {prefix + "between"});
  registerFunction<udf_between<int64_t>, bool, int64_t, int64_t, int64_t>(
      {prefix + "between"});
  registerFunction<udf_between<double>, bool, double, double, double>(
      {prefix + "between"});
  registerFunction<udf_between<float>, bool, float, float, float>(
      {prefix + "between"});
}

} // namespace facebook::velox::functions::sparksql
