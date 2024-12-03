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
#include "velox/expression/SpecialFormRegistry.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/sparksql/In.h"
#include "velox/functions/sparksql/MonotonicallyIncreasingId.h"
#include "velox/functions/sparksql/RaiseError.h"
#include "velox/functions/sparksql/SparkPartitionId.h"
#include "velox/functions/sparksql/UnscaledValueFunction.h"
#include "velox/functions/sparksql/Uuid.h"

namespace facebook::velox::functions::sparksql {
void registerMiscFunctions(const std::string& prefix) {
  registerFunction<MonotonicallyIncreasingIdFunction, int64_t>(
      {prefix + "monotonically_increasing_id"});
  registerFunction<RaiseErrorFunction, UnknownValue, Varchar>(
      {prefix + "raise_error"});
  registerFunction<SparkPartitionIdFunction, int32_t>(
      {prefix + "spark_partition_id"});
  registerIn(prefix);
  exec::registerVectorFunction(
      prefix + "unscaled_value",
      unscaledValueSignatures(),
      makeUnscaledValue());
  registerFunction<UuidFunction, Varchar, Constant<int64_t>>({prefix + "uuid"});
}
} // namespace facebook::velox::functions::sparksql
