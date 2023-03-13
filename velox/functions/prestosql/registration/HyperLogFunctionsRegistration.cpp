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
#include "velox/functions/prestosql/HyperLogLogFunctions.h"

namespace facebook::velox::functions {

void registerHyperLogFunctions(const std::string& prefix) {
  registerHyperLogLogType();

  registerFunction<CardinalityFunction, int64_t, HyperLogLog>(
      {prefix + "cardinality"});

  registerFunction<EmptyApproxSetWithMaxErrorFunction, HyperLogLog, double>(
      {prefix + "empty_approx_set"});
  registerFunction<EmptyApproxSetFunction, HyperLogLog>(
      {prefix + "empty_approx_set"});
}
} // namespace facebook::velox::functions
