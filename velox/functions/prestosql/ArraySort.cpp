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

#include "velox/functions/prestosql/ArraySort.h"

namespace facebook::velox::functions {

std::shared_ptr<exec::VectorFunction> makeArraySortAsc(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  if (inputArgs.size() == 2) {
    return makeArraySortLambdaFunction(name, inputArgs, config, true, true);
  }

  VELOX_CHECK_EQ(inputArgs.size(), 1);
  return makeArraySort(name, inputArgs, config, true, false, true);
}

std::shared_ptr<exec::VectorFunction> makeArraySortDesc(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  if (inputArgs.size() == 2) {
    return makeArraySortLambdaFunction(name, inputArgs, config, false, true);
  }

  VELOX_CHECK_EQ(inputArgs.size(), 1);
  return makeArraySort(name, inputArgs, config, false, false, true);
}

} // namespace facebook::velox::functions
