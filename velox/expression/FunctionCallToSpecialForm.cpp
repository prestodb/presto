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

#include "velox/expression/FunctionCallToSpecialForm.h"

#include "velox/expression/SpecialFormRegistry.h"

namespace facebook::velox::exec {

TypePtr resolveTypeForSpecialForm(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  auto specialForm = specialFormRegistry().getSpecialForm(functionName);
  if (specialForm == nullptr) {
    return nullptr;
  }

  return specialForm->resolveType(argTypes);
}

ExprPtr constructSpecialForm(
    const std::string& functionName,
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  auto specialForm = specialFormRegistry().getSpecialForm(functionName);
  if (specialForm == nullptr) {
    return nullptr;
  }

  return specialForm->constructSpecialForm(
      type, std::move(compiledChildren), trackCpuUsage, config);
}
} // namespace facebook::velox::exec
