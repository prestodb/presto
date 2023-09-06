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

#include "velox/expression/RowConstructor.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::exec {

TypePtr RowConstructorCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  auto numInput = argTypes.size();
  std::vector<std::string> names(numInput);
  std::vector<TypePtr> types(numInput);
  for (auto i = 0; i < numInput; i++) {
    types[i] = argTypes[i];
    names[i] = fmt::format("c{}", i + 1);
  }
  return ROW(std::move(names), std::move(types));
}

ExprPtr RowConstructorCallToSpecialForm::constructSpecialForm(
    const std::string& name,
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  auto rowConstructorVectorFunction =
      vectorFunctionFactories().withRLock([&config, &name](auto& functionMap) {
        auto functionIterator = functionMap.find(name);
        return functionIterator->second.factory(name, {}, config);
      });

  return std::make_shared<Expr>(
      type,
      std::move(compiledChildren),
      rowConstructorVectorFunction,
      name,
      trackCpuUsage);
}

ExprPtr RowConstructorCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  return constructSpecialForm(
      kRowConstructor,
      type,
      std::move(compiledChildren),
      trackCpuUsage,
      config);
}
} // namespace facebook::velox::exec
