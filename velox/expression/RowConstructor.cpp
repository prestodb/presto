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
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  auto [function, metadata] = vectorFunctionFactories().withRLock(
      [&config](auto& functionMap) -> std::pair<
                                       std::shared_ptr<VectorFunction>,
                                       VectorFunctionMetadata> {
        auto functionIterator = functionMap.find(kRowConstructor);
        if (functionIterator != functionMap.end()) {
          return {
              functionIterator->second.factory(kRowConstructor, {}, config),
              functionIterator->second.metadata};
        } else {
          VELOX_FAIL("Function {} is not registered.", kRowConstructor);
        }
      });

  return std::make_shared<Expr>(
      type,
      std::move(compiledChildren),
      function,
      metadata,
      kRowConstructor,
      trackCpuUsage);
}
} // namespace facebook::velox::exec
