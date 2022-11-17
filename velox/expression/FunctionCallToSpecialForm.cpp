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

#include "velox/expression/CastExpr.h"
#include "velox/expression/CoalesceExpr.h"
#include "velox/expression/ConjunctExpr.h"
#include "velox/expression/SwitchExpr.h"
#include "velox/expression/TryExpr.h"

namespace facebook::velox::exec {
namespace {
using RegistryType =
    std::unordered_map<std::string, std::unique_ptr<FunctionCallToSpecialForm>>;

RegistryType makeRegistry() {
  RegistryType registry;
  registry.emplace(
      "and", std::make_unique<ConjunctCallToSpecialForm>(true /* isAnd */));
  registry.emplace("cast", std::make_unique<CastCallToSpecialForm>());
  registry.emplace("coalesce", std::make_unique<CoalesceCallToSpecialForm>());
  registry.emplace("if", std::make_unique<IfCallToSpecialForm>());
  registry.emplace(
      "or", std::make_unique<ConjunctCallToSpecialForm>(false /* isAnd */));
  registry.emplace("switch", std::make_unique<SwitchCallToSpecialForm>());
  registry.emplace("try", std::make_unique<TryCallToSpecialForm>());

  return registry;
}

RegistryType& functionCallToSpecialFormRegistry() {
  static RegistryType registry = makeRegistry();
  return registry;
}
} // namespace

TypePtr resolveTypeForSpecialForm(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  auto& registry = functionCallToSpecialFormRegistry();

  auto it = registry.find(functionName);
  if (it == registry.end()) {
    return nullptr;
  }

  return it->second->resolveType(argTypes);
}

ExprPtr constructSpecialForm(
    const std::string& functionName,
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage) {
  auto& registry = functionCallToSpecialFormRegistry();

  auto it = registry.find(functionName);
  if (it == registry.end()) {
    return nullptr;
  }

  return it->second->constructSpecialForm(
      type, std::move(compiledChildren), trackCpuUsage);
}

bool isFunctionCallToSpecialFormRegistered(const std::string& functionName) {
  const auto& registry = functionCallToSpecialFormRegistry();

  auto it = registry.find(functionName);
  return it != registry.end();
}
} // namespace facebook::velox::exec
