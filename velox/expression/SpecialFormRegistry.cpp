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

namespace facebook::velox::exec {

namespace {

SpecialFormRegistry& specialFormRegistryInternal() {
  static SpecialFormRegistry instance;
  return instance;
}
} // namespace

void SpecialFormRegistry::registerFunctionCallToSpecialForm(
    const std::string& name,
    std::unique_ptr<FunctionCallToSpecialForm> functionCallToSpecialForm) {
  const auto sanitizedName = sanitizeName(name);
  registry_.withWLock([&](auto& map) {
    map[sanitizedName] = std::move(functionCallToSpecialForm);
  });
}

void SpecialFormRegistry::unregisterAllFunctionCallToSpecialForm() {
  registry_.withWLock([&](auto& map) { map.clear(); });
}

FunctionCallToSpecialForm* FOLLY_NULLABLE
SpecialFormRegistry::getSpecialForm(const std::string& name) const {
  const auto sanitizedName = sanitizeName(name);
  FunctionCallToSpecialForm* specialForm = nullptr;
  registry_.withRLock([&](const auto& map) {
    auto it = map.find(sanitizedName);
    if (it != map.end()) {
      specialForm = it->second.get();
    }
  });
  return specialForm;
}

std::vector<std::string> SpecialFormRegistry::getSpecialFormNames() const {
  std::vector<std::string> names;
  registry_.withRLock([&](const auto& map) {
    names.reserve(map.size());
    for (const auto& [name, _] : map) {
      names.push_back(name);
    }
  });
  return names;
}

const SpecialFormRegistry& specialFormRegistry() {
  return specialFormRegistryInternal();
}

SpecialFormRegistry& mutableSpecialFormRegistry() {
  return specialFormRegistryInternal();
}

void registerFunctionCallToSpecialForm(
    const std::string& name,
    std::unique_ptr<FunctionCallToSpecialForm> functionCallToSpecialForm) {
  mutableSpecialFormRegistry().registerFunctionCallToSpecialForm(
      name, std::move(functionCallToSpecialForm));
}

void unregisterAllFunctionCallToSpecialForm() {
  mutableSpecialFormRegistry().unregisterAllFunctionCallToSpecialForm();
}

bool isFunctionCallToSpecialFormRegistered(const std::string& functionName) {
  return mutableSpecialFormRegistry().getSpecialForm(functionName) != nullptr;
}
} // namespace facebook::velox::exec
