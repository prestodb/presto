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
#pragma once

#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {
class FunctionSignature;

template <typename T>
std::shared_ptr<const T> GetSingletonUdfMetadata(
    std::shared_ptr<const Type> returnType) {
  static auto instance = std::make_shared<const T>(std::move(returnType));
  return instance;
}

template <typename Function>
class FunctionRegistry {
  using FunctionFactory = std::function<std::unique_ptr<Function>()>;
  using FunctionEntry = std::unordered_map<FunctionSignature, FunctionFactory>;
  using FunctionMap = std::unordered_map<std::string, FunctionEntry>;

 public:
  template <typename UDF>
  void registerFunction(
      const std::vector<std::string>& aliases = {},
      std::shared_ptr<const Type> returnType = nullptr) {
    auto metadata =
        GetSingletonUdfMetadata<typename UDF::Metadata>(std::move(returnType));
    auto&& names = aliases.empty()
        ? std::vector<std::string>{metadata->getName()}
        : aliases;

    registerFunctionInternal(names, *metadata->signature(), [metadata]() {
      return CreateUdf<UDF>(metadata->returnType());
    });
  }

  std::vector<std::string> getFunctionNames() {
    std::vector<std::string> result;
    result.reserve(registeredFunctions_.size());

    for (const auto& entry : registeredFunctions_) {
      result.push_back(entry.first);
    }

    return result;
  }

  std::vector<const FunctionSignature*> getFunctionSignatures(
      const std::string& name) {
    std::vector<const FunctionSignature*> signatures;
    if (auto entry = getFunctionEntry(name)) {
      signatures.reserve(entry->size());
      for (const auto& pair : *entry) {
        signatures.emplace_back(&pair.first);
      }
    }

    return signatures;
  }

  std::unique_ptr<Function> createFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes) {
    if (auto entry = getFunctionEntry(name)) {
      for (const auto& [candidateSignature, functionFactory] : *entry) {
        if (SignatureBinder(candidateSignature, argTypes).tryBind()) {
          return functionFactory();
        }
      }
    }

    return nullptr;
  }

 private:
  template <typename T>
  static std::unique_ptr<T> CreateUdf(std::shared_ptr<const Type> returnType) {
    return std::make_unique<T>(std::move(returnType));
  }

  void registerFunctionInternal(
      const std::vector<std::string>& names,
      const exec::FunctionSignature& signature,
      const FunctionFactory& factory) {
    for (const auto& name : names) {
      if (auto entry = getFunctionEntry(name)) {
        entry->insert({signature, factory});
      } else {
        registeredFunctions_[name] = FunctionEntry{{signature, factory}};
      }
    }
  }

  FunctionEntry* getFunctionEntry(const std::string& name) {
    auto it = registeredFunctions_.find(name);
    if (it != registeredFunctions_.end()) {
      return &it->second;
    }

    return nullptr;
  }

  FunctionMap registeredFunctions_;
};
} // namespace facebook::velox::exec
