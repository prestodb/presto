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
const std::shared_ptr<const T>& GetSingletonUdfMetadata(
    std::shared_ptr<const Type> returnType) {
  static auto instance = std::make_shared<const T>(std::move(returnType));
  return instance;
}

template <typename Function, typename Metadata>
struct FunctionEntry {
  using FunctionFactory = std::function<std::unique_ptr<Function>()>;

  FunctionEntry(
      const std::shared_ptr<const Metadata>& metadata,
      const FunctionFactory& factory)
      : metadata_{metadata}, factory_{factory} {}

  const Metadata& getMetadata() const {
    return *metadata_;
  }

  std::unique_ptr<Function> createFunction() const {
    return factory_();
  }

 private:
  const std::shared_ptr<const Metadata> metadata_;
  const FunctionFactory factory_;
};

template <typename Function, typename Metadata>
class FunctionRegistry {
  using SignatureMap = std::unordered_map<
      FunctionSignature,
      std::unique_ptr<const FunctionEntry<Function, Metadata>>>;
  using FunctionMap = std::unordered_map<std::string, SignatureMap>;

 public:
  template <typename UDF>
  void registerFunction(
      const std::vector<std::string>& aliases = {},
      std::shared_ptr<const Type> returnType = nullptr) {
    const auto& metadata =
        GetSingletonUdfMetadata<typename UDF::Metadata>(std::move(returnType));
    const auto factory = [metadata]() {
      return CreateUdf<UDF>(metadata->returnType());
    };

    if (aliases.empty()) {
      registerFunctionInternal(metadata->getName(), metadata, factory);
    } else {
      for (const auto& name : aliases) {
        registerFunctionInternal(name, metadata, factory);
      }
    }
  }

  std::vector<std::string> getFunctionNames() const {
    std::vector<std::string> result;
    result.reserve(registeredFunctions_.size());

    for (const auto& entry : registeredFunctions_) {
      result.push_back(entry.first);
    }

    return result;
  }

  std::vector<const FunctionSignature*> getFunctionSignatures(
      const std::string& name) const {
    std::vector<const FunctionSignature*> signatures;
    if (const auto* signatureMap = getSignatureMap(name)) {
      signatures.reserve(signatureMap->size());
      for (const auto& pair : *signatureMap) {
        signatures.emplace_back(&pair.first);
      }
    }

    return signatures;
  }

  const FunctionEntry<Function, Metadata>* resolveFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes) const {
    const FunctionEntry<Function, Metadata>* selectedCandidate = nullptr;

    if (const auto* signatureMap = getSignatureMap(name)) {
      for (const auto& [candidateSignature, functionEntry] : *signatureMap) {
        if (SignatureBinder(candidateSignature, argTypes).tryBind()) {
          auto* currentCandidate = functionEntry.get();
          if (!selectedCandidate ||
              currentCandidate->getMetadata().priority() <
                  selectedCandidate->getMetadata().priority()) {
            selectedCandidate = currentCandidate;
          }
        }
      }
    }

    return selectedCandidate;
  }

 private:
  template <typename T>
  static std::unique_ptr<T> CreateUdf(std::shared_ptr<const Type> returnType) {
    return std::make_unique<T>(std::move(returnType));
  }

  void registerFunctionInternal(
      const std::string& name,
      const std::shared_ptr<const Metadata>& metadata,
      const typename FunctionEntry<Function, Metadata>::FunctionFactory&
          factory) {
    const auto sanitizedName = sanitizeFunctionName(name);
    SignatureMap& signatureMap = registeredFunctions_[sanitizedName];
    signatureMap[*metadata->signature()] =
        std::make_unique<const FunctionEntry<Function, Metadata>>(
            metadata, factory);
  }

  const SignatureMap* getSignatureMap(const std::string& name) const {
    const auto sanitizedName = sanitizeFunctionName(name);
    const auto it = registeredFunctions_.find(sanitizedName);
    return it != registeredFunctions_.end() ? &it->second : nullptr;
  }

  FunctionMap registeredFunctions_;
};
} // namespace facebook::velox::exec
