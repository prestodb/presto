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

#include "velox/expression/SimpleFunctionRegistry.h"

namespace facebook::velox::exec {
namespace {

SimpleFunctionRegistry& simpleFunctionsInternal() {
  static SimpleFunctionRegistry instance;
  return instance;
}
} // namespace

const SimpleFunctionRegistry& simpleFunctions() {
  return simpleFunctionsInternal();
}

SimpleFunctionRegistry& mutableSimpleFunctions() {
  return simpleFunctionsInternal();
}

void SimpleFunctionRegistry::registerFunctionInternal(
    const std::string& name,
    const std::shared_ptr<const Metadata>& metadata,
    const FunctionFactory& factory) {
  const auto sanitizedName = sanitizeName(name);
  registeredFunctions_.withWLock([&](auto& map) {
    SignatureMap& signatureMap = map[sanitizedName];
    signatureMap[*metadata->signature()] =
        std::make_unique<const FunctionEntry>(metadata, factory);
  });
}

namespace {
// This function is not thread safe should be called only from within a
// syncrhronized read region of registeredFunctions_.
const SignatureMap* getSignatureMap(
    const std::string& name,
    const FunctionMap& registeredFunctions) {
  const auto sanitizedName = sanitizeName(name);
  const auto it = registeredFunctions.find(sanitizedName);
  return it != registeredFunctions.end() ? &it->second : nullptr;
}
} // namespace

std::vector<const FunctionSignature*>
SimpleFunctionRegistry::getFunctionSignatures(const std::string& name) const {
  std::vector<const FunctionSignature*> signatures;
  registeredFunctions_.withRLock([&](const auto& map) {
    if (const auto* signatureMap = getSignatureMap(name, map)) {
      signatures.reserve(signatureMap->size());
      for (const auto& pair : *signatureMap) {
        signatures.emplace_back(&pair.first);
      }
    }
  });

  return signatures;
}

std::optional<SimpleFunctionRegistry::ResolvedSimpleFunction>
SimpleFunctionRegistry::resolveFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes) const {
  const FunctionEntry* selectedCandidate = nullptr;
  TypePtr selectedCandidateType = nullptr;
  registeredFunctions_.withRLock([&](const auto& map) {
    if (const auto* signatureMap = getSignatureMap(name, map)) {
      for (const auto& [candidateSignature, functionEntry] : *signatureMap) {
        SignatureBinder binder(candidateSignature, argTypes);
        if (binder.tryBind()) {
          auto* currentCandidate = functionEntry.get();
          if (!selectedCandidate ||
              currentCandidate->getMetadata().priority() <
                  selectedCandidate->getMetadata().priority()) {
            selectedCandidate = currentCandidate;
            selectedCandidateType = binder.tryResolveReturnType();
          }
        }
      }
    }
  });

  VELOX_DCHECK(!selectedCandidate || selectedCandidateType);

  return selectedCandidate
      ? std::optional<ResolvedSimpleFunction>(
            ResolvedSimpleFunction(*selectedCandidate, selectedCandidateType))
      : std::nullopt;
}

} // namespace facebook::velox::exec
