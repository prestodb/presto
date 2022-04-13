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
#include "velox/expression/VectorFunction.h"
#include <unordered_map>
#include "folly/Singleton.h"
#include "folly/Synchronized.h"

namespace facebook::velox::exec {

VectorFunctionMap& vectorFunctionFactories() {
  static VectorFunctionMap factories;
  return factories;
}

std::optional<std::vector<FunctionSignaturePtr>> getVectorFunctionSignatures(
    const std::string& name) {
  return vectorFunctionFactories().withRLock([&name](auto& functions) -> auto {
    auto it = functions.find(name);
    return it == functions.end() ? std::nullopt
                                 : std::optional(it->second.signatures);
  });
}

std::shared_ptr<VectorFunction> getVectorFunction(
    const std::string& name,
    const std::vector<TypePtr>& inputTypes,
    const std::vector<VectorPtr>& constantInputs) {
  if (!constantInputs.empty()) {
    VELOX_CHECK_EQ(inputTypes.size(), constantInputs.size());
  }

  // Zip `inputTypes` and `constantInputs` vectors into a single vector of
  // `VectorFunctionArg`.
  std::vector<VectorFunctionArg> inputArgs;
  inputArgs.reserve(inputTypes.size());

  for (vector_size_t i = 0; i < inputTypes.size(); ++i) {
    inputArgs.push_back({
        inputTypes[i],
        constantInputs.size() > i ? constantInputs[i] : nullptr,
    });
  }

  return vectorFunctionFactories().withRLock(
      [&name, &inputArgs ](auto& functionMap) -> auto {
        auto functionIterator = functionMap.find(name);
        return functionIterator == functionMap.end()
            ? nullptr
            : functionIterator->second.factory(name, inputArgs);
      });
}

/// Registers a new vector function. When overwrite = true, previous functions
/// with the given name will be replaced.
/// Returns true iff an insertion actually happened
bool registerStatefulVectorFunction(
    const std::string& name,
    std::vector<FunctionSignaturePtr> signatures,
    VectorFunctionFactory factory,
    bool overwrite) {
  if (overwrite) {
    vectorFunctionFactories().withWLock([&](auto& functionMap) {
      // Insert/overwrite.
      functionMap[name] = {std::move(signatures), std::move(factory)};
    });
    return true;
  }

  return vectorFunctionFactories().withWLock([&](auto& functionMap) {
    auto [iterator, inserted] =
        functionMap.insert({name, {std::move(signatures), std::move(factory)}});
    return inserted;
  });
}

// Returns true iff an insertion actually happened
bool registerVectorFunction(
    const std::string& name,
    std::vector<FunctionSignaturePtr> signatures,
    std::unique_ptr<VectorFunction> func,
    bool overwrite) {
  std::shared_ptr<VectorFunction> sharedFunc = std::move(func);
  auto factory = [sharedFunc](const auto& /*name*/, const auto& /*vectorArg*/) {
    return sharedFunc;
  };
  return registerStatefulVectorFunction(name, signatures, factory, overwrite);
}

} // namespace facebook::velox::exec
