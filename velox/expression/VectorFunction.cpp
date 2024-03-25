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
#include "velox/expression/SignatureBinder.h"

namespace facebook::velox::exec {

namespace {
template <typename TResult, typename TFunc>
std::optional<TResult> applyToVectorFunctionEntry(
    const std::string& name,
    TFunc applyFunc) {
  auto sanitizedName = sanitizeName(name);

  return vectorFunctionFactories().withRLock(
      [&](auto& functions) -> std::optional<TResult> {
        auto it = functions.find(sanitizedName);
        if (it == functions.end()) {
          return std::nullopt;
        }
        return applyFunc(sanitizedName, it->second);
      });
}

// Zip `inputTypes` and `constantInputs` vectors into a single
// vector of `VectorFunctionArg`.
std::vector<VectorFunctionArg> toVectorFunctionArgs(
    const std::vector<TypePtr>& inputTypes,
    const std::vector<VectorPtr>& constantInputs) {
  std::vector<VectorFunctionArg> args;
  args.reserve(inputTypes.size());

  for (vector_size_t i = 0; i < inputTypes.size(); ++i) {
    args.push_back({
        inputTypes[i],
        constantInputs.size() > i ? constantInputs[i] : nullptr,
    });
  }

  return args;
}
} // namespace

VectorFunctionMap& vectorFunctionFactories() {
  static VectorFunctionMap factories;
  return factories;
}

std::optional<std::vector<FunctionSignaturePtr>> getVectorFunctionSignatures(
    const std::string& name) {
  return applyToVectorFunctionEntry<std::vector<FunctionSignaturePtr>>(
      name, [&](const auto& /*name*/, const auto& entry) {
        return entry.signatures;
      });
}

TypePtr resolveVectorFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  if (auto outputTypeWithMetadata =
          resolveVectorFunctionWithMetadata(functionName, argTypes)) {
    return outputTypeWithMetadata->first;
  }

  return nullptr;
}

std::optional<std::pair<TypePtr, VectorFunctionMetadata>>
resolveVectorFunctionWithMetadata(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  return applyToVectorFunctionEntry<std::pair<TypePtr, VectorFunctionMetadata>>(
      functionName,
      [&](const auto& /*name*/, const auto& entry)
          -> std::optional<std::pair<TypePtr, VectorFunctionMetadata>> {
        for (const auto& signature : entry.signatures) {
          exec::SignatureBinder binder(*signature, argTypes);
          if (binder.tryBind()) {
            return {{binder.tryResolveReturnType(), entry.metadata}};
          }
        }
        return std::nullopt;
      });
}

std::shared_ptr<VectorFunction> getVectorFunction(
    const std::string& name,
    const std::vector<TypePtr>& inputTypes,
    const std::vector<VectorPtr>& constantInputs,
    const core::QueryConfig& config) {
  auto functionWithMetadata =
      getVectorFunctionWithMetadata(name, inputTypes, constantInputs, config);
  if (!functionWithMetadata.has_value()) {
    return nullptr;
  }
  return functionWithMetadata->first;
}

std::optional<
    std::pair<std::shared_ptr<VectorFunction>, VectorFunctionMetadata>>
getVectorFunctionWithMetadata(
    const std::string& name,
    const std::vector<TypePtr>& inputTypes,
    const std::vector<VectorPtr>& constantInputs,
    const core::QueryConfig& config) {
  if (!constantInputs.empty()) {
    VELOX_CHECK_EQ(inputTypes.size(), constantInputs.size());
  }

  return applyToVectorFunctionEntry<
      std::pair<std::shared_ptr<VectorFunction>, VectorFunctionMetadata>>(
      name,
      [&](const auto& sanitizedName, const auto& entry)
          -> std::optional<std::pair<
              std::shared_ptr<VectorFunction>,
              VectorFunctionMetadata>> {
        for (const auto& signature : entry.signatures) {
          exec::SignatureBinder binder(*signature, inputTypes);
          if (binder.tryBind()) {
            auto inputArgs = toVectorFunctionArgs(inputTypes, constantInputs);

            return {
                {entry.factory(sanitizedName, inputArgs, config),
                 entry.metadata}};
          }
        }
        return std::nullopt;
      });
}

/// Registers a new vector function. When overwrite = true, previous functions
/// with the given name will be replaced.
/// Returns true iff an insertion actually happened
bool registerStatefulVectorFunction(
    const std::string& name,
    std::vector<FunctionSignaturePtr> signatures,
    VectorFunctionFactory factory,
    VectorFunctionMetadata metadata,
    bool overwrite) {
  auto sanitizedName = sanitizeName(name);

  if (overwrite) {
    vectorFunctionFactories().withWLock([&](auto& functionMap) {
      // Insert/overwrite.
      functionMap[sanitizedName] = {
          std::move(signatures), std::move(factory), std::move(metadata)};
    });
    return true;
  }

  return vectorFunctionFactories().withWLock([&](auto& functionMap) {
    auto [iterator, inserted] = functionMap.insert(
        {sanitizedName,
         {std::move(signatures), std::move(factory), std::move(metadata)}});
    return inserted;
  });
}

// Returns true iff an insertion actually happened
bool registerVectorFunction(
    const std::string& name,
    std::vector<FunctionSignaturePtr> signatures,
    std::unique_ptr<VectorFunction> func,
    VectorFunctionMetadata metadata,
    bool overwrite) {
  std::shared_ptr<VectorFunction> sharedFunc = std::move(func);
  auto factory = [sharedFunc](
                     const auto& /*name*/,
                     const auto& /*vectorArg*/,
                     const auto& /*config*/) { return sharedFunc; };
  return registerStatefulVectorFunction(
      name, signatures, factory, metadata, overwrite);
}

std::vector<ExpressionRewrite>& expressionRewrites() {
  static std::vector<ExpressionRewrite> rewrites;
  return rewrites;
}

void registerExpressionRewrite(ExpressionRewrite rewrite) {
  expressionRewrites().emplace_back(rewrite);
}

} // namespace facebook::velox::exec
