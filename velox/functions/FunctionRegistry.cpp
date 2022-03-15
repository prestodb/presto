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
#include "velox/functions/FunctionRegistry.h"

#include <boost/algorithm/string.hpp>
#include <algorithm>
#include <iterator>
#include <optional>
#include <sstream>
#include "velox/common/base/Exceptions.h"
#include "velox/core/SimpleFunctionMetadata.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"

namespace facebook::velox {
namespace {

exec::TypeSignature typeToTypeSignature(std::shared_ptr<const Type> type) {
  std::vector<exec::TypeSignature> children;
  if (type->size()) {
    children.reserve(type->size());
    for (auto i = 0; i < type->size(); i++) {
      children.emplace_back(typeToTypeSignature(type->childAt(i)));
    }
  }
  const std::string& kindName = type->kindName();
  return exec::TypeSignature(
      boost::algorithm::to_lower_copy(kindName), std::move(children));
}

void populateSimpleFunctionSignatures(FunctionSignatureMap& map) {
  auto& simpleFunctions = exec::SimpleFunctions();
  auto functionNames = simpleFunctions.getFunctionNames();
  for (const auto& functionName : functionNames) {
    auto signatures = simpleFunctions.getFunctionSignatures(functionName);
    map[functionName] = signatures;
  }
}

void populateVectorFunctionSignatures(FunctionSignatureMap& map) {
  auto vectorFunctions = exec::vectorFunctionFactories();
  vectorFunctions.withRLock([&map](const auto& locked) {
    for (const auto& it : locked) {
      const auto& allSignatures = it.second.signatures;
      auto& curSignatures = map[it.first];
      std::transform(
          allSignatures.begin(),
          allSignatures.end(),
          std::back_inserter(curSignatures),
          [](std::shared_ptr<exec::FunctionSignature> signature)
              -> exec::FunctionSignature* { return signature.get(); });
    }
  });
}

} // namespace

FunctionSignatureMap getFunctionSignatures() {
  FunctionSignatureMap result;
  populateSimpleFunctionSignatures(result);
  populateVectorFunctionSignatures(result);
  return result;
}

std::shared_ptr<const Type> resolveFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  // Check if this is a simple function.
  if (auto returnType = resolveSimpleFunction(functionName, argTypes)) {
    return returnType;
  }

  // Check if VectorFunctions has this function name + signature.
  return resolveVectorFunction(functionName, argTypes);
}

std::shared_ptr<const Type> resolveSimpleFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  auto resolvedFunction =
      exec::SimpleFunctions().resolveFunction(functionName, argTypes);

  if (resolvedFunction) {
    return resolvedFunction->getMetadata()->returnType();
  }

  return nullptr;
}

std::shared_ptr<const Type> resolveVectorFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  if (auto vectorFunctionSignatures =
          exec::getVectorFunctionSignatures(functionName)) {
    for (const auto& signature : vectorFunctionSignatures.value()) {
      exec::SignatureBinder binder(*signature, argTypes);
      if (binder.tryBind()) {
        return binder.tryResolveReturnType();
      }
    }
  }

  return nullptr;
}

} // namespace facebook::velox
