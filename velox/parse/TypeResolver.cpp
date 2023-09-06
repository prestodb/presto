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

#include "velox/parse/TypeResolver.h"
#include "velox/core/ITypedExpr.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/parse/Expressions.h"
#include "velox/type/Type.h"

namespace facebook::velox::parse {
namespace {

std::string toString(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  std::ostringstream signature;
  signature << functionName << "(";
  for (auto i = 0; i < argTypes.size(); i++) {
    if (i > 0) {
      signature << ", ";
    }
    signature << argTypes[i]->toString();
  }
  signature << ")";
  return signature.str();
}

std::string toString(
    const std::vector<const exec::FunctionSignature*>& signatures) {
  std::stringstream out;
  for (auto i = 0; i < signatures.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << signatures[i]->toString();
  }
  return out.str();
}

TypePtr resolveType(
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& inputs,
    const std::shared_ptr<const core::CallExpr>& expr,
    bool nullOnFailure) {
  std::vector<TypePtr> inputTypes;
  inputTypes.reserve(inputs.size());
  for (auto& input : inputs) {
    inputTypes.emplace_back(input->type());
  }

  if (auto resolvedType = exec::resolveTypeForSpecialForm(
          expr->getFunctionName(), inputTypes)) {
    return resolvedType;
  }

  return resolveScalarFunctionType(
      expr->getFunctionName(), inputTypes, nullOnFailure);
}

} // namespace

void registerTypeResolver() {
  core::Expressions::setTypeResolverHook(&resolveType);
}

TypePtr resolveScalarFunctionType(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    bool nullOnFailure) {
  auto returnType = resolveFunction(name, argTypes);
  if (returnType) {
    return returnType;
  }

  if (nullOnFailure) {
    return nullptr;
  }

  auto allSignatures = getFunctionSignatures();
  auto it = allSignatures.find(name);
  if (it == allSignatures.end()) {
    VELOX_USER_FAIL("Scalar function doesn't exist: {}.", name);
  } else {
    const auto& functionSignatures = it->second;
    VELOX_USER_FAIL(
        "Scalar function signature is not supported: {}. Supported signatures: {}.",
        toString(name, argTypes),
        toString(functionSignatures));
  }
}
} // namespace facebook::velox::parse
