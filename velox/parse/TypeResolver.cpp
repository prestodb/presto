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
#include "velox/expression/SignatureBinder.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/parse/Expressions.h"
#include "velox/type/Type.h"

namespace facebook::velox::parse {
namespace {

std::string toString(
    const std::shared_ptr<const core::CallExpr>& expr,
    const std::vector<core::TypedExprPtr>& inputs) {
  std::ostringstream signature;
  signature << expr->getFunctionName() << "(";
  for (auto i = 0; i < inputs.size(); i++) {
    if (i > 0) {
      signature << ", ";
    }
    signature << inputs[i]->type()->toString();
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
  if (expr->getFunctionName() == "if") {
    return !inputs[1]->type()->containsUnknown() ? inputs[1]->type()
                                                 : inputs[2]->type();
  }

  if (expr->getFunctionName() == "switch") {
    auto numInput = inputs.size();
    for (auto i = 1; i < numInput; i += 2) {
      if (!inputs[i]->type()->containsUnknown()) {
        return inputs[i]->type();
      }
    }
    // If all contain unknown, return the type of the else or the last when
    // clause.
    return inputs.back()->type();
  }

  if (expr->getFunctionName() == "and" || expr->getFunctionName() == "or") {
    return BOOLEAN();
  }

  if (expr->getFunctionName() == "try" ||
      expr->getFunctionName() == "coalesce") {
    VELOX_CHECK(!inputs.empty());
    return inputs.front()->type();
  }

  // TODO Replace with struct_pack
  if (expr->getFunctionName() == "row_constructor") {
    auto numInput = inputs.size();
    std::vector<TypePtr> types(numInput);
    std::vector<std::string> names(numInput);
    for (auto i = 0; i < numInput; i++) {
      types[i] = inputs[i]->type();
      names[i] = fmt::format("c{}", i + 1);
    }
    return ROW(std::move(names), std::move(types));
  }

  std::vector<TypePtr> inputTypes;
  inputTypes.reserve(inputs.size());
  for (auto& input : inputs) {
    inputTypes.emplace_back(input->type());
  }

  auto returnType = resolveFunction(expr->getFunctionName(), inputTypes);
  if (returnType) {
    return returnType;
  }

  if (nullOnFailure) {
    return nullptr;
  }

  auto allSignatures = getFunctionSignatures();
  auto it = allSignatures.find(expr->getFunctionName());
  if (it == allSignatures.end()) {
    VELOX_USER_FAIL(
        "Scalar function doesn't exist: {}.", expr->getFunctionName());
  } else {
    const auto& functionSignatures = it->second;
    VELOX_USER_FAIL(
        "Scalar function signature is not supported: {}. Supported signatures: {}.",
        toString(expr, inputs),
        toString(functionSignatures));
  }
}

} // namespace

void registerTypeResolver() {
  core::Expressions::setTypeResolverHook(&resolveType);
}

} // namespace facebook::velox::parse
