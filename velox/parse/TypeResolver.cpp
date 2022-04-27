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
#include "velox/expression/Expr.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/VectorFunction.h"
#include "velox/parse/Expressions.h"
#include "velox/type/Type.h"

namespace facebook::velox::parse {
namespace {

std::shared_ptr<const Type> resolveType(
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& inputs,
    const std::shared_ptr<const core::CallExpr>& expr) {
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

  auto signatures = exec::getVectorFunctionSignatures(expr->getFunctionName());
  if (signatures.has_value()) {
    std::vector<TypePtr> inputTypes;
    inputTypes.reserve(inputs.size());
    for (auto& input : inputs) {
      inputTypes.emplace_back(input->type());
    }

    for (const auto& signature : signatures.value()) {
      exec::SignatureBinder binder(*signature, inputTypes);
      if (binder.tryBind()) {
        return binder.tryResolveReturnType();
      }
    }
  }

  return nullptr;
}

} // namespace

void registerTypeResolver() {
  core::Expressions::setTypeResolverHook(&resolveType);
}

} // namespace facebook::velox::parse
