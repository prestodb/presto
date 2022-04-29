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
#include "Expressions.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/Expressions.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/parse/VariantToVector.h"
#include "velox/type/Type.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::core {

// static
Expressions::TypeResolverHook Expressions::resolverHook_;

std::unordered_map<std::string, std::shared_ptr<core::LambdaTypedExpr>>&
Expressions::lambdaRegistry() {
  static std::unordered_map<std::string, std::shared_ptr<core::LambdaTypedExpr>>
      lambdas;
  return lambdas;
}

namespace {
std::vector<TypePtr> getTypes(
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& inputs) {
  std::vector<std::shared_ptr<const Type>> types{};
  for (auto& i : inputs) {
    types.push_back(i->type());
  }
  return types;
}

// Determine output type based on input types.
std::shared_ptr<const Type> resolveTypeImpl(
    std::vector<std::shared_ptr<const core::ITypedExpr>> inputs,
    const std::shared_ptr<const CallExpr>& expr) {
  // Check expressions which aren't simple functions, e.g. vector functions,
  // and/or, try, etc.
  if (Expressions::getResolverHook()) {
    auto type = Expressions::getResolverHook()(inputs, expr);
    if (type) {
      return type;
    }
  }

  // Check simple functions.
  auto fun = exec::SimpleFunctions().resolveFunction(
      expr->getFunctionName(), getTypes(inputs));
  return fun == nullptr ? nullptr : fun->getMetadata()->returnType();
}

namespace {
std::shared_ptr<const core::CastTypedExpr> makeTypedCast(
    const TypePtr& type,
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& inputs) {
  return std::make_shared<const core::CastTypedExpr>(type, inputs, false);
}

std::vector<TypePtr> implicitCastTargets(const TypePtr& type) {
  std::vector<TypePtr> targetTypes;
  switch (type->kind()) {
    // We decide not to implicitly upcast booleans because it maybe funky.
    case TypeKind::BOOLEAN:
      break;
    case TypeKind::TINYINT:
      targetTypes.emplace_back(SMALLINT());
      FMT_FALLTHROUGH;
    case TypeKind::SMALLINT:
      targetTypes.emplace_back(INTEGER());
      targetTypes.emplace_back(REAL());
      FMT_FALLTHROUGH;
    case TypeKind::INTEGER:
      targetTypes.emplace_back(BIGINT());
      targetTypes.emplace_back(DOUBLE());
      FMT_FALLTHROUGH;
    case TypeKind::BIGINT:
      break;
    case TypeKind::REAL:
      targetTypes.emplace_back(DOUBLE());
      FMT_FALLTHROUGH;
    case TypeKind::DOUBLE:
      break;
    case TypeKind::ARRAY: {
      auto childTargetTypes = implicitCastTargets(type->childAt(0));
      for (auto childTarget : childTargetTypes) {
        targetTypes.emplace_back(ARRAY(childTarget));
      }
      break;
    }
    default: // make compilers happy
        ;
  }
  return targetTypes;
}
} // namespace

// All acceptable implicit casts on this expression.
// TODO: If we get this to be recursive somehow, we can save on cast function
// signatures that need to be compiled and registered.
std::vector<std::shared_ptr<const core::ITypedExpr>> genImplicitCasts(
    const std::shared_ptr<const core::ITypedExpr>& typedExpr) {
  auto targetTypes = implicitCastTargets(typedExpr->type());

  std::vector<std::shared_ptr<const core::ITypedExpr>> implicitCasts;
  implicitCasts.reserve(targetTypes.size());
  for (auto targetType : targetTypes) {
    implicitCasts.emplace_back(makeTypedCast(targetType, {typedExpr}));
  }
  return implicitCasts;
}

// TODO: Arguably all of this could be done with just Types.
std::shared_ptr<const core::ITypedExpr> adjustLastNArguments(
    std::vector<std::shared_ptr<const core::ITypedExpr>> inputs,
    const std::shared_ptr<const CallExpr>& expr,
    size_t n) {
  auto type = resolveTypeImpl(inputs, expr);
  if (type != nullptr) {
    return std::make_unique<CallTypedExpr>(
        type, inputs, std::string{expr->getFunctionName()});
  }

  if (n == 0) {
    return nullptr;
  }

  size_t firstOfLastN = inputs.size() - n;
  // use it
  std::vector<std::shared_ptr<const core::ITypedExpr>> viableExprs{
      inputs[firstOfLastN]};
  // or lose it
  auto&& implicitCasts = genImplicitCasts(inputs[firstOfLastN]);
  std::move(
      implicitCasts.begin(),
      implicitCasts.end(),
      std::back_inserter(viableExprs));

  for (auto& viableExpr : viableExprs) {
    inputs[firstOfLastN] = viableExpr;
    auto adjustedExpr = adjustLastNArguments(inputs, expr, n - 1);
    if (adjustedExpr != nullptr) {
      return adjustedExpr;
    }
  }

  return nullptr;
}

std::string toString(
    const std::shared_ptr<const core::CallExpr>& expr,
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& inputs) {
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

std::shared_ptr<const core::ITypedExpr> createWithImplicitCast(
    std::shared_ptr<const core::CallExpr> expr,
    std::vector<std::shared_ptr<const core::ITypedExpr>> inputs) {
  auto adjusted = adjustLastNArguments(inputs, expr, inputs.size());
  if (adjusted) {
    return adjusted;
  }
  auto type = resolveTypeImpl(inputs, expr);
  if (!type) {
    throw std::invalid_argument{
        "Cannot resolve function call: " + toString(expr, inputs)};
  }
  return std::make_shared<CallTypedExpr>(
      type, move(inputs), std::string{expr->getFunctionName()});
}
} // namespace

std::shared_ptr<const LambdaTypedExpr> Expressions::lookupLambdaExpr(
    std::shared_ptr<const IExpr> expr) {
  if (auto callExpr = std::dynamic_pointer_cast<const CallExpr>(expr)) {
    if ("function" == callExpr->getFunctionName()) {
      auto inputs = callExpr->getInputs();
      if (inputs.size() != 1) {
        return nullptr;
      }
      if (auto constant =
              std::dynamic_pointer_cast<const ConstantExpr>(inputs[0])) {
        auto it = lambdaRegistry().find(constant->value().value<std::string>());
        if (it != lambdaRegistry().end()) {
          return it->second;
        }
      }
    }
  }
  return nullptr;
}

std::shared_ptr<const core::ITypedExpr> Expressions::inferTypes(
    const std::shared_ptr<const core::IExpr>& expr,
    const std::shared_ptr<const Type>& inputRow,
    memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(expr);
  if (auto lambdaExpr = lookupLambdaExpr(expr)) {
    return lambdaExpr;
  }
  std::vector<std::shared_ptr<const core::ITypedExpr>> children{};
  for (auto& child : expr->getInputs()) {
    children.push_back(inferTypes(child, inputRow, pool));
  }
  if (auto fae = std::dynamic_pointer_cast<const FieldAccessExpr>(expr)) {
    if (fae->getFieldName() == "") {
      throw std::invalid_argument{"Cannot refer to anonymous columns"};
    }
    if (children.size() != 1) {
      throw std::invalid_argument{"unwanted children"};
    }
    auto input = children.at(0)->type();
    bool found = false;
    size_t foundIndex;
    auto& row = input->asRow();
    for (size_t idx = 0; idx < input->size(); ++idx) {
      if (row.nameOf(idx) == fae->getFieldName()) {
        if (found) {
          throw std::invalid_argument{
              "duplicate field: " + fae->getFieldName()};
        } else {
          found = true;
          foundIndex = idx;
        }
      }
    }
    if (!found) {
      throw std::invalid_argument{
          "couldn't find field: " + fae->getFieldName()};
    }
    return std::make_shared<FieldAccessTypedExpr>(
        input->childAt(foundIndex),
        children.at(0),
        std::string{fae->getFieldName()});
  } else if (auto fun = std::dynamic_pointer_cast<const CallExpr>(expr)) {
    return createWithImplicitCast(move(fun), move(children));
  } else if (auto input = std::dynamic_pointer_cast<const InputExpr>(expr)) {
    return std::make_shared<const InputTypedExpr>(inputRow);
  } else if (
      auto constant = std::dynamic_pointer_cast<const ConstantExpr>(expr)) {
    if (constant->type()->kind() == TypeKind::ARRAY) {
      // Transform variant vector into an ArrayVector, then wrap it into a
      // ConstantVector<ComplexType>.
      VELOX_CHECK_NOT_NULL(
          pool, "parsing array literals requires a memory pool");
      auto arrayVector = variantArrayToVector(constant->value().array(), pool);
      auto constantVector =
          std::make_shared<ConstantVector<velox::ComplexType>>(
              pool, 1, 0, arrayVector);
      return std::make_shared<const ConstantTypedExpr>(constantVector);
    }
    return std::make_shared<const ConstantTypedExpr>(constant->value());
  } else if (auto cast = std::dynamic_pointer_cast<const CastExpr>(expr)) {
    return std::make_shared<const CastTypedExpr>(
        cast->type(), move(children), cast->nullOnFailure());
  } else if (
      auto alreadyTyped = std::dynamic_pointer_cast<const ITypedExpr>(expr)) {
    return alreadyTyped;
  }
  std::string s{"unknown expression type: "};
  s.append(typeid(expr).name());
  throw std::invalid_argument{s};
}

// This method returns null if the expression doesn't depend on any input row.
std::shared_ptr<const Type> Expressions::getInputRowType(
    const TypedExprPtr& expr) {
  if (auto inputExpr = std::dynamic_pointer_cast<const InputTypedExpr>(expr)) {
    return inputExpr->type();
  }
  std::shared_ptr<const Type> inputRowType;
  for (auto& input : expr->inputs()) {
    auto childRowType = getInputRowType(input);
    if (childRowType) {
      VELOX_USER_CHECK(!inputRowType || *inputRowType == *childRowType);
      inputRowType = childRowType;
    }
  }

  return inputRowType;
}
} // namespace facebook::velox::core
