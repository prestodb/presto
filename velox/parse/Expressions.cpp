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
#include "velox/functions/FunctionRegistry.h"
#include "velox/parse/VariantToVector.h"
#include "velox/type/Type.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::core {

// static
Expressions::TypeResolverHook Expressions::resolverHook_;

namespace {
std::vector<TypePtr> getTypes(const std::vector<TypedExprPtr>& inputs) {
  std::vector<TypePtr> types{};
  for (auto& i : inputs) {
    types.push_back(i->type());
  }
  return types;
}

// Determine output type based on input types.
TypePtr resolveTypeImpl(
    std::vector<TypedExprPtr> inputs,
    const std::shared_ptr<const CallExpr>& expr,
    bool nullOnFailure) {
  VELOX_CHECK_NOT_NULL(Expressions::getResolverHook());
  return Expressions::getResolverHook()(inputs, expr, nullOnFailure);
}

namespace {
std::shared_ptr<const core::CastTypedExpr> makeTypedCast(
    const TypePtr& type,
    const std::vector<TypedExprPtr>& inputs) {
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
std::vector<TypedExprPtr> genImplicitCasts(const TypedExprPtr& typedExpr) {
  auto targetTypes = implicitCastTargets(typedExpr->type());

  std::vector<TypedExprPtr> implicitCasts;
  implicitCasts.reserve(targetTypes.size());
  for (auto targetType : targetTypes) {
    implicitCasts.emplace_back(makeTypedCast(targetType, {typedExpr}));
  }
  return implicitCasts;
}

// TODO: Arguably all of this could be done with just Types.
TypedExprPtr adjustLastNArguments(
    std::vector<TypedExprPtr> inputs,
    const std::shared_ptr<const CallExpr>& expr,
    size_t n) {
  auto type = resolveTypeImpl(inputs, expr, true /*nullOnFailure*/);
  if (type != nullptr) {
    return std::make_unique<CallTypedExpr>(
        type, inputs, std::string{expr->getFunctionName()});
  }

  if (n == 0) {
    return nullptr;
  }

  size_t firstOfLastN = inputs.size() - n;
  // use it
  std::vector<TypedExprPtr> viableExprs{inputs[firstOfLastN]};
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
    const std::vector<TypedExprPtr>& inputs) {
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

TypedExprPtr createWithImplicitCast(
    const std::shared_ptr<const core::CallExpr>& expr,
    const std::vector<TypedExprPtr>& inputs) {
  auto adjusted = adjustLastNArguments(inputs, expr, inputs.size());
  if (adjusted) {
    return adjusted;
  }
  auto type = resolveTypeImpl(inputs, expr, false /*nullOnFailure*/);
  return std::make_shared<CallTypedExpr>(
      type, std::move(inputs), std::string{expr->getFunctionName()});
}

bool isLambdaArgument(const exec::TypeSignature& typeSignature) {
  return typeSignature.baseName() == "function";
}

bool hasLambdaArgument(const exec::FunctionSignature& signature) {
  for (const auto& type : signature.argumentTypes()) {
    if (isLambdaArgument(type)) {
      return true;
    }
  }

  return false;
}
} // namespace

// static
TypedExprPtr Expressions::inferTypes(
    const std::shared_ptr<const core::IExpr>& expr,
    const TypePtr& inputRow,
    memory::MemoryPool* pool) {
  return inferTypes(expr, inputRow, {}, pool);
}

// static
TypedExprPtr Expressions::inferTypes(
    const std::shared_ptr<const core::IExpr>& expr,
    const TypePtr& inputRow,
    const std::vector<TypePtr>& lambdaInputTypes,
    memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(expr);

  if (auto lambdaExpr = std::dynamic_pointer_cast<const LambdaExpr>(expr)) {
    return resolveLambdaExpr(lambdaExpr, inputRow, lambdaInputTypes, pool);
  }

  if (auto call = std::dynamic_pointer_cast<const CallExpr>(expr)) {
    if (!expr->getInputs().empty()) {
      if (auto returnType = tryResolveCallWithLambdas(call, inputRow, pool)) {
        return returnType;
      }
    }
  }

  std::vector<TypedExprPtr> children;
  for (auto& child : expr->getInputs()) {
    children.push_back(inferTypes(child, inputRow, lambdaInputTypes, pool));
  }

  if (auto fae = std::dynamic_pointer_cast<const FieldAccessExpr>(expr)) {
    VELOX_CHECK(
        !fae->getFieldName().empty(), "Anonymous columns are not supported");
    VELOX_CHECK_EQ(
        children.size(), 1, "Unexpected number of children in FieldAccessExpr");
    auto input = children.at(0)->type();
    auto& row = input->asRow();
    auto childIndex = row.getChildIdx(fae->getFieldName());
    return std::make_shared<FieldAccessTypedExpr>(
        input->childAt(childIndex),
        children.at(0),
        std::string{fae->getFieldName()});
  }
  if (auto fun = std::dynamic_pointer_cast<const CallExpr>(expr)) {
    return createWithImplicitCast(fun, std::move(children));
  }
  if (auto input = std::dynamic_pointer_cast<const InputExpr>(expr)) {
    return std::make_shared<const InputTypedExpr>(inputRow);
  }
  if (auto constant = std::dynamic_pointer_cast<const ConstantExpr>(expr)) {
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
  }
  if (auto cast = std::dynamic_pointer_cast<const CastExpr>(expr)) {
    return std::make_shared<const CastTypedExpr>(
        cast->type(), std::move(children), cast->nullOnFailure());
  }
  if (auto alreadyTyped = std::dynamic_pointer_cast<const ITypedExpr>(expr)) {
    return alreadyTyped;
  }

  VELOX_FAIL("Unknown expression type: {}", expr->toString());
}

// static
TypedExprPtr Expressions::resolveLambdaExpr(
    const std::shared_ptr<const core::LambdaExpr>& lambdaExpr,
    const TypePtr& inputRow,
    const std::vector<TypePtr>& lambdaInputTypes,
    memory::MemoryPool* pool) {
  auto names = lambdaExpr->inputNames();
  auto body = lambdaExpr->body();

  VELOX_CHECK_LE(names.size(), lambdaInputTypes.size());
  std::vector<TypePtr> types;
  for (auto i = 0; i < names.size(); ++i) {
    types.push_back(lambdaInputTypes[i]);
  }

  auto signature = ROW(std::move(names), std::move(types));

  names = inputRow->asRow().names();
  types = inputRow->asRow().children();
  for (auto i = 0; i < signature->size(); ++i) {
    names.push_back(signature->names()[i]);
    types.push_back(signature->childAt(i));
  }

  auto lambdaRow = ROW(std::move(names), std::move(types));

  return std::make_shared<LambdaTypedExpr>(
      signature, inferTypes(body, lambdaRow, pool));
}

// static
TypedExprPtr Expressions::tryResolveCallWithLambdas(
    const std::shared_ptr<const CallExpr>& callExpr,
    const TypePtr& inputRow,
    memory::MemoryPool* pool) {
  auto allSignatures = getFunctionSignatures();
  auto it = allSignatures.find(callExpr->getFunctionName());
  if (it == allSignatures.end()) {
    return nullptr;
  }

  const auto& signatures = it->second;
  for (const auto& signature : signatures) {
    if (!hasLambdaArgument(*signature)) {
      return nullptr;
    }

    VELOX_CHECK_EQ(
        1,
        signatures.size(),
        "Lambda functions with multiple signatures are not supported. "
        "Lambda function {} has {} signatures.",
        callExpr->getFunctionName(),
        signatures.size());

    VELOX_CHECK_EQ(
        signature->argumentTypes().size(),
        callExpr->getInputs().size(),
        "Lambda function signature is not supported: {}({} arguments)."
        "Supported signature has {} arguments: {}.",
        callExpr->getFunctionName(),
        callExpr->getInputs().size(),
        signature->argumentTypes().size(),
        signature->toString());

    // Resolve non-lambda arguments first.
    auto numArgs = callExpr->getInputs().size();
    std::vector<TypedExprPtr> children(numArgs);
    std::vector<TypePtr> childTypes(numArgs);
    for (auto i = 0; i < numArgs; ++i) {
      if (!isLambdaArgument(signature->argumentTypes()[i])) {
        children[i] = inferTypes(callExpr->getInputs()[i], inputRow, pool);
        childTypes[i] = children[i]->type();
      }
    }

    // Resolve lambda arguments.
    exec::SignatureBinder binder(*signature, childTypes);
    binder.tryBind();
    for (auto i = 0; i < numArgs; ++i) {
      auto argSignature = signature->argumentTypes()[i];
      if (isLambdaArgument(argSignature)) {
        std::vector<TypePtr> lambdaTypes;
        for (auto j = 0; j < argSignature.parameters().size() - 1; ++j) {
          auto type = binder.tryResolveType(argSignature.parameters()[j]);
          VELOX_CHECK_NOT_NULL(
              type,
              "Cannot resolve lambda argument type: {}.",
              argSignature.toString());
          lambdaTypes.push_back(type);
        }

        children[i] =
            inferTypes(callExpr->getInputs()[i], inputRow, lambdaTypes, pool);
      }
    }

    return createWithImplicitCast(callExpr, std::move(children));
  }

  return nullptr;
}

// This method returns null if the expression doesn't depend on any input row.
TypePtr Expressions::getInputRowType(const TypedExprPtr& expr) {
  if (auto inputExpr = std::dynamic_pointer_cast<const InputTypedExpr>(expr)) {
    return inputExpr->type();
  }
  TypePtr inputRowType;
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
