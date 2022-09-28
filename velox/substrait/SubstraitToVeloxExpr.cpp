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

#include "velox/substrait/SubstraitToVeloxExpr.h"
#include "velox/substrait/TypeUtils.h"

namespace facebook::velox::substrait {

std::shared_ptr<const core::FieldAccessTypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::FieldReference& substraitField,
    const RowTypePtr& inputType) {
  auto typeCase = substraitField.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::FieldReference::ReferenceTypeCase::
        kDirectReference: {
      const auto& directRef = substraitField.direct_reference();
      int32_t colIdx = substraitParser_.parseReferenceSegment(directRef);
      const auto& inputNames = inputType->names();
      const int64_t inputSize = inputNames.size();
      if (colIdx <= inputSize) {
        const auto& inputTypes = inputType->children();
        // Convert type to row.
        return std::make_shared<core::FieldAccessTypedExpr>(
            inputTypes[colIdx],
            std::make_shared<core::InputTypedExpr>(inputTypes[colIdx]),
            inputNames[colIdx]);
      } else {
        VELOX_FAIL("Missing the column with id '{}' .", colIdx);
      }
    }
    default:
      VELOX_NYI(
          "Substrait conversion not supported for Reference '{}'", typeCase);
  }
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::ScalarFunction& substraitFunc,
    const RowTypePtr& inputType) {
  std::vector<core::TypedExprPtr> params;
  params.reserve(substraitFunc.arguments().size());
  for (const auto& sArg : substraitFunc.arguments()) {
    params.emplace_back(toVeloxExpr(sArg.value(), inputType));
  }
  const auto& veloxFunction = substraitParser_.findVeloxFunction(
      functionMap_, substraitFunc.function_reference());
  std::string typeName =
      substraitParser_.parseType(substraitFunc.output_type())->type;
  return std::make_shared<const core::CallTypedExpr>(
      toVeloxType(typeName), std::move(params), veloxFunction);
}

std::shared_ptr<const core::ConstantTypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::Literal& substraitLit) {
  auto typeCase = substraitLit.literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return std::make_shared<core::ConstantTypedExpr>(
          variant(substraitLit.boolean()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return std::make_shared<core::ConstantTypedExpr>(
          variant(substraitLit.i32()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return std::make_shared<core::ConstantTypedExpr>(
          variant(substraitLit.i64()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return std::make_shared<core::ConstantTypedExpr>(
          variant(substraitLit.fp64()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      auto veloxType =
          toVeloxType(substraitParser_.parseType(substraitLit.null())->type);
      return std::make_shared<core::ConstantTypedExpr>(
          veloxType, variant::null(veloxType->kind()));
    }
    default:
      VELOX_NYI(
          "Substrait conversion not supported for type case '{}'", typeCase);
  }
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::Cast& castExpr,
    const RowTypePtr& inputType) {
  auto substraitType = substraitParser_.parseType(castExpr.type());
  auto type = toVeloxType(substraitType->type);
  // TODO add flag in substrait after. now is set false.
  bool nullOnFailure = false;

  std::vector<core::TypedExprPtr> inputs{
      toVeloxExpr(castExpr.input(), inputType)};

  return std::make_shared<core::CastTypedExpr>(type, inputs, nullOnFailure);
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression& substraitExpr,
    const RowTypePtr& inputType) {
  std::shared_ptr<const core::ITypedExpr> veloxExpr;
  auto typeCase = substraitExpr.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kLiteral:
      return toVeloxExpr(substraitExpr.literal());
    case ::substrait::Expression::RexTypeCase::kScalarFunction:
      return toVeloxExpr(substraitExpr.scalar_function(), inputType);
    case ::substrait::Expression::RexTypeCase::kSelection:
      return toVeloxExpr(substraitExpr.selection(), inputType);
    case ::substrait::Expression::RexTypeCase::kCast:
      return toVeloxExpr(substraitExpr.cast(), inputType);
    case ::substrait::Expression::RexTypeCase::kIfThen:
      return toVeloxExpr(substraitExpr.if_then(), inputType);
    default:
      VELOX_NYI(
          "Substrait conversion not supported for Expression '{}'", typeCase);
  }
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression_IfThen& substraitIfThen,
    const RowTypePtr& inputType) {
  std::vector<core::TypedExprPtr> inputs;
  if (substraitIfThen.has_else_()) {
    inputs.reserve(substraitIfThen.ifs_size() * 2 + 1);
  } else {
    inputs.reserve(substraitIfThen.ifs_size() * 2);
  }

  TypePtr resultType;
  for (auto& ifExpr : substraitIfThen.ifs()) {
    auto ifClauseExpr = toVeloxExpr(ifExpr.if_(), inputType);
    inputs.emplace_back(ifClauseExpr);
    auto thenClauseExpr = toVeloxExpr(ifExpr.then(), inputType);
    inputs.emplace_back(thenClauseExpr);

    if (!thenClauseExpr->type()->containsUnknown()) {
      resultType = thenClauseExpr->type();
    }
  }

  if (substraitIfThen.has_else_()) {
    auto elseClauseExpr = toVeloxExpr(substraitIfThen.else_(), inputType);
    inputs.emplace_back(elseClauseExpr);
    if (!resultType && !elseClauseExpr->type()->containsUnknown()) {
      resultType = elseClauseExpr->type();
    }
  }

  VELOX_CHECK_NOT_NULL(resultType, "Result type not found");

  return std::make_shared<const core::CallTypedExpr>(
      resultType, std::move(inputs), "if");
}

} // namespace facebook::velox::substrait
