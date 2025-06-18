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

#include "velox/exec/fuzzer/PrestoQueryRunnerIntermediateTypeTransforms.h"
#include "velox/exec/fuzzer/PrestoQueryRunnerJsonTransform.h"
#include "velox/exec/fuzzer/PrestoQueryRunnerTimestampWithTimeZoneTransform.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/QDigestType.h"
#include "velox/functions/prestosql/types/TDigestType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/parse/Expressions.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::exec::test {
namespace {

const std::unordered_map<TypePtr, std::shared_ptr<IntermediateTypeTransform>>&
intermediateTypeTransforms() {
  static std::unordered_map<TypePtr, std::shared_ptr<IntermediateTypeTransform>>
      intermediateTypeTransforms{
          {TIMESTAMP_WITH_TIME_ZONE(),
           std::make_shared<TimestampWithTimeZoneTransform>()},
          {HYPERLOGLOG(),
           std::make_shared<IntermediateTypeTransformUsingCast>(
               HYPERLOGLOG(), VARBINARY())},
          {TDIGEST(DOUBLE()),
           std::make_shared<IntermediateTypeTransformUsingCast>(
               TDIGEST(DOUBLE()), VARBINARY())},
          {QDIGEST(DOUBLE()),
           std::make_shared<IntermediateTypeTransformUsingCast>(
               QDIGEST(DOUBLE()), VARBINARY())},
          {QDIGEST(BIGINT()),
           std::make_shared<IntermediateTypeTransformUsingCast>(
               QDIGEST(BIGINT()), VARBINARY())},
          {QDIGEST(REAL()),
           std::make_shared<IntermediateTypeTransformUsingCast>(
               QDIGEST(REAL()), VARBINARY())},
          {JSON(), std::make_shared<JsonTransform>()}};
  return intermediateTypeTransforms;
}

const std::shared_ptr<IntermediateTypeTransform>& getIntermediateTypeTransform(
    const TypePtr& type) {
  auto it = intermediateTypeTransforms().find(type);

  if (it == intermediateTypeTransforms().end()) {
    VELOX_FAIL(
        "Attempting to transform unsupported intermediate only type: {}",
        type->toString());
  }

  return it->second;
}

VectorPtr evaluateExpression(
    core::ExprPtr& expression,
    const VectorPtr& input,
    const SelectivityVector& rows) {
  std::shared_ptr<core::QueryCtx> queryCtx_{velox::core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx{
      std::make_unique<core::ExecCtx>(input->pool(), queryCtx_.get())};
  velox::test::VectorMaker vectorMaker(input->pool());
  auto rowVector = vectorMaker.rowVector({input});
  core::TypedExprPtr typedExpr = core::Expressions::inferTypes(
      expression, rowVector->type(), input->pool());
  exec::ExprSet exprSet(
      std::vector<core::TypedExprPtr>{typedExpr}, execCtx.get());
  exec::EvalCtx evalCtx(execCtx.get(), &exprSet, rowVector.get());
  std::vector<VectorPtr> result;
  exprSet.eval(rows, evalCtx, result);
  VELOX_CHECK_EQ(result.size(), 1);
  VELOX_CHECK_NOT_NULL(result[0]);
  return result[0];
}

enum class TransformDirection { TO_INTERMEDIATE, TO_TARGET };

core::ExprPtr getProjection(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias,
    const TransformDirection transformDirection);

// Applies a lambda transform to the elements of an array to convert input
// types <=> intermediate types (where necessary) depending on
// 'transformDirection'.
core::ExprPtr getProjectionForArray(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias,
    const TransformDirection transformDirection) {
  VELOX_CHECK(type->isArray());

  return std::make_shared<core::CallExpr>(
      "transform",
      std::vector<core::ExprPtr>{
          inputExpr,
          std::make_shared<core::LambdaExpr>(
              std::vector<std::string>{"x"},
              getProjection(
                  type->asArray().elementType(),
                  std::make_shared<core::FieldAccessExpr>("x", "x"),
                  "x",
                  transformDirection))},
      columnAlias);
}

// Applies a lambda transform to the keys and values of a map to convert input
// types <=> intermediate types (where necessary) depending on
// 'transformDirection'.
core::ExprPtr getProjectionForMap(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias,
    const TransformDirection transformDirection) {
  VELOX_CHECK(type->isMap());
  const auto& mapType = type->asMap();
  const auto& keysType = mapType.keyType();
  const auto& valuesType = mapType.valueType();

  core::ExprPtr expr = inputExpr;

  if (isIntermediateOnlyType(keysType)) {
    expr = std::make_shared<core::CallExpr>(
        "transform_keys",
        std::vector<core::ExprPtr>{
            expr,
            std::make_shared<core::LambdaExpr>(
                std::vector<std::string>{"k", "v"},
                getProjection(
                    keysType,
                    std::make_shared<core::FieldAccessExpr>("k", "k"),
                    "k",
                    transformDirection))},
        columnAlias);
  }

  if (isIntermediateOnlyType(valuesType)) {
    expr = std::make_shared<core::CallExpr>(
        "transform_values",
        std::vector<core::ExprPtr>{
            expr,
            std::make_shared<core::LambdaExpr>(
                std::vector<std::string>{"k", "v"},
                getProjection(
                    valuesType,
                    std::make_shared<core::FieldAccessExpr>("v", "v"),
                    "v",
                    transformDirection))},
        columnAlias);
  }

  return expr;
}

TypePtr replaceIntermediateWithTargetType(TypePtr type) {
  if (type->isArray()) {
    const auto& arrayType = type->asArray();
    return ARRAY(replaceIntermediateWithTargetType(arrayType.elementType()));
  } else if (type->isMap()) {
    const auto& mapType = type->asMap();
    return MAP(
        replaceIntermediateWithTargetType(mapType.keyType()),
        replaceIntermediateWithTargetType(mapType.valueType()));
  } else if (type->isRow()) {
    const auto& rowType = type->asRow();
    std::vector<std::string> names;
    std::vector<TypePtr> children;
    for (int i = 0; i < rowType.size(); i++) {
      names.push_back(rowType.nameOf(i));
      children.push_back(replaceIntermediateWithTargetType(rowType.childAt(i)));
    }
    return ROW(std::move(names), std::move(children));
  }
  if (isIntermediateOnlyType(type)) {
    const auto& transform = getIntermediateTypeTransform(type);
    return transform->targetType();
  }
  return type;
}

// Applies transforms to the children of a row to convert input
// types <=> intermediate types (where necessary) depending on
// 'transformDirection', and reconstructs the row via row_constructor.
core::ExprPtr getProjectionForRow(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias,
    const TransformDirection transformDirection) {
  VELOX_CHECK(type->isRow());
  const auto& rowType = type->asRow();

  std::vector<core::ExprPtr> children;
  for (int i = 0; i < rowType.size(); i++) {
    if (isIntermediateOnlyType(rowType.childAt(i))) {
      children.push_back(getProjection(
          rowType.childAt(i),
          std::make_shared<core::FieldAccessExpr>(
              rowType.nameOf(i),
              rowType.nameOf(i),
              std::vector<core::ExprPtr>{inputExpr}),
          rowType.nameOf(i),
          transformDirection));
    } else {
      children.push_back(std::make_shared<core::FieldAccessExpr>(
          rowType.nameOf(i),
          rowType.nameOf(i),
          std::vector<core::ExprPtr>{inputExpr}));
    }
  }

  TypePtr outputRowType;
  if (transformDirection == TransformDirection::TO_TARGET) {
    outputRowType = replaceIntermediateWithTargetType(type);
  } else {
    outputRowType = type;
  }

  return std::make_shared<core::CallExpr>(
      "switch",
      std::vector<core::ExprPtr>{
          std::make_shared<core::CallExpr>(
              "is_null", std::vector<core::ExprPtr>{inputExpr}, std::nullopt),
          std::make_shared<core::ConstantExpr>(
              outputRowType, variant::null(TypeKind::ROW), std::nullopt),
          std::make_shared<core::CastExpr>(
              outputRowType,
              std::make_shared<core::CallExpr>(
                  "row_constructor", std::move(children), std::nullopt),
              false,
              std::nullopt)},
      columnAlias);
}

core::ExprPtr getProjection(
    const TypePtr& originaltype,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias,
    const TransformDirection transformDirection) {
  if (originaltype->isArray()) {
    return getProjectionForArray(
        originaltype, inputExpr, columnAlias, transformDirection);
  } else if (originaltype->isMap()) {
    return getProjectionForMap(
        originaltype, inputExpr, columnAlias, transformDirection);
  } else if (originaltype->isRow()) {
    return getProjectionForRow(
        originaltype, inputExpr, columnAlias, transformDirection);
  }
  const auto& transform = getIntermediateTypeTransform(originaltype);
  if (transformDirection == TransformDirection::TO_TARGET) {
    return transform->projectToTargetType(inputExpr, columnAlias);
  }
  return transform->projectToIntermediateType(inputExpr, columnAlias);
}

VectorPtr transformIntermediateTypes(
    const VectorPtr& vector,
    const SelectivityVector& rows) {
  const auto& type = vector->type();
  auto expression = getProjection(
      type,
      std::make_shared<core::FieldAccessExpr>("c0", "c0"),
      "c0",
      TransformDirection::TO_TARGET);

  return evaluateExpression(expression, vector, rows);
}

} // namespace

bool isIntermediateOnlyType(const TypePtr& type) {
  if (intermediateTypeTransforms().find(type) !=
      intermediateTypeTransforms().end()) {
    return true;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (isIntermediateOnlyType(type->childAt(i))) {
      return true;
    }
  }

  return false;
}

VectorPtr transformIntermediateTypes(const VectorPtr& vector) {
  return transformIntermediateTypes(
      vector, SelectivityVector(vector->size(), true));
}

core::ExprPtr getProjectionsToIntermediateTypes(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) {
  return getProjection(
      type, inputExpr, columnAlias, TransformDirection::TO_INTERMEDIATE);
}

core::ExprPtr IntermediateTypeTransformUsingCast::projectToTargetType(
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
  return std::make_shared<core::CastExpr>(
      targetType_, inputExpr, false, columnAlias);
}

core::ExprPtr IntermediateTypeTransformUsingCast::projectToIntermediateType(
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
  return std::make_shared<core::CastExpr>(
      intermediateType_, inputExpr, false, columnAlias);
}
} // namespace facebook::velox::exec::test
