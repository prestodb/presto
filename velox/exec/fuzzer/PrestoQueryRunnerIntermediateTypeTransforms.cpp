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
#include "velox/exec/fuzzer/PrestoQueryRunnerTimestampWithTimeZoneTransform.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/parse/Expressions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/fuzzer/Utils.h"

namespace facebook::velox::exec::test {
namespace {
class ArrayTransform {
 public:
  VectorPtr transform(const VectorPtr& vector, const SelectivityVector& rows)
      const;

  core::ExprPtr projectionExpr(
      const TypePtr& type,
      const core::ExprPtr& inputExpr,
      const std::string& columnAlias) const;
};

class MapTransform {
 public:
  VectorPtr transform(const VectorPtr& vector, const SelectivityVector& rows)
      const;

  core::ExprPtr projectionExpr(
      const TypePtr& type,
      const core::ExprPtr& inputExpr,
      const std::string& columnAlias) const;
};

class RowTransform {
 public:
  VectorPtr transform(const VectorPtr& vector, const SelectivityVector& rows)
      const;

  core::ExprPtr projectionExpr(
      const TypePtr& type,
      const core::ExprPtr& inputExpr,
      const std::string& columnAlias) const;
};

const ArrayTransform kArrayTransform;
const MapTransform kMapTransform;
const RowTransform kRowTransform;

const std::unordered_map<TypePtr, std::shared_ptr<IntermediateTypeTransform>>&
intermediateTypeTransforms() {
  static std::unordered_map<TypePtr, std::shared_ptr<IntermediateTypeTransform>>
      intermediateTypeTransforms{
          {TIMESTAMP_WITH_TIME_ZONE(),
           std::make_shared<TimestampWithTimeZoneTransform>()}};
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

VectorPtr transformIntermediateOnlyType(
    const VectorPtr& vector,
    const SelectivityVector& rows) {
  const auto& type = vector->type();
  if (type->isArray()) {
    return kArrayTransform.transform(vector, rows);
  } else if (type->isMap()) {
    return kMapTransform.transform(vector, rows);
  } else if (type->isRow()) {
    return kRowTransform.transform(vector, rows);
  } else {
    const auto& transform = getIntermediateTypeTransform(type);
    DecodedVector decoded(*vector);
    const auto* base = decoded.base();

    VectorPtr result;
    const auto& transformedType = transform->transformedType();
    BaseVector::ensureWritable(
        SelectivityVector(base->size()), transformedType, base->pool(), result);
    VectorWriter<Any> writer;
    writer.init(*result);

    if (base->isConstantEncoding()) {
      if (rows.countSelected() > 0) {
        if (base->isNullAt(0)) {
          result = BaseVector::createNullConstant(
              transformedType, base->size(), base->pool());
        } else {
          const auto value = transform->transform(base, 0);

          if (value.isNull()) {
            result = BaseVector::createNullConstant(
                transformedType, base->size(), base->pool());
          } else {
            writer.setOffset(0);
            VELOX_DYNAMIC_TYPE_DISPATCH(
                fuzzer::writeOne,
                transformedType->kind(),
                value,
                writer.current());
            writer.commit(true);
            result = BaseVector::wrapInConstant(base->size(), 0, result);
          }
        }
      }
    } else {
      rows.applyToSelected([&](vector_size_t row) {
        auto index = decoded.index(row);
        writer.setOffset(index);

        if (base->isNullAt(index)) {
          writer.commitNull();
        } else {
          const auto value = transform->transform(base, index);

          if (value.isNull()) {
            writer.commitNull();
          } else {
            VELOX_DYNAMIC_TYPE_DISPATCH(
                fuzzer::writeOne,
                transformedType->kind(),
                value,
                writer.current());
            writer.commit(true);
          }
        }
      });
    }

    if (!decoded.isIdentityMapping()) {
      result = decoded.wrap(result, *vector, vector->size());
    }

    return result;
  }
}

// Converts an ArrayVector so that any intermediate only types in the elements
// are transformed.
VectorPtr ArrayTransform::transform(
    const VectorPtr& vector,
    const SelectivityVector& rows) const {
  VELOX_CHECK(vector->type()->isArray());
  DecodedVector decoded(*vector);
  const auto* base = decoded.base()->as<ArrayVector>();

  SelectivityVector elementRows(base->elements()->size(), false);
  rows.applyToSelected([&](vector_size_t row) {
    if (!decoded.isNullAt(row)) {
      auto index = decoded.index(row);
      elementRows.setValidRange(
          base->offsetAt(index),
          base->offsetAt(index) + base->sizeAt(index),
          true);
    }
  });
  elementRows.updateBounds();

  VectorPtr elementsVector =
      transformIntermediateOnlyType(base->elements(), elementRows);

  VectorPtr array = std::make_shared<ArrayVector>(
      base->pool(),
      ARRAY(elementsVector->type()),
      base->nulls(),
      base->size(),
      base->offsets(),
      base->sizes(),
      elementsVector);

  if (!decoded.isIdentityMapping()) {
    array = decoded.wrap(array, *vector, vector->size());
  }

  return array;
}

// Applies a lambda transform to the elements of an array to convert input
// types to intermediate only types where necessary.
core::ExprPtr ArrayTransform::projectionExpr(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
  VELOX_CHECK(type->isArray());

  return std::make_shared<core::CallExpr>(
      "transform",
      std::vector<core::ExprPtr>{
          inputExpr,
          std::make_shared<core::LambdaExpr>(
              std::vector<std::string>{"x"},
              getIntermediateOnlyTypeProjectionExpr(
                  type->asArray().elementType(),
                  std::make_shared<core::FieldAccessExpr>("x", "x"),
                  "x"))},
      columnAlias);
}

// Converts an MapVector so that any intermediate only types in the keys and
// values are transformed.
VectorPtr MapTransform::transform(
    const VectorPtr& vector,
    const SelectivityVector& rows) const {
  VELOX_CHECK(vector->type()->isMap());
  DecodedVector decoded(*vector);
  const auto* base = decoded.base()->as<MapVector>();

  VectorPtr keysVector = base->mapKeys();
  VectorPtr valuesVector = base->mapValues();
  const auto& keysType = keysVector->type();
  const auto& valuesType = valuesVector->type();

  SelectivityVector elementRows(keysVector->size(), false);
  rows.applyToSelected([&](vector_size_t row) {
    if (!decoded.isNullAt(row)) {
      auto index = decoded.index(row);
      elementRows.setValidRange(
          base->offsetAt(index),
          base->offsetAt(index) + base->sizeAt(index),
          true);
    }
  });
  elementRows.updateBounds();

  if (isIntermediateOnlyType(keysType)) {
    keysVector = transformIntermediateOnlyType(keysVector, elementRows);
  }

  if (isIntermediateOnlyType(valuesType)) {
    valuesVector = transformIntermediateOnlyType(valuesVector, elementRows);
  }

  VectorPtr map = std::make_shared<MapVector>(
      base->pool(),
      MAP(keysVector->type(), valuesVector->type()),
      base->nulls(),
      base->size(),
      base->offsets(),
      base->sizes(),
      keysVector,
      valuesVector);

  if (!decoded.isIdentityMapping()) {
    map = decoded.wrap(map, *vector, vector->size());
  }

  return map;
}

// Applies a lambda transform to the keys and values of a map to convert input
// types to intermediate only types where necessary.
core::ExprPtr MapTransform::projectionExpr(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
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
                getIntermediateOnlyTypeProjectionExpr(
                    keysType,
                    std::make_shared<core::FieldAccessExpr>("k", "k"),
                    "k"))},
        columnAlias);
  }

  if (isIntermediateOnlyType(valuesType)) {
    expr = std::make_shared<core::CallExpr>(
        "transform_values",
        std::vector<core::ExprPtr>{
            expr,
            std::make_shared<core::LambdaExpr>(
                std::vector<std::string>{"k", "v"},
                getIntermediateOnlyTypeProjectionExpr(
                    valuesType,
                    std::make_shared<core::FieldAccessExpr>("v", "v"),
                    "v"))},
        columnAlias);
  }

  return expr;
}

// Converts an RowVector so that any intermediate only types in the children
// are transformed.
VectorPtr RowTransform::transform(
    const VectorPtr& vector,
    const SelectivityVector& rows) const {
  VELOX_CHECK(vector->type()->isRow());
  DecodedVector decoded(*vector);
  const auto* base = decoded.base()->as<RowVector>();

  SelectivityVector childRows(base->size(), false);
  rows.applyToSelected([&](vector_size_t row) {
    if (!decoded.isNullAt(row)) {
      childRows.setValid(decoded.index(row), true);
    }
  });
  childRows.updateBounds();

  std::vector<VectorPtr> children;
  std::vector<TypePtr> childrenTypes;
  std::vector<std::string> childrenNames = base->type()->asRow().names();
  for (const auto& child : base->children()) {
    if (isIntermediateOnlyType(child->type())) {
      children.push_back(transformIntermediateOnlyType(child, childRows));
      childrenTypes.push_back(children.back()->type());
    } else {
      children.push_back(child);
      childrenTypes.push_back(child->type());
    }
  }

  VectorPtr row = std::make_shared<RowVector>(
      base->pool(),
      ROW(std::move(childrenNames), std::move(childrenTypes)),
      base->nulls(),
      base->size(),
      std::move(children));

  if (!decoded.isIdentityMapping()) {
    row = decoded.wrap(row, *vector, vector->size());
  }

  return row;
}

// Applies transforms to the children of a row to convert input types to
// intermediate only types where necessary, and reconstructs the row via
// row_constructor.
core::ExprPtr RowTransform::projectionExpr(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
  VELOX_CHECK(type->isRow());
  const auto& rowType = type->asRow();

  std::vector<core::ExprPtr> children;
  for (int i = 0; i < rowType.size(); i++) {
    if (isIntermediateOnlyType(rowType.childAt(i))) {
      children.push_back(getIntermediateOnlyTypeProjectionExpr(
          rowType.childAt(i),
          std::make_shared<core::FieldAccessExpr>(
              rowType.nameOf(i),
              rowType.nameOf(i),
              std::vector<core::ExprPtr>{inputExpr}),
          rowType.nameOf(i)));
    } else {
      children.push_back(std::make_shared<core::FieldAccessExpr>(
          rowType.nameOf(i),
          rowType.nameOf(i),
          std::vector<core::ExprPtr>{inputExpr}));
    }
  }

  return std::make_shared<core::CallExpr>(
      "switch",
      std::vector<core::ExprPtr>{
          std::make_shared<core::CallExpr>(
              "is_null", std::vector<core::ExprPtr>{inputExpr}, std::nullopt),
          std::make_shared<core::ConstantExpr>(
              type, variant::null(TypeKind::ROW), std::nullopt),
          std::make_shared<core::CastExpr>(
              type,
              std::make_shared<core::CallExpr>(
                  "row_constructor", std::move(children), std::nullopt),
              false,
              std::nullopt)},
      columnAlias);
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

VectorPtr transformIntermediateOnlyType(const VectorPtr& vector) {
  return transformIntermediateOnlyType(
      vector, SelectivityVector(vector->size(), true));
}

core::ExprPtr getIntermediateOnlyTypeProjectionExpr(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) {
  if (type->isArray()) {
    return kArrayTransform.projectionExpr(type, inputExpr, columnAlias);
  } else if (type->isMap()) {
    return kMapTransform.projectionExpr(type, inputExpr, columnAlias);
  } else if (type->isRow()) {
    return kRowTransform.projectionExpr(type, inputExpr, columnAlias);
  } else {
    return getIntermediateTypeTransform(type)->projectionExpr(
        inputExpr, columnAlias);
  }
}
} // namespace facebook::velox::exec::test
