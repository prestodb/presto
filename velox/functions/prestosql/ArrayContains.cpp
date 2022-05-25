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
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::functions {
namespace {

template <TypeKind kind>
void applyTyped(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    FlatVector<bool>& flatResult) {
  using T = typename TypeTraits<kind>::NativeType;

  auto baseArray = arrayDecoded.base()->as<ArrayVector>();
  auto rawSizes = baseArray->rawSizes();
  auto rawOffsets = baseArray->rawOffsets();
  auto indices = arrayDecoded.indices();

  constexpr bool isBoolType = std::is_same_v<bool, T>;

  if (!isBoolType && elementsDecoded.isIdentityMapping() &&
      !elementsDecoded.mayHaveNulls() && searchDecoded.isConstantMapping()) {
    auto rawElements = elementsDecoded.data<T>();
    auto search = searchDecoded.valueAt<T>(0);

    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[indices[row]];
      auto offset = rawOffsets[indices[row]];

      for (auto i = 0; i < size; i++) {
        if (rawElements[offset + i] == search) {
          flatResult.set(row, true);
          return;
        }
      }

      flatResult.set(row, false);
    });
  } else {
    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[indices[row]];
      auto offset = rawOffsets[indices[row]];

      auto search = searchDecoded.valueAt<T>(row);

      bool foundNull = false;

      for (auto i = 0; i < size; i++) {
        if (elementsDecoded.isNullAt(offset + i)) {
          foundNull = true;
        } else if (elementsDecoded.valueAt<T>(offset + i) == search) {
          flatResult.set(row, true);
          return;
        }
      }

      if (foundNull) {
        flatResult.setNull(row, true);
      } else {
        flatResult.set(row, false);
      }
    });
  }
}

void applyComplexType(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    FlatVector<bool>& flatResult) {
  auto baseArray = arrayDecoded.base()->as<ArrayVector>();
  auto rawSizes = baseArray->rawSizes();
  auto rawOffsets = baseArray->rawOffsets();
  auto indices = arrayDecoded.indices();

  auto elementsBase = elementsDecoded.base();

  auto searchBase = searchDecoded.base();
  auto searchIndices = searchDecoded.indices();

  rows.applyToSelected([&](auto row) {
    auto size = rawSizes[indices[row]];
    auto offset = rawOffsets[indices[row]];

    bool foundNull = false;

    auto searchIndex = searchIndices[row];
    for (auto i = 0; i < size; i++) {
      if (elementsBase->isNullAt(offset + i)) {
        foundNull = true;
      } else if (elementsBase->equalValueAt(
                     searchBase, offset + i, searchIndex)) {
        flatResult.set(row, true);
        return;
      }
    }

    if (foundNull) {
      flatResult.setNull(row, true);
    } else {
      flatResult.set(row, false);
    }
  });
}

template <>
void applyTyped<TypeKind::ARRAY>(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    FlatVector<bool>& flatResult) {
  applyComplexType(
      rows, arrayDecoded, elementsDecoded, searchDecoded, flatResult);
}

template <>
void applyTyped<TypeKind::MAP>(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    FlatVector<bool>& flatResult) {
  applyComplexType(
      rows, arrayDecoded, elementsDecoded, searchDecoded, flatResult);
}

template <>
void applyTyped<TypeKind::ROW>(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    FlatVector<bool>& flatResult) {
  applyComplexType(
      rows, arrayDecoded, elementsDecoded, searchDecoded, flatResult);
}

class ArrayContainsFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 2);
    const auto& arrayVector = args[0];
    const auto& searchVector = args[1];

    VELOX_CHECK(arrayVector->type()->isArray());
    VELOX_CHECK(arrayVector->type()->asArray().elementType()->kindEquals(
        searchVector->type()));

    BaseVector::ensureWritable(rows, BOOLEAN(), context->pool(), result);
    auto flatResult = (*result)->asFlatVector<bool>();

    exec::LocalDecodedVector arrayHolder(context, *arrayVector, rows);
    auto elements = arrayHolder.get()->base()->as<ArrayVector>()->elements();

    exec::LocalSelectivityVector nestedRows(context, elements->size());
    nestedRows.get()->setAll();

    exec::LocalDecodedVector elementsHolder(
        context, *elements, *nestedRows.get());

    exec::LocalDecodedVector searchHolder(context, *searchVector, rows);

    VELOX_DYNAMIC_TYPE_DISPATCH(
        applyTyped,
        searchVector->typeKind(),
        rows,
        *arrayHolder.get(),
        *elementsHolder.get(),
        *searchHolder.get(),
        *flatResult);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T), T -> boolean
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("boolean")
                .argumentType("array(T)")
                .argumentType("T")
                .build()};
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_contains,
    ArrayContainsFunction::signatures(),
    std::make_unique<ArrayContainsFunction>());

} // namespace facebook::velox::functions
