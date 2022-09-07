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
#include <folly/CPortability.h>

#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::functions {
namespace {

// Find the index of the first match for primitive types.
template <
    TypeKind kind,
    typename std::enable_if_t<TypeTraits<kind>::isPrimitiveType, int> = 0>
void applyTypedFirstMatch(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    const DecodedVector& elementsDecoded,
    const DecodedVector& searchDecoded,
    FlatVector<int64_t>& flatResult) {
  using T = typename TypeTraits<kind>::NativeType;

  auto baseArray = arrayDecoded.base()->as<ArrayVector>();
  auto rawSizes = baseArray->rawSizes();
  auto rawOffsets = baseArray->rawOffsets();
  auto indices = arrayDecoded.indices();

  if (elementsDecoded.isIdentityMapping() && !elementsDecoded.mayHaveNulls() &&
      searchDecoded.isConstantMapping()) {
    // Fast path for array vector of boolean.
    if constexpr (std::is_same_v<bool, T>) {
      auto rawElements = elementsDecoded.data<uint64_t>();

      auto search = bits::isBitSet(searchDecoded.data<uint64_t>(), 0);

      rows.applyToSelected([&](auto row) {
        auto size = rawSizes[indices[row]];
        auto offset = rawOffsets[indices[row]];

        int i;
        for (i = 0; i < size; i++) {
          if (bits::isBitSet(rawElements, offset + i) == search) {
            flatResult.set(row, i + 1);
            break;
          }
        }
        if (i == size) {
          flatResult.set(row, 0);
        }
      });
      return;
    }

    // Fast path for array vector of types other than boolean.
    auto rawElements = elementsDecoded.data<T>();

    auto search = searchDecoded.valueAt<T>(0);

    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[indices[row]];
      auto offset = rawOffsets[indices[row]];

      int i;
      for (i = 0; i < size; i++) {
        if (rawElements[offset + i] == search) {
          flatResult.set(row, i + 1);
          break;
        }
      }
      if (i == size) {
        flatResult.set(row, 0);
      }
    });
    return;
  }

  // Regular path where no assumption is made about the encodings of
  // searchDecoded and elementsDecoded.
  rows.applyToSelected([&](auto row) {
    auto size = rawSizes[indices[row]];
    auto offset = rawOffsets[indices[row]];

    auto search = searchDecoded.valueAt<T>(row);

    int i;
    for (i = 0; i < size; i++) {
      if (!elementsDecoded.isNullAt(offset + i) &&
          elementsDecoded.valueAt<T>(offset + i) == search) {
        flatResult.set(row, i + 1);
        break;
      }
    }
    if (i == size) {
      flatResult.set(row, 0);
    }
  });
}

// Find the index of the first match for complex types.
template <
    TypeKind kind,
    typename std::enable_if_t<!TypeTraits<kind>::isPrimitiveType, int> = 0>
void applyTypedFirstMatch(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    const DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    FlatVector<int64_t>& flatResult) {
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
    auto searchIndex = searchIndices[row];

    int i;
    for (i = 0; i < size; i++) {
      if (!elementsBase->isNullAt(offset + i) &&
          elementsBase->equalValueAt(searchBase, offset + i, searchIndex)) {
        flatResult.set(row, i + 1);
        break;
      }
    }
    if (i == size) {
      flatResult.set(row, 0);
    }
  });
}

FOLLY_ALWAYS_INLINE void getLoopBoundary(
    const int* rawSizes,
    const int* indices,
    vector_size_t row,
    int64_t& instance,
    int& startIndex,
    int& endIndex,
    int& step) {
  auto size = rawSizes[indices[row]];

  startIndex = instance > 0 ? 0 : size - 1;
  endIndex = instance > 0 ? size : -1;
  step = instance > 0 ? 1 : -1;
  instance = std::abs(instance);
}

// Find the index of the instance-th match for primitive types.
template <
    TypeKind kind,
    typename std::enable_if_t<TypeTraits<kind>::isPrimitiveType, int> = 0>
void applyTypedWithInstance(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    const DecodedVector& elementsDecoded,
    const DecodedVector& searchDecoded,
    const DecodedVector& instanceDecoded,
    FlatVector<int64_t>& flatResult) {
  using T = typename TypeTraits<kind>::NativeType;

  auto baseArray = arrayDecoded.base()->as<ArrayVector>();
  auto rawSizes = baseArray->rawSizes();
  auto rawOffsets = baseArray->rawOffsets();
  auto indices = arrayDecoded.indices();

  int startIndex;
  int endIndex;
  int step;

  if (elementsDecoded.isIdentityMapping() && !elementsDecoded.mayHaveNulls() &&
      searchDecoded.isConstantMapping() &&
      instanceDecoded.isConstantMapping()) {
    auto instance = instanceDecoded.valueAt<int64_t>(0);
    VELOX_USER_CHECK_NE(
        instance,
        0,
        "array_position cannot take a 0-valued instance argument.");

    // Fast path for array vector of boolean.
    if constexpr (std::is_same_v<bool, T>) {
      auto rawElements = elementsDecoded.data<uint64_t>();

      auto search = bits::isBitSet(searchDecoded.data<uint64_t>(), 0);

      rows.applyToSelected([&](auto row) {
        auto offset = rawOffsets[indices[row]];
        getLoopBoundary(
            rawSizes, indices, row, instance, startIndex, endIndex, step);

        int i;
        for (i = startIndex; i != endIndex; i += step) {
          if (bits::isBitSet(rawElements, offset + i) == search) {
            --instance;
            if (instance == 0) {
              flatResult.set(row, i + 1);
              break;
            }
          }
        }
        if (i == endIndex) {
          flatResult.set(row, 0);
        }
      });
      return;
    }

    // Fast path for array vector of types other than boolean.
    auto rawElements = elementsDecoded.data<T>();

    auto search = searchDecoded.valueAt<T>(0);

    rows.applyToSelected([&](auto row) {
      auto offset = rawOffsets[indices[row]];
      getLoopBoundary(
          rawSizes, indices, row, instance, startIndex, endIndex, step);

      int i;
      for (i = startIndex; i != endIndex; i += step) {
        if (rawElements[offset + i] == search) {
          --instance;
          if (instance == 0) {
            flatResult.set(row, i + 1);
            break;
          }
        }
      }
      if (i == endIndex) {
        flatResult.set(row, 0);
      }
    });
    return;
  }

  // Regular path where no assumption is made about the encodings of
  // searchDecoded and elementsDecoded.
  rows.applyToSelected([&](auto row) {
    auto offset = rawOffsets[indices[row]];
    auto search = searchDecoded.valueAt<T>(row);

    auto instance = instanceDecoded.valueAt<int64_t>(row);
    VELOX_USER_CHECK_NE(
        instance,
        0,
        "array_position cannot take a 0-valued instance argument.");

    getLoopBoundary(
        rawSizes, indices, row, instance, startIndex, endIndex, step);

    int i;
    for (i = startIndex; i != endIndex; i += step) {
      if (!elementsDecoded.isNullAt(offset + i) &&
          elementsDecoded.valueAt<T>(offset + i) == search) {
        --instance;
        if (instance == 0) {
          flatResult.set(row, i + 1);
          break;
        }
      }
    }
    if (i == endIndex) {
      flatResult.set(row, 0);
    }
  });
}

// Find the index of the instance-th match for complex types.
template <
    TypeKind kind,
    typename std::enable_if_t<!TypeTraits<kind>::isPrimitiveType, int> = 0>
void applyTypedWithInstance(
    const SelectivityVector& rows,
    DecodedVector& arrayDecoded,
    const DecodedVector& elementsDecoded,
    DecodedVector& searchDecoded,
    const DecodedVector& instanceDecoded,
    FlatVector<int64_t>& flatResult) {
  auto baseArray = arrayDecoded.base()->as<ArrayVector>();
  auto rawSizes = baseArray->rawSizes();
  auto rawOffsets = baseArray->rawOffsets();
  auto indices = arrayDecoded.indices();

  auto elementsBase = elementsDecoded.base();

  auto searchBase = searchDecoded.base();
  auto searchIndices = searchDecoded.indices();

  int startIndex;
  int endIndex;
  int step;

  rows.applyToSelected([&](auto row) {
    auto offset = rawOffsets[indices[row]];
    auto searchIndex = searchIndices[row];

    auto instance = instanceDecoded.valueAt<int64_t>(row);
    VELOX_USER_CHECK_NE(
        instance,
        0,
        "array_position cannot take a 0-valued instance argument.");

    getLoopBoundary(
        rawSizes, indices, row, instance, startIndex, endIndex, step);

    int i;
    for (i = startIndex; i != endIndex; i += step) {
      if (!elementsBase->isNullAt(offset + i) &&
          elementsBase->equalValueAt(searchBase, offset + i, searchIndex)) {
        --instance;
        if (instance == 0) {
          flatResult.set(row, i + 1);
          break;
        }
      }
    }
    if (i == endIndex) {
      flatResult.set(row, 0);
    }
  });
}

class ArrayPositionFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto& arrayVector = args[0];
    const auto& searchVector = args[1];
    VELOX_CHECK(arrayVector->type()->isArray());
    VELOX_CHECK(arrayVector->type()->asArray().elementType()->kindEquals(
        searchVector->type()));

    context.ensureWritable(rows, BIGINT(), result);
    auto flatResult = result->asFlatVector<int64_t>();

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto elements = decodedArgs.at(0)->base()->as<ArrayVector>()->elements();
    exec::LocalSelectivityVector nestedRows(context, elements->size());
    nestedRows.get()->setAll();
    exec::LocalDecodedVector elementsHolder(
        context, *elements, *nestedRows.get());

    if (args.size() == 2) {
      VELOX_DYNAMIC_TYPE_DISPATCH(
          applyTypedFirstMatch,
          searchVector->typeKind(),
          rows,
          *decodedArgs.at(0),
          *elementsHolder.get(),
          *decodedArgs.at(1),
          *flatResult);
    } else {
      const auto& instanceVector = args[2];
      VELOX_CHECK(instanceVector->type()->isBigint());

      VELOX_DYNAMIC_TYPE_DISPATCH(
          applyTypedWithInstance,
          searchVector->typeKind(),
          rows,
          *decodedArgs.at(0),
          *elementsHolder.get(),
          *decodedArgs.at(1),
          *decodedArgs.at(2),
          *flatResult);
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {// array(T), T -> int64_t
            exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("bigint")
                .argumentType("array(T)")
                .argumentType("T")
                .build(),

            // array(T), T, int64_t -> int64_t
            exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("bigint")
                .argumentType("array(T)")
                .argumentType("T")
                .argumentType("bigint")
                .build()};
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_position,
    ArrayPositionFunction::signatures(),
    std::make_unique<ArrayPositionFunction>());

} // namespace facebook::velox::functions
