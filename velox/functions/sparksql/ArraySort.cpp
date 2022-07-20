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
#include "velox/functions/sparksql/ArraySort.h"

#include <memory>
#include "velox/common/base/Exceptions.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/sparksql/Comparisons.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::functions::sparksql {
namespace {

void applyComplexType(
    const SelectivityVector& rows,
    ArrayVector* inputArray,
    bool ascending,
    bool nullsFirst,
    exec::EvalCtx* context,
    VectorPtr* resultElements) {
  auto elementsVector = inputArray->elements();

  // Allocate new vectors for indices.
  BufferPtr indices = allocateIndices(elementsVector->size(), context->pool());
  vector_size_t* rawIndices = indices->asMutable<vector_size_t>();

  const CompareFlags flags{.nullsFirst = nullsFirst, .ascending = ascending};
  // Note: Reusing offsets and sizes isn't safe if the input array had two
  // arrays that had overlapping (but not identical) ranges in the input.
  rows.applyToSelected([&](vector_size_t row) {
    auto size = inputArray->sizeAt(row);
    auto offset = inputArray->offsetAt(row);

    for (auto i = 0; i < size; ++i) {
      rawIndices[offset + i] = offset + i;
    }
    std::sort(
        rawIndices + offset,
        rawIndices + offset + size,
        [&](vector_size_t& a, vector_size_t& b) {
          return elementsVector->compare(elementsVector.get(), a, b, flags) < 0;
        });
  });

  *resultElements = BaseVector::transpose(indices, std::move(elementsVector));
}

template <typename T>
inline void swapWithNull(
    FlatVector<T>* vector,
    vector_size_t index,
    vector_size_t nullIndex) {
  // Values are already present in vector stringBuffers. Don't create additional
  // copy.
  if constexpr (std::is_same<T, StringView>::value) {
    vector->setNoCopy(nullIndex, vector->valueAt(index));
  } else {
    vector->set(nullIndex, vector->valueAt(index));
  }
  vector->setNull(index, true);
}

template <TypeKind kind>
void applyTyped(
    const SelectivityVector& rows,
    const ArrayVector* inputArray,
    bool ascending,
    bool nullsFirst,
    exec::EvalCtx* context,
    VectorPtr* resultElements) {
  using T = typename TypeTraits<kind>::NativeType;

  // Copy array elements to new vector.
  const VectorPtr& inputElements = inputArray->elements();
  SelectivityVector elementRows =
      toElementRows(inputElements->size(), rows, inputArray);

  *resultElements = BaseVector::create(
      inputElements->type(), inputElements->size(), context->pool());
  (*resultElements)
      ->copy(inputElements.get(), elementRows, /*toSourceRow=*/nullptr);

  auto flatResults = (*resultElements)->asFlatVector<T>();
  T* resultRawValues = flatResults->mutableRawValues();

  auto processRow = [&](vector_size_t row) {
    auto size = inputArray->sizeAt(row);
    auto offset = inputArray->offsetAt(row);
    if (size == 0) {
      return;
    }
    vector_size_t numNulls = 0;
    if (nullsFirst) {
      // Move nulls to beginning of array.
      for (vector_size_t i = 0; i < size; ++i) {
        if (flatResults->isNullAt(offset + i)) {
          swapWithNull<T>(flatResults, offset + numNulls, offset + i);
          ++numNulls;
        }
      }
    } else {
      // Move nulls to end of array.
      for (vector_size_t i = size - 1; i >= 0; --i) {
        if (flatResults->isNullAt(offset + i)) {
          swapWithNull<T>(
              flatResults, offset + size - numNulls - 1, offset + i);
          ++numNulls;
        }
      }
    }
    // Exclude null values while sorting.
    auto rowBegin = offset + (nullsFirst ? numNulls : 0);
    auto rowEnd = rowBegin + size - numNulls;

    if constexpr (kind == TypeKind::BOOLEAN) {
      uint64_t* rawBits = flatResults->template mutableRawValues<uint64_t>();
      auto numSetBits = bits::countBits(rawBits, rowBegin, rowEnd);
      // If ascending, false is placed before true, otherwise true is placed
      // before false.
      bool smallerValue = !ascending;
      auto mid = ascending ? rowEnd - numSetBits : rowBegin + numSetBits;
      bits::fillBits(rawBits, rowBegin, mid, smallerValue);
      bits::fillBits(rawBits, mid, rowEnd, !smallerValue);
    } else {
      if (ascending) {
        std::sort(
            resultRawValues + rowBegin, resultRawValues + rowEnd, Less<T>());
      } else {
        std::sort(
            resultRawValues + rowBegin, resultRawValues + rowEnd, Greater<T>());
      }
    }
  };

  rows.applyToSelected(processRow);
}
} // namespace

void ArraySort::apply(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    const TypePtr& /*outputType*/,
    exec::EvalCtx* context,
    VectorPtr* result) const {
  auto& arg = args[0];

  VectorPtr localResult;

  // Input can be constant or flat.
  if (arg->isConstantEncoding()) {
    auto* constantArray = arg->as<ConstantVector<ComplexType>>();
    const auto& flatArray = constantArray->valueVector();
    const auto flatIndex = constantArray->index();

    SelectivityVector singleRow(flatIndex + 1, false);
    singleRow.setValid(flatIndex, true);
    singleRow.updateBounds();

    localResult = applyFlat(singleRow, flatArray, context);
    localResult =
        BaseVector::wrapInConstant(rows.size(), flatIndex, localResult);
  } else {
    localResult = applyFlat(rows, arg, context);
  }

  context->moveOrCopyResult(localResult, rows, result);
}

VectorPtr ArraySort::applyFlat(
    const SelectivityVector& rows,
    const VectorPtr& arg,
    exec::EvalCtx* context) const {
  ArrayVector* inputArray = arg->as<ArrayVector>();
  VectorPtr resultElements;

  auto typeKind = inputArray->elements()->typeKind();
  if (typeKind == TypeKind::MAP || typeKind == TypeKind::ARRAY ||
      typeKind == TypeKind::ROW) {
    applyComplexType(
        rows, inputArray, ascending_, nullsFirst_, context, &resultElements);
  } else {
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        applyTyped,
        typeKind,
        rows,
        inputArray,
        ascending_,
        nullsFirst_,
        context,
        &resultElements);
  }

  return std::make_shared<ArrayVector>(
      context->pool(),
      inputArray->type(),
      inputArray->nulls(),
      rows.end(),
      inputArray->offsets(),
      inputArray->sizes(),
      resultElements,
      inputArray->getNullCount());
}

// Signature: array_sort(array(T)) -> array(T)
std::vector<std::shared_ptr<exec::FunctionSignature>> arraySortSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("array(T)")
          .returnType("array(T)")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeArraySort(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 1);
  // Nulls are considered largest.
  return std::make_shared<ArraySort>(/*ascending=*/true, /*nullsFirst=*/false);
}

// Signatures:
//   sort_array(array(T)) -> array(T)
//   sort_array(array(T), boolean) -> array(T)
std::vector<std::shared_ptr<exec::FunctionSignature>> sortArraySignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("array(T)")
          .returnType("array(T)")
          .build(),
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("array(T)")
          .argumentType("boolean")
          .returnType("array(T)")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeSortArray(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK(
      inputArgs.size() == 1 || inputArgs.size() == 2,
      "Invalid number of arguments {}, expected 1 or 2",
      inputArgs.size());
  bool ascending = true;
  // Read optional sort ascending flag.
  if (inputArgs.size() == 2) {
    BaseVector* boolVector = inputArgs[1].constantValue.get();
    if (!boolVector || !boolVector->isConstantEncoding()) {
      VELOX_USER_FAIL(
          "{} requires a constant bool as the second argument.", name);
    }
    ascending = boolVector->as<ConstantVector<bool>>()->valueAt(0);
  }
  // Nulls are considered smallest.
  bool nullsFirst = ascending;
  return std::make_shared<ArraySort>(ascending, nullsFirst);
}
} // namespace facebook::velox::functions::sparksql
