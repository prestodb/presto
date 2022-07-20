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

#include <folly/container/F14Set.h>

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"

namespace facebook::velox::functions {
namespace {

void applyComplexType(
    const SelectivityVector& rows,
    ArrayVector* inputArray,
    exec::EvalCtx* context,
    VectorPtr* resultElements) {
  auto inputElements = inputArray->elements();
  const SelectivityVector inputElementRows =
      toElementRows(inputElements->size(), rows, inputArray);
  exec::LocalDecodedVector decodedElements(
      context, *inputElements, inputElementRows);
  const auto* baseElementsVector = decodedElements->base();

  // Allocate new vectors for indices.
  BufferPtr indices = allocateIndices(inputElements->size(), context->pool());
  vector_size_t* rawIndices = indices->asMutable<vector_size_t>();

  const CompareFlags flags{.nullsFirst = false, .ascending = true};
  rows.applyToSelected([&](vector_size_t row) {
    const auto size = inputArray->sizeAt(row);
    const auto offset = inputArray->offsetAt(row);
    for (auto i = offset; i < offset + size; ++i) {
      rawIndices[i] = i;
    }
    std::sort(
        rawIndices + offset,
        rawIndices + offset + size,
        [&](vector_size_t& a, vector_size_t& b) {
          return baseElementsVector->compare(baseElementsVector, a, b, flags) <
              0;
        });
  });

  *resultElements = BaseVector::transpose(indices, std::move(inputElements));
}

template <typename T>
inline void swapWithNull(
    FlatVector<T>* vector,
    vector_size_t index,
    vector_size_t nullIndex) {
  // Values are already present in vector stringBuffers. Don't create additional
  // copy.
  if constexpr (std::is_same_v<T, StringView>) {
    vector->setNoCopy(nullIndex, vector->valueAt(index));
  } else {
    vector->set(nullIndex, vector->valueAt(index));
  }
  vector->setNull(index, true);
}

template <TypeKind kind>
void applyScalarType(
    const SelectivityVector& rows,
    const ArrayVector* inputArray,
    exec::EvalCtx* context,
    VectorPtr* resultElements) {
  using T = typename TypeTraits<kind>::NativeType;

  // Copy array elements to new vector.
  const VectorPtr& inputElements = inputArray->elements();
  VELOX_DCHECK(kind == inputElements->typeKind());
  const SelectivityVector inputElementRows =
      toElementRows(inputElements->size(), rows, inputArray);
  exec::LocalDecodedVector decodedElements(
      context, *inputElements, inputElementRows);
  const vector_size_t elementsCount = inputElementRows.size();

  // TODO: consider to use dictionary wrapping to avoid the direct sorting on
  // the scalar values as we do for complex data type if this runs slow in
  // practice.
  *resultElements =
      BaseVector::create(inputElements->type(), elementsCount, context->pool());
  (*resultElements)
      ->copy(
          decodedElements->base(), inputElementRows, /*toSourceRow=*/nullptr);

  auto flatResults = (*resultElements)->asFlatVector<T>();

  auto processRow = [&](vector_size_t row) {
    const auto size = inputArray->sizeAt(row);
    const auto offset = inputArray->offsetAt(row);
    if (size == 0) {
      return;
    }
    vector_size_t numNulls = 0;
    // Move nulls to end of array.
    for (vector_size_t i = size - 1; i >= 0; --i) {
      if (flatResults->isNullAt(offset + i)) {
        swapWithNull<T>(flatResults, offset + size - numNulls - 1, offset + i);
        ++numNulls;
      }
    }
    // Exclude null values while sorting.
    const auto startRow = offset;
    const auto endRow = startRow + size - numNulls;

    if constexpr (kind == TypeKind::BOOLEAN) {
      uint64_t* rawBits = flatResults->template mutableRawValues<uint64_t>();
      const auto numOneBits = bits::countBits(rawBits, startRow, endRow);
      const auto endZeroRow = endRow - numOneBits;
      bits::fillBits(rawBits, startRow, endZeroRow, bits::kNull);
      bits::fillBits(rawBits, endZeroRow, endRow, bits::kNotNull);
    } else {
      T* resultRawValues = flatResults->mutableRawValues();
      std::sort(resultRawValues + startRow, resultRawValues + endRow);
    }
  };
  rows.applyToSelected(processRow);
}

// See documentation at https://prestodb.io/docs/current/functions/array.html
template <TypeKind T>
class ArraySortFunction : public exec::VectorFunction {
 public:
  /// This class implements the array_sort query function. Takes an array as
  /// input and sorts it in ascending order and null elements will be placed at
  /// the end of the returned array.
  ///
  /// Along with the set, we maintain a `hasNull` flag that indicates whether
  /// null is present in the array.
  ///
  /// Zero element copy for complex data type:
  ///
  /// In order to prevent copies of array elements with complex data type, the
  /// function reuses the internal elements() vector from the original
  /// ArrayVector. A new vector is created containing the indices of the sorted
  /// elements in the output, and wrapped into a DictionaryVector. The 'lengths'
  /// and 'offsets' vectors that control where output arrays start and end
  /// remain the same in the output ArrayVector.

  ArraySortFunction() {}

  // Execute function.
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
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

 private:
  VectorPtr applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx* context) const {
    // Acquire the array elements vector.
    auto inputArray = arg->as<ArrayVector>();
    VectorPtr resultElements;

    if (velox::TypeTraits<T>::isPrimitiveType) {
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          applyScalarType, T, rows, inputArray, context, &resultElements);

    } else {
      applyComplexType(rows, inputArray, context, &resultElements);
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
};

// Validate number of parameters and types.
void validateType(const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_USER_CHECK_EQ(
      inputArgs.size(), 1, "array_sort requires exactly one parameter");

  auto arrayType = inputArgs.front().type;
  VELOX_USER_CHECK_EQ(
      arrayType->kind(),
      TypeKind::ARRAY,
      "array_sort requires arguments of type ARRAY");
}

// Create function template based on type.
template <TypeKind kind>
std::shared_ptr<exec::VectorFunction> createTyped(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 1);
  return std::make_shared<ArraySortFunction<kind>>();
}

// Create function.
std::shared_ptr<exec::VectorFunction> create(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  validateType(inputArgs);
  auto elementType = inputArgs.front().type->childAt(0);
  return VELOX_DYNAMIC_TYPE_DISPATCH(
      createTyped, elementType->kind(), inputArgs);
}

// Define function signature.
std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // array(T) -> array(T)
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("array(T)")
              .argumentType("array(T)")
              .build()};
}

} // namespace

// Register function.
VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(udf_array_sort, signatures(), create);

} // namespace facebook::velox::functions
