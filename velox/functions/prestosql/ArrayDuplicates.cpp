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

#include <folly/container/F14Map.h>

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/ComparatorUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"

namespace facebook::velox::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/array.html
///
/// Implements the array_duplicates function.
///
/// Along with the hash map, we maintain a `hasNull` flag that indicates
/// whether null is present in the array.
///
/// Zero element copy:
///
/// In order to prevent copies of array elements, the function reuses the
/// internal elements() vector from the original ArrayVector.
///
/// First a new vector is created containing the indices of the elements
/// which will be present in the output, and wrapped into a DictionaryVector.
/// Next the `sizes` and `offsets` vectors that control where output arrays
/// start and end are wrapped into the output ArrayVector.
template <typename T>
class ArrayDuplicatesFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
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

    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  VectorPtr applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx& context) const {
    auto arrayVector = arg->as<ArrayVector>();
    VELOX_CHECK(arrayVector);
    auto elementsVector = arrayVector->elements();

    auto elementsRows =
        toElementRows(elementsVector->size(), rows, arrayVector);
    exec::LocalDecodedVector elements(context, *elementsVector, elementsRows);

    vector_size_t numElements = elementsRows.size();
    vector_size_t numRows = arrayVector->size();

    // Allocate new vectors for indices, length and offsets.
    memory::MemoryPool* pool = context.pool();
    BufferPtr newIndices = allocateIndices(numElements, pool);
    BufferPtr newSizes = allocateSizes(numRows, pool);
    BufferPtr newOffsets = allocateOffsets(numRows, pool);

    // Pointers and cursor to the raw data.
    vector_size_t indexCursor = 0;
    auto* rawIndices = newIndices->asMutable<vector_size_t>();
    auto* rawSizes = newSizes->asMutable<vector_size_t>();
    auto* rawOffsets = newOffsets->asMutable<vector_size_t>();

    // Process the rows: use a hashmap to store unique values and
    // whether it occurred once or more than once.
    folly::F14FastMap<T, bool> uniqueMap;

    rows.applyToSelected([&](vector_size_t row) {
      auto size = arrayVector->sizeAt(row);
      auto offset = arrayVector->offsetAt(row);

      rawOffsets[row] = indexCursor;
      vector_size_t numNulls = 0;

      for (vector_size_t i = offset; i < offset + size; ++i) {
        if (elements->isNullAt(i)) {
          numNulls++;
          if (numNulls == 2) {
            rawIndices[indexCursor] = i;
            indexCursor++;
          }
        } else {
          T value = elements->valueAt<T>(i);
          auto it = uniqueMap.find(value);
          if (it == uniqueMap.end()) {
            uniqueMap[value] = true;
          } else if (it->second) {
            it->second = false;
            rawIndices[indexCursor] = i;
            indexCursor++;
          }
        }
      }

      uniqueMap.clear();
      rawSizes[row] = indexCursor - rawOffsets[row];

      std::sort(
          rawIndices + rawOffsets[row],
          rawIndices + indexCursor,
          lib::Index2ValueNullableLess<T>(elements));
    });

    newIndices->setSize(indexCursor * sizeof(vector_size_t));
    auto newElements =
        BaseVector::transpose(newIndices, std::move(elementsVector));

    return std::make_shared<ArrayVector>(
        pool,
        arrayVector->type(),
        nullptr,
        numRows,
        std::move(newOffsets),
        std::move(newSizes),
        std::move(newElements));
  }
};

// Validate number of parameters and types.
void validateType(const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_USER_CHECK_EQ(
      inputArgs.size(), 1, "array_duplicates requires exactly one parameter");

  auto arrayType = inputArgs.front().type;
  VELOX_USER_CHECK_EQ(
      arrayType->kind(),
      TypeKind::ARRAY,
      "array_duplicates requires arguments of type ARRAY");
}

// Create function template based on type.
template <TypeKind kind>
std::shared_ptr<exec::VectorFunction> createTyped(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 1);

  using T = typename TypeTraits<kind>::NativeType;
  return std::make_shared<ArrayDuplicatesFunction<T>>();
}

// Create function.
std::shared_ptr<exec::VectorFunction> create(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  validateType(inputArgs);
  auto elementType = inputArgs.front().type->childAt(0);

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      createTyped, elementType->kind(), inputArgs);
}

// Define function signature.
// array(T) -> array(T) where T must be bigint or varchar.
std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("array(bigint)")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("array(varchar)")
          .build()};
}

} // namespace

// Register function.
VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_duplicates,
    signatures(),
    create);

} // namespace facebook::velox::functions
