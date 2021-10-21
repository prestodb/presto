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

// See documentation at https://prestodb.io/docs/current/functions/array.html
template <typename T>
class ArrayDistinctFunction : public exec::VectorFunction {
 public:
  /// This class implements the array_distinct query function.
  ///
  /// Along with the set, we maintain a `hasNull` flag that indicates whether
  /// null is present in the array.
  ///
  /// Zero element copy:
  ///
  /// In order to prevent copies of array elements, the function reuses the
  /// internal elements() vector from the original ArrayVector.
  ///
  /// First a new vector is created containing the indices of the elements
  /// which will be present in the output, and wrapped into a DictionaryVector.
  /// Next the `lengths` and `offsets` vectors that control where output arrays
  /// start and end are wrapped into the output ArrayVector.

  ArrayDistinctFunction() {}

  // Execute function.
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    // Acquire the array elements vector.
    auto arrayVector = args.front()->as<ArrayVector>();
    auto elementsVector = arrayVector->elements();
    auto elementsRows =
        toElementRows(elementsVector->size(), rows, arrayVector);
    exec::LocalDecodedVector elements(context, *elementsVector, elementsRows);

    vector_size_t elementsCount = elementsRows.size();
    vector_size_t rowCount = arrayVector->size();

    // Allocate new vectors for indices, length and offsets.
    memory::MemoryPool* pool = context->pool();
    BufferPtr newIndices = allocateIndices(elementsCount, pool);
    BufferPtr newLengths = allocateSizes(rowCount, pool);
    BufferPtr newOffsets = allocateOffsets(rowCount, pool);

    // Pointers and cursors to the raw data.
    vector_size_t indicesCursor = 0;
    auto* rawNewIndices = newIndices->asMutable<vector_size_t>();
    auto* rawSizes = newLengths->asMutable<vector_size_t>();
    auto* rawOffsets = newOffsets->asMutable<vector_size_t>();

    // Process the rows: store unique values in the hash table.
    folly::F14FastSet<T> uniqueSet;

    rows.applyToSelected([&](vector_size_t row) {
      auto size = arrayVector->sizeAt(row);
      auto offset = arrayVector->offsetAt(row);

      rawOffsets[row] = indicesCursor;
      bool hasNulls = false;
      for (vector_size_t i = offset; i < offset + size; ++i) {
        if (elements->isNullAt(i)) {
          if (!hasNulls) {
            hasNulls = true;
            rawNewIndices[indicesCursor++] = i;
          }
        } else {
          auto value = elements->valueAt<T>(i);

          if (uniqueSet.insert(value).second) {
            rawNewIndices[indicesCursor++] = i;
          }
        }
      }

      uniqueSet.clear();
      rawSizes[row] = indicesCursor - rawOffsets[row];
    });

    newIndices->setSize(indicesCursor * sizeof(vector_size_t));
    auto newElements =
        BaseVector::transpose(newIndices, std::move(elementsVector));

    // Prepare and return result set.
    auto resultArray = std::make_shared<ArrayVector>(
        pool,
        outputType,
        nullptr,
        rowCount,
        std::move(newOffsets),
        std::move(newLengths),
        std::move(newElements),
        0);
    context->moveOrCopyResult(resultArray, rows, result);
  }
};

// Validate number of parameters and types.
void validateType(const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_USER_CHECK_EQ(
      inputArgs.size(), 1, "array_distinct requires exactly one parameter");

  auto arrayType = inputArgs.front().type;
  VELOX_USER_CHECK_EQ(
      arrayType->kind(),
      TypeKind::ARRAY,
      "array_distinct requires arguments of type ARRAY");
}

// Create function template based on type.
template <TypeKind kind>
std::shared_ptr<exec::VectorFunction> createTyped(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 1);

  using T = typename TypeTraits<kind>::NativeType;
  return std::make_shared<ArrayDistinctFunction<T>>();
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
VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_distinct,
    signatures(),
    create);

} // namespace facebook::velox::functions
