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
#include "velox/expression/VarSetter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

/// Populates indices of the n-th elements of the arrays.
/// Selects 'row' in 'arrayRows' if corresponding array has an n-th element.
/// Sets elementIndices[row] to the index of the n-th element in the 'elements'
/// vector.
/// Returns true if at least one array has n-th element.
bool toNthElementRows(
    const ArrayVectorPtr& arrayVector,
    const SelectivityVector& rows,
    vector_size_t n,
    SelectivityVector& arrayRows,
    BufferPtr& elementIndices) {
  auto* rawSizes = arrayVector->rawSizes();
  auto* rawOffsets = arrayVector->rawOffsets();
  auto* rawNulls = arrayVector->rawNulls();

  auto* rawElementIndices = elementIndices->asMutable<vector_size_t>();

  arrayRows.clearAll();
  memset(rawElementIndices, 0, elementIndices->size());

  rows.applyToSelected([&](auto row) {
    if (!rawNulls || !bits::isBitNull(rawNulls, row)) {
      if (n < rawSizes[row]) {
        arrayRows.setValid(row, true);
        rawElementIndices[row] = rawOffsets[row] + n;
      }
    }
  });
  arrayRows.updateBounds();

  return arrayRows.hasSelections();
}

/// See documentation at
/// https://prestodb.io/docs/current/functions/array.html#reduce
class ReduceFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // reduce is null preserving for the array. But since an
    // expr tree with a lambda depends on all named fields, including
    // captures, a null in a capture does not automatically make a
    // null result.
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 4);

    // Flatten input array.
    exec::LocalDecodedVector arrayDecoder(context, *args[0], rows);
    auto& decodedArray = *arrayDecoder.get();

    auto flatArray = flattenArray(rows, args[0], decodedArray);

    // Loop over lambda functions and apply these to elements of the base array.
    // In most cases there will be only one function and the loop will run once.
    auto inputFuncIt = args[2]->asUnchecked<FunctionVector>()->iterator(&rows);

    SelectivityVector arrayRows(flatArray->size(), false);
    BufferPtr elementIndices =
        allocateIndices(flatArray->size(), context->pool());

    const auto& initialState = args[1];
    auto partialResult =
        BaseVector::create(initialState->type(), rows.end(), context->pool());

    // Process null and empty arrays.
    auto* rawNulls = flatArray->rawNulls();
    auto* rawSizes = flatArray->rawSizes();
    rows.applyToSelected([&](auto row) {
      if (rawNulls && bits::isBitNull(rawNulls, row)) {
        partialResult->setNull(row, true);
      } else if (rawSizes[row] == 0) {
        partialResult->copy(initialState.get(), row, row, 1);
      }
    });

    // Fix finalSelection at "rows" unless already fixed.
    VarSetter finalSelection(
        context->mutableFinalSelection(), &rows, context->isFinalSelection());
    VarSetter isFinalSelection(context->mutableIsFinalSelection(), false);

    // Iteratively apply input function to array elements.
    // First, apply input function to first elements of all arrays.
    // Then, apply input function to second elements of all arrays.
    // And so on until all elements of all arrays have been processed.
    // At each step the number of arrays being processed will get smaller as
    // some arrays will run out of elements.
    while (auto entry = inputFuncIt.next()) {
      VectorPtr state = initialState;

      int n = 0;
      while (true) {
        if (!toNthElementRows(
                flatArray, *entry.rows, n, arrayRows, elementIndices)) {
          break; // Ran out of elements in all arrays.
        }

        auto nthElement = BaseVector::wrapInDictionary(
            BufferPtr(nullptr),
            elementIndices,
            flatArray->size(),
            flatArray->elements());

        std::vector<VectorPtr> lambdaArgs = {state, nthElement};
        entry.callable->apply(
            arrayRows, rows, nullptr, context, lambdaArgs, &partialResult);
        state = partialResult;
        n++;
      }
    }

    // Apply output function.
    auto outputFuncIt = args[3]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = outputFuncIt.next()) {
      std::vector<VectorPtr> lambdaArgs = {partialResult};
      entry.callable->apply(
          *entry.rows, rows, nullptr, context, lambdaArgs, result);
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T), S, function(S, T, S), function(S, R) -> R
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .typeVariable("S")
                .typeVariable("R")
                .returnType("R")
                .argumentType("array(T)")
                .argumentType("S")
                .argumentType("function(S,T,S)")
                .argumentType("function(S,R)")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_reduce,
    ReduceFunction::signatures(),
    std::make_unique<ReduceFunction>());

} // namespace facebook::velox::functions
