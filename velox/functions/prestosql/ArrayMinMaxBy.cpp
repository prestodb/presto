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

#include <boost/range/irange.hpp>
#include <folly/container/F14Set.h>
#include <velox/type/SimpleFunctionApi.h>
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"

namespace facebook::velox::functions {
namespace {

template <bool isMaxBy>
class ArrayMinMaxByFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);

    exec::LocalDecodedVector arrayDecoder(context, *args[0], rows);
    auto& decodedArray = *arrayDecoder.get();

    // Flatten input array.
    auto flatArray = flattenArray(rows, args[0], decodedArray);

    std::vector<VectorPtr> lambdaArgs = {flatArray->elements()};
    auto newNumElements = flatArray->elements()->size();

    SelectivityVector validRowsInReusedResult =
        toElementRows<ArrayVector>(newNumElements, rows, flatArray.get());

    VectorPtr newElements;
    auto elementToTopLevelRows = getElementToTopLevelRows(
        newNumElements, rows, flatArray.get(), context.pool());

    // Loop over lambda functions and apply these to elements of the base array.
    // In most cases there will be only one function and the loop will run once.
    auto it = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      auto elementRows = toElementRows<ArrayVector>(
          newNumElements, *entry.rows, flatArray.get());
      auto wrapCapture = toWrapCapture<ArrayVector>(
          newNumElements, entry.callable, *entry.rows, flatArray);

      entry.callable->apply(
          elementRows,
          &validRowsInReusedResult,
          wrapCapture,
          &context,
          lambdaArgs,
          elementToTopLevelRows,
          &newElements);
    }

    // New elements after the lambda function is applied
    exec::LocalDecodedVector decodedElements(
        context, *newElements, validRowsInReusedResult);

    BufferPtr resultIndices = allocateIndices(rows.size(), context.pool());
    auto* rawResultIndices = resultIndices->asMutable<vector_size_t>();

    BufferPtr resultNulls = allocateNulls(rows.size(), context.pool());
    auto* mutableNulls = resultNulls->asMutable<uint64_t>();

    // Get the max/min value out of the output of the lambda
    auto decodedIndices = decodedElements->indices();
    const auto* baseElementsVector = decodedElements->base();

    CompareFlags flags{.nullsFirst = false, .ascending = isMaxBy};
    auto compareLogic = [&](vector_size_t a, vector_size_t b) {
      if (a == b) {
        return false;
      }
      bool aNull = decodedElements->isNullAt(a);
      bool bNull = decodedElements->isNullAt(b);
      if (aNull) {
        return false;
      }
      if (bNull) {
        return true;
      }
      std::optional<int32_t> result = baseElementsVector->compare(
          baseElementsVector, decodedIndices[a], decodedIndices[b], flags);
      // If lambda returns the same value, then return the last element
      return result.value() <= 0;
    };

    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      const auto size = flatArray->sizeAt(row);
      const auto offset = flatArray->offsetAt(row);
      const auto range = boost::irange(offset, offset + size);

      int minMaxIndex =
          *std::max_element(range.begin(), range.end(), compareLogic);
      if (decodedElements->isNullAt(minMaxIndex)) {
        bits::setNull(mutableNulls, row);
      }
      rawResultIndices[row] = minMaxIndex;
    });

    auto localResult = BaseVector::wrapInDictionary(
        resultNulls, resultIndices, rows.end(), flatArray->elements());
    context.moveOrCopyResult(localResult, rows, result);
  }
};
class ArrayMaxByFunction : public ArrayMinMaxByFunction<true> {};
class ArrayMinByFunction : public ArrayMinMaxByFunction<false> {};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> signature() {
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .typeVariable("U")
              .returnType("T")
              .argumentType("array(T)")
              .argumentType("function(T, U)")
              .build()};
}

// Register function.
VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_max_by,
    signature(),
    std::make_unique<ArrayMaxByFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_min_by,
    signature(),
    std::make_unique<ArrayMinByFunction>());
} // namespace facebook::velox::functions
