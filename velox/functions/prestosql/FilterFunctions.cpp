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
#include "velox/expression/Expr.h"
#include "velox/expression/LambdaExpr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

class FilterFunctionBase : public exec::VectorFunction {
 protected:
  // Applies filter functions to elements of maps or arrays and returns the
  // number of elements that passed the filters. Stores the number of elements
  // in each array or map that passed the filter in resultSizes. Stores the
  // indices of passing elements in selectedIndices. resultOffsets is populated
  // assuming sequential layout of the passing elements.
  template <typename T>
  static vector_size_t doApply(
      const SelectivityVector& rows,
      const std::shared_ptr<T>& input,
      const VectorPtr& lambdas,
      const std::vector<VectorPtr>& lambdaArgs,
      exec::EvalCtx& context,
      BufferPtr& resultOffsets,
      BufferPtr& resultSizes,
      BufferPtr& selectedIndices) {
    const auto* inputOffsets = input->rawOffsets();
    const auto* inputSizes = input->rawSizes();

    auto* pool = context.pool();
    resultSizes = allocateSizes(rows.end(), pool);
    resultOffsets = allocateOffsets(rows.end(), pool);
    auto* rawResultSizes = resultSizes->asMutable<vector_size_t>();
    auto* rawResultOffsets = resultOffsets->asMutable<vector_size_t>();

    const auto numElements = lambdaArgs[0]->size();

    selectedIndices = allocateIndices(numElements, pool);
    auto* rawSelectedIndices = selectedIndices->asMutable<vector_size_t>();

    vector_size_t numSelected = 0;

    auto elementToTopLevelRows =
        getElementToTopLevelRows(numElements, rows, input.get(), pool);

    exec::LocalDecodedVector bitsDecoder(context);
    auto iter = lambdas->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = iter.next()) {
      auto elementRows =
          toElementRows<T>(numElements, *entry.rows, input.get());
      auto wrapCapture =
          toWrapCapture<T>(numElements, entry.callable, *entry.rows, input);

      VectorPtr bits;
      entry.callable->apply(
          elementRows,
          nullptr,
          wrapCapture,
          &context,
          lambdaArgs,
          elementToTopLevelRows,
          &bits);
      bitsDecoder.get()->decode(*bits, elementRows);
      entry.rows->applyToSelected([&](vector_size_t row) {
        if (input->isNullAt(row)) {
          return;
        }
        auto size = inputSizes[row];
        auto offset = inputOffsets[row];
        rawResultOffsets[row] = numSelected;
        for (auto i = 0; i < size; ++i) {
          if (!bitsDecoder.get()->isNullAt(offset + i) &&
              bitsDecoder.get()->valueAt<bool>(offset + i)) {
            ++rawResultSizes[row];
            rawSelectedIndices[numSelected] = offset + i;
            ++numSelected;
          }
        }
      });
    }

    selectedIndices->setSize(numSelected * sizeof(vector_size_t));

    return numSelected;
  }
};

// See documentation at
//    - https://prestodb.io/docs/current/functions/array.html
//    - https://prestodb.io/docs/current/functions/lambda.html
//    - https://prestodb.io/blog/2020/03/02/presto-lambda
class ArrayFilterFunction : public FilterFunctionBase {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);
    exec::LocalDecodedVector arrayDecoder(context, *args[0], rows);
    auto& decodedArray = *arrayDecoder.get();

    auto flatArray = flattenArray(rows, args[0], decodedArray);

    VectorPtr elements = flatArray->elements();
    BufferPtr resultSizes;
    BufferPtr resultOffsets;
    BufferPtr selectedIndices;
    auto numSelected = doApply(
        rows,
        flatArray,
        args[1],
        {elements},
        context,
        resultOffsets,
        resultSizes,
        selectedIndices);

    auto wrappedElements = numSelected ? BaseVector::wrapInDictionary(
                                             BufferPtr(nullptr),
                                             std::move(selectedIndices),
                                             numSelected,
                                             std::move(elements))
                                       : nullptr;
    // Set nulls for rows not present in 'rows'.
    BufferPtr newNulls = addNullsForUnselectedRows(flatArray, rows);
    auto localResult = std::make_shared<ArrayVector>(
        flatArray->pool(),
        flatArray->type(),
        std::move(newNulls),
        rows.end(),
        std::move(resultOffsets),
        std::move(resultSizes),
        wrappedElements);
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T), function(T, boolean) -> array(T)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("array(T)")
                .argumentType("array(T)")
                .argumentType("function(T,boolean)")
                .build()};
  }
};

// See documentation at
//    - https://prestodb.io/docs/current/functions/map.html
//    - https://prestodb.io/docs/current/functions/lambda.html
//    - https://prestodb.io/blog/2020/03/02/presto-lambda
class MapFilterFunction : public FilterFunctionBase {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);
    exec::LocalDecodedVector mapDecoder(context, *args[0], rows);
    auto& decodedMap = *mapDecoder.get();

    auto flatMap = flattenMap(rows, args[0], decodedMap);

    VectorPtr keys = flatMap->mapKeys();
    VectorPtr values = flatMap->mapValues();
    BufferPtr resultSizes;
    BufferPtr resultOffsets;
    BufferPtr selectedIndices;
    auto numSelected = doApply(
        rows,
        flatMap,
        args[1],
        {keys, values},
        context,
        resultOffsets,
        resultSizes,
        selectedIndices);

    auto wrappedKeys = numSelected
        ? BaseVector::wrapInDictionary(
              BufferPtr(nullptr), selectedIndices, numSelected, std::move(keys))
        : nullptr;
    auto wrappedValues = numSelected ? BaseVector::wrapInDictionary(
                                           BufferPtr(nullptr),
                                           selectedIndices,
                                           numSelected,
                                           std::move(values))
                                     : nullptr;
    // Set nulls for rows not present in 'rows'.
    BufferPtr newNulls = addNullsForUnselectedRows(flatMap, rows);
    auto localResult = std::make_shared<MapVector>(
        flatMap->pool(),
        outputType,
        std::move(newNulls),
        rows.end(),
        std::move(resultOffsets),
        std::move(resultSizes),
        wrappedKeys,
        wrappedValues);
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K,V), function(K,V,boolean) -> map(K,V)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("map(K,V)")
                .argumentType("map(K,V)")
                .argumentType("function(K,V,boolean)")
                .build()};
  }
};
} // namespace

/// array_filter and map_filter are null preserving for the array and the
/// map. But since an expr tree with a lambda depends on all named fields,
/// including captures, a null in a capture does not automatically make a
/// null result.

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_array_filter,
    ArrayFilterFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<ArrayFilterFunction>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_map_filter,
    MapFilterFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<MapFilterFunction>());

} // namespace facebook::velox::functions
