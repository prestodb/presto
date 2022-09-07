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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

class FilterFunctionBase : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // array_filter and map_filter are null preserving for the array and the
    // map. But since an expr tree with a lambda depends on all named fields,
    // including captures, a null in a capture does not automatically make a
    // null result.
    return false;
  }

 protected:
  static void appendToIndicesBuffer(
      BufferPtr* buffer,
      memory::MemoryPool* pool,
      vector_size_t data) {
    if (!*buffer) {
      *buffer = AlignedBuffer::allocate<decltype(data)>(100, pool);
      (*buffer)->setSize(0);
    }
    AlignedBuffer::appendTo(buffer, &data, 1);
  }

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
    auto inputOffsets = input->rawOffsets();
    auto inputSizes = input->rawSizes();

    auto* pool = context.pool();
    resultSizes = allocateSizes(rows.size(), pool);
    resultOffsets = allocateOffsets(rows.size(), pool);
    auto rawResultSizes = resultSizes->asMutable<vector_size_t>();
    auto rawResultOffsets = resultOffsets->asMutable<vector_size_t>();
    auto numElements = lambdaArgs[0]->size();

    SelectivityVector finalSelection;
    if (!context.isFinalSelection()) {
      finalSelection =
          toElementRows<T>(numElements, *context.finalSelection(), input.get());
    }

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
          finalSelection,
          wrapCapture,
          &context,
          lambdaArgs,
          &bits);
      bitsDecoder.get()->decode(*bits, elementRows);
      entry.rows->applyToSelected([&](vector_size_t row) {
        if (input->isNullAt(row)) {
          return;
        }
        auto size = inputSizes[row];
        auto offset = inputOffsets[row];
        rawResultOffsets[row] = selectedIndices
            ? selectedIndices->size() / sizeof(vector_size_t)
            : 0;
        for (auto i = 0; i < size; ++i) {
          if (!bitsDecoder.get()->isNullAt(offset + i) &&
              bitsDecoder.get()->valueAt<bool>(offset + i)) {
            ++rawResultSizes[row];
            appendToIndicesBuffer(&selectedIndices, pool, offset + i);
          }
        }
      });
    }

    return selectedIndices ? selectedIndices->size() / sizeof(vector_size_t)
                           : 0;
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
    auto localResult = std::make_shared<ArrayVector>(
        flatArray->pool(),
        flatArray->type(),
        flatArray->nulls(),
        rows.size(),
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
    auto localResult = std::make_shared<MapVector>(
        flatMap->pool(),
        outputType,
        flatMap->nulls(),
        rows.size(),
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

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_filter,
    ArrayFilterFunction::signatures(),
    std::make_unique<ArrayFilterFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map_filter,
    MapFilterFunction::signatures(),
    std::make_unique<MapFilterFunction>());

} // namespace facebook::velox::functions
