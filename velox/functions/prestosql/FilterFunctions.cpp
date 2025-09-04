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
#include "velox/vector/FlatMapVector.h"
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
      const VectorPtr& lambda,
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
    auto iter = lambda->asUnchecked<FunctionVector>()->iterator(&rows);
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

    // Filter can pass along very large elements vectors that can hold onto
    // memory and copy operations on them can further put memory pressure. We
    // try to flatten them if the dictionary layer is much smaller than the
    // elements vector.
    auto wrappedElements = numSelected ? BaseVector::wrapInDictionary(
                                             BufferPtr(nullptr),
                                             std::move(selectedIndices),
                                             numSelected,
                                             std::move(elements),
                                             true /*flattenIfRedundant*/)
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
 private:
  // Builds a SelectivityVector based upon FlatMapVector's inMap buffer. This
  // buffer indicates whether or not a particular key is present in that map's
  // row. When applying lambda functions we should avoid executing on key-value
  // pairs that are not present in that particular row. SelectivityVector
  // rowsToFilterOn is used to filter out those rows.
  void buildInMapSelectivityVector(
      SelectivityVector& rowsToFilterOn,
      BufferPtr flattenedInMap,
      BufferPtr inMap,
      vector_size_t inMapSize,
      const SelectivityVector& rows,
      const vector_size_t* decodedIndices) const {
    // Flatten inMap buffer.
    auto* mutableFlattedInMap = flattenedInMap->asMutable<uint64_t>();
    bits::fillBits(mutableFlattedInMap, 0, inMapSize, false);
    auto* mutableInMap = inMap ? inMap->asMutable<uint64_t>() : nullptr;
    rows.applyToSelected([&](vector_size_t row) {
      // If inMap is null, short circuit and set bit because key is present in
      // all rows.
      if (!inMap || bits::isBitSet(mutableInMap, decodedIndices[row])) {
        bits::setBit(mutableFlattedInMap, decodedIndices[row]);
      }
    });

    // Extract flattened inMap buffer values for next lambda call.
    rowsToFilterOn.clearAll();
    auto bits = rowsToFilterOn.asMutableRange().bits();
    bits::orBits(bits, mutableFlattedInMap, 0, rowsToFilterOn.size());
    rowsToFilterOn.updateBounds();
  }

  // Apply filter function to vector of encoding FlatMapVector. Because the
  // entirety of the map values are stored in in a list of vectors (one vector
  // per key), we will need to apply the filter function on each vector and
  // associated inMap buffer. Additionally, we will have to reduce the number of
  // distinct keys stored in the FlatMapVector if they key list changes.
  void applyFlatMapVector(
      DecodedVector& decodedMap,
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    // Current map and fields
    const FlatMapVector& flatMap =
        *(decodedMap.base())->template as<FlatMapVector>();
    auto distinctKeys = flatMap.distinctKeys();
    auto mapValues = flatMap.mapValues();
    auto numRows = rows.size();
    BufferPtr decodedIndices =
        AlignedBuffer::allocate<vector_size_t>(numRows, flatMap.pool());
    BufferPtr nulls = allocateNulls(numRows, flatMap.pool());
    auto mutableIndices = decodedIndices->asMutable<vector_size_t>();
    auto rawNulls = nulls->asMutable<uint64_t>();
    for (int i = 0; i < decodedMap.size(); i++) {
      mutableIndices[i] = decodedMap.indices()[i];
      if (decodedMap.isNullAt(i)) {
        bits::setNull(rawNulls, i, true);
      }
    }

    // Result map fields
    auto filteredKeysIndices = AlignedBuffer::allocate<vector_size_t>(
        distinctKeys->size(), context.pool());
    std::vector<VectorPtr> filteredMapValues;
    std::vector<BufferPtr> filteredInMaps;
    uint64_t* filteredInMap;
    auto numDistinct = 0;
    auto rawIndices = filteredKeysIndices->asMutable<vector_size_t>();

    // Lambda function
    auto iter = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);
    exec::LocalDecodedVector bitsDecoder(context);
    SelectivityVector rowsToFilterOn(flatMap.size());
    // Selectivity vector to help ignore filtering for key-value pairs
    // identified by inMap buffer. Let's allocate here to avoid during each
    // iteration.
    auto flattenedInMap =
        AlignedBuffer::allocate<bool>(flatMap.size(), context.pool(), 0);

    // Apply lambda function to each map value vector and its associated key
    // from our flat map vector. If the key is not filtered out, we will copy it
    // to our result vector.
    while (auto entry = iter.next()) {
      for (int channel = 0; channel < mapValues.size(); ++channel) {
        // Only apply lambda function to values that are in the map.
        buildInMapSelectivityVector(
            rowsToFilterOn,
            flattenedInMap,
            flatMap.inMaps()[channel],
            flatMap.size(),
            *entry.rows,
            decodedMap.indices());

        // Call lambda function and decode its output bit vector. We will
        // use it to determine what will persist to the final result vector.
        VectorPtr lambdaResultBits;
        entry.callable->apply(
            rowsToFilterOn,
            nullptr,
            nullptr,
            &context,
            {
                BaseVector::wrapInConstant(
                    flatMap.size(), channel, distinctKeys),
                mapValues[channel],
            },
            decodedIndices,
            &lambdaResultBits);
        bitsDecoder.get()->decode(*lambdaResultBits);

        bool isFilteredIn = false;
        entry.rows->applyToSelected([&](vector_size_t row) {
          row = decodedMap.indices()[row];
          if (rowsToFilterOn.isValid(row) &&
              !bitsDecoder.get()->isNullAt(row) &&
              bitsDecoder.get()->valueAt<bool>(row)) {
            // First time seeing this key; let's copy over its associated values
            // vector and define a new filtered inMap buffer. Let's also note
            // the index of this key for key filtering.
            if (!isFilteredIn) {
              filteredMapValues.push_back(
                  BaseVector::copy(*mapValues[channel]));
              filteredInMaps.push_back(
                  AlignedBuffer::allocate<bool>(numRows, context.pool(), 0));
              filteredInMap = filteredInMaps.back()->asMutable<uint64_t>();
              rawIndices[numDistinct++] = channel;
              isFilteredIn = true;
            }
            bits::setBit(filteredInMap, row);
          }
        });
      }
    }

    // Resize filtered distinct keys indices in order to wrap in dictionary and
    // create our result filtered flat map vector
    filteredKeysIndices->setSize(numDistinct * sizeof(vector_size_t));
    auto localResult = std::make_shared<FlatMapVector>(
        context.pool(),
        outputType,
        flatMap.nulls(),
        flatMap.size(),
        BaseVector::wrapInDictionary(
            BufferPtr(nullptr), filteredKeysIndices, numDistinct, distinctKeys),
        std::move(filteredMapValues),
        std::move(filteredInMaps));

    // Handle wrapped encoding if necessary
    if (decodedMap.isIdentityMapping()) {
      context.moveOrCopyResult(localResult, rows, result);
    } else {
      context.moveOrCopyResult(
          BaseVector::wrapInDictionary(
              std::move(nulls), decodedIndices, decodedMap.size(), localResult),
          rows,
          result);
    }
  }

  // Applies filter function on traditional map vector.
  void applyMapVector(
      DecodedVector& decodedMap,
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    auto mapVector = flattenMap(rows, args[0], decodedMap);
    VectorPtr keys = mapVector->mapKeys();
    VectorPtr values = mapVector->mapValues();
    BufferPtr resultSizes;
    BufferPtr resultOffsets;
    BufferPtr selectedIndices;
    auto numSelected = doApply(
        rows,
        mapVector,
        args[1],
        {keys, values},
        context,
        resultOffsets,
        resultSizes,
        selectedIndices);

    // Filter can pass along very large elements vectors that can hold onto
    // memory and copy operations on them can further put memory pressure. We
    // try to flatten them if the dictionary layer is much smaller than the
    // elements vector.
    auto wrappedKeys = numSelected ? BaseVector::wrapInDictionary(
                                         BufferPtr(nullptr),
                                         selectedIndices,
                                         numSelected,
                                         std::move(keys),
                                         true /*flattenIfRedundant*/)
                                   : nullptr;
    auto wrappedValues = numSelected ? BaseVector::wrapInDictionary(
                                           BufferPtr(nullptr),
                                           selectedIndices,
                                           numSelected,
                                           std::move(values),
                                           true /*flattenIfRedundant*/)
                                     : nullptr;
    // Set nulls for rows not present in 'rows'.
    BufferPtr newNulls = addNullsForUnselectedRows(mapVector, rows);
    auto localResult = std::make_shared<MapVector>(
        mapVector->pool(),
        outputType,
        std::move(newNulls),
        rows.end(),
        std::move(resultOffsets),
        std::move(resultSizes),
        wrappedKeys,
        wrappedValues);
    context.moveOrCopyResult(localResult, rows, result);
  }

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

    // Flattening input maps will peel if possible, but may simply cast if the
    // vector is an identify mapping.
    switch (decodedMap.base()->encoding()) {
      case VectorEncoding::Simple::FLAT_MAP: {
        applyFlatMapVector(decodedMap, rows, args, outputType, context, result);
        break;
      }
      case VectorEncoding::Simple::MAP: {
        applyMapVector(decodedMap, rows, args, outputType, context, result);
        break;
      }
      default:
        VELOX_UNSUPPORTED(
            "map_filter not supported for encoding: {}",
            decodedMap.base()->encoding());
    }
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
