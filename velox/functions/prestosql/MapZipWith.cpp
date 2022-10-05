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

// See documentation at
// https://prestodb.io/docs/current/functions/map.html#map_zip_with
class MapZipWithFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // map_zip_with is null preserving for the map, but since an
    // expr tree with a lambda depends on all named fields, including
    // captures, a null in a capture does not automatically make a
    // null result.
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 3);
    exec::DecodedArgs decodedArgs(rows, {args[0], args[1]}, context);
    auto decodedLeft = decodedArgs.at(0);
    auto decodedRight = decodedArgs.at(1);
    auto* baseLeft = decodedLeft->base()->asUnchecked<MapVector>();
    auto* baseRight = decodedRight->base()->asUnchecked<MapVector>();

    // Merge two maps. Allocate nulls, offsets and sizes buffers for the merged
    // maps.
    BufferPtr newNulls = allocateNulls(rows.end(), context.pool());
    auto* rawNewNulls = newNulls->asMutable<uint64_t>();
    BufferPtr newOffsets = allocateIndices(rows.end(), context.pool());
    auto* rawNewOffsets = newOffsets->asMutable<vector_size_t>();
    BufferPtr newSizes = allocateIndices(rows.end(), context.pool());
    auto* rawNewSizes = newSizes->asMutable<vector_size_t>();

    auto numLeftElements = countElements<MapVector>(rows, *decodedLeft);
    auto numRightElements = countElements<MapVector>(rows, *decodedRight);

    // The total number of elements in the merged maps will not exceed the total
    // sum of elements in the input maps.
    auto maxElements = numLeftElements + numRightElements;

    BufferPtr leftIndices = allocateIndices(maxElements, context.pool());
    auto* rawLeftIndices = leftIndices->asMutable<vector_size_t>();
    BufferPtr leftNulls = allocateNulls(maxElements, context.pool());
    auto* rawLeftNulls = leftNulls->asMutable<uint64_t>();

    BufferPtr rightIndices = allocateIndices(maxElements, context.pool());
    auto* rawRightIndices = rightIndices->asMutable<vector_size_t>();
    BufferPtr rightNulls = allocateNulls(maxElements, context.pool());
    auto* rawRightNulls = rightNulls->asMutable<uint64_t>();

    auto leftKeys = baseLeft->mapKeys();
    auto rightKeys = baseRight->mapKeys();

    vector_size_t index = 0;
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedLeft->isNullAt(row) || decodedRight->isNullAt(row)) {
        bits::setNull(rawNewNulls, row);
        return;
      }

      rawNewOffsets[row] = index;

      auto leftRow = decodedLeft->index(row);
      auto rightRow = decodedRight->index(row);

      auto leftSorted = baseLeft->sortedKeyIndices(leftRow);
      auto rightSorted = baseRight->sortedKeyIndices(rightRow);

      mergeKeys(
          leftKeys,
          rightKeys,
          leftSorted,
          rightSorted,
          rawLeftNulls,
          rawRightNulls,
          rawLeftIndices,
          rawRightIndices,
          index);

      rawNewSizes[row] = index - rawNewOffsets[row];
    });

    auto mergedLeftValues = BaseVector::wrapInDictionary(
        leftNulls, leftIndices, index, baseLeft->mapValues());
    auto mergedRightValues = BaseVector::wrapInDictionary(
        rightNulls, rightIndices, index, baseRight->mapValues());

    // Merge keys.
    auto mergedKeys =
        BaseVector::create(leftKeys->type(), index, context.pool());
    for (auto i = 0; i < index; ++i) {
      if (bits::isBitNull(rawLeftNulls, i)) {
        // Copy right key.
        mergedKeys->copy(rightKeys.get(), i, rawRightIndices[i], 1);
      } else {
        // Copy left key.
        mergedKeys->copy(leftKeys.get(), i, rawLeftIndices[i], 1);
      }
    }

    std::vector<VectorPtr> lambdaArgs = {
        mergedKeys, mergedLeftValues, mergedRightValues};

    const SelectivityVector allElementRows(index);

    VectorPtr mergedValues;

    // Loop over lambda functions and apply these to (mergedKeys,
    // mergedLeftValues, mergedRightValues). In most cases there will be only
    // one function and the loop will run once.
    auto it = args[2]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      SelectivityVector elementRows(index, false);
      entry.rows->applyToSelected([&](auto row) {
        elementRows.setValidRange(
            rawNewOffsets[row], rawNewOffsets[row] + rawNewSizes[row], true);
      });
      elementRows.updateBounds();

      BufferPtr wrapCapture;
      if (entry.callable->hasCapture()) {
        wrapCapture =
            makeWrapCapture(*entry.rows, index, rawNewSizes, context.pool());
      }

      // Make sure already populated entries in newElements do not get
      // overwritten.
      exec::ScopedFinalSelectionSetter(context, &allElementRows, true, true);

      entry.callable->apply(
          elementRows,
          allElementRows,
          wrapCapture,
          &context,
          lambdaArgs,
          &mergedValues);
    }

    auto localResult = std::make_shared<MapVector>(
        context.pool(),
        outputType,
        newNulls,
        rows.end(),
        newOffsets,
        newSizes,
        mergedKeys,
        mergedValues);
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K, V1), map(K, V2), function(K, V1, V2, V3) -> map(K, V3)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V1")
                .typeVariable("V2")
                .typeVariable("V3")
                .returnType("map(K,V3)")
                .argumentType("map(K,V1)")
                .argumentType("map(K,V2)")
                .argumentType("function(K,V1,V2,V3)")
                .build()};
  }

 private:
  static void mergeKeys(
      const VectorPtr& leftKeys,
      const VectorPtr& rightKeys,
      const std::vector<vector_size_t>& leftSorted,
      const std::vector<vector_size_t>& rightSorted,
      uint64_t* rawLeftNulls,
      uint64_t* rawRightNulls,
      vector_size_t* rawLeftIndices,
      vector_size_t* rawRightIndices,
      vector_size_t& index) {
    const auto numLeft = leftSorted.size();
    const auto numRight = rightSorted.size();

    vector_size_t leftIndex = 0;
    vector_size_t rightIndex = 0;
    while (leftIndex < numLeft && rightIndex < numRight) {
      auto compare = leftKeys->compare(
          rightKeys.get(), leftSorted[leftIndex], rightSorted[rightIndex]);
      if (compare == 0) {
        // Left key == right key.
        rawLeftIndices[index] = leftSorted[leftIndex];
        rawRightIndices[index] = rightSorted[rightIndex];
        ++leftIndex;
        ++rightIndex;
      } else if (compare < 0) {
        // Left key < right key.
        rawLeftIndices[index] = leftSorted[leftIndex];
        bits::setNull(rawRightNulls, index);
        ++leftIndex;
      } else {
        // Left key > right key.
        bits::setNull(rawLeftNulls, index);
        rawRightIndices[index] = rightSorted[rightIndex];
        ++rightIndex;
      }
      ++index;
    }

    for (; leftIndex < numLeft; ++leftIndex) {
      rawLeftIndices[index] = leftSorted[leftIndex];
      bits::setNull(rawRightNulls, index);
      ++index;
    }

    for (; rightIndex < numRight; ++rightIndex) {
      bits::setNull(rawLeftNulls, index);
      rawRightIndices[index] = rightSorted[rightIndex];
      ++index;
    }
  }

  static BufferPtr makeWrapCapture(
      const SelectivityVector& rows,
      vector_size_t size,
      vector_size_t* rawSizes,
      memory::MemoryPool* pool) {
    BufferPtr wrapCapture = allocateIndices(size, pool);
    auto rawWrapCaptures = wrapCapture->asMutable<vector_size_t>();

    vector_size_t offset = 0;
    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[row];
      std::fill(rawWrapCaptures + offset, rawWrapCaptures + offset + size, row);
      offset += size;
    });

    return wrapCapture;
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map_zip_with,
    MapZipWithFunction::signatures(),
    std::make_unique<MapZipWithFunction>());

} // namespace facebook::velox::functions
