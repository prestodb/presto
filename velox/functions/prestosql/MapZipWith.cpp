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
#include <optional>
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

struct DecodedInputs {
  DecodedVector* decodedLeft;
  DecodedVector* decodedRight;
  const MapVector* baseLeft;
  const MapVector* baseRight;

  DecodedInputs(DecodedVector* _decodedLeft, DecodedVector* _decodeRight)
      : decodedLeft{_decodedLeft},
        decodedRight{_decodeRight},
        baseLeft{decodedLeft->base()->asUnchecked<MapVector>()},
        baseRight{decodedRight->base()->asUnchecked<MapVector>()} {}
};

struct MergeResults {
  BufferPtr newNulls;
  uint64_t* rawNewNulls;
  BufferPtr newOffsets;
  vector_size_t* rawNewOffsets;
  BufferPtr newSizes;
  vector_size_t* rawNewSizes;

  BufferPtr leftKeyNulls;
  uint64_t* rawLeftKeyNulls;
  BufferPtr leftKeyIndices;
  vector_size_t* rawLeftKeyIndices;

  BufferPtr rightKeyNulls;
  uint64_t* rawRightKeyNulls;
  BufferPtr rightKeyIndices;
  vector_size_t* rawRightKeyIndices;

  MergeResults(
      vector_size_t numMaps,
      vector_size_t maxNumKeys,
      memory::MemoryPool* pool)
      : newNulls{allocateNulls(numMaps, pool)},
        rawNewNulls(newNulls->asMutable<uint64_t>()),
        newOffsets(allocateOffsets(numMaps, pool)),
        rawNewOffsets(newOffsets->asMutable<vector_size_t>()),
        newSizes(allocateSizes(numMaps, pool)),
        rawNewSizes(newSizes->asMutable<vector_size_t>()),
        leftKeyNulls(allocateNulls(maxNumKeys, pool)),
        rawLeftKeyNulls(leftKeyNulls->asMutable<uint64_t>()),
        leftKeyIndices(allocateIndices(maxNumKeys, pool)),
        rawLeftKeyIndices(leftKeyIndices->asMutable<vector_size_t>()),
        rightKeyNulls(allocateNulls(maxNumKeys, pool)),
        rawRightKeyNulls(rightKeyNulls->asMutable<uint64_t>()),
        rightKeyIndices(allocateIndices(maxNumKeys, pool)),
        rawRightKeyIndices(rightKeyIndices->asMutable<vector_size_t>()) {}
};

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
    DecodedInputs decodedInputs(decodedArgs.at(0), decodedArgs.at(1));

    // Merge two maps. The total number of elements in the merged maps will not
    // exceed the total sum of elements in the input maps.
    auto numLeftElements = countMapElements(rows, *decodedInputs.decodedLeft);
    auto numRightElements = countMapElements(rows, *decodedInputs.decodedRight);

    auto maxElements = numLeftElements + numRightElements;

    MergeResults mergeResults(rows.end(), maxElements, context.pool());

    auto leftKeys = decodedInputs.baseLeft->mapKeys();
    auto rightKeys = decodedInputs.baseRight->mapKeys();

    vector_size_t index;

    // Take fast path for non-null keys of primitive types.
    if (leftKeys->type()->isPrimitiveType() && !leftKeys->mayHaveNulls() &&
        !rightKeys->mayHaveNulls()) {
      index = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          mergeScalarNoNullKeys,
          leftKeys->typeKind(),
          rows,
          decodedInputs,
          mergeResults);
    } else {
      index = mergeKeys(
          rows,
          decodedInputs,
          [&](auto left, auto right) {
            return leftKeys->compare(rightKeys.get(), left, right);
          },
          mergeResults);
    }

    auto mergedLeftValues = BaseVector::wrapInDictionary(
        mergeResults.leftKeyNulls,
        mergeResults.leftKeyIndices,
        index,
        decodedInputs.baseLeft->mapValues());

    auto mergedRightValues = BaseVector::wrapInDictionary(
        mergeResults.rightKeyNulls,
        mergeResults.rightKeyIndices,
        index,
        decodedInputs.baseRight->mapValues());

    auto mergedKeys = materializeMergedKeys(
        index, leftKeys, rightKeys, mergeResults, context.pool());

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
            mergeResults.rawNewOffsets[row],
            mergeResults.rawNewOffsets[row] + mergeResults.rawNewSizes[row],
            true);
      });
      elementRows.updateBounds();

      BufferPtr wrapCapture;
      if (entry.callable->hasCapture()) {
        wrapCapture = makeWrapCapture(
            *entry.rows, index, mergeResults.rawNewSizes, context.pool());
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
        mergeResults.newNulls,
        rows.end(),
        mergeResults.newOffsets,
        mergeResults.newSizes,
        mergedKeys,
        mergedValues,
        std::nullopt, /* nullCount */
        true /* sortedKeys */);
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
  static vector_size_t countMapElements(
      const SelectivityVector& rows,
      DecodedVector& decodedMap) {
    return countElements<MapVector>(rows, decodedMap);
  }

  template <TypeKind kind>
  static vector_size_t mergeScalarNoNullKeys(
      const SelectivityVector& rows,
      const DecodedInputs& decodedInputs,
      MergeResults& mergeResults) {
    using T = typename TypeTraits<kind>::NativeType;

    auto leftKeys = decodedInputs.baseLeft->mapKeys();
    SelectivityVector leftKeyRows(leftKeys->size());
    DecodedVector decodedLeftKeys(*leftKeys, leftKeyRows);

    auto rightKeys = decodedInputs.baseRight->mapKeys();
    SelectivityVector rightKeyRows(rightKeys->size());
    DecodedVector decodedRightKeys(*rightKeys, rightKeyRows);

    if (decodedLeftKeys.isConstantMapping() ||
        decodedRightKeys.isConstantMapping()) {
      return mergeKeys(
          rows,
          decodedInputs,
          [&](auto left, auto right) {
            return leftKeys->compare(
                rightKeys.get(), left, right, CompareFlags());
          },
          mergeResults);
    }

    auto baseLeftKeys = decodedLeftKeys.base()->asUnchecked<FlatVector<T>>();
    auto baseRightKeys = decodedRightKeys.base()->asUnchecked<FlatVector<T>>();

    if (decodedLeftKeys.isIdentityMapping() &&
        decodedRightKeys.isIdentityMapping()) {
      return mergeDecodedKeys(
          rows,
          decodedInputs,
          decodedLeftKeys,
          decodedRightKeys,
          [&](auto left, auto right) {
            return baseLeftKeys->template compareFlat<false>(
                baseRightKeys, left, right, CompareFlags());
          },
          mergeResults);
    }

    auto* leftMapping = decodedLeftKeys.indices();
    auto* rightMapping = decodedRightKeys.indices();

    return mergeDecodedKeys(
        rows,
        decodedInputs,
        decodedLeftKeys,
        decodedRightKeys,
        [&](auto left, auto right) {
          return baseLeftKeys->template compareFlat<false>(
              baseRightKeys,
              leftMapping[left],
              rightMapping[right],
              CompareFlags());
        },
        mergeResults);
  }

  template <typename TCompare>
  static vector_size_t mergeKeys(
      const SelectivityVector& rows,
      const DecodedInputs& decodedInputs,
      TCompare doCompare,
      MergeResults& mergeResults) {
    vector_size_t index = 0;
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedInputs.decodedLeft->isNullAt(row) ||
          decodedInputs.decodedRight->isNullAt(row)) {
        bits::setNull(mergeResults.rawNewNulls, row);
        return;
      }

      mergeResults.rawNewOffsets[row] = index;

      auto leftRow = decodedInputs.decodedLeft->index(row);
      auto rightRow = decodedInputs.decodedRight->index(row);

      auto leftSorted = decodedInputs.baseLeft->sortedKeyIndices(leftRow);
      auto rightSorted = decodedInputs.baseRight->sortedKeyIndices(rightRow);

      mergeSingleMapKeys(
          leftSorted,
          rightSorted,
          doCompare,
          mergeResults.rawLeftKeyNulls,
          mergeResults.rawRightKeyNulls,
          mergeResults.rawLeftKeyIndices,
          mergeResults.rawRightKeyIndices,
          index);

      mergeResults.rawNewSizes[row] = index - mergeResults.rawNewOffsets[row];
    });
    return index;
  }

  // Sorts keys of a single map and writes sorted key indices into provided
  // 'sorted' vector.
  static void sortKeys(
      const MapVector* map,
      vector_size_t mapRow,
      DecodedVector& decodedKeys,
      std::vector<vector_size_t>& sorted) {
    auto size = map->sizeAt(mapRow);
    auto offset = map->offsetAt(mapRow);

    sorted.resize(size);
    std::iota(sorted.begin(), sorted.end(), offset);

    if (!map->hasSortedKeys()) {
      if (decodedKeys.isIdentityMapping()) {
        decodedKeys.base()->sortIndices(sorted, CompareFlags());
      } else {
        decodedKeys.base()->sortIndices(
            sorted, decodedKeys.indices(), CompareFlags());
      }
    }
  }

  template <typename TCompare>
  static vector_size_t mergeDecodedKeys(
      const SelectivityVector& rows,
      const DecodedInputs& decodedInputs,
      DecodedVector& decodedLeftKeys,
      DecodedVector& decodedRightKeys,
      TCompare doCompare,
      MergeResults& mergeResults) {
    vector_size_t index = 0;

    std::vector<vector_size_t> leftSorted;
    std::vector<vector_size_t> rightSorted;

    rows.applyToSelected([&](vector_size_t row) {
      if (decodedInputs.decodedLeft->isNullAt(row) ||
          decodedInputs.decodedRight->isNullAt(row)) {
        bits::setNull(mergeResults.rawNewNulls, row);
        return;
      }

      mergeResults.rawNewOffsets[row] = index;

      auto leftRow = decodedInputs.decodedLeft->index(row);
      auto rightRow = decodedInputs.decodedRight->index(row);

      sortKeys(decodedInputs.baseLeft, leftRow, decodedLeftKeys, leftSorted);
      sortKeys(
          decodedInputs.baseRight, rightRow, decodedRightKeys, rightSorted);

      mergeSingleMapKeys(
          leftSorted,
          rightSorted,
          doCompare,
          mergeResults.rawLeftKeyNulls,
          mergeResults.rawRightKeyNulls,
          mergeResults.rawLeftKeyIndices,
          mergeResults.rawRightKeyIndices,
          index);

      mergeResults.rawNewSizes[row] = index - mergeResults.rawNewOffsets[row];
    });
    return index;
  }

  template <typename TCompare>
  static void mergeSingleMapKeys(
      const std::vector<vector_size_t>& leftSorted,
      const std::vector<vector_size_t>& rightSorted,
      TCompare doCompare,
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
      auto compare = doCompare(leftSorted[leftIndex], rightSorted[rightIndex]);
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

  static VectorPtr materializeMergedKeys(
      vector_size_t size,
      const VectorPtr& leftKeys,
      const VectorPtr& rightKeys,
      const MergeResults& mergeResults,
      memory::MemoryPool* pool) {
    auto mergedKeys = BaseVector::create(leftKeys->type(), size, pool);

    // Copy left keys.
    SelectivityVector copyRows(size);
    copyRows.deselectNulls(mergeResults.rawLeftKeyNulls, 0, size);
    mergedKeys->copy(leftKeys.get(), copyRows, mergeResults.rawLeftKeyIndices);

    // Copy right keys.
    copyRows.setAll();
    copyRows.deselectNonNulls(mergeResults.rawLeftKeyNulls, 0, size);
    mergedKeys->copy(
        rightKeys.get(), copyRows, mergeResults.rawRightKeyIndices);

    return mergedKeys;
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
