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
#include "velox/functions/prestosql/aggregates/MapAggregateBase.h"
#include <velox/common/base/Exceptions.h>

namespace facebook::velox::aggregate {

void MapAggregateBase::extractValues(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  auto mapVector = (*result)->as<MapVector>();
  VELOX_CHECK(mapVector);
  mapVector->resize(numGroups);
  auto mapKeys = mapVector->mapKeys();
  auto mapValues = mapVector->mapValues();
  auto numElements = countElements(groups, numGroups);
  mapKeys->resize(numElements);
  mapValues->resize(numElements);

  auto* rawNulls = getRawNulls(mapVector);
  vector_size_t offset = 0;

  for (int32_t i = 0; i < numGroups; ++i) {
    char* group = groups[i];
    clearNull(rawNulls, i);

    auto accumulator = value<MapAccumulator>(group);
    auto mapSize = accumulator->keys.size();
    if (mapSize) {
      ValueListReader keysReader(accumulator->keys);
      ValueListReader valuesReader(accumulator->values);
      for (auto index = 0; index < mapSize; ++index) {
        keysReader.next(*mapKeys, offset + index);
        valuesReader.next(*mapValues, offset + index);
      }
      mapVector->setOffsetAndSize(i, offset, mapSize);
      offset += mapSize;
    } else {
      mapVector->setOffsetAndSize(i, offset, 0);
    }
  }

  // Canonicalize requires a singly referenced MapVector. std::move
  // inside the cast does not clear *result, so we clear this
  // manually.
  auto mapVectorPtr = std::static_pointer_cast<MapVector>(std::move(*result));
  *result = nullptr;
  *result = removeDuplicates(mapVectorPtr);
}

VectorPtr MapAggregateBase::removeDuplicates(MapVectorPtr& mapVector) const {
  MapVector::canonicalize(mapVector);

  auto offsets = mapVector->rawOffsets();
  auto sizes = mapVector->rawSizes();
  auto mapKeys = mapVector->mapKeys();

  auto numRows = mapVector->size();
  auto numElements = mapKeys->size();

  BufferPtr newSizes;
  vector_size_t* rawNewSizes = nullptr;

  BufferPtr elementIndices;
  vector_size_t* rawElementIndices = nullptr;

  // Check for duplicate keys.
  for (vector_size_t row = 0; row < numRows; row++) {
    auto offset = offsets[row];
    auto size = sizes[row];
    auto duplicateCnt = 0;
    for (vector_size_t i = 1; i < size; i++) {
      if (mapKeys->equalValueAt(mapKeys.get(), offset + i, offset + i - 1)) {
        // Duplicate key found.
        duplicateCnt++;
        if (!rawNewSizes) {
          newSizes = allocateSizes(numElements, mapVector->pool());
          rawNewSizes = newSizes->asMutable<vector_size_t>();

          elementIndices = allocateIndices(numElements, mapVector->pool());
          rawElementIndices = elementIndices->asMutable<vector_size_t>();

          memcpy(rawNewSizes, sizes, row * sizeof(vector_size_t));
          std::iota(rawElementIndices, rawElementIndices + numElements, 0);
        }
      } else if (rawNewSizes) {
        rawElementIndices[offset + i - duplicateCnt] = offset + i;
      }
    }
    if (rawNewSizes) {
      rawNewSizes[row] = size - duplicateCnt;
    }
  };

  if (rawNewSizes) {
    return std::make_shared<MapVector>(
        mapVector->pool(),
        mapVector->type(),
        mapVector->nulls(),
        mapVector->size(),
        mapVector->offsets(),
        newSizes,
        BaseVector::wrapInDictionary(
            BufferPtr(nullptr), elementIndices, numElements, mapKeys),
        BaseVector::wrapInDictionary(
            BufferPtr(nullptr),
            elementIndices,
            numElements,
            mapVector->mapValues()));
  } else {
    return mapVector;
  }
}

void MapAggregateBase::addMapInputToAccumulator(
    char** groups,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    bool /*mayPushdown*/) {
  decodedMaps_.decode(*args[0], rows);
  auto mapVector = decodedMaps_.base()->as<MapVector>();

  VELOX_CHECK_NOT_NULL(mapVector);
  auto& mapKeys = mapVector->mapKeys();
  auto& mapValues = mapVector->mapValues();
  rows.applyToSelected([&](vector_size_t row) {
    auto group = groups[row];
    auto accumulator = value<MapAccumulator>(group);

    auto decodedRow = decodedMaps_.index(row);
    auto offset = mapVector->offsetAt(decodedRow);
    auto size = mapVector->sizeAt(decodedRow);
    auto tracker = trackRowSize(group);
    accumulator->keys.appendRange(mapKeys, offset, size, allocator_);
    accumulator->values.appendRange(mapValues, offset, size, allocator_);
  });
}

void MapAggregateBase::addSingleGroupMapInputToAccumulator(
    char* group,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    bool /*mayPushdown*/) {
  decodedMaps_.decode(*args[0], rows);
  auto mapVector = decodedMaps_.base()->as<MapVector>();

  auto accumulator = value<MapAccumulator>(group);
  auto& keys = accumulator->keys;
  auto& values = accumulator->values;

  VELOX_CHECK_NOT_NULL(mapVector);
  auto& mapKeys = mapVector->mapKeys();
  auto& mapValues = mapVector->mapValues();
  rows.applyToSelected([&](vector_size_t row) {
    auto decodedRow = decodedMaps_.index(row);
    auto offset = mapVector->offsetAt(decodedRow);
    auto size = mapVector->sizeAt(decodedRow);
    keys.appendRange(mapKeys, offset, size, allocator_);
    values.appendRange(mapValues, offset, size, allocator_);
  });
}
} // namespace facebook::velox::aggregate
