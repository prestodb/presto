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

#include "velox/dwio/parquet/reader/NestedStructureDecoder.h"

#include "velox/dwio/common/BufferUtil.h"

namespace facebook::velox::parquet {

int64_t NestedStructureDecoder::readOffsetsAndNulls(
    const uint8_t* definitionLevels,
    const uint8_t* repetitionLevels,
    int64_t numValues,
    uint8_t maxDefinition,
    uint8_t maxRepeat,
    BufferPtr& offsetsBuffer,
    BufferPtr& lengthsBuffer,
    BufferPtr& nullsBuffer,
    memory::MemoryPool& pool) {
  dwio::common::ensureCapacity<uint8_t>(
      nullsBuffer, bits::nbytes(numValues), &pool);
  dwio::common::ensureCapacity<vector_size_t>(
      offsetsBuffer, numValues + 1, &pool);
  dwio::common::ensureCapacity<vector_size_t>(lengthsBuffer, numValues, &pool);

  auto offsets = offsetsBuffer->asMutable<vector_size_t>();
  auto lengths = lengthsBuffer->asMutable<vector_size_t>();
  auto nulls = nullsBuffer->asMutable<uint64_t>();

  int64_t offset = 0;
  int64_t lastOffset = 0;
  bool wasLastCollectionNull = definitionLevels[0] == (maxDefinition - 1);
  bits::setNull(nulls, 0, wasLastCollectionNull);
  offsets[0] = 0;

  int64_t outputIndex = 1;
  for (int64_t i = 1; i < numValues; ++i) {
    uint8_t definitionLevel = definitionLevels[i];
    uint8_t repetitionLevel = repetitionLevels[i];

    // empty means it belongs to a row that is null in one of its ancestor
    // levels.
    bool isEmpty = definitionLevel < (maxDefinition - 1);
    bool isNull = definitionLevel == (maxDefinition - 1);
    bool isCollectionBegin = (repetitionLevel < maxRepeat) & !isEmpty;
    bool isEntryBegin = (repetitionLevel <= maxRepeat) & !isEmpty;

    offset += isEntryBegin & !wasLastCollectionNull;
    offsets[outputIndex] = offset;
    lengths[outputIndex - 1] = offset - offsets[outputIndex - 1];
    bits::setNull(nulls, outputIndex, isNull);

    // Always update the outputs, but only increase the outputIndex when the
    // current entry is the begin of a new collection, and it's not empty.
    // Benchmark shows skipping non-collection-begin rows is worse than this
    // solution by nearly 2x because of extra branchings added for skipping.
    outputIndex += isCollectionBegin;
    wasLastCollectionNull = isEmpty ? wasLastCollectionNull : isNull;
    lastOffset = isCollectionBegin ? offset : lastOffset;
  }

  offset += !wasLastCollectionNull;
  offsets[outputIndex] = offset;
  lengths[outputIndex - 1] = offset - lastOffset;

  return outputIndex;
}

} // namespace facebook::velox::parquet
