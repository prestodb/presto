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
#include "velox/functions/lib/RowsTranslationUtil.h"

namespace facebook::velox::functions {

BufferPtr getElementToTopLevelRows(
    vector_size_t numElements,
    const SelectivityVector& topLevelRows,
    const vector_size_t* rawOffsets,
    const vector_size_t* rawSizes,
    const uint64_t* rawNulls,
    memory::MemoryPool* pool) {
  auto toTopLevelRows = allocateIndices(numElements, pool);
  auto rawToTopLevelRows = toTopLevelRows->asMutable<vector_size_t>();
  topLevelRows.applyToSelected([&](vector_size_t row) {
    if (rawNulls && bits::isBitNull(rawNulls, row)) {
      return;
    }
    auto size = rawSizes[row];
    auto offset = rawOffsets[row];
    for (int i = 0; i < size; ++i) {
      rawToTopLevelRows[offset + i] = row;
    }
  });
  return toTopLevelRows;
}

} // namespace facebook::velox::functions
