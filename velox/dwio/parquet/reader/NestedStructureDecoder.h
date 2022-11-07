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

#pragma once

#include "velox/buffer/Buffer.h"

namespace facebook::velox::parquet {

class NestedStructureDecoder {
 public:
  /// This function constructs the offsets, lengths and nulls arrays for the
  /// current level for complext types including ARRAY and MAP. The level is
  /// identified by the max definition level and repetition level for that
  /// level. For example, ARRAY<ARRAY<INTEGER>> has the following max definition
  /// and repetition levels:
  ///
  /// type    | ARRAY | ARRAY | INTEGER
  /// level   |   1   |   2   |   3
  /// maxDef  |   1   |   3   |   5
  /// maxRep  |   1   |   2   |   2
  ///
  /// If maxDefinition = 3 and maxRepeat = 2, this function will output
  /// offsets/lengths/nulls for the second level ARRAY.
  ///
  /// @param definitionLevels The definition levels for the leaf level
  /// @param repetitionLevels The repetition levels for the leaf level
  /// @param numValues The number of elements in definitionLevels or
  /// repetitionLevels
  /// @param maxDefinition The maximum possible definition level for this nested
  /// level
  /// @param maxRepeat The maximum possible repetition level for this nested
  /// level
  /// @param offsetsBuffer The output buffer for the offsets integer array.
  /// @param lengthsBuffer The output buffer for the lengths integer array. The
  /// elements are the number of elements in the designated level of collection.
  /// @param nullsBuffer The output buffer for the nulls bitmap.
  /// @return The number of elements for the current nested level.
  static int64_t readOffsetsAndNulls(
      const uint8_t* definitionLevels,
      const uint8_t* repetitionLevels,
      int64_t numValues,
      uint8_t maxDefinition,
      uint8_t maxRepeat,
      BufferPtr& offsetsBuffer,
      BufferPtr& lengthsBuffer,
      BufferPtr& nullsBuffer,
      memory::MemoryPool& pool);

 private:
  NestedStructureDecoder() {}
};
} // namespace facebook::velox::parquet
