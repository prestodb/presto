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

#include "velox/experimental/wave/common/Cuda.h"

/// Sample header for testing Block.cuh

namespace facebook::velox::wave {

class BlockTestStream : public Stream {
 public:
  /// In each block of 256 bools in bools[i], counts the number of
  /// true and writes the indices of true lanes into the corresponding
  /// indices[i]. Stors the number of true values to sizes[i].
  void testBoolToIndices(
      int32_t numBlocks,
      uint8_t** flags,
      int32_t** indices,
      int32_t* sizes,
      int64_t* times);

  // calculates the sum over blocks of 256 int64s and returns the result for
  // numbers[i * 256] ... numbers[(i + 1) * 256 - 1] inclusive  in results[i].
  void testSum64(int32_t numBlocks, int64_t* numbers, int64_t* results);

  /// Sorts 'rows'[i] using ids[i] as keys and stores the sorted order in
  /// 'result[i]'.
  // void dedup(int32_t numBlocks, uint16_t** ids, uint16_t** rows, uint16_t**
  // resultRows);
};

} // namespace facebook::velox::wave
