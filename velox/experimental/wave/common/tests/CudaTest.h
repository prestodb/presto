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

/// Sample header for testing Cuda.h

namespace facebook::velox::wave {

struct WideParams {
  int32_t size;
  int32_t* numbers;
  int32_t stride;
  int32_t repeat;
  char data[4000];
  void* result;
};

class TestStream : public Stream {
 public:
  // Queues a kernel to add 1 to numbers[0...size - 1]. The kernel repeats
  // 'repeat' times.
  void
  incOne(int32_t* numbers, int size, int32_t repeat = 1, int32_t width = 10240);

  /// Like incOne but adds idx & 31 to numbers[idx].
  void
  addOne(int32_t* numbers, int size, int32_t repeat = 1, int32_t width = 10240);

  void addOneWide(
      int32_t* numbers,
      int32_t size,
      int32_t repeat = 1,
      int32_t width = 10240);

  /// Like addOne but uses shared memory for intermediates, with global
  /// ead/write at start/end.
  void addOneShared(
      int32_t* numbers,
      int32_t size,
      int32_t repeat = 1,
      int32_t width = 10240);

  /// Like addOne but uses registers for intermediates.
  void addOneReg(
      int32_t* numbers,
      int32_t size,
      int32_t repeat = 1,
      int32_t width = 10240);

  /// Increments each of 'numbers by a deterministic pseudorandom
  /// increment from 'lookup'. If 'numLocal is non-0, also accesses
  /// 'numLocal' adjacent positions in 'lookup' with a stride of
  /// 'localStride'.  If 'emptyWarps' is true, odd warps do no work
  /// but still sync with the other ones with __syncthreads().  If
  /// 'emptyThreads' is true, odd lanes do no work and even lanes do
  /// their work instead.
  void addOneRandom(
      int32_t* numbers,
      const int32_t* lookup,
      int size,
      int32_t repeat = 1,
      int32_t width = 10240,
      int32_t numLocal = 0,
      int32_t localStride = 0,
      bool emptyWarps = false,
      bool emptyLanes = false);

  // Makes random lookup keys and increments, starting at 'startCount'
  // columns[0] is keys. 'powerOfTwo' is the next power of two from
  // 'keyRange'. If 'powerOfTwo' is 0 the key columns are set to
  // zero. Otherwise the key column values are incremented by a a
  // delta + index of column where delta for element 0 is startCount &
  // (powerOfTwo - 1).
  void makeInput(
      int32_t numRows,
      int32_t keyRange,
      int32_t powerOfTwo,
      int32_t startCount,
      uint64_t* hash,
      uint8_t numColumns,
      int64_t** columns);
};

} // namespace facebook::velox::wave
