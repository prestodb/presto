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
  void addOne(int32_t* numbers, int size, int32_t repeat = 1);

  void addOneWide(int32_t* numbers, int32_t size, int32_t repeat = 1);

  void addOneRandom(
      int32_t* numbers,
      const int32_t* lookup,
      int size,
      int32_t repeat = 1);
};

} // namespace facebook::velox::wave
