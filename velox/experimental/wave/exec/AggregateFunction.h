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

#include "velox/experimental/wave/vector/Operand.h"

namespace facebook::velox::wave::aggregation {

struct AggregateFunction {
  int accumulatorSize;

  ErrorCode (
      *addRawInput)(int numInputs, Operand* inputs, int i, void* accumulator);

  ErrorCode (*extractValues)(void* accumulator, Operand* result, int i);

  void* (*mergeAccumulators)(void* a, void* b);
};

} // namespace facebook::velox::wave::aggregation
