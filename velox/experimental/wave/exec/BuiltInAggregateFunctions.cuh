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

#include "velox/experimental/wave/exec/AggregateFunction.h"

#include <cassert>
#include "velox/experimental/wave/exec/WaveCore.cuh"

namespace facebook::velox::wave::aggregation {

template <typename Func>
__global__ void createFunction(AggregateFunction* func) {
  func->accumulatorSize = Func::kAccumulatorSize;
  func->addRawInput = &Func::addRawInput;
  func->extractValues = &Func::extractValues;
  func->mergeAccumulators = &Func::mergeAccumulators;
}

struct Count {
  static constexpr int kAccumulatorSize = sizeof(int64_t);

  __device__ static ErrorCode addRawInput(
      int /*numInputs*/,
      Operand* /*inputs*/,
      int /*i*/,
      void* accumulator) {
    ++*reinterpret_cast<int64_t*>(accumulator);
    return ErrorCode::kOk;
  }

  __device__ static ErrorCode
  extractValues(void* accumulator, Operand* result, int i) {
    reinterpret_cast<int64_t*>(result->base)[i] =
        *reinterpret_cast<int64_t*>(accumulator);
    return ErrorCode::kOk;
  }

  __device__ static void* mergeAccumulators(void* a, void* b) {
    *reinterpret_cast<int64_t*>(a) += *reinterpret_cast<int64_t*>(b);
    return a;
  }
};

template <typename T, typename A>
struct Sum {
  static constexpr int kAccumulatorSize = sizeof(A);

  __device__ static ErrorCode
  addRawInput(int numInputs, Operand* inputs, int i, void* accumulator) {
    assert(numInputs == 1);
    *reinterpret_cast<A*>(accumulator) += value<T>(&inputs[0], i);
    return ErrorCode::kOk;
  }

  __device__ static ErrorCode
  extractValues(void* accumulator, Operand* result, int i) {
    reinterpret_cast<A*>(result->base)[i] = *reinterpret_cast<A*>(accumulator);
    return ErrorCode::kOk;
  }

  __device__ static void* mergeAccumulators(void* a, void* b) {
    *reinterpret_cast<A*>(a) += *reinterpret_cast<A*>(b);
    return a;
  }
};

struct AvgAccumulator {
  double sum;
  int64_t count;
};

template <typename T>
struct Avg {
  static constexpr int kAccumulatorSize = sizeof(AvgAccumulator);

  __device__ static ErrorCode
  addRawInput(int numInputs, Operand* inputs, int i, void* accumulator) {
    assert(numInputs == 1);
    auto* avgAccumulator = reinterpret_cast<AvgAccumulator*>(accumulator);
    avgAccumulator->sum += value<T>(&inputs[0], i);
    ++avgAccumulator->count;
    return ErrorCode::kOk;
  }

  __device__ static ErrorCode
  extractValues(void* accumulator, Operand* result, int i) {
    auto* avgAccumulator = reinterpret_cast<AvgAccumulator*>(accumulator);
    reinterpret_cast<double*>(result->base)[i] =
        avgAccumulator->sum / avgAccumulator->count;
    return ErrorCode::kOk;
  }

  __device__ static void* mergeAccumulators(void* a, void* b) {
    auto* aa = reinterpret_cast<AvgAccumulator*>(a);
    auto* bb = reinterpret_cast<AvgAccumulator*>(b);
    aa->sum += bb->sum;
    aa->count += bb->count;
    return aa;
  }
};

} // namespace facebook::velox::wave::aggregation
