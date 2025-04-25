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

#include <breeze/functions/reduce.h>
#include <breeze/platforms/platform.h>
#include <breeze/utils/device_vector.h>
#include <breeze/platforms/cuda.cuh>

#include <gtest/gtest.h>

namespace breeze {
namespace {

using namespace functions;
using namespace utils;

constexpr int kBlockThreads = 256;
constexpr int kItemsPerThread = 8;
constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
constexpr int kNumItems = 250'000;
constexpr int kNumBlocks = (kNumItems + kBlockItems - 1) / kBlockItems;

__global__ __launch_bounds__(kBlockThreads) void reduceKernel(int* out) {
  CudaPlatform<kBlockThreads, 32> p;
  using BlockReduceT = BlockReduce<decltype(p), int>;
  __shared__ typename BlockReduceT::Scratch scratch;
  int items[kItemsPerThread];
  for (int i = 0; i < kItemsPerThread; ++i) {
    items[i] = 1;
  }
  int aggregate = BlockReduceT::template Reduce<ReduceOpAdd, kItemsPerThread>(
      p,
      make_slice(items),
      make_slice(&scratch).template reinterpret<SHARED>());
  if (p.thread_idx() == 0) {
    out[p.block_idx()] = aggregate;
  }
}

TEST(BreezeCudaTest, reduce) {
  device_vector<int> result(kNumBlocks);
  reduceKernel<<<kNumBlocks, kBlockThreads>>>(result.data());
  std::vector<int> actual(kNumBlocks);
  result.copy_to_host(actual.data(), actual.size());
  std::vector<int> expected(kNumBlocks, kBlockThreads * kItemsPerThread);
  ASSERT_EQ(actual, expected);
}

} // namespace
} // namespace breeze
