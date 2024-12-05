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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file is auto-generated from test_fixture_generator.py
 * DO NOT EDIT!
 */

#include <gtest/gtest.h>

#include <vector>

#include "breeze/platforms/cuda.cuh"
#include "test/generated/algorithms/kernels-cuda.cuh"
#include "test/platforms/cuda_test.cuh"

template <typename T>
class AlgorithmTest : public ::testing::Test {
 protected:
  template <typename ReduceOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            typename U>
  void Reduce(const std::vector<T>& in, U* out, int num_blocks) {
    std::vector<U> vec_out(1, *out);
    CudaTestLaunch<BLOCK_THREADS>(
        num_blocks,
        &kernels::Reduce<ReduceOp, BLOCK_THREADS, ITEMS_PER_THREAD, T, U>, in,
        vec_out, in.size());
    *out = vec_out[0];
  }

  template <typename ScanOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            int LOOKBACK_DISTANCE, typename U, typename V>
  void Scan(const std::vector<T>& in, std::vector<U>& out, int* next_blocks_idx,
            std::vector<V>& blocks, int num_blocks) {
    std::vector<int> vec_next_blocks_idx(1, *next_blocks_idx);
    CudaTestLaunch<BLOCK_THREADS>(
        num_blocks,
        &kernels::Scan<ScanOp, BLOCK_THREADS, ITEMS_PER_THREAD,
                       LOOKBACK_DISTANCE, T, U, V>,
        in, out, vec_next_blocks_idx, blocks, in.size());
    *next_blocks_idx = vec_next_blocks_idx[0];
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int TILE_SIZE,
            int RADIX_BITS>
  void RadixSortHistogram(const std::vector<T>& in, std::vector<unsigned>& out,
                          int num_blocks) {
    CudaTestLaunch<BLOCK_THREADS>(
        num_blocks,
        &kernels::RadixSortHistogram<BLOCK_THREADS, ITEMS_PER_THREAD, TILE_SIZE,
                                     RADIX_BITS, T>,
        in, out, in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename U>
  void RadixSort(const std::vector<T>& in_keys, const std::vector<U>& in_values,
                 const std::vector<unsigned>& in_offsets, int start_bit,
                 int num_pass_bits, std::vector<T>& out_keys,
                 std::vector<U>& out_values, std::vector<int>& next_block_idx,
                 std::vector<unsigned>& blocks, int num_blocks) {
    const std::vector<int> vec_start_bit(1, start_bit);
    const std::vector<int> vec_num_pass_bits(1, num_pass_bits);
    CudaTestLaunch<BLOCK_THREADS>(
        num_blocks,
        &kernels::RadixSort<BLOCK_THREADS, ITEMS_PER_THREAD, RADIX_BITS, T, U>,
        in_keys, in_values, in_offsets, vec_start_bit, vec_num_pass_bits,
        out_keys, out_values, next_block_idx, blocks, in_keys.size());
  }
};
