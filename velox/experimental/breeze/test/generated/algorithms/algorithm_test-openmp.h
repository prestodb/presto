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
#include <omp.h>

#include <cassert>
#include <vector>

#include "breeze/platforms/openmp.h"
#include "test/generated/algorithms/kernels-openmp.h"
#include "test/platforms/openmp_test.h"

template <typename T>
class AlgorithmTest : public ::testing::Test {
 protected:
  template <typename ReduceOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            typename U>
  void Reduce(const std::vector<T>& in, U* out, int num_blocks) {
    using PlatformT =
        OpenMPPlatform<BLOCK_THREADS, /*WARP_THREADS=*/BLOCK_THREADS>;
    using SharedMemType =
        typename breeze::algorithms::DeviceReduce<PlatformT, U>::Scratch;
    OpenMPTestLaunch<BLOCK_THREADS, SharedMemType>(
        num_blocks,
        &kernels::Reduce<ReduceOp, BLOCK_THREADS, ITEMS_PER_THREAD, T, U,
                         SharedMemType>,
        in.data(), out, in.size());
  }

  template <typename ScanOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            int LOOKBACK_DISTANCE, typename U, typename V>
  void Scan(const std::vector<T>& in, std::vector<U>& out, int* next_blocks_idx,
            std::vector<V>& blocks, int num_blocks) {
    using PlatformT =
        OpenMPPlatform<BLOCK_THREADS, /*WARP_THREADS=*/BLOCK_THREADS>;
    using SharedMemType =
        typename breeze::algorithms::DeviceScan<PlatformT, U, ITEMS_PER_THREAD,
                                                LOOKBACK_DISTANCE>::Scratch;
    OpenMPTestLaunch<BLOCK_THREADS, SharedMemType>(
        num_blocks,
        &kernels::Scan<ScanOp, BLOCK_THREADS, ITEMS_PER_THREAD,
                       LOOKBACK_DISTANCE, T, U, V, SharedMemType>,
        in.data(), out.data(), next_blocks_idx, blocks.data(), in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int TILE_SIZE,
            int RADIX_BITS>
  void RadixSortHistogram(const std::vector<T>& in, std::vector<unsigned>& out,
                          int num_blocks) {
    using SharedMemType =
        typename breeze::algorithms::DeviceRadixSortHistogram<RADIX_BITS,
                                                              T>::Scratch;
    OpenMPTestLaunch<BLOCK_THREADS, SharedMemType>(
        num_blocks,
        &kernels::RadixSortHistogram<BLOCK_THREADS, ITEMS_PER_THREAD, TILE_SIZE,
                                     RADIX_BITS, T, SharedMemType>,
        in.data(), out.data(), in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename U>
  void RadixSort(const std::vector<T>& in_keys, const std::vector<U>& in_values,
                 const std::vector<unsigned>& in_offsets, int start_bit,
                 int num_pass_bits, std::vector<T>& out_keys,
                 std::vector<U>& out_values, std::vector<int>& next_block_idx,
                 std::vector<unsigned>& blocks, int num_blocks) {
    using PlatformT =
        OpenMPPlatform<BLOCK_THREADS, /*WARP_THREADS=*/BLOCK_THREADS>;
    using SharedMemType = typename breeze::algorithms::DeviceRadixSort<
        PlatformT, ITEMS_PER_THREAD, RADIX_BITS, T, U>::Scratch;
    OpenMPTestLaunch<BLOCK_THREADS, SharedMemType>(
        num_blocks,
        &kernels::RadixSort<BLOCK_THREADS, ITEMS_PER_THREAD, RADIX_BITS, T, U,
                            SharedMemType>,
        in_keys.data(), in_values.data(), in_offsets.data(), &start_bit,
        &num_pass_bits, out_keys.data(), out_values.data(),
        next_block_idx.data(), blocks.data(), in_keys.size());
  }
};
