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
#include "test/generated/functions/kernels-cuda.cuh"
#include "test/platforms/cuda_test.cuh"

template <typename T>
class FunctionTest : public ::testing::Test {
 protected:
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockLoad(const std::vector<T>& in, std::vector<T>& out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in, out,
        in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockLoadIf(const std::vector<T>& in,
                   const std::vector<int>& selection_flags,
                   std::vector<T>& out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockLoadIf<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in,
        selection_flags, out, in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockLoadFrom(const std::vector<T>& in, const std::vector<int>& offsets,
                     std::vector<T>& out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockLoadFrom<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in,
        offsets, out, in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStore(const std::vector<T>& in, std::vector<T>& out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in, out,
        out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStoreIf(const std::vector<T>& in,
                    const std::vector<int>& selection_flags,
                    std::vector<T>& out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockStoreIf<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in,
        selection_flags, out, out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStoreAt(const std::vector<T>& in, const std::vector<int>& offsets,
                    std::vector<T>& out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in, offsets,
        out, out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStoreAtIf(const std::vector<T>& in, const std::vector<int>& offsets,
                      const std::vector<int>& selection_flags,
                      std::vector<T>& out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockStoreAtIf<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in,
        offsets, selection_flags, out, out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockFill(T value, std::vector<T>& out) {
    const std::vector<T> vec_value(1, value);
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockFill<BLOCK_THREADS, ITEMS_PER_THREAD, T>, vec_value, out,
        out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockFillAtIf(T value, const std::vector<int>& offsets,
                     const std::vector<int>& selection_flags,
                     std::vector<T>& out) {
    const std::vector<T> vec_value(1, value);
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockFillAtIf<BLOCK_THREADS, ITEMS_PER_THREAD, T>, vec_value,
        offsets, selection_flags, out, out.size());
  }

  template <typename ReduceOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            typename U>
  void BlockReduce(const std::vector<T>& in, U* out) {
    std::vector<U> vec_out(1, *out);
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockReduce<ReduceOp, BLOCK_THREADS, ITEMS_PER_THREAD, T, U>,
        in, vec_out, in.size());
    *out = vec_out[0];
  }

  template <typename ScanOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            typename U>
  void BlockScan(const std::vector<T>& in, std::vector<U>& out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockScan<ScanOp, BLOCK_THREADS, ITEMS_PER_THREAD, T, U>, in,
        out, in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS>
  void BlockRadixRank(const std::vector<T>& in, std::vector<int>& out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockRadixRank<BLOCK_THREADS, ITEMS_PER_THREAD, RADIX_BITS,
                                 T>,
        in, out, in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename U>
  void BlockRadixSort(const std::vector<T>& keys_in,
                      const std::vector<U>& values_in, std::vector<T>& keys_out,
                      std::vector<U>& values_out) {
    CudaTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockRadixSort<BLOCK_THREADS, ITEMS_PER_THREAD, RADIX_BITS, T,
                                 U>,
        keys_in, values_in, keys_out, values_out, keys_in.size());
  }
};
