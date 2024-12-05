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
#include "test/generated/functions/kernels-openmp.h"
#include "test/platforms/openmp_test.h"

template <typename T>
class FunctionTest : public ::testing::Test {
 protected:
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockLoad(const std::vector<T>& in, std::vector<T>& out) {
    OpenMPTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in.data(),
        out.data(), in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockLoadIf(const std::vector<T>& in,
                   const std::vector<int>& selection_flags,
                   std::vector<T>& out) {
    OpenMPTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockLoadIf<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in.data(),
        selection_flags.data(), out.data(), in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockLoadFrom(const std::vector<T>& in, const std::vector<int>& offsets,
                     std::vector<T>& out) {
    OpenMPTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockLoadFrom<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in.data(),
        offsets.data(), out.data(), in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStore(const std::vector<T>& in, std::vector<T>& out) {
    OpenMPTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in.data(),
        out.data(), out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStoreIf(const std::vector<T>& in,
                    const std::vector<int>& selection_flags,
                    std::vector<T>& out) {
    OpenMPTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockStoreIf<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in.data(),
        selection_flags.data(), out.data(), out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStoreAt(const std::vector<T>& in, const std::vector<int>& offsets,
                    std::vector<T>& out) {
    OpenMPTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in.data(),
        offsets.data(), out.data(), out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStoreAtIf(const std::vector<T>& in, const std::vector<int>& offsets,
                      const std::vector<int>& selection_flags,
                      std::vector<T>& out) {
    OpenMPTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockStoreAtIf<BLOCK_THREADS, ITEMS_PER_THREAD, T>, in.data(),
        offsets.data(), selection_flags.data(), out.data(), out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockFill(T value, std::vector<T>& out) {
    OpenMPTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockFill<BLOCK_THREADS, ITEMS_PER_THREAD, T>, &value,
        out.data(), out.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockFillAtIf(T value, const std::vector<int>& offsets,
                     const std::vector<int>& selection_flags,
                     std::vector<T>& out) {
    OpenMPTestLaunch<BLOCK_THREADS>(
        /*num_blocks=*/1,
        &kernels::BlockFillAtIf<BLOCK_THREADS, ITEMS_PER_THREAD, T>, &value,
        offsets.data(), selection_flags.data(), out.data(), out.size());
  }

  template <typename ReduceOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            typename U>
  void BlockReduce(const std::vector<T>& in, U* out) {
    using PlatformT =
        OpenMPPlatform<BLOCK_THREADS, /*WARP_THREADS=*/BLOCK_THREADS>;
    using SharedMemType =
        typename breeze::functions::BlockReduce<PlatformT, U>::Scratch;
    OpenMPTestLaunch<BLOCK_THREADS, SharedMemType>(
        /*num_blocks=*/1,
        &kernels::BlockReduce<ReduceOp, BLOCK_THREADS, ITEMS_PER_THREAD, T, U,
                              SharedMemType>,
        in.data(), out, in.size());
  }

  template <typename ScanOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            typename U>
  void BlockScan(const std::vector<T>& in, std::vector<U>& out) {
    using PlatformT =
        OpenMPPlatform<BLOCK_THREADS, /*WARP_THREADS=*/BLOCK_THREADS>;
    using SharedMemType =
        typename breeze::functions::BlockScan<PlatformT, U,
                                              ITEMS_PER_THREAD>::Scratch;
    OpenMPTestLaunch<BLOCK_THREADS, SharedMemType>(
        /*num_blocks=*/1,
        &kernels::BlockScan<ScanOp, BLOCK_THREADS, ITEMS_PER_THREAD, T, U,
                            SharedMemType>,
        in.data(), out.data(), in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS>
  void BlockRadixRank(const std::vector<T>& in, std::vector<int>& out) {
    using PlatformT =
        OpenMPPlatform<BLOCK_THREADS, /*WARP_THREADS=*/BLOCK_THREADS>;
    using SharedMemType =
        typename breeze::functions::BlockRadixRank<PlatformT, ITEMS_PER_THREAD,
                                                   RADIX_BITS>::Scratch;
    OpenMPTestLaunch<BLOCK_THREADS, SharedMemType>(
        /*num_blocks=*/1,
        &kernels::BlockRadixRank<BLOCK_THREADS, ITEMS_PER_THREAD, RADIX_BITS, T,
                                 SharedMemType>,
        in.data(), out.data(), in.size());
  }

  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename U>
  void BlockRadixSort(const std::vector<T>& keys_in,
                      const std::vector<U>& values_in, std::vector<T>& keys_out,
                      std::vector<U>& values_out) {
    using PlatformT =
        OpenMPPlatform<BLOCK_THREADS, /*WARP_THREADS=*/BLOCK_THREADS>;
    using SharedMemType =
        typename breeze::functions::BlockRadixSort<PlatformT, ITEMS_PER_THREAD,
                                                   RADIX_BITS, T, U>::Scratch;
    OpenMPTestLaunch<BLOCK_THREADS, SharedMemType>(
        /*num_blocks=*/1,
        &kernels::BlockRadixSort<BLOCK_THREADS, ITEMS_PER_THREAD, RADIX_BITS, T,
                                 U, SharedMemType>,
        keys_in.data(), values_in.data(), keys_out.data(), values_out.data(),
        keys_in.size());
  }
};
