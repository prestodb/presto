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

#define UNTYPED [[clang::annotate("untyped")]]
#define USE_AS_SIZE [[clang::annotate("use_as_size")]]
#define BLOCK_COUNT [[clang::annotate("block_count")]]
#define SHARED_MEM_TYPE(T) [[clang::annotate("shared_mem_type=" T)]]

// Forward declarations to avoid having to parse the whole STL.
//
// WARNING: For the std namespace this is technically undefined behaviour,
// but since this file is solely being parsed I'll assume it won't cause
// too much trouble.
namespace std {
template <typename T>
class vector;
template <typename T>
class unique_ptr;
}  // namespace std
namespace testing {
struct Test {};
}  // namespace testing

template <typename T>
class AlgorithmTest : public ::testing::Test {
 protected:
  template <typename ReduceOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            typename U>
  SHARED_MEM_TYPE(
      "typename breeze::algorithms::DeviceReduce<PlatformT, U>::Scratch")
  void Reduce(USE_AS_SIZE const std::vector<T>& in, U* out,
              BLOCK_COUNT int num_blocks);
  template <typename ScanOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            int LOOKBACK_DISTANCE, typename U, typename V>
  SHARED_MEM_TYPE(
      "typename breeze::algorithms::DeviceScan<PlatformT, U, ITEMS_PER_THREAD, LOOKBACK_DISTANCE>::Scratch")
  void Scan(USE_AS_SIZE const std::vector<T>& in, std::vector<U>& out,
            int* next_blocks_idx, std::vector<V>& blocks,
            BLOCK_COUNT int num_blocks);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int TILE_SIZE,
            int RADIX_BITS>
  SHARED_MEM_TYPE(
      "typename breeze::algorithms::DeviceRadixSortHistogram<RADIX_BITS, T>::Scratch")
  void RadixSortHistogram(USE_AS_SIZE const std::vector<T>& in,
                          std::vector<unsigned>& out,
                          BLOCK_COUNT int num_blocks);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename U>
  SHARED_MEM_TYPE(
      "typename breeze::algorithms::DeviceRadixSort<PlatformT, ITEMS_PER_THREAD, RADIX_BITS, T, U>::Scratch")
  void RadixSort(USE_AS_SIZE const std::vector<T>& in_keys,
                 const std::vector<U>& in_values,
                 const std::vector<unsigned>& in_offsets, int start_bit,
                 int num_pass_bits, std::vector<T>& out_keys,
                 std::vector<U>& out_values, std::vector<int>& next_block_idx,
                 std::vector<unsigned>& blocks, BLOCK_COUNT int num_blocks);
};
