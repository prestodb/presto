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
class FunctionTest : public ::testing::Test {
 protected:
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockLoad(USE_AS_SIZE const std::vector<T>& in, std::vector<T>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockLoadIf(USE_AS_SIZE const std::vector<T>& in,
                   const std::vector<int>& selection_flags,
                   std::vector<T>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockLoadFrom(USE_AS_SIZE const std::vector<T>& in,
                     const std::vector<int>& offsets, std::vector<T>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStore(const std::vector<T>& in, USE_AS_SIZE std::vector<T>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStoreIf(const std::vector<T>& in,
                    const std::vector<int>& selection_flags,
                    USE_AS_SIZE std::vector<T>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStoreAt(const std::vector<T>& in, const std::vector<int>& offsets,
                    USE_AS_SIZE std::vector<T>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockStoreAtIf(const std::vector<T>& in, const std::vector<int>& offsets,
                      const std::vector<int>& selection_flags,
                      USE_AS_SIZE std::vector<T>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockFill(T value, USE_AS_SIZE std::vector<T>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD>
  void BlockFillAtIf(T value, const std::vector<int>& offsets,
                     const std::vector<int>& selection_flags,
                     USE_AS_SIZE std::vector<T>& out);
  template <typename ReduceOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            typename U>
  SHARED_MEM_TYPE(
      "typename breeze::functions::BlockReduce<PlatformT, U>::Scratch")
  void BlockReduce(USE_AS_SIZE const std::vector<T>& in, U* out);
  template <typename ScanOp, int BLOCK_THREADS, int ITEMS_PER_THREAD,
            typename U>
  SHARED_MEM_TYPE(
      "typename breeze::functions::BlockScan<PlatformT, U, ITEMS_PER_THREAD>::Scratch")
  void BlockScan(USE_AS_SIZE const std::vector<T>& in, std::vector<U>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS>
  SHARED_MEM_TYPE(
      "typename breeze::functions::BlockRadixRank<PlatformT, ITEMS_PER_THREAD, RADIX_BITS>::Scratch")
  void BlockRadixRank(USE_AS_SIZE const std::vector<T>& in,
                      std::vector<int>& out);
  template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename U>
  SHARED_MEM_TYPE(
      "typename breeze::functions::BlockRadixSort<PlatformT, ITEMS_PER_THREAD, RADIX_BITS, T, U>::Scratch")
  void BlockRadixSort(USE_AS_SIZE const std::vector<T>& keys_in,
                      const std::vector<U>& values_in, std::vector<T>& keys_out,
                      std::vector<U>& values_out);
};
