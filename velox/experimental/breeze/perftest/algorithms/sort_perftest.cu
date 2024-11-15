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

#include <gtest/gtest.h>

#include <climits>
#include <vector>

#include "breeze/algorithms/sort.h"
#include "breeze/platforms/platform.h"
#include "breeze/utils/block_details.h"
#include "breeze/utils/device_vector.h"
#include "perftest/perftest.h"

namespace breeze {

using namespace algorithms;
using namespace functions;
using namespace utils;

namespace test {
namespace kernels {

enum { CUDA_WARP_THREADS = 32 };

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename T,
          typename U, typename BlockT>
__global__ __launch_bounds__(BLOCK_THREADS) void RadixSort(
    const T* in, const U* in_offsets, int start_bit, int num_pass_bits, T* out,
    int* next_block_idx, BlockT* blocks, int num_items) {
  CudaPlatform<BLOCK_THREADS, CUDA_WARP_THREADS> p;
  using DeviceRadixSortT =
      DeviceRadixSort<decltype(p), ITEMS_PER_THREAD, RADIX_BITS, T>;
  __shared__ typename DeviceRadixSortT::Scratch scratch;
  DeviceRadixSortT::template Sort<BlockT>(
      p, make_slice<GLOBAL>(in), make_slice<GLOBAL>(in_offsets), start_bit,
      num_pass_bits, make_slice<GLOBAL>(out),
      make_slice<GLOBAL>(next_block_idx), make_slice<GLOBAL>(blocks),
      make_slice(&scratch).template reinterpret<SHARED>(), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int TILE_SIZE,
          int RADIX_BITS, typename T, typename U>
__global__ __launch_bounds__(BLOCK_THREADS) void RadixSortHistogram(
    const T* in, U* histogram, int num_items) {
  CudaPlatform<BLOCK_THREADS, CUDA_WARP_THREADS> p;
  using DeviceRadixSortHistogramT = DeviceRadixSortHistogram<RADIX_BITS, T>;
  __shared__ typename DeviceRadixSortHistogramT::Scratch scratch;
  DeviceRadixSortHistogramT::template Build<ITEMS_PER_THREAD, TILE_SIZE>(
      p, make_slice<GLOBAL>(in), make_slice<GLOBAL>(histogram),
      make_slice(&scratch).template reinterpret<SHARED>(), num_items);
}

}  // namespace kernels

template <typename T>
typename std::enable_if<!std::is_unsigned<T>::value, T>::type to_bit_ordered(
    T value) {
  return value ^ (static_cast<T>(1) << (sizeof(T) * CHAR_BIT - 1));
}

template <typename T>
typename std::enable_if<std::is_unsigned<T>::value, T>::type to_bit_ordered(
    T value) {
  return value;
}

template <typename T>
T extract_bits(T value, int start_bit, int num_pass_bits) {
  return (value >> start_bit) & ((1u << num_pass_bits) - 1);
}

using SortConfig = PerfTestArrayConfig<11>;

const SortConfig kConfig = {{{"num_input_rows", "400000"},
                             {"num_input_rows_short", "6400"},
                             {"num_input_rows_grande", "6400000"},
                             {"num_input_rows_venti", "64000000"},
                             {"input_generate_method", "RANDOM"},
                             {"input_random_engine", "MT19937"},
                             {"input_random_shuffle", "1"},
                             {"input_random_stride", "1000"},
                             {"input_random_stride_short", "10"},
                             {"input_random_stride_grande", "100000"},
                             {"input_random_stride_venti", "100000"}}};

template <typename TypeParam>
class SortPerfTest : public PerfTest<SortConfig>, public testing::Test {
 public:
  template <typename T>
  T GetConfigValue(const char* key, T default_value) const {
    return kConfig.get<T>(key, default_value);
  }
  template <typename T>
  std::vector<T> GetConfigColumn(const char* prefix) const {
    return kConfig.get_column<T>(prefix);
  }
};

template <int N>
struct RadixBits {
  enum {
    VALUE = N,
  };
};

template <typename LaunchParamsAndItemTypeT, typename RadixBitsT>
struct SortTestType {
  using launch_params_and_item_type_type = LaunchParamsAndItemTypeT;
  using item_type = typename LaunchParamsAndItemTypeT::item_type;
  using launch_params = typename LaunchParamsAndItemTypeT::launch_params;
  enum {
    RADIX_BITS = RadixBitsT::VALUE,
  };

  static std::string GetName() {
    return launch_params_and_item_type_type::GetName() + ".RadixBits" +
           std::to_string(RADIX_BITS);
  }
};

using LaunchParamsTypes =
    std::tuple<LaunchParams<256, 8>, LaunchParams<256, 16>,
               LaunchParams<256, 24>>;

using LaunchParamsAndItemTypes =
    CombineLaunchParamsAndTypes<LaunchParamsAndItemType, LaunchParamsTypes, int,
                                unsigned, long long, unsigned long long>;

using TestTypes =
    MakeTestTypes<CombineTestTypes<SortTestType, LaunchParamsAndItemTypes,
                                   RadixBits<8>>>::types;

TYPED_TEST_SUITE(SortPerfTest, TestTypes, TestTypeNames);

TYPED_TEST(SortPerfTest, RadixSort) {
  using value_type = typename TypeParam::item_type::type;
  using size_type = unsigned;
  using block_type = unsigned;

  auto input = this->template GetConfigColumn<value_type>("input");
  ASSERT_NE(input.size(), 0u);

  auto check_result = this->GetConfigValue("check_result", true);

  device_vector<value_type> items(input.size());

  constexpr int kBlockThreads = TypeParam::launch_params::BLOCK_THREADS;
  constexpr int kItemsPerThread = TypeParam::launch_params::ITEMS_PER_THREAD;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
  constexpr int kRadixBits = TypeParam::RADIX_BITS;
  constexpr int kEndBit = sizeof(value_type) * /*BITS_PER_BYTE=*/8;
  constexpr int kNumBins = 1 << kRadixBits;

  auto start_bit = this->GetConfigValue("start_bit", 0);
  ASSERT_LT(start_bit, kEndBit);

  int num_pass_bits = std::min(kRadixBits, kEndBit - start_bit);
  std::vector<size_type> input_histogram(kNumBins);
  for (const auto& value : input) {
    int bin = extract_bits(to_bit_ordered(value), start_bit, num_pass_bits);
    input_histogram[bin] += 1u;
  }
  size_type sum = 0;
  std::vector<size_type> input_prefix_sum(kNumBins);
  for (size_t i = 0; i < kNumBins; ++i) {
    input_prefix_sum[i] = sum;
    sum += input_histogram[i];
  }

  int num_blocks = (input.size() + kBlockItems - 1) / kBlockItems;

  device_vector<int> next_block_idx(1);
  device_vector<block_type> blocks(num_blocks * kNumBins);
  device_vector<size_type> prefix_sum(kNumBins);
  device_vector<size_type> offsets(kNumBins);
  device_vector<value_type> out(input.size());

  // copy input to device memory
  items.copy_from_host(input.data(), input.size());
  prefix_sum.copy_from_host(input_prefix_sum.data(), input_prefix_sum.size());

  // provide throughput information
  this->set_element_count(input.size());
  this->set_element_size(sizeof(value_type));
  this->set_elements_per_thread(kItemsPerThread);
  this->template set_global_memory_loads<value_type>(input.size());
  this->template set_global_memory_stores<value_type>(input.size());

  this->MeasureWithSetup(
      kConfig,
      [&]() {
        cudaMemsetAsync(next_block_idx.data(), 0, sizeof(int));
        cudaMemsetAsync(blocks.data(), 0,
                        sizeof(block_type) * num_blocks * kNumBins);
        cudaMemcpyAsync(offsets.data(), prefix_sum.data(),
                        sizeof(size_type) * kNumBins, cudaMemcpyDeviceToDevice);
      },
      [&]() {
        kernels::RadixSort<kBlockThreads, kItemsPerThread, kRadixBits>
            <<<num_blocks, kBlockThreads>>>(
                items.data(), offsets.data(), start_bit, num_pass_bits,
                out.data(), next_block_idx.data(), blocks.data(), items.size());
      });

  if (check_result) {
    std::vector<value_type> actual_result(out.size());
    out.copy_to_host(actual_result.data(), actual_result.size());
    std::vector<value_type> expected_result = input;
    std::stable_sort(expected_result.begin(), expected_result.end(),
                     [start_bit, num_pass_bits](value_type a, value_type b) {
                       return extract_bits(a, start_bit, num_pass_bits) <
                              extract_bits(b, start_bit, num_pass_bits);
                     });
    EXPECT_EQ(expected_result, actual_result);
  }
}

const SortConfig kHistogramConfig = {{{"num_input_rows", "16750000"},
                                      {"num_input_rows_short", "2048000"},
                                      {"num_input_rows_grande", "268000000"},
                                      {"num_input_rows_venti", "2144000000"},
                                      {"input_generate_method", "RANDOM"},
                                      {"input_random_engine", "MT19937"},
                                      {"input_random_shuffle", "1"},
                                      {"input_random_stride", "1000"},
                                      {"input_random_stride_short", "10"},
                                      {"input_random_stride_grande", "100000"},
                                      {"input_random_stride_venti", "100000"}}};

template <typename TypeParam>
class SortHistogramPerfTest : public PerfTest<SortConfig>,
                              public testing::Test {
 public:
  template <typename T>
  T GetConfigValue(const char* key, T default_value) const {
    return kHistogramConfig.get<T>(key, default_value);
  }
  template <typename T>
  std::vector<T> GetConfigColumn(const char* prefix) const {
    return kHistogramConfig.get_column<T>(prefix);
  }
};

template <int N>
struct TileSize {
  enum {
    VALUE = N,
  };
};

template <typename SortTestTypeT, typename TileSizeT>
struct SortHistogramTestType {
  using launch_params_and_item_type_type =
      typename SortTestTypeT::launch_params_and_item_type_type;
  using item_type = typename launch_params_and_item_type_type::item_type;
  using launch_params =
      typename launch_params_and_item_type_type::launch_params;
  enum {
    RADIX_BITS = SortTestTypeT::RADIX_BITS,
    TILE_SIZE = TileSizeT::VALUE,
  };

  static std::string GetName() {
    return SortTestTypeT::GetName() + ".TileSize" + std::to_string(TILE_SIZE);
  }
};

using HistogramLaunchParamsTypes =
    std::tuple<LaunchParams<256, 4>, LaunchParams<256, 8>>;

using HistogramLaunchParamsAndItemTypes =
    CombineLaunchParamsAndTypes<LaunchParamsAndItemType,
                                HistogramLaunchParamsTypes, int, unsigned,
                                long long, unsigned long long>;

using HistogramSortTestTypes =
    CombineTestTypes<SortTestType, HistogramLaunchParamsAndItemTypes,
                     RadixBits<8>>;

using SortHistogramTestTypes = MakeTestTypes<
    CombineTestTypes<SortHistogramTestType, HistogramSortTestTypes,
                     TileSize<16>, TileSize<32>>>::types;

TYPED_TEST_SUITE(SortHistogramPerfTest, SortHistogramTestTypes, TestTypeNames);

TYPED_TEST(SortHistogramPerfTest, RadixSortHistogram) {
  using value_type = typename TypeParam::item_type::type;
  using size_type = unsigned;

  auto input = this->template GetConfigColumn<value_type>("input");
  ASSERT_NE(input.size(), 0u);

  auto check_result = this->GetConfigValue("check_result", true);

  constexpr int kBlockThreads = TypeParam::launch_params::BLOCK_THREADS;
  constexpr int kItemsPerThread = TypeParam::launch_params::ITEMS_PER_THREAD;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
  constexpr int kTileSize = TypeParam::TILE_SIZE;
  constexpr int kRadixBits = TypeParam::RADIX_BITS;
  constexpr int kTileItems = kBlockItems * kTileSize;
  constexpr int kEndBit = sizeof(value_type) * /*BITS_PER_BYTE=*/8;
  constexpr int kNumPasses = DivideAndRoundUp<kEndBit, kRadixBits>::VALUE;
  constexpr int kNumBins = 1 << kRadixBits;
  constexpr int kHistogramSize = kNumBins * kNumPasses;

  device_vector<value_type> items(input.size());
  device_vector<size_type> histogram(kHistogramSize);

  int num_blocks = (input.size() + kTileItems - 1) / kTileItems;

  // copy input to device memory
  items.copy_from_host(input.data(), input.size());

  // provide throughput information
  this->set_element_count(input.size());
  this->set_element_size(sizeof(value_type));
  this->set_elements_per_thread(kItemsPerThread);
  // count each atomic add as 1 load + 1 store
  int num_atomic_adds = kHistogramSize * num_blocks;
  this->set_global_memory_loads(input.size() * sizeof(value_type) +
                                num_atomic_adds * sizeof(size_type));
  this->set_global_memory_stores(num_atomic_adds * sizeof(size_type));

  this->MeasureWithSetup(
      kConfig,
      [&]() {
        cudaMemsetAsync(histogram.data(), 0,
                        sizeof(size_type) * kHistogramSize);
      },
      [&]() {
        kernels::RadixSortHistogram<kBlockThreads, kItemsPerThread, kTileSize,
                                    kRadixBits><<<num_blocks, kBlockThreads>>>(
            items.data(), histogram.data(), items.size());
      });

  if (check_result) {
    std::vector<size_type> actual_histogram(histogram.size());
    histogram.copy_to_host(actual_histogram.data(), actual_histogram.size());
    std::vector<size_type> expected_histogram(histogram.size());
    int start_bit = 0;
    for (int j = 0; j < kNumPasses; ++j) {
      int num_pass_bits = std::min(kRadixBits, kEndBit - start_bit);
      for (const auto& value : input) {
        int bin = extract_bits(to_bit_ordered(value), start_bit, num_pass_bits);
        expected_histogram[j * kNumBins + bin] += 1u;
      }
      start_bit += kRadixBits;
    }
    EXPECT_EQ(expected_histogram, actual_histogram);
  }
}

}  // namespace test
}  // namespace breeze
