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

#include <algorithm>
#include <climits>
#include <numeric>
#include <random>
#include <vector>

#include "algorithm_test.h"

#if defined(PLATFORM_METAL) || defined(PLATFORM_OPENCL)
using TestTypes = ::testing::Types<int, unsigned>;
#else
using TestTypes =
    ::testing::Types<int, unsigned, long long, unsigned long long>;
#endif

// OpenMP is limited to 32 threads or less right now
#if defined(PLATFORM_OPENMP)
constexpr int kBlockThreads = 32;
constexpr int kItemsPerThread = 4;
#else
constexpr int kBlockThreads = 64;
constexpr int kItemsPerThread = 2;
#endif

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

TYPED_TEST_SUITE(AlgorithmTest, TestTypes);

TYPED_TEST(AlgorithmTest, RadixSortHistogram) {
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
  constexpr int kTileSize = 1;
  constexpr int kTileItems = kBlockItems * kTileSize;
  constexpr int kRadixBits = 6;
  constexpr int kEndBit = sizeof(TypeParam) * /*BITS_PER_BYTE=*/8;
  constexpr int kNumPasses = (kEndBit + kRadixBits - 1) / kRadixBits;
  constexpr int kNumBins = 1 << kRadixBits;
  constexpr int kHistogramSize = kNumBins * kNumPasses;

  std::vector<TypeParam> in(400, 0);
  std::iota(in.begin(), in.end(), std::is_signed<TypeParam>::value ? -199 : 1);
  static std::minstd_rand rng;
  std::shuffle(in.begin(), in.end(), rng);
  std::vector<unsigned> out(kHistogramSize, 0);
  int num_blocks = (in.size() + kTileItems - 1) / kTileItems;

  this->template RadixSortHistogram<kBlockThreads, kItemsPerThread, kTileSize,
                                    kRadixBits>(in, out, num_blocks);

  std::vector<unsigned> expected_result(kHistogramSize);
  int start_bit = 0;
  for (int j = 0; j < kNumPasses; ++j) {
    int num_pass_bits = std::min(kRadixBits, kEndBit - start_bit);
    for (const auto& value : in) {
      int bin = extract_bits(to_bit_ordered(value), start_bit, num_pass_bits);
      expected_result[j * kNumBins + bin] += 1u;
    }
    start_bit += kRadixBits;
  }
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(AlgorithmTest, RadixSortHistogramLargeTiles) {
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
  constexpr int kTileSize = 4;
  constexpr int kTileItems = kBlockItems * kTileSize;
  constexpr int kRadixBits = 6;
  constexpr int kEndBit = sizeof(TypeParam) * /*BITS_PER_BYTE=*/8;
  constexpr int kNumPasses = (kEndBit + kRadixBits - 1) / kRadixBits;
  constexpr int kNumBins = 1 << kRadixBits;
  constexpr int kHistogramSize = kNumBins * kNumPasses;

  std::vector<TypeParam> in(800, 0);
  std::iota(in.begin(), in.end(), std::is_signed<TypeParam>::value ? -399 : 1);
  static std::minstd_rand rng;
  std::shuffle(in.begin(), in.end(), rng);
  std::vector<unsigned> out(kHistogramSize, 0);
  int num_blocks = (in.size() + kTileItems - 1) / kTileItems;

  this->template RadixSortHistogram<kBlockThreads, kItemsPerThread, kTileSize,
                                    kRadixBits>(in, out, num_blocks);

  std::vector<unsigned> expected_result(kHistogramSize);
  int start_bit = 0;
  for (int j = 0; j < kNumPasses; ++j) {
    int num_pass_bits = std::min(kRadixBits, kEndBit - start_bit);
    for (const auto& value : in) {
      int bin = extract_bits(to_bit_ordered(value), start_bit, num_pass_bits);
      expected_result[j * kNumBins + bin] += 1u;
    }
    start_bit += kRadixBits;
  }
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(AlgorithmTest, RadixSort) {
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
  constexpr int kRadixBits = 6;
  constexpr int kStartBit = 0;
  constexpr int kNumBins = 1 << kRadixBits;

  std::vector<TypeParam> in(400, 0);
  std::iota(in.begin(), in.end(), std::is_signed<TypeParam>::value ? -199 : 1);
  static std::minstd_rand rng;
  std::shuffle(in.begin(), in.end(), rng);

  std::vector<unsigned> in_histogram(kNumBins);
  for (const auto& value : in) {
    int bin = extract_bits(to_bit_ordered(value), kStartBit, kRadixBits);
    in_histogram[bin] += 1u;
  }
  unsigned sum = 0;
  std::vector<unsigned> in_offsets(kNumBins);
  for (size_t i = 0; i < kNumBins; ++i) {
    in_offsets[i] = sum;
    sum += in_histogram[i];
  }
  std::vector<TypeParam> out(in.size(), 0);
  int num_blocks = (in.size() + kBlockItems - 1) / kBlockItems;
  std::vector<int> next_block_idx(1);
  std::vector<unsigned> blocks(num_blocks * kNumBins);
  std::vector<breeze::utils::NullType> ignored_values(1);

  this->template RadixSort<kBlockThreads, kItemsPerThread, kRadixBits>(
      in, ignored_values, in_offsets, kStartBit, kRadixBits, out,
      ignored_values, next_block_idx, blocks, num_blocks);

  std::vector<TypeParam> expected_result = in;
  std::stable_sort(
      expected_result.begin(), expected_result.end(),
      [start_bit = kStartBit, num_pass_bits = kRadixBits](TypeParam a,
                                                          TypeParam b) {
        return extract_bits(to_bit_ordered(a), start_bit, num_pass_bits) <
               extract_bits(to_bit_ordered(b), start_bit, num_pass_bits);
      });
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(AlgorithmTest, RadixSortKeyValues) {
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
  constexpr int kRadixBits = 6;
  constexpr int kStartBit = 0;
  constexpr int kNumBins = 1 << kRadixBits;

  std::vector<TypeParam> in_keys(400, 0);
  std::iota(in_keys.begin(), in_keys.end(),
            std::is_signed<TypeParam>::value ? -199 : 1);
  static std::minstd_rand rng;
  std::shuffle(in_keys.begin(), in_keys.end(), rng);

  std::vector<unsigned> in_histogram(kNumBins);
  for (const auto& value : in_keys) {
    int bin = extract_bits(to_bit_ordered(value), kStartBit, kRadixBits);
    in_histogram[bin] += 1u;
  }
  unsigned sum = 0;
  std::vector<unsigned> in_offsets(kNumBins);
  for (size_t i = 0; i < kNumBins; ++i) {
    in_offsets[i] = sum;
    sum += in_histogram[i];
  }
  std::vector<unsigned> indices(in_keys.size());
  std::iota(indices.begin(), indices.end(), 0);
  std::vector<TypeParam> out_keys(in_keys.size(), 0);
  std::vector<unsigned> out_values(in_keys.size(), 0);
  int num_blocks = (in_keys.size() + kBlockItems - 1) / kBlockItems;
  std::vector<int> next_block_idx(1);
  std::vector<unsigned> blocks(num_blocks * kNumBins);

  this->template RadixSort<kBlockThreads, kItemsPerThread, kRadixBits>(
      in_keys, indices, in_offsets, kStartBit, kRadixBits, out_keys, out_values,
      next_block_idx, blocks, num_blocks);

  std::vector<unsigned> sorted_indices = indices;
  std::stable_sort(sorted_indices.begin(), sorted_indices.end(),
                   [&in_keys, start_bit = kStartBit,
                    num_pass_bits = kRadixBits](unsigned a, unsigned b) {
                     return extract_bits(to_bit_ordered(in_keys[a]), start_bit,
                                         num_pass_bits) <
                            extract_bits(to_bit_ordered(in_keys[b]), start_bit,
                                         num_pass_bits);
                   });
  std::vector<TypeParam> expected_out_keys(in_keys.size());
  std::vector<unsigned> expected_out_values(in_keys.size());
  for (size_t i = 0; i < sorted_indices.size(); ++i) {
    expected_out_keys[i] = in_keys[sorted_indices[i]];
    expected_out_values[i] = indices[sorted_indices[i]];
  }
  EXPECT_EQ(expected_out_keys, out_keys);
  EXPECT_EQ(expected_out_values, out_values);
}
