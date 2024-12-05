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
#include <numeric>
#include <vector>

#include "function_test.h"

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

TYPED_TEST_SUITE(FunctionTest, TestTypes);

TYPED_TEST(FunctionTest, RadixRank) {
  TypeParam src[] = {9,  33, 44, 32, 4,  1,  3,  31, 50, 43, 54, 29, 45, 8,  0,
                     56, 5,  39, 42, 11, 27, 63, 9,  2,  54, 33, 41, 3,  36, 43,
                     47, 3,  0,  14, 21, 18, 21, 18, 57, 40, 52, 3,  4,  21, 32,
                     37, 46, 8,  7,  1,  9,  34, 44, 28, 17, 53, 10, 22, 22, 50,
                     59, 11, 28, 43, 51, 15, 22, 13, 47, 51, 41, 57, 63, 13, 24,
                     38, 51, 37, 15, 35, 29, 16, 35, 26, 55, 42, 33, 32, 7,  13,
                     42, 14, 40, 32, 7,  61, 17, 4,  30, 50, 30, 36, 52, 7,  26,
                     17, 25, 20, 14, 48, 19, 55, 15, 4,  59, 30, 17, 48, 26, 18,
                     58, 2,  50, 59, 60, 46, 1,  12};
  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<int> out(in.size(), 0);

  this->template BlockRadixRank<kBlockThreads, kItemsPerThread,
                                /*RADIX_BITS=*/6>(in, out);

  int expected_result[] = {
      22,  72,  94,  68,  11,  2,   7,   67, 103, 91,  113, 62,  96,  20, 0,
      117, 15,  83,  88,  26,  59,  126, 23, 5,   114, 73,  86,  8,   78, 92,
      99,  9,   1,   32,  48,  43,  49,  44, 118, 84,  110, 10,  12,  50, 69,
      80,  97,  21,  16,  3,   24,  75,  95, 60,  39,  112, 25,  51,  52, 104,
      121, 27,  61,  93,  107, 35,  53,  29, 100, 108, 87,  119, 127, 30, 54,
      82,  109, 81,  36,  76,  63,  38,  77, 56,  115, 89,  74,  70,  17, 31,
      90,  33,  85,  71,  18,  125, 40,  13, 64,  105, 65,  79,  111, 19, 57,
      41,  55,  47,  34,  101, 46,  116, 37, 14,  122, 66,  42,  102, 58, 45,
      120, 6,   106, 123, 124, 98,  4,   28};
  std::vector<int> expected_out(std::begin(expected_result),
                                std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, RadixRankFewItems) {
  TypeParam src[] = {12, 5,  42, 42, 44, 49, 41, 32, 16, 21, 51, 54, 20, 27, 12,
                     16, 29, 1,  23, 24, 20, 51, 3,  55, 59, 61, 57, 45, 62, 57,
                     28, 48, 61, 49, 50, 34, 63, 40, 24, 58, 10, 53, 60, 57, 58,
                     22, 9,  4,  30, 54, 31, 12, 4,  37, 40, 61, 8,  57, 14, 35,
                     33, 14, 40, 61, 37, 18, 33, 32, 30, 41, 5,  25, 63, 11, 22,
                     42, 17, 31, 14, 41, 33, 51, 28, 39, 37, 22, 42, 56, 39, 9,
                     13, 45, 49, 17, 40, 47, 13, 6,  5,  31};
  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<int> out(in.size(), 0);

  this->template BlockRadixRank<kBlockThreads, kItemsPerThread,
                                /*RADIX_BITS=*/6>(in, out);

  int expected_result[] = {
      13, 4,  64, 65, 68, 73, 61, 45, 21, 28, 77, 81, 26, 36, 14, 22, 39,
      0,  32, 33, 27, 78, 1,  83, 91, 93, 85, 69, 97, 86, 37, 72, 94, 74,
      76, 50, 98, 57, 34, 89, 11, 80, 92, 87, 90, 29, 9,  2,  40, 82, 42,
      15, 3,  52, 58, 95, 8,  88, 18, 51, 47, 19, 59, 96, 53, 25, 48, 46,
      41, 62, 5,  35, 99, 12, 30, 66, 23, 43, 20, 63, 49, 79, 38, 55, 54,
      31, 67, 84, 56, 10, 16, 70, 75, 24, 60, 71, 17, 7,  6,  44};
  std::vector<int> expected_out(std::begin(expected_result),
                                std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, RadixSort) {
  TypeParam src[] = {
      664706,   7283945,  30790572, 41729031, 58086583, 99033166, 74066752,
      45600109, 99632667, 211279,   42680291, 22703047, 30319982, 43100887,
      96460159, 23257400, 96962715, 23316382, 59789810, 83194959, 53381293,
      75827488, 57636843, 7184743,  35077319, 90816367, 42193643, 71989577,
      91960720, 93369958, 9942200,  33961178, 86249484, 24723473, 51741384,
      90450193, 506337,   92942655, 13791692, 13393639, 2123526,  87280105,
      5494602,  89002308, 59231705, 72124268, 19717164, 1739645,  72234512,
      99367836, 5659995,  15269072, 35403527, 27060867, 85559139, 55826479,
      20806132, 19143775, 35216845, 27517682, 33106917, 13119302, 87001443,
      4807604,  2298219,  70229144, 80304195, 99099492, 5169670,  6172249,
      11836730, 8306124,  90557875, 41075067, 31225392, 37125881, 89649595,
      38678961, 67589395, 42879775, 3166326,  97228718, 88207584, 5133002,
      1255209,  54025139, 76626674, 48367350, 34868803, 73110392, 43917250,
      92579856, 68656933, 50150510, 35412933, 11936379, 14453176, 80852691,
      31102111, 53472193, 93216884, 3662856,  29557058, 59115812, 81359416,
      79202507, 71443030, 32010919, 39815613, 51592724, 81950730, 11887642,
      31923420, 71171896, 17266095, 2759356,  51851856, 65395926, 21122781,
      30208203, 87656369, 96618376, 22171167, 42230319, 71720531, 12640665,
      58437611, 13449056};

  // flip the sign of every other item if test type is signed
  if (std::is_signed<TypeParam>::value) {
    for (size_t i = 0; i < sizeof(src) / sizeof(TypeParam); i += 2) {
      src[i] = -src[i];
    }
  }

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<TypeParam> out(in.size(), 0);
  std::vector<breeze::utils::NullType> ignored_values(1);

  this->template BlockRadixSort<kBlockThreads, kItemsPerThread,
                                /*RADIX_BITS=*/6>(in, ignored_values, out,
                                                  ignored_values);

  std::vector<TypeParam> expected_out(std::begin(src), std::end(src));
  std::sort(expected_out.begin(), expected_out.end());
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, RadixSortFewItems) {
  TypeParam src[] = {
      25529826, 61919686, 62330816, 15232051, 7849881,  43025218, 78752072,
      52136080, 19555696, 24060484, 87158662, 42535624, 9089185,  95542843,
      27097772, 27473217, 29173119, 59830837, 91700195, 6726776,  32433372,
      24929053, 4416523,  4145406,  62425985, 99128615, 444726,   74847349,
      55855678, 68094486, 85377796, 82624672, 62544042, 16521723, 96705951,
      46774849, 46502724, 54871531, 37327914, 49071663, 12664116, 93305379,
      67184816, 71048336, 17874848, 55288923, 99167712, 85723321, 3330007,
      90055811, 89417277, 51428646, 25651689, 43518173, 32763907, 17912182,
      33572450, 66748560, 68954050, 17936116, 8095076,  58962551, 46897393,
      89798812, 61729759, 4651830,  37429621, 20002125, 95024229, 82835813,
      44979723, 56697769, 15214337, 22675885, 81127517, 97755822, 44890469,
      30482901, 69211711, 6697665,  77684997, 43266817, 21249972, 15919965,
      42864425, 60967663, 89160941, 74473816, 51845647, 82597165, 42832468,
      20517308, 61400782, 55438736, 9495043,  11779110, 87413215, 33301559,
      9692714,  8107692};

  // flip the sign of every other item if test type is signed
  if (std::is_signed<TypeParam>::value) {
    for (size_t i = 0; i < sizeof(src) / sizeof(TypeParam); i += 2) {
      src[i] = -src[i];
    }
  }

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<TypeParam> out(in.size(), 0);
  std::vector<breeze::utils::NullType> ignored_values(1);

  this->template BlockRadixSort<kBlockThreads, kItemsPerThread,
                                /*RADIX_BITS=*/6>(in, ignored_values, out,
                                                  ignored_values);

  std::vector<TypeParam> expected_out(std::begin(src), std::end(src));
  std::sort(expected_out.begin(), expected_out.end());
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, RadixSortKeyValues) {
  TypeParam src[] = {
      664706,   7283945,  30790572, 41729031, 58086583, 99033166, 74066752,
      45600109, 99632667, 211279,   42680291, 22703047, 30319982, 43100887,
      96460159, 23257400, 96962715, 23316382, 59789810, 83194959, 53381293,
      75827488, 57636843, 7184743,  35077319, 90816367, 42193643, 71989577,
      91960720, 93369958, 9942200,  33961178, 86249484, 24723473, 51741384,
      90450193, 506337,   92942655, 13791692, 13393639, 2123526,  87280105,
      5494602,  89002308, 59231705, 72124268, 19717164, 1739645,  72234512,
      99367836, 5659995,  15269072, 35403527, 27060867, 85559139, 55826479,
      20806132, 19143775, 35216845, 27517682, 33106917, 13119302, 87001443,
      4807604,  2298219,  70229144, 80304195, 99099492, 5169670,  6172249,
      11836730, 8306124,  90557875, 41075067, 31225392, 37125881, 89649595,
      38678961, 67589395, 42879775, 3166326,  97228718, 88207584, 5133002,
      1255209,  54025139, 76626674, 48367350, 34868803, 73110392, 43917250,
      92579856, 68656933, 50150510, 35412933, 11936379, 14453176, 80852691,
      31102111, 53472193, 93216884, 3662856,  29557058, 59115812, 81359416,
      79202507, 71443030, 32010919, 39815613, 51592724, 81950730, 11887642,
      31923420, 71171896, 17266095, 2759356,  51851856, 65395926, 21122781,
      30208203, 87656369, 96618376, 22171167, 42230319, 71720531, 12640665,
      58437611, 13449056};

  // flip the sign of every other item if test type is signed
  if (std::is_signed<TypeParam>::value) {
    for (size_t i = 0; i < sizeof(src) / sizeof(TypeParam); i += 2) {
      src[i] = -src[i];
    }
  }

  std::vector<TypeParam> in_keys(std::begin(src), std::end(src));
  std::vector<unsigned> indices(in_keys.size());
  std::iota(indices.begin(), indices.end(), 0);
  std::vector<TypeParam> out_keys(in_keys.size(), 0);
  std::vector<unsigned> out_values(in_keys.size(), 0);

  this->template BlockRadixSort<kBlockThreads, kItemsPerThread,
                                /*RADIX_BITS=*/6>(in_keys, indices, out_keys,
                                                  out_values);

  std::vector<unsigned> sorted_indices = indices;
  std::sort(sorted_indices.begin(), sorted_indices.end(),
            [&src](unsigned a, unsigned b) { return src[a] < src[b]; });
  std::vector<TypeParam> expected_out_keys(in_keys.size());
  std::vector<unsigned> expected_out_values(in_keys.size());
  for (size_t i = 0; i < sorted_indices.size(); ++i) {
    expected_out_keys[i] = in_keys[sorted_indices[i]];
    expected_out_values[i] = indices[sorted_indices[i]];
  }
  EXPECT_EQ(expected_out_keys, out_keys);
  EXPECT_EQ(expected_out_values, out_values);
}
