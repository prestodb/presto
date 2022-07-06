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

#include <gtest/gtest.h>

#include "velox/functions/lib/ZetaDistribution.h"

namespace facebook::velox::functions {
namespace {

TEST(ZetaDistributionTest, basic) {
  constexpr int N = 200;
  ZetaDistribution dist(1.1, N);
  int freq[N]{};
  unsigned seed = 0;
  // seed = std::random_device()();
  std::default_random_engine gen(seed);
  for (int i = 0; i < 100'000; ++i) {
    int k = dist(gen);
    ASSERT_GE(k, 1);
    ASSERT_LE(k, N);
    ++freq[k - 1];
  }
  // Strictly decreasing for the first 10.
  for (int i = 1; i < 10; ++i) {
    EXPECT_GT(freq[i - 1], freq[i]) << i;
  }
  // Moving window sum decreasing for the first 50.
  int sum = std::accumulate(freq, freq + 10, 0);
  for (int i = 10; i < 50; ++i) {
    int newSum = sum - freq[i - 10] + freq[i];
    EXPECT_LT(newSum, sum) << i;
    sum = newSum;
  }
  // The rest is long tail with few hits.
  for (int i = 50; i < N; ++i) {
    EXPECT_LT(freq[i], freq[40]) << i;
  }
}

} // namespace
} // namespace facebook::velox::functions
