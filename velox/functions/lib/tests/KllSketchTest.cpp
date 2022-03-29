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

#include "velox/functions/lib/KllSketch.h"
#include <gtest/gtest.h>

namespace facebook::velox::functions::kll::test {
namespace {

// Error bound for k = 200.
constexpr double kEpsilon = 0.0133;

// Generate linearly spaced values between [0, 1].
std::vector<double> linspace(int len) {
  VELOX_DCHECK_GE(len, 2);
  std::vector<double> out(len);
  double step = 1.0 / (len - 1);
  for (int i = 0; i < len; ++i) {
    out[i] = i * step;
  }
  return out;
}

TEST(KllSketchTest, oneItem) {
  KllSketch<double> kll;
  EXPECT_EQ(kll.totalCount(), 0);
  kll.insert(1.0);
  EXPECT_EQ(kll.totalCount(), 1);
  EXPECT_EQ(kll.estimateQuantile(0.0), 1.0);
  EXPECT_EQ(kll.estimateQuantile(0.5), 1.0);
  EXPECT_EQ(kll.estimateQuantile(1.0), 1.0);
}

TEST(KllSketchTest, exactMode) {
  constexpr int N = 128;
  KllSketch<int> kll(N);
  for (int i = 0; i < N; ++i) {
    kll.insert(i);
    EXPECT_EQ(kll.totalCount(), i + 1);
  }
  EXPECT_EQ(kll.estimateQuantile(0.0), 0);
  EXPECT_EQ(kll.estimateQuantile(0.5), N / 2);
  EXPECT_EQ(kll.estimateQuantile(1.0), N - 1);
  auto q = linspace(N);
  auto v = kll.estimateQuantiles(folly::Range(q.begin(), q.end()));
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(v[i], i);
  }
}

TEST(KllSketchTest, estimationMode) {
  constexpr int N = 1e5;
  constexpr int M = 1001;
  KllSketch<double> kll(200, {}, 0);
  for (int i = 0; i < N; ++i) {
    kll.insert(i);
    EXPECT_EQ(kll.totalCount(), i + 1);
  }
  EXPECT_EQ(kll.estimateQuantile(0.0), 0);
  EXPECT_EQ(kll.estimateQuantile(1.0), N - 1);
  auto q = linspace(M);
  auto v = kll.estimateQuantiles(folly::Range(q.begin(), q.end()));
  ASSERT_TRUE(std::is_sorted(std::begin(v), std::end(v)));
  for (int i = 0; i < M; ++i) {
    EXPECT_NEAR(q[i], v[i] / N, kEpsilon);
  }
}

TEST(KllSketchTest, randomInput) {
  constexpr int N = 1e5;
  constexpr int M = 1001;
  KllSketch<double> kll(kDefaultK, {}, 0);
  std::default_random_engine gen(0);
  std::normal_distribution<> dist;
  double values[N];
  for (int i = 0; i < N; ++i) {
    values[i] = dist(gen);
    kll.insert(values[i]);
  }
  EXPECT_EQ(kll.totalCount(), N);
  std::sort(std::begin(values), std::end(values));
  auto q = linspace(M);
  auto v = kll.estimateQuantiles(folly::Range(q.begin(), q.end()));
  ASSERT_TRUE(std::is_sorted(std::begin(v), std::end(v)));
  for (int i = 0; i < M; ++i) {
    auto it = std::lower_bound(std::begin(values), std::end(values), v[i]);
    double actualQ = 1.0 * (it - std::begin(values)) / N;
    EXPECT_NEAR(q[i], actualQ, kEpsilon);
  }
}

TEST(KllSketchTest, merge) {
  constexpr int N = 1e4;
  constexpr int M = 1001;
  KllSketch<double> kll1(kDefaultK, {}, 0);
  KllSketch<double> kll2(kDefaultK, {}, 0);
  for (int i = 0; i < N; ++i) {
    kll1.insert(i);
    kll2.insert(2 * N - i - 1);
  }
  kll1.merge(folly::Range(&kll2, 1));
  EXPECT_EQ(kll1.totalCount(), 2 * N);
  auto q = linspace(M);
  auto v = kll1.estimateQuantiles(folly::Range(q.begin(), q.end()));
  ASSERT_TRUE(std::is_sorted(std::begin(v), std::end(v)));
  for (int i = 0; i < M; ++i) {
    EXPECT_NEAR(q[i], v[i] / (2 * N), kEpsilon);
  }
}

TEST(KllSketchTest, mergeRandom) {
  constexpr int N = 1e4;
  constexpr int M = 1001;
  std::default_random_engine gen(0);
  std::uniform_int_distribution<> distN(1, N);
  int n1 = distN(gen), n2 = distN(gen);
  std::vector<double> values;
  values.reserve(n1 + n2);
  KllSketch<double> kll1(kDefaultK, {}, 0);
  KllSketch<double> kll2(kDefaultK, {}, 0);
  std::normal_distribution<> distV;
  for (int i = 0; i < n1; ++i) {
    double v = distV(gen);
    values.push_back(v);
    kll1.insert(v);
  }
  for (int i = 0; i < n2; ++i) {
    double v = distV(gen);
    values.push_back(v);
    kll2.insert(v);
  }
  std::sort(values.begin(), values.end());
  kll1.merge(folly::Range(&kll2, 1));
  EXPECT_EQ(kll1.totalCount(), n1 + n2);
  auto q = linspace(M);
  auto v = kll1.estimateQuantiles(folly::Range(q.begin(), q.end()));
  ASSERT_TRUE(std::is_sorted(std::begin(v), std::end(v)));
  for (int i = 0; i < M; ++i) {
    auto it = std::lower_bound(std::begin(values), std::end(values), v[i]);
    double actualQ = 1.0 * (it - std::begin(values)) / values.size();
    EXPECT_NEAR(q[i], actualQ, kEpsilon);
  }
}

TEST(KllSketchTest, mergeMultiple) {
  constexpr int N = 1e4;
  constexpr int M = 1001;
  constexpr int kSketchCount = 10;
  std::vector<KllSketch<double>> sketches;
  for (int i = 0; i < kSketchCount; ++i) {
    KllSketch<double> kll(kDefaultK, {}, 0);
    for (int j = 0; j < N; ++j) {
      kll.insert(j + i * N);
    }
    sketches.push_back(std::move(kll));
  }
  KllSketch<double> kll(kDefaultK, {}, 0);
  kll.merge(folly::Range(sketches.begin(), sketches.end()));
  EXPECT_EQ(kll.totalCount(), N * kSketchCount);
  auto q = linspace(M);
  auto v = kll.estimateQuantiles(folly::Range(q.begin(), q.end()));
  ASSERT_TRUE(std::is_sorted(std::begin(v), std::end(v)));
  for (int i = 0; i < M; ++i) {
    EXPECT_NEAR(q[i], v[i] / (N * kSketchCount), kEpsilon);
  }
}

TEST(KllSketchTest, mergeEmpty) {
  KllSketch<double> kll, kll2;
  kll.insert(1.0);
  kll.merge(folly::Range(&kll2, 1));
  EXPECT_EQ(kll.totalCount(), 1);
  EXPECT_EQ(kll.estimateQuantile(0.5), 1.0);
  kll2.merge(folly::Range(&kll, 1));
  EXPECT_EQ(kll2.totalCount(), 1);
  EXPECT_EQ(kll2.estimateQuantile(0.5), 1.0);
}

TEST(KllSketchTest, kFromEpsilon) {
  EXPECT_EQ(kFromEpsilon(kEpsilon), kDefaultK);
}

} // namespace
} // namespace facebook::velox::functions::kll::test
