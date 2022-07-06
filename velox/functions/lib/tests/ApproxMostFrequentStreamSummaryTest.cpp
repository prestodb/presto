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

#include "velox/functions/lib/ApproxMostFrequentStreamSummary.h"
#include "velox/functions/lib/ZetaDistribution.h"

namespace facebook::velox::functions {
namespace {

int capacity(int k, double alpha) {
  return k * pow(k / alpha, 1 / alpha);
}

TEST(ApproxMostFrequentStreamSummaryTest, exact) {
  const int capacity = 10;
  for (int totalCount : {0, 5, 10}) {
    for (int k : {0, 5, 10, 15}) {
      VLOG(1) << "capacity=" << capacity << " totalCount=" << totalCount
              << " k=" << k;
      ApproxMostFrequentStreamSummary<int> summary;
      summary.setCapacity(capacity);
      for (int i = 1; i <= totalCount; ++i) {
        for (int j = 0; j < i; ++j) {
          summary.insert(i);
        }
      }
      auto topK = summary.topK(k);
      k = std::min({k, capacity, totalCount});
      ASSERT_EQ(topK.size(), k);
      for (int i = 0; i < k; ++i) {
        std::pair<int, int64_t> expected{totalCount - i, totalCount - i};
        EXPECT_EQ(topK[i], expected);
      }
    }
  }
}

TEST(ApproxMostFrequentStreamSummaryTest, exactRandom) {
  constexpr int M = 1000;
  ApproxMostFrequentStreamSummary<int> summary;
  summary.setCapacity(M);
  int freq[M + 1]{};
  ZetaDistribution dist(1.02, M);
  std::default_random_engine gen(0);
  for (int i = 0; i < 100'000; ++i) {
    int v = dist(gen);
    ++freq[v];
    summary.insert(v);
  }
  auto topK = summary.topK(M);
  for (int i = 1; i < topK.size(); ++i) {
    EXPECT_GE(topK[i - 1].second, topK[i].second);
  }
  for (auto [v, c] : topK) {
    EXPECT_GT(c, 0);
    EXPECT_EQ(c, freq[v]);
  }
  EXPECT_EQ(topK.size() + std::count(freq + 1, freq + M + 1, 0), M);
}

TEST(ApproxMostFrequentStreamSummaryTest, approx) {
  constexpr int kCardinality = 1000;
  constexpr double kAlpha = 1.01;
  constexpr int K = 10;
  const int kCapacity = std::min(kCardinality, capacity(K, kAlpha));
  unsigned seed = 0;
  // seed = std::random_device()();
  VLOG(1) << "capacity=" << kCapacity << " seed=" << seed;
  std::default_random_engine gen(seed);
  ZetaDistribution dist(kAlpha, kCardinality);
  ApproxMostFrequentStreamSummary<int> summary;
  summary.setCapacity(kCapacity);
  int freq[kCardinality + 1]{};
  for (int i = 0; i < 100'000; ++i) {
    int v = dist(gen);
    ++freq[v];
    summary.insert(v);
  }
  std::vector<std::pair<int, int64_t>> expected;
  for (int i = 1; i <= kCardinality; ++i) {
    if (freq[i] > 0) {
      expected.emplace_back(i, freq[i]);
    }
  }
  std::sort(expected.begin(), expected.end(), [](auto& x, auto& y) {
    return x.second > y.second;
  });
  ASSERT_GT(expected.size(), K);
  expected.resize(K);
  auto actual = summary.topK(K);
  ASSERT_EQ(actual.size(), K);
  for (int i = 0; i < K; ++i) {
    EXPECT_EQ(actual[i].first, expected[i].first);
    EXPECT_GE(actual[i].second, expected[i].second);
  }
}

TEST(ApproxMostFrequentStreamSummaryTest, serialize) {
  ApproxMostFrequentStreamSummary<int> summary;
  summary.setCapacity(100);
  for (int i = 1; i <= 100; ++i) {
    for (int j = 0; j < i; ++j) {
      summary.insert(i);
    }
  }
  std::vector<char> data(summary.serializedByteSize());
  summary.serialize(data.data());
  ApproxMostFrequentStreamSummary<int> summary2;
  summary2.setCapacity(100);
  summary2.mergeSerialized(data.data());
  EXPECT_EQ(summary.topK(10), summary2.topK(10));
}

TEST(ApproxMostFrequentStreamSummaryTest, serializeStringView) {
  std::vector<std::string> strings;
  for (int i = 0; i < 26; ++i) {
    strings.emplace_back(StringView::kInlineSize, 'a' + i);
    strings.emplace_back(StringView::kInlineSize + 1, 'a' + i);
  }
  ApproxMostFrequentStreamSummary<StringView> summary;
  summary.setCapacity(100);
  for (int i = 1; i <= 52; ++i) {
    for (int j = 0; j < i; ++j) {
      summary.insert(StringView(strings[i - 1]));
    }
  }
  std::vector<char> data(summary.serializedByteSize());
  summary.serialize(data.data());
  ApproxMostFrequentStreamSummary<StringView> summary2;
  summary2.setCapacity(100);
  summary2.mergeSerialized(data.data());
  auto topK = summary2.topK(10);
  EXPECT_EQ(topK, summary.topK(10));
  for (auto& s : strings) {
    for (char& c : s) {
      c = toupper(c);
    }
  }
  EXPECT_NE(summary.topK(10), topK);
  // Serialzation should keep deep copy of original strings.
  EXPECT_EQ(summary2.topK(10), topK);
}

TEST(ApproxMostFrequentStreamSummaryTest, mergeSerialized) {
  constexpr int kSummaryCount = 10;
  constexpr int kCapacity = 30;
  std::vector<char> data[kSummaryCount];
  std::default_random_engine gen(0);
  ZetaDistribution dist(1.02, 100);
  int64_t freq[101]{};
  ApproxMostFrequentStreamSummary<int> summary;
  summary.setCapacity(kCapacity);
  for (int i = 0; i < kSummaryCount; ++i) {
    ApproxMostFrequentStreamSummary<int> summary2;
    summary2.setCapacity(kCapacity);
    for (int j = 0; j < 100; ++j) {
      int v = dist(gen);
      summary2.insert(v);
      ++freq[v];
    }
    data[i].resize(summary2.serializedByteSize());
    summary2.serialize(data[i].data());
    summary.mergeSerialized(data[i].data());
  }
  auto topK = summary.topK(3);
  ASSERT_EQ(topK.size(), 3);
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(topK[i], std::make_pair(i + 1, freq[i + 1]));
  }
}

} // namespace
} // namespace facebook::velox::functions
