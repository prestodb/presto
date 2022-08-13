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
#include "velox/exec/tests/utils/MergeTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class TreeOfLosersTest : public testing::Test, public MergeTestBase {
 protected:
  void SetUp() override {
    seed(1);
  }

  void testBoth(int32_t numValues, int32_t numStreams) {
    TestData testData = makeTestData(numValues, numStreams);
    test<TreeOfLosers<TestingStream>>(testData, true);
    test<MergeArray<TestingStream>>(testData, true);
  }
};

TEST_F(TreeOfLosersTest, merge) {
  testBoth(11, 2);
  testBoth(16, 32);
  testBoth(17, 17);
  testBoth(0, 9);
  testBoth(5000000, 37);
  testBoth(500, 1);
}

TEST_F(TreeOfLosersTest, nextWithEquals) {
  constexpr int32_t kNumStreams = 17;
  std::vector<std::vector<uint32_t>> streams(kNumStreams);
  // Each stream is filled with consecutive integers. The probability of each
  // integer i being in any stream depends on the integer i, so that some values
  // will occur in many streams and some in few or none.
  for (auto i = 10000; i >= 0; --i) {
    for (auto stream = 0; stream < streams.size(); ++stream) {
      if (folly::Random::rand32(rng_) % 31 > i % 31) {
        streams[stream].push_back(i);
      }
    }
  }
  std::vector<uint32_t> allNumbers;
  std::vector<std::unique_ptr<TestingStream>> mergeStreams;

  for (auto& stream : streams) {
    allNumbers.insert(allNumbers.end(), stream.begin(), stream.end());
    mergeStreams.push_back(std::make_unique<TestingStream>(std::move(stream)));
  }
  std::sort(allNumbers.begin(), allNumbers.end());
  TreeOfLosers<TestingStream> merge(std::move(mergeStreams));
  bool expectRepeat = false;
  for (auto i = 0; i < allNumbers.size(); ++i) {
    auto result = merge.nextWithEquals();
    if (result.first == nullptr) {
      FAIL() << "Merge ends too soon";
      break;
    }
    auto number = result.first->current()->value();
    EXPECT_EQ(allNumbers[i], number);
    if (expectRepeat) {
      EXPECT_EQ(allNumbers[i], allNumbers[i - 1]);
    } else if (i > 0) {
      EXPECT_NE(allNumbers[i], allNumbers[i - 1]);
    }
    expectRepeat = result.second;
    result.first->pop();
  }
}

TEST_F(TreeOfLosersTest, singleWithEquals) {
  std::vector<uint32_t> allNumbers = {1, 2, 3, 4};
  // TestingStream produces reverse order.
  std::vector<uint32_t> stream = {4, 3, 2, 1};
  std::vector<std::unique_ptr<TestingStream>> mergeStreams;
  mergeStreams.push_back(std::make_unique<TestingStream>(std::move(stream)));
  TreeOfLosers<TestingStream> merge(std::move(mergeStreams));
  for (auto i = 0; i < allNumbers.size(); ++i) {
    auto result = merge.nextWithEquals();
    if (result.first == nullptr) {
      FAIL() << "Merge ends too soon";
      break;
    }
    auto number = result.first->current()->value();
    EXPECT_EQ(allNumbers[i], number);
    EXPECT_FALSE(result.second);
    result.first->pop();
  }
}

TEST_F(TreeOfLosersTest, allDuplicates) {
  const int kNumsPerStream = 40;
  const int kNumStreams = 20;
  const uint32_t kValue = 10;
  for (bool testNextEqual : {false, true}) {
    SCOPED_TRACE(fmt::format("testNextEqual: {}", testNextEqual));
    std::vector<std::unique_ptr<TestingStream>> mergeStreams;
    for (int i = 0; i < kNumStreams; ++i) {
      std::vector<uint32_t> streamNumbers;
      for (int j = 0; j < kNumsPerStream; ++j) {
        streamNumbers.push_back(kValue);
      }
      mergeStreams.push_back(
          std::make_unique<TestingStream>(std::move(streamNumbers)));
    }
    TreeOfLosers<TestingStream> merge(std::move(mergeStreams));
    for (auto i = 0; i < kNumStreams * kNumsPerStream; ++i) {
      TestingStream* stream;
      if (testNextEqual) {
        auto result = merge.nextWithEquals();
        stream = result.first;
        // NOTE: the last stream has no other stream with equal value.
        if (i < (kNumStreams - 1) * kNumsPerStream) {
          ASSERT_TRUE(result.second) << i;
        } else {
          ASSERT_FALSE(result.second) << i;
        }
      } else {
        stream = merge.next();
      }
      ASSERT_TRUE(stream != nullptr) << i;
      ASSERT_EQ(stream->current()->value(), kValue) << i;
      stream->pop();
    }
  }
}

TEST_F(TreeOfLosersTest, allSorted) {
  const int kNumsPerStream = 40;
  const int kNumStreams = 20;
  const uint32_t kStartValue = 10;
  for (bool testNextEqual : {false, true}) {
    SCOPED_TRACE(fmt::format("testNextEqual: {}", testNextEqual));
    std::vector<std::unique_ptr<TestingStream>> mergeStreams;
    for (int i = 0; i < kNumStreams; ++i) {
      std::vector<uint32_t> streamNumbers;
      for (int j = 0; j < kNumsPerStream; ++j) {
        streamNumbers.push_back(kStartValue + i * kNumsPerStream + j);
      }
      std::reverse(streamNumbers.begin(), streamNumbers.end());
      mergeStreams.push_back(
          std::make_unique<TestingStream>(std::move(streamNumbers)));
    }
    TreeOfLosers<TestingStream> merge(std::move(mergeStreams));
    for (auto i = 0; i < kNumStreams * kNumsPerStream; ++i) {
      TestingStream* stream;
      if (testNextEqual) {
        auto result = merge.nextWithEquals();
        stream = result.first;
        ASSERT_FALSE(result.second) << i;
      } else {
        stream = merge.next();
      }
      ASSERT_TRUE(stream != nullptr) << i;
      EXPECT_EQ(stream->current()->value(), kStartValue + i) << i;
      stream->pop();
    }
  }
}

TEST_F(TreeOfLosersTest, allEmpty) {
  for (bool testNextEqual : {false, true}) {
    for (int numStreams : {0, 1, 5, 100}) {
      SCOPED_TRACE(fmt::format(
          "numStreams: {}, testNextEqual", numStreams, testNextEqual));
      std::vector<std::unique_ptr<TestingStream>> mergeStreams;
      for (int i = 0; i < numStreams; ++i) {
        mergeStreams.push_back(
            std::make_unique<TestingStream>(std::vector<uint32_t>{}));
      }
      if (numStreams == 0) {
        EXPECT_ANY_THROW(
            TreeOfLosers<TestingStream> merge(std::move(mergeStreams)));
        continue;
      }
      TreeOfLosers<TestingStream> merge(std::move(mergeStreams));
      if (testNextEqual) {
        ASSERT_TRUE(merge.nextWithEquals().first == nullptr);
      } else {
        ASSERT_TRUE(merge.next() == nullptr);
      }
    }
  }
}

TEST_F(TreeOfLosersTest, randomWithDuplicates) {
  rng_.seed(1);
  for (bool testNextEqual : {false, true}) {
    for (int iter = 0; iter < 10; ++iter) {
      const int numCount = std::max<int>(1, folly::Random::rand32(1000'000));
      const int numStreams = std::max<int>(3, folly::Random::rand32(100));
      SCOPED_TRACE(fmt::format(
          "iter: {}, testNextEqual: {}, numCount: {}, numStreams: {}",
          iter,
          testNextEqual,
          numCount,
          numStreams));
      std::vector<std::vector<uint32_t>> streamNumVectors(numStreams);
      for (int i = 0; i < numCount; ++i) {
        const int streamIndex = folly::Random::rand32(numStreams);
        streamNumVectors[streamIndex].push_back(numCount - i);
        streamNumVectors[(streamIndex + 1) % numStreams].push_back(
            numCount - i);
        streamNumVectors[(streamIndex + 2) % numStreams].push_back(
            numCount - i);
      }
      std::vector<std::unique_ptr<TestingStream>> mergeStreams;
      for (int i = 0; i < numStreams; ++i) {
        mergeStreams.push_back(
            std::make_unique<TestingStream>(std::move(streamNumVectors[i])));
      }
      TreeOfLosers<TestingStream> merge(std::move(mergeStreams));
      for (auto i = 3; i <= 3 * numCount; ++i) {
        TestingStream* stream;
        if (testNextEqual) {
          auto result = merge.nextWithEquals();
          stream = result.first;
          // ASSERT_FALSE(result.second) << i;
          //  We duplicate each number once on different stream.
          ASSERT_EQ(result.second, i % 3 != 2) << i;
        } else {
          stream = merge.next();
        }
        ASSERT_TRUE(stream != nullptr);
        ASSERT_EQ(stream->current()->value(), i / 3);
        stream->pop();
      }
    }
  }
}
