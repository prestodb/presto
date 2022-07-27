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
