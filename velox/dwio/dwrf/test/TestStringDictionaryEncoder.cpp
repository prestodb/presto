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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "velox/dwio/dwrf/writer/StringDictionaryEncoder.h"

DECLARE_bool(velox_enable_memory_usage_track_in_default_memory_pool);

using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {

class TestStringDictionaryEncoder : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    FLAGS_velox_enable_memory_usage_track_in_default_memory_pool = true;
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(TestStringDictionaryEncoder, AddKey) {
  struct TestCase {
    explicit TestCase(
        const std::vector<folly::StringPiece>& addKeySequence,
        const std::vector<size_t>& encodedSequence)
        : addKeySequence{addKeySequence}, encodedSequence{encodedSequence} {}
    std::vector<folly::StringPiece> addKeySequence;
    std::vector<size_t> encodedSequence;
  };

  std::vector<TestCase> testCases{
      TestCase{{"Hello World!"}, {0}},
      TestCase{{"life", "is", "somewhat", "hard", "enough"}, {0, 1, 2, 3, 4}},
      TestCase{{"a", "b", "c", "d", "e"}, {0, 1, 2, 3, 4}},
      TestCase{{"Tiger!", "Tiger!", "Tiger!"}, {0, 0, 0}},
      TestCase{{"doe", "sow", "sow", "doe", "sow"}, {0, 1, 1, 0, 1}}};

  for (const auto& testCase : testCases) {
    auto pool = memoryManager()->addLeafPool();
    StringDictionaryEncoder stringDictEncoder{*pool, *pool};
    std::vector<size_t> actualEncodedSequence{};
    for (const auto& key : testCase.addKeySequence) {
      actualEncodedSequence.push_back(stringDictEncoder.addKey(key, 0));
    }
    EXPECT_EQ(testCase.encodedSequence, actualEncodedSequence);
  }
}

TEST_F(TestStringDictionaryEncoder, GetIndex) {
  struct TestCase {
    explicit TestCase(
        const std::vector<folly::StringPiece>& addKeySequence,
        const std::vector<folly::StringPiece>& getIndexSequence,
        const std::vector<size_t>& encodedSequence)
        : addKeySequence{addKeySequence},
          getIndexSequence{getIndexSequence},
          encodedSequence{encodedSequence} {}
    std::vector<folly::StringPiece> addKeySequence;
    std::vector<folly::StringPiece> getIndexSequence;
    std::vector<size_t> encodedSequence;
  };

  std::vector<TestCase> testCases{
      TestCase{{"lol"}, {"lol", "lol", "lol"}, {0, 0, 0}},
      TestCase{
          {"Have", "yourself", "a", "nice", "day"},
          {"Have",
           "nice",
           "day",
           "a",
           "yourself",
           "nice",
           "a",
           "day",
           "a",
           "Have",
           "yourself",
           "Have",
           "nice"},
          {0, 3, 4, 2, 1, 3, 2, 4, 2, 0, 1, 0, 3}}};

  for (const auto& testCase : testCases) {
    auto pool = memoryManager()->addLeafPool();
    StringDictionaryEncoder stringDictEncoder{*pool, *pool};
    for (const auto& key : testCase.addKeySequence) {
      stringDictEncoder.addKey(key, 0);
    }

    std::vector<size_t> actualEncodedSequence{};
    for (const auto& key : testCase.getIndexSequence) {
      actualEncodedSequence.push_back(stringDictEncoder.getIndex(key));
    }
    EXPECT_EQ(testCase.encodedSequence, actualEncodedSequence);
  }
}

TEST_F(TestStringDictionaryEncoder, GetCount) {
  struct TestCase {
    explicit TestCase(
        const std::vector<folly::StringPiece>& addKeySequence,
        const std::vector<folly::StringPiece>& getCountSequence,
        const std::vector<size_t>& countSequence)
        : addKeySequence{addKeySequence},
          getCountSequence{getCountSequence},
          countSequence{countSequence} {}
    std::vector<folly::StringPiece> addKeySequence;
    std::vector<folly::StringPiece> getCountSequence;
    std::vector<size_t> countSequence;
  };

  std::vector<TestCase> testCases{
      TestCase{{"Gee"}, {"Gee", "Gee", "Gee", "Gee"}, {1, 1, 1, 1}},
      TestCase{{"Gee", "Gee", "Gee", "Gee"}, {"Gee", "Gee", "Gee"}, {4, 4, 4}},
      TestCase{
          {"Have",
           "nice",
           "day",
           "a",
           "yourself",
           "nice",
           "a",
           "day",
           "a",
           "Have",
           "yourself",
           "Have",
           "nice"},
          {"Have", "yourself", "a", "nice", "day"},
          {3, 2, 3, 3, 2}}};

  for (const auto& testCase : testCases) {
    auto pool = memoryManager()->addLeafPool();
    StringDictionaryEncoder stringDictEncoder{*pool, *pool};
    for (const auto& key : testCase.addKeySequence) {
      stringDictEncoder.addKey(key, 0);
    }

    std::vector<size_t> actualCountSequence{};
    for (const auto& key : testCase.getCountSequence) {
      actualCountSequence.push_back(
          stringDictEncoder.getCount(stringDictEncoder.getIndex(key)));
    }
    EXPECT_EQ(testCase.countSequence, actualCountSequence);
  }
}

TEST_F(TestStringDictionaryEncoder, GetStride) {
  struct TestCase {
    explicit TestCase(
        const std::vector<std::pair<folly::StringPiece, size_t>>&
            addKeySequence,
        const std::vector<folly::StringPiece>& getStrideSequence,
        const std::vector<size_t>& strideSequence)
        : addKeySequence{addKeySequence},
          getStrideSequence{getStrideSequence},
          strideSequence{strideSequence} {}
    std::vector<std::pair<folly::StringPiece, size_t>> addKeySequence;
    std::vector<folly::StringPiece> getStrideSequence;
    std::vector<size_t> strideSequence;
  };

  std::vector<TestCase> testCases{
      TestCase{{{"Gee", 42}}, {"Gee"}, {42}},
      TestCase{
          {{"Gee", 42}, {"Gee", 41}, {"Gee", 40}},
          {"Gee", "Gee", "Gee"},
          {42, 42, 42}},
      TestCase{
          {{"Have", 1},
           {"nice", 3},
           {"day", 4},
           {"a", 6},
           {"yourself", 1},
           {"nice", 5},
           {"a", 3},
           {"day", 8},
           {"a", 11},
           {"Have", 10},
           {"yourself", 5},
           {"Have", 0},
           {"nice", 9}},
          {"Have", "yourself", "a", "nice", "day"},
          {1, 1, 6, 3, 4}}};

  for (const auto& testCase : testCases) {
    auto pool = memoryManager()->addLeafPool();
    StringDictionaryEncoder stringDictEncoder{*pool, *pool};
    for (const auto& kv : testCase.addKeySequence) {
      stringDictEncoder.addKey(kv.first, kv.second);
    }

    std::vector<size_t> actualStrideSequence{};
    for (const auto& key : testCase.getStrideSequence) {
      actualStrideSequence.push_back(
          stringDictEncoder.getStride(stringDictEncoder.getIndex(key)));
    }
    EXPECT_EQ(testCase.strideSequence, actualStrideSequence);
  }
}

std::string genPaddedIntegerString(size_t integer, size_t length) {
  std::string origString = folly::to<std::string>(integer);
  // ASSERT_GE(length, origString.size());
  std::string padding = std::string('0', length - origString.size());
  return padding + origString;
}

TEST_F(TestStringDictionaryEncoder, Clear) {
  auto pool = memoryManager()->addLeafPool();
  StringDictionaryEncoder stringDictEncoder{*pool, *pool};
  std::string baseString{"jjkkll"};
  for (size_t i = 0; i != 2500; ++i) {
    stringDictEncoder.addKey(baseString + genPaddedIntegerString(i, 4), 0);
  }
  auto peakMemory = pool->usedBytes();
  stringDictEncoder.clear();
  EXPECT_EQ(0, stringDictEncoder.size());
  EXPECT_EQ(0, stringDictEncoder.keyIndex_.size());
  EXPECT_EQ(0, stringDictEncoder.keyBytes_.size());
  EXPECT_EQ(0, stringDictEncoder.keyBytes_.capacity());
  EXPECT_EQ(1, stringDictEncoder.keyOffsets_.size());
  EXPECT_EQ(1, stringDictEncoder.keyOffsets_.capacity());
  EXPECT_EQ(0, stringDictEncoder.counts_.size());
  EXPECT_EQ(0, stringDictEncoder.counts_.capacity());
  EXPECT_EQ(0, stringDictEncoder.firstSeenStrideIndex_.size());
  EXPECT_EQ(0, stringDictEncoder.firstSeenStrideIndex_.capacity());
  EXPECT_LT(pool->usedBytes(), peakMemory);
}

TEST_F(TestStringDictionaryEncoder, MemBenchmark) {
  auto pool = memoryManager()->addLeafPool();
  StringDictionaryEncoder stringDictEncoder{*pool, *pool};
  std::string baseString{"jjkkll"};
  for (size_t i = 0; i != 10000; ++i) {
    stringDictEncoder.addKey(baseString + genPaddedIntegerString(i, 4), 0);
  }

  LOG(INFO) << "Total memory bytes: " << pool->usedBytes();
}

TEST_F(TestStringDictionaryEncoder, Limit) {
  auto pool = memoryManager()->addLeafPool();
  StringDictionaryEncoder encoder{*pool, *pool};
  encoder.addKey(folly::StringPiece{"abc"}, 0);
  dwio::common::DataBuffer<char> buf{*pool};
  buf.resize(std::numeric_limits<uint32_t>::max());

  ASSERT_THROW(
      encoder.addKey(
          folly::StringPiece{
              buf.data(), std::numeric_limits<uint32_t>::max() - 3},
          0),
      dwio::common::exception::LoggedException);
}

} // namespace facebook::velox::dwrf
