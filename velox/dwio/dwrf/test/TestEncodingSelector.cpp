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
#include "velox/dwio/dwrf/writer/EntropyEncodingSelector.h"

using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {

TEST(TestEntropyEncodingSelector, Ctor) {
  auto pool = addDefaultLeafMemoryPool();
  float slightlyOver = 1.0f + std::numeric_limits<float>::epsilon() * 2;
  float slightlyUnder = -std::numeric_limits<float>::epsilon();
  EXPECT_ANY_THROW(
      EntropyEncodingSelector(*pool, slightlyOver, 0.5f, 0, 0.01, 0));
  EXPECT_ANY_THROW(
      EntropyEncodingSelector(*pool, slightlyUnder, 0.5f, 0, 0.01, 0));
  EXPECT_ANY_THROW(
      EntropyEncodingSelector(*pool, 0.5f, slightlyOver, 0, 0.01, 0));
  EXPECT_ANY_THROW(
      EntropyEncodingSelector(*pool, 0.5f, slightlyUnder, 0, 0.01, 0));
  EXPECT_ANY_THROW(
      EntropyEncodingSelector(*pool, 0.5f, 0.5f, 0, slightlyOver, 0));
  EXPECT_ANY_THROW(
      EntropyEncodingSelector(*pool, 0.5f, 0.5f, 0, slightlyUnder, 0));
}

class EntropyEncodingSelectorTest {
  size_t size_;
  std::function<std::string(size_t, size_t)> genData_;
  const float dictionaryKeySizeThreshold_;
  const float entropyKeySizeThreshold_;
  const size_t entropyMinSamples_;
  const float entropyDictSampleFraction_;
  const size_t entropyThreshold_;
  const bool decision_;

 public:
  explicit EntropyEncodingSelectorTest(
      size_t size,
      std::function<std::string(size_t, size_t)> genData,
      const float dictionaryKeySizeThreshold,
      const float entropyKeySizeThreshold,
      const size_t entropyMinSamples,
      const float entropyDictSampleFraction,
      const size_t entropyThreshold,
      bool decision)
      : size_{size},
        genData_{genData},
        dictionaryKeySizeThreshold_{dictionaryKeySizeThreshold},
        entropyKeySizeThreshold_{entropyKeySizeThreshold},
        entropyMinSamples_{entropyMinSamples},
        entropyDictSampleFraction_{entropyDictSampleFraction},
        entropyThreshold_{entropyThreshold},
        decision_{decision} {}

  void runTest() const {
    auto pool = addDefaultLeafMemoryPool();
    StringDictionaryEncoder stringDictEncoder{*pool, *pool};
    dwio::common::DataBuffer<uint32_t> rows{*pool};
    rows.reserve(size_);
    for (size_t i = 0; i != size_; ++i) {
      rows.append(stringDictEncoder.addKey(genData_(i, size_), 0));
    }
    EntropyEncodingSelector selector{
        *pool,
        dictionaryKeySizeThreshold_,
        entropyKeySizeThreshold_,
        entropyMinSamples_,
        entropyDictSampleFraction_,
        entropyThreshold_};
    EXPECT_EQ(decision_, selector.useDictionary(stringDictEncoder, size_));
  }
};

TEST(TestEntropyEncodingSelector, NoHeuristic) {
  class TestCase : public EntropyEncodingSelectorTest {
   public:
    explicit TestCase(
        const std::vector<std::string>& rowValues,
        const float dictionaryKeySizeThreshold,
        bool decision)
        : EntropyEncodingSelectorTest{
              rowValues.size(),
              [rowValues](size_t i, size_t /* unused */) -> std::string {
                return rowValues[i];
              },
              dictionaryKeySizeThreshold,
              0.0f,
              0,
              0.0f,
              0,
              decision} {}
  };

  std::vector<TestCase> testCases{
      // Empty cases are asserted against in lib.
      TestCase{{"Hello World!"}, 1.0f, true},
      TestCase{{"Hello World!"}, 0.0f, false},
      TestCase{{"life", "is", "somewhat", "hard", "enough"}, 1.0f, true},
      TestCase{{"life", "is", "somewhat", "hard", "enough"}, 0.0f, false},
      TestCase{{"Tiger!", "Tiger!", "Tiger!", "Tiger!", "Tiger!"}, 0.2f, true},
      TestCase{
          {"Tiger!", "Tiger!", "Tiger!", "Tiger!", "Tiger!"},
          0.2f - std::numeric_limits<float>::epsilon(),
          false},
      TestCase{{"doe", "sow", "sow", "doe", "sow"}, 0.4f, true},
      TestCase{
          {"doe", "sow", "sow", "doe", "sow"},
          0.4f - std::numeric_limits<float>::epsilon(),
          false},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(TestEntropyEncodingSelector, NoSampling) {
  class TestCase : public EntropyEncodingSelectorTest {
   public:
    explicit TestCase(
        const std::vector<std::string>& rowValues,
        const float dictionaryKeySizeThreshold,
        const size_t entropyThreshold,
        bool decision)
        : EntropyEncodingSelectorTest{
              rowValues.size(),
              [rowValues](size_t i, size_t /* unused */) -> std::string {
                return rowValues[i];
              },
              dictionaryKeySizeThreshold,
              1.0f,
              std::numeric_limits<size_t>::max(),
              0.0f,
              entropyThreshold,
              decision} {}
  };

  std::vector<TestCase> testCases{
      // Empty cases are asserted against in lib.
      TestCase{{"Hello World!"}, 1.0f, 0, true},
      TestCase{{"Hello World!"}, 0.0f, 0, false},
      TestCase{{"Hello World!"}, 1.0f, 20, false},
      TestCase{{"a", "b", "c", "d", "e", "f", "g"}, 1.0f, 6, true},
      TestCase{{"a", "b", "c", "d", "e", "f", "g"}, 1.0f, 7, false},
      TestCase{{"a", "b", "c", "d", "e", "f", "g"}, 0.0f, 0, false},
      TestCase{
          {"h", "e", "l", "l", "o", "w", "o", "l", "r", "d"}, 1.0f, 6, true},
      TestCase{
          {"h", "e", "l", "l", "o", "w", "o", "l", "r", "d"}, 1.0f, 7, false},
      TestCase{
          {"h", "e", "l", "l", "o", "w", "o", "l", "r", "d"},
          0.3f - std::numeric_limits<float>::epsilon(),
          6,
          false},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

std::string alphabeticRoundRobin(size_t index, size_t size) {
  char element = 97 + index % 26;
  return std::string(size % (index + 1), element);
}

TEST(TestEntropyEncodingSelector, Sampling) {
  class TestCase : public EntropyEncodingSelectorTest {
   public:
    explicit TestCase(
        size_t size,
        std::function<std::string(size_t, size_t)> genData,
        const size_t entropyThreshold,
        bool decision)
        : EntropyEncodingSelectorTest{
              size,
              genData,
              1.0f,
              1.0f,
              0,
              0.5f,
              entropyThreshold,
              decision} {}
  };

  std::vector<TestCase> testCases{
      // actually missing parts of the alphabet.
      TestCase{50, alphabeticRoundRobin, 25, false},
      TestCase{1000, alphabeticRoundRobin, 25, true},
      TestCase{1000, alphabeticRoundRobin, 30, false},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

} // namespace facebook::velox::dwrf
