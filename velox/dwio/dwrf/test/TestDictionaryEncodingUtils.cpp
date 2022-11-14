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
#include "velox/dwio/dwrf/writer/DictionaryEncodingUtils.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"

using namespace testing;
using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {
TEST(TestDictionaryEncodingUtils, StringGetSortedIndexLookupTable) {
  struct TestCase {
    explicit TestCase(
        bool sort,
        std::function<bool(const StringDictionaryEncoder&, size_t, size_t)>
            ordering,
        const std::vector<folly::StringPiece>& addKeySequence,
        const std::vector<uint32_t>& lookupTable)
        : sort{sort},
          ordering{ordering},
          addKeySequence{addKeySequence},
          lookupTable{lookupTable} {}
    bool sort;
    std::function<bool(const StringDictionaryEncoder&, size_t, size_t)>
        ordering;
    std::vector<folly::StringPiece> addKeySequence;
    std::vector<uint32_t> lookupTable;
  };

  std::vector<TestCase> testCases{
      TestCase{false, DictionaryEncodingUtils::frequencyOrdering, {}, {}},
      TestCase{
          false, DictionaryEncodingUtils::frequencyOrdering, {"Hello"}, {0}},
      TestCase{
          true,
          DictionaryEncodingUtils::frequencyOrdering,
          {"Hey", "Ho", "Hey"},
          {0, 1}},
      TestCase{
          true,
          DictionaryEncodingUtils::frequencyOrdering,
          {"Hey", "Ho", "Ho"},
          {1, 0}},
      TestCase{
          true,
          DictionaryEncodingUtils::frequencyOrdering,
          {"Have",
           "a",
           "nice",
           "day",
           "eh",
           "a",
           "nice",
           "day",
           "eh",
           "nice",
           "day",
           "eh",
           "day",
           "eh",
           "eh"},
          {4, 3, 2, 1, 0}},
      TestCase{
          false,
          DictionaryEncodingUtils::frequencyOrdering,
          {"Have",
           "a",
           "nice",
           "day",
           "a",
           "nice",
           "day",
           "nice",
           "day",
           "day"},
          {0, 1, 2, 3}}};

  for (const auto& testCase : testCases) {
    auto pool = getDefaultMemoryPool();
    StringDictionaryEncoder stringDictEncoder{*pool, *pool};
    for (const auto& key : testCase.addKeySequence) {
      stringDictEncoder.addKey(key, 0);
    }

    dwio::common::DataBuffer<bool> inDict{*pool};
    dwio::common::DataBuffer<uint32_t> lookupTable{*pool};
    dwio::common::DataBuffer<uint32_t> strideDictCounts{*pool};
    size_t pos = 0;
    EXPECT_EQ(
        stringDictEncoder.size(),
        DictionaryEncodingUtils::getSortedIndexLookupTable(
            stringDictEncoder,
            *pool,
            testCase.sort,
            testCase.ordering,
            false,
            lookupTable,
            inDict,
            strideDictCounts,
            [&](auto buf, auto size) {
              auto sp = stringDictEncoder.getKey(testCase.lookupTable[pos++]);
              ASSERT_EQ(sp.data(), buf);
              ASSERT_EQ(sp.size(), size);
            },
            [&](auto buf, auto size) {
              ASSERT_EQ(stringDictEncoder.size(), size);
              std::vector<uint32_t> dumpOrder;
              for (size_t i = 0; i < testCase.lookupTable.size(); ++i) {
                dumpOrder.push_back(
                    stringDictEncoder.getKey(testCase.lookupTable[i]).size());
              }
              EXPECT_THAT(
                  std::vector<uint32_t>(buf, buf + size),
                  ElementsAreArray(dumpOrder));
            }));
    ASSERT_EQ(pos, stringDictEncoder.size());
    ASSERT_EQ(stringDictEncoder.size(), lookupTable.capacity());
    EXPECT_THAT(
        std::vector<size_t>(
            lookupTable.data(), lookupTable.data() + lookupTable.capacity()),
        ElementsAreArray(testCase.lookupTable));
  }
}

TEST(TestDictionaryEncodingUtils, StringStrideDictOptimization) {
  constexpr size_t kStrideSize{10};
  struct TestCase {
    explicit TestCase(
        bool sort,
        std::function<bool(const StringDictionaryEncoder&, size_t, size_t)>
            ordering,
        const std::vector<folly::StringPiece>& addKeySequence,
        const std::vector<uint32_t>& lookupTable,
        const std::vector<bool>& inDict,
        size_t finalDictSize,
        const std::vector<uint32_t>& strideDictSizes)
        : sort{sort},
          ordering{ordering},
          addKeySequence{addKeySequence},
          lookupTable{lookupTable},
          inDict{inDict},
          finalDictSize{finalDictSize},
          strideDictSizes{strideDictSizes} {}
    bool sort;
    std::function<bool(const StringDictionaryEncoder&, size_t, size_t)>
        ordering;
    std::vector<folly::StringPiece> addKeySequence;
    std::vector<uint32_t> lookupTable;
    std::vector<bool> inDict;
    size_t finalDictSize;
    std::vector<uint32_t> strideDictSizes;

    static bool stableFrequencyOrdering(
        const StringDictionaryEncoder& dictEncoder,
        size_t lhs,
        size_t rhs) {
      // Sort first by reverse order of frequency
      if (dictEncoder.getCount(lhs) != dictEncoder.getCount(rhs)) {
        return dictEncoder.getCount(lhs) > dictEncoder.getCount(rhs);
      }
      // If frequency matches, sort by reverse order of Presence(Index).
      return lhs > rhs;
    }
  };

  std::vector<TestCase> testCases{
      TestCase{
          false,
          DictionaryEncodingUtils::frequencyOrdering,
          {},
          {},
          {},
          0,
          {0}},
      TestCase{
          false,
          DictionaryEncodingUtils::frequencyOrdering,
          {"Hello"},
          {0},
          {false},
          0,
          {1}},
      TestCase{
          true,
          DictionaryEncodingUtils::frequencyOrdering,
          {"Hey", "Ho", "Hey"},
          {0, 0},
          {true, false},
          1,
          {1}},
      TestCase{
          false,
          DictionaryEncodingUtils::frequencyOrdering,
          {"a", "b", "a", "c", "b", "d"},
          {0, 1, 0, 1},
          {true, true, false, false},
          2,
          {2}},
      TestCase{
          true,
          TestCase::stableFrequencyOrdering,
          {"solitude",
           "is",
           "painful",
           "when",
           "one",
           "is",
           "young",
           "but",
           "delightful",
           "when",
           "one",
           "is",
           "more",
           "mature",
           "I",
           "live",
           "in",
           "that",
           "solitude",
           "which",
           "was",
           "painful",
           "in",
           "my",
           "youth ",
           "but",
           "seems",
           "delicious",
           "now",
           "in",
           "the",
           "years",
           "of",
           "my",
           "maturity",
           "now",
           "it",
           "gives",
           "me",
           "great",
           "pleasure",
           "indeed",
           "to",
           "see",
           "the",
           "stubbornness",
           "of",
           "an",
           "incorrigible",
           "nonconformist",
           "so",
           "warmly",
           "acclaimed",
           "and",
           "yet",
           "it",
           "seems",
           "vastly",
           "strange",
           "to",
           "be",
           "known",
           "so",
           "universally",
           "and",
           "yet",
           "be",
           "so",
           "lonely"},
          {17, 2, 16, 15, 14, 1, 13, 0, 5, 4, 3, 2, 1, 1, 0, 2,
           12, 1, 11, 0,  10, 9, 4,  8, 3, 7, 2, 1, 0, 6, 5, 6,
           4,  3, 2,  1,  0,  0, 3,  2, 5, 4, 1, 0, 3, 2, 1, 0},
          {true,  true,  true,  true,  true,  false, true,  false, false, false,
           false, false, true,  false, false, false, true,  false, true,  false,
           true,  true,  false, true,  false, true,  false, false, false, false,
           false, true,  false, false, false, false, false, true,  false, false,
           true,  true,  false, false, true,  false, false, false},
          18,
          {2, 6, 3, 5, 7, 4, 3}},
  };

  for (const auto& testCase : testCases) {
    auto pool = getDefaultMemoryPool();
    StringDictionaryEncoder stringDictEncoder{*pool, *pool};
    size_t rowCount = 0;
    for (const auto& key : testCase.addKeySequence) {
      stringDictEncoder.addKey(key, rowCount++ / kStrideSize);
    }

    dwio::common::DataBuffer<bool> inDict{*pool};
    dwio::common::DataBuffer<uint32_t> lookupTable{*pool};
    dwio::common::DataBuffer<uint32_t> strideDictSizes{
        *pool, rowCount / kStrideSize + 1};

    std::vector<folly::StringPiece> expected{testCase.finalDictSize};
    std::vector<uint32_t> expectedSize(testCase.finalDictSize);
    for (size_t i = 0; i < testCase.lookupTable.size(); ++i) {
      if (testCase.inDict[i]) {
        auto index = testCase.lookupTable[i];
        auto sp = stringDictEncoder.getKey(i);
        expected[index] = sp;
        expectedSize[index] = sp.size();
      }
    }

    size_t pos = 0;
    EXPECT_EQ(
        testCase.finalDictSize,
        DictionaryEncodingUtils::getSortedIndexLookupTable(
            stringDictEncoder,
            *pool,
            testCase.sort,
            testCase.ordering,
            true,
            lookupTable,
            inDict,
            strideDictSizes,
            [&](auto buf, auto size) {
              auto sp = expected[pos++];
              ASSERT_EQ(sp.data(), buf);
              ASSERT_EQ(sp.size(), size);
            },
            [&](auto buf, auto size) {
              ASSERT_EQ(testCase.finalDictSize, size);
              EXPECT_THAT(
                  std::vector<uint32_t>(buf, buf + size),
                  ElementsAreArray(expectedSize));
            }));
    ASSERT_EQ(pos, expected.size());
    ASSERT_EQ(stringDictEncoder.size(), lookupTable.capacity());
    EXPECT_THAT(
        std::vector<size_t>(
            lookupTable.data(), lookupTable.data() + lookupTable.capacity()),
        ElementsAreArray(testCase.lookupTable));
    ASSERT_EQ(stringDictEncoder.size(), inDict.capacity());
    EXPECT_THAT(
        std::vector<bool>(inDict.data(), inDict.data() + inDict.capacity()),
        ElementsAreArray(testCase.inDict));
    ASSERT_EQ(rowCount / kStrideSize + 1, strideDictSizes.capacity());
    EXPECT_THAT(
        std::vector<size_t>(
            strideDictSizes.data(),
            strideDictSizes.data() + strideDictSizes.capacity()),
        ElementsAreArray(testCase.strideDictSizes));
  }
}

} // namespace facebook::velox::dwrf
