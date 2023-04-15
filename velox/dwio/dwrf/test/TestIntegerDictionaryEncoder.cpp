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
#include "velox/dwio/dwrf/writer/IntegerDictionaryEncoder.h"

using namespace testing;
using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {

TEST(TestIntegerDictionaryEncoder, AddKey) {
  struct TestCase {
    explicit TestCase(
        const std::vector<int64_t>& addKeySequence,
        const std::vector<size_t>& encodedSequence)
        : addKeySequence{addKeySequence}, encodedSequence{encodedSequence} {}
    std::vector<int64_t> addKeySequence;
    std::vector<size_t> encodedSequence;
  };

  std::vector<TestCase> testCases{
      TestCase{{42}, {0}},
      TestCase{{-2, -1, 0, 1, 2}, {0, 1, 2, 3, 4}},
      TestCase{{5, 4, 3, 2, 1}, {0, 1, 2, 3, 4}},
      TestCase{{42, 42, 42, 42}, {0, 0, 0, 0}},
      TestCase{{-2, 2, 2, -2, 2}, {0, 1, 1, 0, 1}}};

  for (const auto& testCase : testCases) {
    auto pool = addDefaultLeafMemoryPool();
    IntegerDictionaryEncoder<int64_t> intDictEncoder{*pool, *pool};
    std::vector<size_t> actualEncodedSequence{};
    for (const auto& key : testCase.addKeySequence) {
      actualEncodedSequence.push_back(intDictEncoder.addKey(key));
    }
    EXPECT_EQ(testCase.encodedSequence, actualEncodedSequence);
  }
}

TEST(TestIntegerDictionaryEncoder, GetCount) {
  struct TestCase {
    explicit TestCase(
        const std::vector<int64_t>& addKeySequence,
        const std::vector<int64_t>& getCountSequence,
        const std::vector<size_t>& countSequence)
        : addKeySequence{addKeySequence},
          getCountSequence{getCountSequence},
          countSequence{countSequence} {}
    std::vector<int64_t> addKeySequence;
    std::vector<int64_t> getCountSequence;
    std::vector<size_t> countSequence;
  };

  std::vector<TestCase> testCases{
      TestCase{{42}, {42, 42, 42, 42}, {1, 1, 1, 1}},
      TestCase{{42, 42, 42, 42}, {42, 42, 42}, {4, 4, 4}},
      TestCase{
          {-2, 1, 2, 0, -1, 1, 0, 2, 0, -2, -1, -2, 1},
          {-2, -1, 0, 1, 2},
          {3, 2, 3, 3, 2}}};

  for (const auto& testCase : testCases) {
    auto pool = addDefaultLeafMemoryPool();
    IntegerDictionaryEncoder<int64_t> intDictEncoder{*pool, *pool};
    for (const auto& key : testCase.addKeySequence) {
      intDictEncoder.addKey(key);
    }

    std::vector<size_t> actualCountSequence{};
    for (const auto& key : testCase.getCountSequence) {
      actualCountSequence.push_back(
          intDictEncoder.getCount(intDictEncoder.getIndex(key)));
    }
    EXPECT_EQ(testCase.countSequence, actualCountSequence);
  }
}

TEST(TestIntegerDictionaryEncoder, GetTotalCount) {
  struct TestCase {
    explicit TestCase(
        const std::vector<int64_t>& addKeySequence,
        size_t totalCount)
        : addKeySequence{addKeySequence}, totalCount{totalCount} {}
    std::vector<int64_t> addKeySequence;
    size_t totalCount;
  };

  std::vector<TestCase> testCases{
      TestCase{{}, 0},
      TestCase{{42}, 1},
      TestCase{{42, 42, 42, 42}, 4},
      TestCase{{-2, 1, 2, 0, -1, 1, 0, 2, 0, -2, -1, -2, 1}, 13}};

  for (const auto& testCase : testCases) {
    auto pool = addDefaultLeafMemoryPool();
    IntegerDictionaryEncoder<int64_t> intDictEncoder{*pool, *pool};
    for (const auto& key : testCase.addKeySequence) {
      intDictEncoder.addKey(key);
    }

    EXPECT_EQ(testCase.totalCount, intDictEncoder.getTotalCount());
  }
}

TEST(TestIntegerDictionaryEncoder, Clear) {
  auto pool = addDefaultLeafMemoryPool();
  {
    IntegerDictionaryEncoder<int64_t> intDictEncoder{*pool, *pool};
    EXPECT_EQ(1, intDictEncoder.refCount_);
    EXPECT_EQ(0, intDictEncoder.clearCount_);
    for (size_t i = 0; i != 2500; ++i) {
      intDictEncoder.addKey(i);
    }
    EXPECT_EQ(2500, intDictEncoder.size());
    EXPECT_EQ(2500, intDictEncoder.keyIndex_.size());
    EXPECT_EQ(2500, intDictEncoder.keys_.size());
    EXPECT_EQ(2500, intDictEncoder.counts_.size());
    EXPECT_EQ(2500, intDictEncoder.getTotalCount());
    EXPECT_EQ(1, intDictEncoder.refCount_);
    EXPECT_EQ(0, intDictEncoder.clearCount_);
    // auto peakMemory = pool.getCurrentBytes();
    intDictEncoder.clear();
    EXPECT_EQ(0, intDictEncoder.size());
    EXPECT_EQ(0, intDictEncoder.keyIndex_.size());
    EXPECT_EQ(0, intDictEncoder.keys_.size());
    EXPECT_EQ(0, intDictEncoder.keys_.capacity());
    EXPECT_EQ(0, intDictEncoder.counts_.size());
    EXPECT_EQ(0, intDictEncoder.counts_.capacity());
    EXPECT_EQ(0, intDictEncoder.getTotalCount());
    EXPECT_EQ(1, intDictEncoder.refCount_);
    EXPECT_EQ(0, intDictEncoder.clearCount_);
    // Folly's F14 map when compiled with ASAN, it re-allocates after
    // deallocating the memory. So the overall bytes allocated does not go
    // down. On test experiment it deallocated 4K and rellocated 64K.
    // When not compiled with ASAN, it correctly frees up the memory.
    // so disabling this check in the test for now.
    // EXPECT_LT(pool.getCurrentBytes(), peakMemory);
  }
  {
    IntegerDictionaryEncoder<int64_t> intDictEncoder{*pool, *pool};
    intDictEncoder.bumpRefCount();
    intDictEncoder.bumpRefCount();
    intDictEncoder.bumpRefCount();
    EXPECT_EQ(4, intDictEncoder.refCount_);
    EXPECT_EQ(0, intDictEncoder.clearCount_);
    for (size_t i = 0; i != 2500; ++i) {
      intDictEncoder.addKey(i);
    }
    EXPECT_EQ(2500, intDictEncoder.size());
    EXPECT_EQ(2500, intDictEncoder.keyIndex_.size());
    EXPECT_EQ(2500, intDictEncoder.keys_.size());
    EXPECT_EQ(2500, intDictEncoder.counts_.size());
    EXPECT_EQ(2500, intDictEncoder.getTotalCount());
    EXPECT_EQ(4, intDictEncoder.refCount_);
    EXPECT_EQ(0, intDictEncoder.clearCount_);

    // auto peakMemory = pool.getCurrentBytes();
    intDictEncoder.clear();
    intDictEncoder.clear();
    EXPECT_EQ(2500, intDictEncoder.size());
    EXPECT_EQ(2500, intDictEncoder.keyIndex_.size());
    EXPECT_EQ(2500, intDictEncoder.keys_.size());
    EXPECT_EQ(2500, intDictEncoder.counts_.size());
    EXPECT_EQ(2500, intDictEncoder.getTotalCount());
    EXPECT_EQ(4, intDictEncoder.refCount_);
    EXPECT_EQ(2, intDictEncoder.clearCount_);

    intDictEncoder.clear();
    intDictEncoder.clear();
    EXPECT_EQ(0, intDictEncoder.size());
    EXPECT_EQ(0, intDictEncoder.keyIndex_.size());
    EXPECT_EQ(0, intDictEncoder.keys_.size());
    EXPECT_EQ(0, intDictEncoder.keys_.capacity());
    EXPECT_EQ(0, intDictEncoder.counts_.size());
    EXPECT_EQ(0, intDictEncoder.counts_.capacity());
    EXPECT_EQ(0, intDictEncoder.getTotalCount());
    EXPECT_EQ(4, intDictEncoder.refCount_);
    EXPECT_EQ(0, intDictEncoder.clearCount_);
    // Folly's F14 map when compiled with ASAN, it re-allocates after
    // deallocating the memory. So the overall bytes allocated does not go
    // down. On test experiment it deallocated 4K and rellocated 64K.
    // When not compiled with ASAN, it correctly frees up the memory.
    // so disabling this check in the test for now.
    // EXPECT_LT(pool.getCurrentBytes(), peakMemory);
  }
}

TEST(TestIntegerDictionaryEncoder, RepeatedFlush) {
  auto pool = addDefaultLeafMemoryPool();
  IntegerDictionaryEncoder<int64_t> intDictEncoder{*pool, *pool};
  std::vector<int> keys{0, 1, 4, 9, 16, 25, 9, 1};
  for (const auto& key : keys) {
    intDictEncoder.addKey(key);
  }

  EXPECT_ANY_THROW(intDictEncoder.getInDict());
  EXPECT_ANY_THROW(intDictEncoder.getLookupTable());

  proto::ColumnEncoding encoding;
  auto finalDictSize = intDictEncoder.flush();

  EXPECT_NO_THROW(intDictEncoder.getInDict());
  EXPECT_NO_THROW(intDictEncoder.getLookupTable());

  for (size_t i = 0; i < 25; ++i) {
    EXPECT_EQ(finalDictSize, intDictEncoder.flush());
  }

  intDictEncoder.clear();
  EXPECT_ANY_THROW(intDictEncoder.getInDict());
  EXPECT_ANY_THROW(intDictEncoder.getLookupTable());
}

TEST(TestIntegerDictionaryEncoder, Limit) {
  auto pool = addDefaultLeafMemoryPool();
  IntegerDictionaryEncoder<int16_t> intDictEncoder{*pool, *pool};
  for (size_t iter = 0; iter < 2; ++iter) {
    int16_t val = std::numeric_limits<int16_t>::min();
    uint16_t offset = 0;
    while (val < std::numeric_limits<int16_t>::max()) {
      ASSERT_EQ(intDictEncoder.addKey(val++), offset++);
    }
    ASSERT_EQ(intDictEncoder.addKey(val), offset);
    ASSERT_EQ(offset, std::numeric_limits<uint16_t>::max());
  }
}

template <typename T>
void testGetSortedIndexLookupTable() {
  struct TestCase {
    explicit TestCase(
        const std::vector<T>& addKeySequence,
        bool sort,
        const std::vector<T>& lookupTable)
        : addKeySequence{addKeySequence},
          sort{sort},
          lookupTable{lookupTable} {}
    std::vector<T> addKeySequence;
    bool sort;
    std::vector<T> lookupTable;
  };

  std::vector<TestCase> testCases{
      TestCase{{}, false, {}},
      TestCase{{42}, false, {0}},
      TestCase{{-2, -1, 0, 1, 2}, true, {0, 1, 2, 3, 4}},
      TestCase{{-2, -1, 0, 1, 2}, false, {0, 1, 2, 3, 4}},
      TestCase{{-1, 0, -2, 2, 1}, true, {1, 2, 0, 4, 3}},
      TestCase{{-1, 0, -2, 2, 1}, false, {0, 1, 2, 3, 4}}};

  for (const auto& testCase : testCases) {
    auto pool = addDefaultLeafMemoryPool();
    IntegerDictionaryEncoder<T> intDictEncoder{*pool, *pool};
    for (const auto& key : testCase.addKeySequence) {
      intDictEncoder.addKey(key);
    }

    dwio::common::DataBuffer<bool> inDict{*pool};
    dwio::common::DataBuffer<T> lookupTable{*pool};
    dwio::common::DataBuffer<char> writeBuffer{*pool, 1024};
    EXPECT_EQ(
        intDictEncoder.size(),
        IntegerDictionaryEncoder<T>::template getSortedIndexLookupTable<T>(
            intDictEncoder,
            *pool,
            false,
            testCase.sort,
            lookupTable,
            inDict,
            writeBuffer,
            [&](auto buf, auto size) {
              ASSERT_EQ(size, intDictEncoder.size());
              std::vector<T> dumpOrder(size);
              for (size_t i = 0; i < size; ++i) {
                dumpOrder[testCase.lookupTable[i]] = testCase.addKeySequence[i];
              }
              EXPECT_THAT(
                  std::vector<T>(buf, buf + size), ElementsAreArray(dumpOrder));
            }));
    ASSERT_EQ(intDictEncoder.size(), inDict.capacity());
    for (size_t i = 0; i != inDict.capacity(); ++i) {
      ASSERT_TRUE(inDict[i]);
    }
    EXPECT_THAT(
        std::vector<T>(
            lookupTable.data(), lookupTable.data() + lookupTable.capacity()),
        ElementsAreArray(testCase.lookupTable));
  }
}

TEST(TestIntegerDictionaryEncoder, GetSortedIndexLookupTable) {
  testGetSortedIndexLookupTable<int16_t>();
  testGetSortedIndexLookupTable<int32_t>();
  testGetSortedIndexLookupTable<int64_t>();
}

TEST(TestIntegerDictionaryEncoder, ShortIntegerDictionary) {
  // DictionaryEncoding lookupTable can contain the index into dictionary
  // or the actual value. For short integer, index can be  [0,2^16-1]
  // and the values can be from [-2^15, 2^15-1]. Integer writers always
  // assumes the data to be unsigned and can misinterpret the index
  // from [2^15,2^16-1]. To prevent this, for short lookup tables
  // are promoted to int32. This test verifies that by forcing index
  // for 2^15+1 values and using direct encoding(values) for the rest.

  std::vector<int16_t> values;
  // All postive numbers(2^15=1), and individual values -1 and 0
  for (int32_t i = -1; i <= std::numeric_limits<int16_t>::max(); i++) {
    // Add them twice to force dictionary encoding.
    values.emplace_back(static_cast<int16_t>(i));
    values.emplace_back(static_cast<int16_t>(i));
  }

  for (int32_t i = std::numeric_limits<int16_t>::min(); i < -1; i++) {
    // Add all the negative values once to force direct encoding (values).
    values.emplace_back(static_cast<int16_t>(i));
  }

  auto pool = addDefaultLeafMemoryPool();
  IntegerDictionaryEncoder<int16_t> intDictEncoder{*pool, *pool};
  dwio::common::DataBuffer<bool> inDict{*pool};
  dwio::common::DataBuffer<int16_t> lookupTable{*pool};
  dwio::common::DataBuffer<char> writeBuffer{*pool, 1024};
  for (const auto& key : values) {
    intDictEncoder.addKey(key);
  }

  int32_t dictSize = (int32_t)2 + std::numeric_limits<int16_t>::max();
  std::vector<int16_t> dictValues;

  auto actualSize =
      IntegerDictionaryEncoder<int16_t>::template getSortedIndexLookupTable<
          int16_t>(
          intDictEncoder,
          *pool,
          /* dropInFrequentKeys */ true,
          /* sort */ true,
          lookupTable,
          inDict,
          writeBuffer,
          [&](auto buf, auto size) {
            std::vector<int16_t> bufVector(buf, buf + size);
            dictValues.insert(
                dictValues.end(), bufVector.begin(), bufVector.end());
          });

  EXPECT_EQ(dictSize, actualSize);
  EXPECT_EQ(dictSize, dictValues.size());

  // verify
  int16_t expected = -1;
  for (int32_t i = 0; i < dictSize; i++) {
    ASSERT_EQ(true, inDict[i]) << " i: " << i;
    // Since the added values are sorted, the index should match as it is.
    // LookupTable stores the index as signed integer, but dictionary offsets
    // always need to be cast to unsigned.
    uint16_t dictIndex = static_cast<uint16_t>(lookupTable[i]);
    ASSERT_EQ(i, dictIndex) << " i: " << i;
    ASSERT_EQ(expected++, dictValues[dictIndex]) << " i: " << i;
  }

  expected = std::numeric_limits<int16_t>::min();
  for (int32_t i = dictSize; i <= std::numeric_limits<uint16_t>::max(); i++) {
    ASSERT_EQ(false, inDict[i]) << " i: " << i;
    ASSERT_EQ(expected++, lookupTable[i]) << " i: " << i;
  }
}

template <typename T>
void testInfrequentKeyOptimization() {
  struct TestCase {
    explicit TestCase(
        const std::vector<T>& addKeySequence,
        bool sort,
        const std::vector<T>& lookupTable,
        const std::vector<bool>& inDict,
        size_t finalDictSize)
        : addKeySequence{addKeySequence},
          sort{sort},
          lookupTable{lookupTable},
          inDict{inDict},
          finalDictSize{finalDictSize} {}

    std::vector<T> addKeySequence;
    bool sort;
    std::vector<T> lookupTable;
    std::vector<bool> inDict;
    size_t finalDictSize;
  };

  std::vector<TestCase> testCases{
      TestCase{{}, false, {}, {}, 0},
      TestCase{{42}, true, {42}, {false}, 0},
      TestCase{
          {-2, -1, 0, 1, 2},
          true,
          {-2, -1, 0, 1, 2},
          {false, false, false, false, false},
          0},
      TestCase{
          {-1, 0, -2, 2, 2, 1, 1},
          false,
          {-1, 0, -2, 0, 1},
          {false, false, false, true, true},
          2},
      TestCase{
          {-1, 0, -2, 2, 2, 1, 1},
          true,
          {-1, 0, -2, 1, 0},
          {false, false, false, true, true},
          2},
      TestCase{
          {0,  0,  1,  0,  0,  4,  2,  4,  1,  0,  1,  4,  9,  2,  10, 4,  15,
           10, 5,  0,  16, 12, 8,  4,  0,  22, 19, 16, 13, 10, 7,  4,  1,  32,
           30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8,  6,  4,  2,  0,  49,
           48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32,
           31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15,
           14, 13, 12, 11, 10, 9,  8,  7,  6,  5,  4,  3,  2,  1,  0},
          true,
          {0,  1,  3,  2,  8,  9,  13, 4,  14, 10, 7,  18, 16, 11, 6,  23, 22,
           21, 20, 19, 17, 15, 12, 5,  49, 48, 47, 46, 45, 44, 43, 42, 41, 40,
           39, 38, 37, 36, 35, 34, 33, 31, 29, 27, 25, 23, 21, 17, 11, 3},
          {true,  true,  true,  true,  true,  true,  true,  true,  true,
           true,  true,  true,  true,  true,  true,  true,  true,  true,
           true,  true,  true,  true,  true,  true,  false, false, false,
           false, false, false, false, false, false, false, false, false,
           false, false, false, false, false, false, false, false, false,
           false, false, false, false, false},
          24}};

  for (const auto& testCase : testCases) {
    auto pool = addDefaultLeafMemoryPool();
    IntegerDictionaryEncoder<T> intDictEncoder{*pool, *pool};
    for (const auto& key : testCase.addKeySequence) {
      intDictEncoder.addKey(key);
    }

    dwio::common::DataBuffer<bool> inDict{*pool};
    dwio::common::DataBuffer<T> lookupTable{*pool};
    dwio::common::DataBuffer<char> writeBuffer{*pool, 1024};
    EXPECT_EQ(
        testCase.finalDictSize,
        IntegerDictionaryEncoder<T>::template getSortedIndexLookupTable<T>(
            intDictEncoder,
            *pool,
            true,
            testCase.sort,
            lookupTable,
            inDict,
            writeBuffer,
            [&](auto buf, auto size) {
              ASSERT_EQ(testCase.finalDictSize, size);
              std::vector<T> dumpOrder(size);
              for (size_t i = 0; i < testCase.lookupTable.size(); ++i) {
                if (testCase.inDict[i]) {
                  dumpOrder[testCase.lookupTable[i]] = intDictEncoder.getKey(i);
                }
              }
              EXPECT_THAT(
                  std::vector<T>(buf, buf + size), ElementsAreArray(dumpOrder));
            }));
    ASSERT_EQ(intDictEncoder.size(), lookupTable.capacity());
    EXPECT_THAT(
        std::vector<T>(
            lookupTable.data(), lookupTable.data() + lookupTable.capacity()),
        ElementsAreArray(testCase.lookupTable));
    ASSERT_EQ(intDictEncoder.size(), inDict.capacity());
    EXPECT_THAT(
        std::vector<bool>(inDict.data(), inDict.data() + inDict.capacity()),
        ElementsAreArray(testCase.inDict));
  }
}

TEST(TestIntegerDictionaryEncoder, InfrequentKeyOptimization) {
  testInfrequentKeyOptimization<int16_t>();
  testInfrequentKeyOptimization<int32_t>();
  testInfrequentKeyOptimization<int64_t>();
}

} // namespace facebook::velox::dwrf
