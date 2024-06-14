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

#include "velox/dwio/dwrf/test/ColumnStatisticsBase.h"
#include "velox/type/fbhive/HiveTypeParser.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;
using facebook::velox::type::fbhive::HiveTypeParser;

class ColumnStatisticsTest : public ::testing::Test,
                             public ColumnStatisticsBase {};

TEST_F(ColumnStatisticsTest, size) {
  testSize();
}

TEST_F(ColumnStatisticsTest, integer) {
  testInteger();
}

TEST_F(ColumnStatisticsTest, integerMissingStats) {
  proto::ColumnStatistics proto;
  auto intProto = proto.mutable_intstatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testIntegerMissingStats(columnStatisticsWrapper, intProto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, integerEmptyStats) {
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testIntegerEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, integerOverflow) {
  testIntegerOverflow();
}

TEST_F(ColumnStatisticsTest, doubles) {
  testDoubles();
}

TEST_F(ColumnStatisticsTest, doubleMissingStats) {
  proto::ColumnStatistics proto;
  auto doubleProto = proto.mutable_doublestatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testDoubleMissingStats(
      columnStatisticsWrapper, doubleProto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, doubleEmptyStats) {
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testDoubleEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, doubleNaN) {
  testDoubleNaN();
}

TEST_F(ColumnStatisticsTest, string) {
  testString();
}

TEST_F(ColumnStatisticsTest, stringMissingStats) {
  proto::ColumnStatistics proto;
  auto strProto = proto.mutable_stringstatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testStringMissingStats(columnStatisticsWrapper, strProto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, stringEmptyStats) {
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testStringEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, stringLengthThreshold) {
  testStringLengthThreshold();
}

TEST_F(ColumnStatisticsTest, stringLengthOverflow) {
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(1);
  auto strProto = proto.mutable_stringstatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testStringLengthOverflow(
      columnStatisticsWrapper, strProto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, boolean) {
  testBoolean();
}

TEST_F(ColumnStatisticsTest, booleanMissingStats) {
  proto::ColumnStatistics proto;
  auto boolProto = proto.mutable_bucketstatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBooleanMissingStats(
      columnStatisticsWrapper, boolProto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, booleanEmptyStats) {
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBooleanEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, basic) {
  testBasic();
}

TEST_F(ColumnStatisticsTest, basicMissingStats) {
  proto::ColumnStatistics proto;
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBasicMissingStats(columnStatisticsWrapper);
}

TEST_F(ColumnStatisticsTest, basicHasNull) {
  proto::ColumnStatistics proto;
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBasicHasNull(columnStatisticsWrapper, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, binary) {
  testBinary();
}

TEST_F(ColumnStatisticsTest, binaryMissingStats) {
  proto::ColumnStatistics proto;
  auto binProto = proto.mutable_binarystatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBinaryMissingStats(columnStatisticsWrapper, binProto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, binaryEmptyStats) {
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBinaryEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, binaryLengthOverflow) {
  proto::ColumnStatistics proto;
  auto binProto = proto.mutable_binarystatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBinaryLengthOverflow(
      columnStatisticsWrapper, binProto, DwrfFormat::kDwrf);
}

TEST_F(ColumnStatisticsTest, initialSize) {
  testInitialSize();
}

proto::KeyInfo createKeyInfo(int64_t key) {
  proto::KeyInfo keyInfo;
  keyInfo.set_intkey(key);
  return keyInfo;
}

inline bool operator==(
    const ColumnStatistics& lhs,
    const ColumnStatistics& rhs) {
  return (lhs.hasNull() == rhs.hasNull()) &&
      (lhs.getNumberOfValues() == rhs.getNumberOfValues()) &&
      (lhs.getRawSize() == rhs.getRawSize());
}

void checkEntries(
    const std::vector<ColumnStatistics>& entries,
    const std::vector<ColumnStatistics>& expectedEntries) {
  EXPECT_EQ(expectedEntries.size(), entries.size());
  for (const auto& entry : entries) {
    EXPECT_NE(
        std::find_if(
            expectedEntries.begin(),
            expectedEntries.end(),
            [&](const ColumnStatistics& expectedStats) {
              return expectedStats == entry;
            }),
        expectedEntries.end());
  }
}

struct TestKeyStats {
  TestKeyStats(int64_t key, bool hasNull, uint64_t valueCount, uint64_t rawSize)
      : key{key}, hasNull{hasNull}, valueCount{valueCount}, rawSize{rawSize} {}

  int64_t key;
  bool hasNull;
  uint64_t valueCount;
  uint64_t rawSize;
};

struct MapStatsAddValueTestCase {
  explicit MapStatsAddValueTestCase(
      const std::vector<TestKeyStats>& input,
      const std::vector<TestKeyStats>& expected)
      : input{input}, expected{expected} {}

  std::vector<TestKeyStats> input;
  std::vector<TestKeyStats> expected;
};

class MapStatisticsBuilderAddValueTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<MapStatsAddValueTestCase> {};

StatisticsBuilderOptions options{16};
TEST_P(MapStatisticsBuilderAddValueTest, addValues) {
  auto type = HiveTypeParser{}.parse("map<int, float>");
  MapStatisticsBuilder mapStatsBuilder{*type, options};

  for (const auto& entry : GetParam().input) {
    StatisticsBuilder statsBuilder{options};
    if (entry.hasNull) {
      statsBuilder.setHasNull();
    }
    statsBuilder.increaseValueCount(entry.valueCount);
    statsBuilder.increaseRawSize(entry.rawSize);
    mapStatsBuilder.addValues(createKeyInfo(entry.key), statsBuilder);
  }

  const auto& expectedTestEntries = GetParam().expected;
  std::vector<ColumnStatistics> expectedEntryStats{};
  expectedEntryStats.reserve(expectedTestEntries.size());
  for (const auto& entry : expectedTestEntries) {
    StatisticsBuilder statsBuilder{options};
    if (entry.hasNull) {
      statsBuilder.setHasNull();
    }
    statsBuilder.increaseValueCount(entry.valueCount);
    statsBuilder.increaseRawSize(entry.rawSize);
    expectedEntryStats.push_back(statsBuilder);
  }

  std::vector<ColumnStatistics> entryStats;
  const auto& outputEntries = mapStatsBuilder.getEntryStatistics();
  entryStats.reserve(outputEntries.size());
  for (const auto& entry : outputEntries) {
    entryStats.push_back(*entry.second);
  }

  checkEntries(entryStats, expectedEntryStats);
}

INSTANTIATE_TEST_SUITE_P(
    MapStatisticsBuilderAddValueTestSuite,
    MapStatisticsBuilderAddValueTest,
    testing::Values(
        MapStatsAddValueTestCase{{}, {}},
        MapStatsAddValueTestCase{
            {TestKeyStats{1, false, 1, 21}},
            {TestKeyStats{1, false, 1, 21}}},
        MapStatsAddValueTestCase{
            {TestKeyStats{1, false, 1, 21}, TestKeyStats{1, true, 3, 42}},
            {TestKeyStats{1, true, 4, 63}}},
        MapStatsAddValueTestCase{
            {TestKeyStats{1, false, 1, 21}, TestKeyStats{2, true, 3, 42}},
            {TestKeyStats{1, false, 1, 21}, TestKeyStats{2, true, 3, 42}}},
        MapStatsAddValueTestCase{
            {TestKeyStats{1, false, 1, 21},
             TestKeyStats{2, false, 3, 42},
             TestKeyStats{2, false, 3, 42},
             TestKeyStats{1, true, 1, 42}},
            {TestKeyStats{1, true, 2, 63}, TestKeyStats{2, false, 6, 84}}}));

struct MapStatsMergeTestCase {
  std::vector<std::vector<TestKeyStats>> inputs;
  std::vector<TestKeyStats> expected;
};

class MapStatisticsBuilderMergeTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<MapStatsMergeTestCase> {};

TEST_P(MapStatisticsBuilderMergeTest, addValues) {
  auto type = HiveTypeParser{}.parse("map<int, float>");

  const auto& inputTestEntries = GetParam().inputs;
  std::vector<std::unique_ptr<MapStatisticsBuilder>> mapStatsBuilders;
  mapStatsBuilders.reserve(inputTestEntries.size());
  for (const auto& input : inputTestEntries) {
    std::unique_ptr<MapStatisticsBuilder> mapStatsBuilder =
        std::make_unique<MapStatisticsBuilder>(*type, options);
    for (const auto& entry : input) {
      StatisticsBuilder statsBuilder{options};
      if (entry.hasNull) {
        statsBuilder.setHasNull();
      }
      statsBuilder.increaseValueCount(entry.valueCount);
      statsBuilder.increaseRawSize(entry.rawSize);
      mapStatsBuilder->addValues(createKeyInfo(entry.key), statsBuilder);
    }
    mapStatsBuilders.push_back(std::move(mapStatsBuilder));
  }

  MapStatisticsBuilder aggregateMapStatsBuilder{*type, options};
  for (const auto& mapStatsBuilder : mapStatsBuilders) {
    aggregateMapStatsBuilder.merge(*mapStatsBuilder);
  }

  const auto& expectedTestEntries = GetParam().expected;
  std::vector<ColumnStatistics> expectedEntryStats{};
  expectedEntryStats.reserve(expectedTestEntries.size());
  for (const auto& entry : expectedTestEntries) {
    StatisticsBuilder statsBuilder{options};
    if (entry.hasNull) {
      statsBuilder.setHasNull();
    }
    statsBuilder.increaseValueCount(entry.valueCount);
    statsBuilder.increaseRawSize(entry.rawSize);
    expectedEntryStats.push_back(statsBuilder);
  }

  std::vector<ColumnStatistics> entryStats;
  const auto& aggregatedEntries = aggregateMapStatsBuilder.getEntryStatistics();
  entryStats.reserve(aggregatedEntries.size());
  for (const auto& entry : aggregatedEntries) {
    entryStats.push_back(*entry.second);
  }

  checkEntries(entryStats, expectedEntryStats);
}

INSTANTIATE_TEST_SUITE_P(
    MapStatisticsBuilderMergeTestSuite,
    MapStatisticsBuilderMergeTest,
    testing::Values(
        MapStatsMergeTestCase{{}, {}},
        MapStatsMergeTestCase{
            {{TestKeyStats{1, false, 1, 21}}},
            {TestKeyStats{1, false, 1, 21}}},
        MapStatsMergeTestCase{
            {{}, {TestKeyStats{1, false, 1, 21}}},
            {TestKeyStats{1, false, 1, 21}}},
        MapStatsMergeTestCase{
            {{TestKeyStats{1, false, 1, 21}}, {TestKeyStats{1, true, 3, 42}}},
            {TestKeyStats{1, true, 4, 63}}},
        MapStatsMergeTestCase{
            {{TestKeyStats{1, false, 1, 21}}, {TestKeyStats{2, true, 3, 42}}},
            {TestKeyStats{1, false, 1, 21}, TestKeyStats{2, true, 3, 42}}},
        MapStatsMergeTestCase{
            {{TestKeyStats{1, false, 1, 21}, TestKeyStats{2, false, 3, 42}},
             {TestKeyStats{2, false, 3, 42}},
             {TestKeyStats{1, true, 1, 42}}},
            {TestKeyStats{1, true, 2, 63}, TestKeyStats{2, false, 6, 84}}}));

TEST(MapStatisticsBuilderTest, innerStatsType) {
  {
    auto type = HiveTypeParser{}.parse("map<int, float>");
    MapStatisticsBuilder mapStatsBuilder{*type, options};

    DoubleStatisticsBuilder statsBuilder{options};
    statsBuilder.addValues(0.1);
    statsBuilder.addValues(1.0);
    mapStatsBuilder.addValues(createKeyInfo(1), statsBuilder);

    auto& doubleStats = dynamic_cast<DoubleColumnStatistics&>(
        *mapStatsBuilder.getEntryStatistics().at(KeyInfo{1}));

    EXPECT_EQ(0.1, doubleStats.getMinimum());
    EXPECT_EQ(1.0, doubleStats.getMaximum());
  }
  {
    auto type = HiveTypeParser{}.parse("map<bigint, bigint>");
    MapStatisticsBuilder mapStatsBuilder{*type, options};

    IntegerStatisticsBuilder statsBuilder{options};
    statsBuilder.addValues(1);
    statsBuilder.addValues(2);
    mapStatsBuilder.addValues(createKeyInfo(1), statsBuilder);

    auto& intStats = dynamic_cast<IntegerColumnStatistics&>(
        *mapStatsBuilder.getEntryStatistics().at(KeyInfo{1}));

    EXPECT_EQ(1, intStats.getMinimum());
    EXPECT_EQ(2, intStats.getMaximum());
    EXPECT_EQ(3, intStats.getSum());
  }
}

TEST(MapStatisticsBuilderTest, incrementSize) {
  auto type = HiveTypeParser{}.parse("map<int, float>");
  MapStatisticsBuilder mapStatsBuilder{*type, options};

  DoubleStatisticsBuilder statsBuilder1{options};
  statsBuilder1.addValues(0.1);
  statsBuilder1.addValues(1.0);
  ASSERT_FALSE(statsBuilder1.getSize().has_value());
  mapStatsBuilder.addValues(createKeyInfo(1), statsBuilder1);
  EXPECT_FALSE(mapStatsBuilder.getEntryStatistics()
                   .at(KeyInfo{1})
                   ->getSize()
                   .has_value());

  DoubleStatisticsBuilder statsBuilder2{options};
  statsBuilder2.addValues(0.3);
  statsBuilder2.addValues(3.0);
  ASSERT_FALSE(statsBuilder2.getSize().has_value());
  mapStatsBuilder.addValues(createKeyInfo(2), statsBuilder2);
  EXPECT_FALSE(mapStatsBuilder.getEntryStatistics()
                   .at(KeyInfo{2})
                   ->getSize()
                   .has_value());

  mapStatsBuilder.incrementSize(createKeyInfo(1), 4);
  ASSERT_TRUE(mapStatsBuilder.getEntryStatistics()
                  .at(KeyInfo{1})
                  ->getSize()
                  .has_value());
  EXPECT_EQ(
      4,
      mapStatsBuilder.getEntryStatistics().at(KeyInfo{1})->getSize().value());
  ASSERT_FALSE(mapStatsBuilder.getEntryStatistics()
                   .at(KeyInfo{2})
                   ->getSize()
                   .has_value());
  mapStatsBuilder.incrementSize(createKeyInfo(2), 8);
  ASSERT_TRUE(mapStatsBuilder.getEntryStatistics()
                  .at(KeyInfo{1})
                  ->getSize()
                  .has_value());
  EXPECT_EQ(
      4,
      mapStatsBuilder.getEntryStatistics().at(KeyInfo{1})->getSize().value());
  ASSERT_TRUE(mapStatsBuilder.getEntryStatistics()
                  .at(KeyInfo{2})
                  ->getSize()
                  .has_value());
  EXPECT_EQ(
      8,
      mapStatsBuilder.getEntryStatistics().at(KeyInfo{2})->getSize().value());

  mapStatsBuilder.incrementSize(createKeyInfo(1), 8);
  ASSERT_TRUE(mapStatsBuilder.getEntryStatistics()
                  .at(KeyInfo{1})
                  ->getSize()
                  .has_value());
  EXPECT_EQ(
      12,
      mapStatsBuilder.getEntryStatistics().at(KeyInfo{1})->getSize().value());
  ASSERT_TRUE(mapStatsBuilder.getEntryStatistics()
                  .at(KeyInfo{2})
                  ->getSize()
                  .has_value());
  EXPECT_EQ(
      8,
      mapStatsBuilder.getEntryStatistics().at(KeyInfo{2})->getSize().value());
}

TEST(MapStatisticsBuilderTest, mergeKeyStats) {
  auto type = HiveTypeParser{}.parse("map<bigint, bigint>");
  MapStatisticsBuilder mapStatsBuilder{*type, options};

  mapStatsBuilder.incrementSize(createKeyInfo(1), 42);
  auto& keyStats = dynamic_cast<StatisticsBuilder&>(
      *mapStatsBuilder.getEntryStatistics().at(KeyInfo{1}));
  ASSERT_EQ(0, keyStats.getNumberOfValues());
  ASSERT_TRUE(keyStats.getRawSize().has_value());
  ASSERT_EQ(0, keyStats.getRawSize().value());
  ASSERT_TRUE(keyStats.getSize().has_value());
  ASSERT_EQ(42, keyStats.getSize().value());

  IntegerStatisticsBuilder statsBuilder{options};
  statsBuilder.addValues(1);
  statsBuilder.addValues(2);
  statsBuilder.increaseRawSize(8);
  mapStatsBuilder.addValues(createKeyInfo(1), statsBuilder);

  keyStats = dynamic_cast<StatisticsBuilder&>(
      *mapStatsBuilder.getEntryStatistics().at(KeyInfo{1}));
  ASSERT_EQ(2, keyStats.getNumberOfValues());
  ASSERT_TRUE(keyStats.getRawSize().has_value());
  ASSERT_EQ(8, keyStats.getRawSize().value());
  EXPECT_TRUE(keyStats.getSize().has_value());
  EXPECT_EQ(42, keyStats.getSize().value());
}
