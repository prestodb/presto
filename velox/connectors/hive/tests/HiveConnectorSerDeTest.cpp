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
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/expression/ExprToSubfieldFilter.h"

namespace facebook::velox::connector::hive::test {
namespace {

using namespace facebook::velox::exec;

class HiveConnectorSerDeTest : public exec::test::HiveConnectorTestBase {
 protected:
  HiveConnectorSerDeTest() {
    Type::registerSerDe();
    common::Filter::registerSerDe();
    core::ITypedExpr::registerSerDe();
    HiveTableHandle::registerSerDe();
    HiveColumnHandle::registerSerDe();
    LocationHandle::registerSerDe();
    HiveInsertTableHandle::registerSerDe();
    HiveBucketProperty::registerSerDe();
    HiveSortingColumn::registerSerDe();
    HiveConnectorSplit::registerSerDe();
  }

  template <typename T>
  static void testSerde(const T& handle) {
    auto str = handle.toString();
    auto obj = handle.serialize();
    auto clone = ISerializable::deserialize<T>(obj);
    ASSERT_EQ(clone->toString(), str);
  }

  static void testSerde(const HiveTableHandle& handle) {
    auto str = handle.toString();
    auto obj = handle.serialize();
    auto clone = ISerializable::deserialize<HiveTableHandle>(obj);
    ASSERT_EQ(clone->toString(), str);
    ASSERT_EQ(
        handle.remainingFilter()->type(), clone->remainingFilter()->type());

    auto& filters = handle.subfieldFilters();
    auto& cloneFilters = clone->subfieldFilters();
    ASSERT_EQ(filters.size(), cloneFilters.size());
    for (const auto& [subfield, filter] : handle.subfieldFilters()) {
      ASSERT_NE(cloneFilters.find(subfield), cloneFilters.end());
      ASSERT_TRUE(filter->testingEquals(*cloneFilters.at(subfield)));
    }
  }

  static void testSerde(const HiveConnectorSplit& split) {
    const auto str = split.toString();
    const auto obj = split.serialize();
    const auto clone = ISerializable::deserialize<HiveConnectorSplit>(obj);
    ASSERT_EQ(clone->toString(), str);
    ASSERT_EQ(split.partitionKeys.size(), clone->partitionKeys.size());
    for (const auto& [key, value] : split.partitionKeys) {
      ASSERT_EQ(value, clone->partitionKeys.at(key));
    }

    ASSERT_EQ(split.tableBucketNumber, clone->tableBucketNumber);
    ASSERT_EQ(split.customSplitInfo.size(), clone->customSplitInfo.size());
    for (const auto& [key, value] : split.customSplitInfo) {
      ASSERT_EQ(value, clone->customSplitInfo.at(key));
    }

    ASSERT_EQ(*split.extraFileInfo, *clone->extraFileInfo);
    ASSERT_EQ(split.serdeParameters.size(), clone->serdeParameters.size());
    for (const auto& [key, value] : split.serdeParameters) {
      ASSERT_EQ(value, clone->serdeParameters.at(key));
    }

    ASSERT_EQ(split.infoColumns.size(), clone->infoColumns.size());
    for (const auto& [key, value] : split.infoColumns) {
      ASSERT_EQ(value, clone->infoColumns.at(key));
    }

    if (split.properties.has_value()) {
      ASSERT_TRUE(clone->properties.has_value());
      ASSERT_EQ(split.properties->fileSize, clone->properties->fileSize);
      ASSERT_EQ(
          split.properties->modificationTime,
          clone->properties->modificationTime);
    } else {
      ASSERT_FALSE(clone->properties.has_value());
    }
  }
};

TEST_F(HiveConnectorSerDeTest, hiveTableHandle) {
  auto rowType =
      ROW({"c0c0", "c1", "c2", "c3", "c4", "c5"},
          {INTEGER(), BIGINT(), DOUBLE(), BOOLEAN(), BIGINT(), VARCHAR()});
  auto tableHandle = makeTableHandle(
      common::test::SubfieldFiltersBuilder()
          .add("c0.c0", isNotNull())
          .add(
              "c1",
              lessThanOrEqualHugeint(std::numeric_limits<int128_t>::max()))
          .add("c2", greaterThanOrEqualDouble(3.1415))
          .add("c3", boolEqual(true))
          .add("c4", in({0xdeadbeaf, 0xcafecafe}))
          .add("c2", notIn({0xdeadbeaf, 0xcafecafe}))
          .add(
              "c5",
              orFilter(between("abc", "efg"), greaterThanOrEqual("dragon")))
          .build(),
      parseExpr("c1 > c4 and c3 = true", rowType),
      "hive_table",
      ROW({"c0", "c1"}, {BIGINT(), VARCHAR()}));
  testSerde(*tableHandle);
}

TEST_F(HiveConnectorSerDeTest, hiveColumnHandle) {
  auto columnType = ROW(
      {{"c0c0", BIGINT()},
       {"c0c1",
        ARRAY(MAP(
            VARCHAR(), ROW({{"c0c1c0", BIGINT()}, {"c0c1c1", BIGINT()}})))}});
  auto columnHandle = exec::test::HiveConnectorTestBase::makeColumnHandle(
      "columnHandle", columnType, {"c0.c0c1[3][\"foo\"].c0c1c0"});

  testSerde(*columnHandle);
}

TEST_F(HiveConnectorSerDeTest, locationHandle) {
  auto locationHandle = exec::test::HiveConnectorTestBase::makeLocationHandle(
      "targetDirectory",
      std::optional("writeDirectory"),
      LocationHandle::TableType::kNew);
  testSerde(*locationHandle);
}

TEST_F(HiveConnectorSerDeTest, hiveInsertTableHandle) {
  auto tableColumnNames = std::vector<std::string>{"id", "row", "arr", "loc"};
  auto bigintType = TypeFactory<TypeKind::BIGINT>::create();
  auto rowType{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
  auto arrType =
      ARRAY(ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}, {"c0c2", BIGINT()}}));
  auto varcharType = TypeFactory<TypeKind::VARCHAR>::create();
  std::vector<TypePtr> tableColumnTypes;
  tableColumnTypes.reserve(4);
  tableColumnTypes.emplace_back(bigintType);
  tableColumnTypes.emplace_back(rowType);
  tableColumnTypes.emplace_back(arrType);
  tableColumnTypes.emplace_back(varcharType);

  auto locationHandle = exec::test::HiveConnectorTestBase::makeLocationHandle(
      "targetDirectory",
      std::optional("writeDirectory"),
      LocationHandle::TableType::kNew);

  auto bucketProperty = std::make_shared<HiveBucketProperty>(
      HiveBucketProperty::Kind::kPrestoNative,
      1024,
      std::vector<std::string>{"id", "row"},
      std::vector<TypePtr>{VARCHAR(), BOOLEAN()},
      std::vector<std::shared_ptr<const HiveSortingColumn>>{
          std::make_shared<HiveSortingColumn>(
              "id", core::SortOrder{true, true})});

  std::unordered_map<std::string, std::string> serdeParameters = {
      {"key1", "value1"},
      {"key2", "value2"},
  };

  auto hiveInsertTableHandle =
      exec::test::HiveConnectorTestBase::makeHiveInsertTableHandle(
          tableColumnNames,
          tableColumnTypes,
          {"loc"},
          bucketProperty,
          locationHandle,
          dwio::common::FileFormat::NIMBLE,
          common::CompressionKind::CompressionKind_SNAPPY,
          serdeParameters);
  testSerde(*hiveInsertTableHandle);
}

TEST_F(HiveConnectorSerDeTest, hiveConnectorSplit) {
  const auto connectorId = "testSerde";
  constexpr auto splitWeight = 1;
  constexpr auto filePath = "/testSerde/p";
  constexpr auto fileFormat = dwio::common::FileFormat::DWRF;
  constexpr auto start = 0;
  constexpr auto length = 1024;
  const std::unordered_map<std::string, std::optional<std::string>>
      partitionKeys{{"p0", "0"}, {"p1", "1"}};
  constexpr auto tableBucketNumber = std::optional<int32_t>(4);
  const std::unordered_map<std::string, std::string> customSplitInfo{
      {"s0", "0"}, {"s1", "1"}};
  const auto extraFileInfo = std::make_shared<std::string>("testSerdeFileInfo");
  const std::unordered_map<std::string, std::string> serdeParameters{
      {"k1", "1"}, {"k2", "v2"}};
  const std::unordered_map<std::string, std::string> infoColumns{
      {"c0", "0"}, {"c1", "1"}};
  FileProperties fileProperties{
      .fileSize = 2048, .modificationTime = std::nullopt};
  const auto properties = std::optional<FileProperties>(fileProperties);
  const auto split = HiveConnectorSplit(
      connectorId,
      filePath,
      fileFormat,
      start,
      length,
      partitionKeys,
      tableBucketNumber,
      customSplitInfo,
      extraFileInfo,
      serdeParameters,
      splitWeight,
      infoColumns,
      std::nullopt);
  testSerde(split);
}

} // namespace
} // namespace facebook::velox::connector::hive::test
