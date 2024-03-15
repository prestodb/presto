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
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/Config.h"

namespace facebook::velox::connector {

using namespace dwio::common;

class HiveConnectorUtilTest : public exec::test::HiveConnectorTestBase {
 protected:
  static bool compareSerDeOptions(
      const SerDeOptions& l,
      const SerDeOptions& r) {
    return l.isEscaped == r.isEscaped && l.escapeChar == r.escapeChar &&
        l.lastColumnTakesRest == r.lastColumnTakesRest &&
        l.nullString == r.nullString && l.separators == r.separators;
  }

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
};

TEST_F(HiveConnectorUtilTest, configureReaderOptions) {
  core::MemConfig sessionProperties;
  auto hiveConfig =
      std::make_shared<hive::HiveConfig>(std::make_shared<core::MemConfig>());
  const std::unordered_map<std::string, std::optional<std::string>>
      partitionKeys;
  const std::unordered_map<std::string, std::string> customSplitInfo;

  // Dynamic parameters.
  dwio::common::ReaderOptions readerOptions(pool_.get());
  FileFormat fileFormat{FileFormat::DWRF};
  std::unordered_map<std::string, std::string> tableParameters;
  std::unordered_map<std::string, std::string> serdeParameters;
  SerDeOptions expectedSerDe;

  auto createTableHandle = [&]() {
    return std::make_shared<hive::HiveTableHandle>(
        "testConnectorId",
        "testTable",
        false,
        hive::SubfieldFilters{},
        nullptr,
        nullptr,
        tableParameters);
  };

  auto createSplit = [&]() {
    return std::make_shared<hive::HiveConnectorSplit>(
        "testConnectorId",
        "/tmp/",
        fileFormat,
        0UL,
        std::numeric_limits<uint64_t>::max(),
        partitionKeys,
        std::nullopt,
        customSplitInfo,
        nullptr,
        serdeParameters);
  };

  auto performConfigure = [&]() {
    auto tableHandle = createTableHandle();
    auto split = createSplit();
    configureReaderOptions(
        readerOptions, hiveConfig, &sessionProperties, tableHandle, split);
  };

  auto clearDynamicParameters = [&](FileFormat newFileFormat) {
    readerOptions = dwio::common::ReaderOptions(pool_.get());
    fileFormat = newFileFormat;
    tableParameters.clear();
    serdeParameters.clear();
    expectedSerDe = SerDeOptions{};
  };

  // Default.
  performConfigure();
  EXPECT_EQ(readerOptions.getFileFormat(), fileFormat);
  EXPECT_TRUE(
      compareSerDeOptions(readerOptions.getSerDeOptions(), expectedSerDe));
  EXPECT_EQ(readerOptions.loadQuantum(), hiveConfig->loadQuantum());
  EXPECT_EQ(readerOptions.maxCoalesceBytes(), hiveConfig->maxCoalescedBytes());
  EXPECT_EQ(
      readerOptions.maxCoalesceDistance(),
      hiveConfig->maxCoalescedDistanceBytes());
  EXPECT_EQ(
      readerOptions.isFileColumnNamesReadAsLowerCase(),
      hiveConfig->isFileColumnNamesReadAsLowerCase(&sessionProperties));
  EXPECT_EQ(
      readerOptions.isUseColumnNamesForColumnMapping(),
      hiveConfig->isOrcUseColumnNames(&sessionProperties));
  EXPECT_EQ(
      readerOptions.getFooterEstimatedSize(),
      hiveConfig->footerEstimatedSize());
  EXPECT_EQ(
      readerOptions.getFilePreloadThreshold(),
      hiveConfig->filePreloadThreshold());

  // Modify field delimiter and change the file format.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kFieldDelim] = '\t';
  expectedSerDe.separators[size_t(SerDeSeparator::FIELD_DELIM)] = '\t';
  performConfigure();
  EXPECT_EQ(readerOptions.getFileFormat(), fileFormat);
  EXPECT_TRUE(
      compareSerDeOptions(readerOptions.getSerDeOptions(), expectedSerDe));

  // Modify collection delimiter.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kCollectionDelim] = '=';
  expectedSerDe.separators[size_t(SerDeSeparator::COLLECTION_DELIM)] = '=';
  performConfigure();
  EXPECT_TRUE(
      compareSerDeOptions(readerOptions.getSerDeOptions(), expectedSerDe));

  // Modify map key delimiter.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kMapKeyDelim] = '&';
  expectedSerDe.separators[size_t(SerDeSeparator::MAP_KEY_DELIM)] = '&';
  performConfigure();
  EXPECT_TRUE(
      compareSerDeOptions(readerOptions.getSerDeOptions(), expectedSerDe));

  // Modify null string.
  clearDynamicParameters(FileFormat::TEXT);
  tableParameters[TableParameter::kSerializationNullFormat] = "x-x";
  expectedSerDe.nullString = "x-x";
  performConfigure();
  EXPECT_TRUE(
      compareSerDeOptions(readerOptions.getSerDeOptions(), expectedSerDe));

  // Modify all previous together.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kFieldDelim] = '~';
  expectedSerDe.separators[size_t(SerDeSeparator::FIELD_DELIM)] = '~';
  serdeParameters[SerDeOptions::kCollectionDelim] = '$';
  expectedSerDe.separators[size_t(SerDeSeparator::COLLECTION_DELIM)] = '$';
  serdeParameters[SerDeOptions::kMapKeyDelim] = '*';
  expectedSerDe.separators[size_t(SerDeSeparator::MAP_KEY_DELIM)] = '*';
  tableParameters[TableParameter::kSerializationNullFormat] = "";
  expectedSerDe.nullString = "";
  performConfigure();
  EXPECT_TRUE(
      compareSerDeOptions(readerOptions.getSerDeOptions(), expectedSerDe));
  EXPECT_TRUE(
      compareSerDeOptions(readerOptions.getSerDeOptions(), expectedSerDe));
  EXPECT_TRUE(
      compareSerDeOptions(readerOptions.getSerDeOptions(), expectedSerDe));
  EXPECT_TRUE(
      compareSerDeOptions(readerOptions.getSerDeOptions(), expectedSerDe));

  // Tests other custom reader options.
  clearDynamicParameters(FileFormat::TEXT);
  std::unordered_map<std::string, std::string> customHiveConfigProps;
  customHiveConfigProps[hive::HiveConfig::kLoadQuantum] = "321";
  customHiveConfigProps[hive::HiveConfig::kMaxCoalescedBytes] = "129";
  customHiveConfigProps[hive::HiveConfig::kMaxCoalescedDistanceBytes] = "513";
  customHiveConfigProps[hive::HiveConfig::kFileColumnNamesReadAsLowerCase] =
      "true";
  customHiveConfigProps[hive::HiveConfig::kOrcUseColumnNames] = "true";
  customHiveConfigProps[hive::HiveConfig::kFooterEstimatedSize] = "1111";
  customHiveConfigProps[hive::HiveConfig::kFilePreloadThreshold] = "9999";
  hiveConfig = std::make_shared<hive::HiveConfig>(
      std::make_shared<core::MemConfig>(customHiveConfigProps));
  performConfigure();
  EXPECT_EQ(readerOptions.loadQuantum(), hiveConfig->loadQuantum());
  EXPECT_EQ(readerOptions.maxCoalesceBytes(), hiveConfig->maxCoalescedBytes());
  EXPECT_EQ(
      readerOptions.maxCoalesceDistance(),
      hiveConfig->maxCoalescedDistanceBytes());
  EXPECT_EQ(
      readerOptions.isFileColumnNamesReadAsLowerCase(),
      hiveConfig->isFileColumnNamesReadAsLowerCase(&sessionProperties));
  EXPECT_EQ(
      readerOptions.isUseColumnNamesForColumnMapping(),
      hiveConfig->isOrcUseColumnNames(&sessionProperties));
  EXPECT_EQ(
      readerOptions.getFooterEstimatedSize(),
      hiveConfig->footerEstimatedSize());
  EXPECT_EQ(
      readerOptions.getFilePreloadThreshold(),
      hiveConfig->filePreloadThreshold());
}

TEST_F(HiveConnectorUtilTest, configureRowReaderOptions) {
  auto split =
      std::make_shared<hive::HiveConnectorSplit>("", "", FileFormat::UNKNOWN);
  auto rowType = ROW({{"float_features", MAP(INTEGER(), REAL())}});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*rowType);
  auto* float_features = spec->childByName("float_features");
  float_features->childByName(common::ScanSpec::kMapKeysFieldName)
      ->setFilter(common::createBigintValues({1, 3}, false));
  float_features->setFlatMapFeatureSelection({"1", "3"});
  RowReaderOptions options;
  configureRowReaderOptions(options, {}, spec, nullptr, rowType, split);
  auto& nodes = options.getSelector()->getProjection();
  ASSERT_EQ(nodes.size(), 1);
  ASSERT_EQ(nodes[0].expression, "[1,3]");
}

} // namespace facebook::velox::connector
