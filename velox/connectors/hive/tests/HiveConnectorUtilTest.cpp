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

#include "velox/connectors/hive/HiveConnectorUtil.h"
#include <gtest/gtest.h>
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

#include "velox/dwio/dwrf/writer/Writer.h"

#ifdef VELOX_ENABLE_PARQUET
#include "velox/dwio/parquet/writer/Writer.h"
#endif

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
  config::ConfigBase sessionProperties({});
  auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
      pool_.get(),
      pool_.get(),
      &sessionProperties,
      nullptr,
      common::PrefixSortConfig(),
      nullptr,
      nullptr,
      "query.HiveConnectorUtilTest",
      "task.HiveConnectorUtilTest",
      "planNodeId.HiveConnectorUtilTest",
      0,
      "");
  auto hiveConfig =
      std::make_shared<hive::HiveConfig>(std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>()));
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
        common::SubfieldFilters{},
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
        hiveConfig, connectorQueryCtx.get(), tableHandle, split, readerOptions);
  };

  auto clearDynamicParameters = [&](FileFormat newFileFormat) {
    readerOptions = dwio::common::ReaderOptions(pool_.get());
    fileFormat = newFileFormat;
    tableParameters.clear();
    serdeParameters.clear();
    expectedSerDe = SerDeOptions{};
  };

  auto checkUseColumnNamesForColumnMapping = [&]() {
    if (fileFormat == FileFormat::DWRF || fileFormat == FileFormat::ORC) {
      EXPECT_EQ(
          readerOptions.useColumnNamesForColumnMapping(),
          hiveConfig->isOrcUseColumnNames(&sessionProperties));
    } else if (fileFormat == FileFormat::PARQUET) {
      EXPECT_EQ(
          readerOptions.useColumnNamesForColumnMapping(),
          hiveConfig->isParquetUseColumnNames(&sessionProperties));
    }
  };

  // Default.
  performConfigure();
  EXPECT_EQ(readerOptions.fileFormat(), fileFormat);
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));
  EXPECT_EQ(
      readerOptions.loadQuantum(), hiveConfig->loadQuantum(&sessionProperties));
  EXPECT_EQ(
      readerOptions.maxCoalesceBytes(),
      hiveConfig->maxCoalescedBytes(&sessionProperties));
  EXPECT_EQ(
      readerOptions.maxCoalesceDistance(),
      hiveConfig->maxCoalescedDistanceBytes(&sessionProperties));
  EXPECT_EQ(
      readerOptions.fileColumnNamesReadAsLowerCase(),
      hiveConfig->isFileColumnNamesReadAsLowerCase(&sessionProperties));
  checkUseColumnNamesForColumnMapping();
  EXPECT_EQ(
      readerOptions.footerEstimatedSize(), hiveConfig->footerEstimatedSize());
  EXPECT_EQ(
      readerOptions.filePreloadThreshold(), hiveConfig->filePreloadThreshold());
  EXPECT_EQ(readerOptions.prefetchRowGroups(), hiveConfig->prefetchRowGroups());

  // Modify field delimiter and change the file format.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kFieldDelim] = '\t';
  expectedSerDe.separators[size_t(SerDeSeparator::FIELD_DELIM)] = '\t';
  performConfigure();
  EXPECT_EQ(readerOptions.fileFormat(), fileFormat);
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));

  // Modify collection delimiter.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kCollectionDelim] = '=';
  expectedSerDe.separators[size_t(SerDeSeparator::COLLECTION_DELIM)] = '=';
  performConfigure();
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));

  // Modify map key delimiter.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kMapKeyDelim] = '&';
  expectedSerDe.separators[size_t(SerDeSeparator::MAP_KEY_DELIM)] = '&';
  performConfigure();
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));

  // Modify null string.
  clearDynamicParameters(FileFormat::TEXT);
  tableParameters[TableParameter::kSerializationNullFormat] = "x-x";
  expectedSerDe.nullString = "x-x";
  performConfigure();
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));

  // Empty escape delim means default escape char.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kEscapeChar] = "";
  expectedSerDe.escapeChar = '\\';
  expectedSerDe.isEscaped = true;
  performConfigure();
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));

  // Convertible to byte escape char - use it.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kEscapeChar] = "38";
  expectedSerDe.escapeChar = '&';
  expectedSerDe.isEscaped = true;
  performConfigure();
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));

  // Overflow byte escape char - fall back to the 1st character of the string.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kEscapeChar] = "381";
  expectedSerDe.escapeChar = '3';
  expectedSerDe.isEscaped = true;
  performConfigure();
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));

  // Not convertible string - fall back to the 1st character of the string.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kEscapeChar] = "7!";
  expectedSerDe.escapeChar = '7';
  expectedSerDe.isEscaped = true;
  performConfigure();
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));

  // Modify all previous together.
  clearDynamicParameters(FileFormat::TEXT);
  serdeParameters[SerDeOptions::kFieldDelim] = '~';
  expectedSerDe.separators[size_t(SerDeSeparator::FIELD_DELIM)] = '~';
  serdeParameters[SerDeOptions::kCollectionDelim] = '$';
  expectedSerDe.separators[size_t(SerDeSeparator::COLLECTION_DELIM)] = '$';
  serdeParameters[SerDeOptions::kMapKeyDelim] = '*';
  expectedSerDe.separators[size_t(SerDeSeparator::MAP_KEY_DELIM)] = '*';
  serdeParameters[SerDeOptions::kEscapeChar] = '*';
  expectedSerDe.escapeChar = '*';
  expectedSerDe.isEscaped = true;
  tableParameters[TableParameter::kSerializationNullFormat] = "";
  expectedSerDe.nullString = "";
  performConfigure();
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));

  // Tests other custom reader options.
  clearDynamicParameters(FileFormat::TEXT);
  std::unordered_map<std::string, std::string> customHiveConfigProps;
  customHiveConfigProps[hive::HiveConfig::kLoadQuantum] = "321";
  customHiveConfigProps[hive::HiveConfig::kMaxCoalescedBytes] = "129";
  customHiveConfigProps[hive::HiveConfig::kMaxCoalescedDistance] = "513KB";
  customHiveConfigProps[hive::HiveConfig::kFileColumnNamesReadAsLowerCase] =
      "true";
  customHiveConfigProps[hive::HiveConfig::kOrcUseColumnNames] = "true";
  customHiveConfigProps[hive::HiveConfig::kFooterEstimatedSize] = "1111";
  customHiveConfigProps[hive::HiveConfig::kFilePreloadThreshold] = "9999";
  customHiveConfigProps[hive::HiveConfig::kPrefetchRowGroups] = "10";
  hiveConfig = std::make_shared<hive::HiveConfig>(
      std::make_shared<config::ConfigBase>(std::move(customHiveConfigProps)));
  performConfigure();
  EXPECT_EQ(
      readerOptions.loadQuantum(), hiveConfig->loadQuantum(&sessionProperties));
  EXPECT_EQ(
      readerOptions.maxCoalesceBytes(),
      hiveConfig->maxCoalescedBytes(&sessionProperties));
  EXPECT_EQ(
      readerOptions.maxCoalesceDistance(),
      hiveConfig->maxCoalescedDistanceBytes(&sessionProperties));
  EXPECT_EQ(
      readerOptions.fileColumnNamesReadAsLowerCase(),
      hiveConfig->isFileColumnNamesReadAsLowerCase(&sessionProperties));
  EXPECT_EQ(
      readerOptions.footerEstimatedSize(), hiveConfig->footerEstimatedSize());
  EXPECT_EQ(
      readerOptions.filePreloadThreshold(), hiveConfig->filePreloadThreshold());
  EXPECT_EQ(readerOptions.prefetchRowGroups(), hiveConfig->prefetchRowGroups());
  clearDynamicParameters(FileFormat::ORC);
  performConfigure();
  checkUseColumnNamesForColumnMapping();
  clearDynamicParameters(FileFormat::PARQUET);
  performConfigure();
  checkUseColumnNamesForColumnMapping();
}

TEST_F(HiveConnectorUtilTest, cacheRetention) {
  struct {
    bool splitCacheable;
    bool expectedNoCacheRetention;

    std::string debugString() const {
      return fmt::format(
          "splitCacheable {}, expectedNoCacheRetention {}",
          splitCacheable,
          expectedNoCacheRetention);
    }
  } testSettings[] = {{false, true}, {true, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    config::ConfigBase sessionProperties({});
    auto hiveConfig =
        std::make_shared<hive::HiveConfig>(std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>()));

    auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
        pool_.get(),
        pool_.get(),
        &sessionProperties,
        nullptr,
        common::PrefixSortConfig(),
        nullptr,
        nullptr,
        "query.HiveConnectorUtilTest",
        "task.HiveConnectorUtilTest",
        "planNodeId.HiveConnectorUtilTest",
        0,
        "");

    dwio::common::ReaderOptions readerOptions(pool_.get());

    auto tableHandle = std::make_shared<hive::HiveTableHandle>(
        "testConnectorId",
        "testTable",
        false,
        common::SubfieldFilters{},
        nullptr,
        nullptr,
        std::unordered_map<std::string, std::string>{});

    auto hiveSplit = std::make_shared<hive::HiveConnectorSplit>(
        "testConnectorId",
        "/tmp/",
        FileFormat::DWRF,
        0UL,
        std::numeric_limits<uint64_t>::max(),
        std::unordered_map<std::string, std::optional<std::string>>{},
        std::nullopt,
        std::unordered_map<std::string, std::string>{},
        std::shared_ptr<std::string>{},
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::string>{},
        0,
        testData.splitCacheable);

    configureReaderOptions(
        hiveConfig,
        connectorQueryCtx.get(),
        tableHandle,
        hiveSplit,
        readerOptions);

    ASSERT_EQ(
        readerOptions.noCacheRetention(), testData.expectedNoCacheRetention);
  }
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
}

TEST_F(HiveConnectorUtilTest, configureStoragePamatersRowReaderOptions) {
  dwio::common::RowReaderOptions rowReaderOpts;
  auto hiveSplit =
      std::make_shared<hive::HiveConnectorSplit>("", "", FileFormat::SST);
  hiveSplit->storageParameters = {
      {"key_col_indices", "0,1,2"},
      {"value_col_indices", "4,5"},
  };
  configureRowReaderOptions(
      /*tableParameters=*/{},
      /*scanSpec=*/nullptr,
      /*metadataFilter=*/nullptr,
      /*rowType=*/nullptr,
      /*hiveSplit=*/hiveSplit,
      /*hiveConfig=*/nullptr,
      /*sessionProperties=*/nullptr,
      /*rowReaderOptions=*/rowReaderOpts);

  EXPECT_EQ(rowReaderOpts.storageParameters(), hiveSplit->storageParameters);
}

} // namespace facebook::velox::connector
