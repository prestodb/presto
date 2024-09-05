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
#include "velox/exec/tests/utils/PrefixSortUtils.h"

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
      exec::test::defaultPrefixSortConfig(),
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
        readerOptions, hiveConfig, connectorQueryCtx.get(), tableHandle, split);
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
  EXPECT_EQ(readerOptions.fileFormat(), fileFormat);
  EXPECT_TRUE(compareSerDeOptions(readerOptions.serDeOptions(), expectedSerDe));
  EXPECT_EQ(readerOptions.loadQuantum(), hiveConfig->loadQuantum());
  EXPECT_EQ(readerOptions.maxCoalesceBytes(), hiveConfig->maxCoalescedBytes());
  EXPECT_EQ(
      readerOptions.maxCoalesceDistance(),
      hiveConfig->maxCoalescedDistanceBytes());
  EXPECT_EQ(
      readerOptions.fileColumnNamesReadAsLowerCase(),
      hiveConfig->isFileColumnNamesReadAsLowerCase(&sessionProperties));
  EXPECT_EQ(
      readerOptions.useColumnNamesForColumnMapping(),
      hiveConfig->isOrcUseColumnNames(&sessionProperties));
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
  customHiveConfigProps[hive::HiveConfig::kMaxCoalescedDistanceBytes] = "513";
  customHiveConfigProps[hive::HiveConfig::kFileColumnNamesReadAsLowerCase] =
      "true";
  customHiveConfigProps[hive::HiveConfig::kOrcUseColumnNames] = "true";
  customHiveConfigProps[hive::HiveConfig::kFooterEstimatedSize] = "1111";
  customHiveConfigProps[hive::HiveConfig::kFilePreloadThreshold] = "9999";
  customHiveConfigProps[hive::HiveConfig::kPrefetchRowGroups] = "10";
  hiveConfig = std::make_shared<hive::HiveConfig>(
      std::make_shared<config::ConfigBase>(std::move(customHiveConfigProps)));
  performConfigure();
  EXPECT_EQ(readerOptions.loadQuantum(), hiveConfig->loadQuantum());
  EXPECT_EQ(readerOptions.maxCoalesceBytes(), hiveConfig->maxCoalescedBytes());
  EXPECT_EQ(
      readerOptions.maxCoalesceDistance(),
      hiveConfig->maxCoalescedDistanceBytes());
  EXPECT_EQ(
      readerOptions.fileColumnNamesReadAsLowerCase(),
      hiveConfig->isFileColumnNamesReadAsLowerCase(&sessionProperties));
  EXPECT_EQ(
      readerOptions.useColumnNamesForColumnMapping(),
      hiveConfig->isOrcUseColumnNames(&sessionProperties));
  EXPECT_EQ(
      readerOptions.footerEstimatedSize(), hiveConfig->footerEstimatedSize());
  EXPECT_EQ(
      readerOptions.filePreloadThreshold(), hiveConfig->filePreloadThreshold());
  EXPECT_EQ(readerOptions.prefetchRowGroups(), hiveConfig->prefetchRowGroups());
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

TEST_F(
    HiveConnectorUtilTest,
    updateWriterOptionsFromHiveConfigDWRFWithoutSessionProperties) {
  auto fileFormat = dwio::common::FileFormat::DWRF;
  std::unordered_map<std::string, std::string> connectorConfig = {
      {hive::HiveConfig::kOrcWriterMaxStripeSize, "100MB"},
      {hive::HiveConfig::kOrcWriterMaxDictionaryMemory, "128MB"},
      {hive::HiveConfig::kOrcWriterIntegerDictionaryEncodingEnabled, "true"},
      {hive::HiveConfig::kOrcWriterStringDictionaryEncodingEnabled, "false"},
      {hive::HiveConfig::kOrcWriterLinearStripeSizeHeuristics, "true"},
      {hive::HiveConfig::kOrcWriterMinCompressionSize, "512"},
      {hive::HiveConfig::kOrcWriterCompressionLevel, "1"}};
  auto hiveConfig = std::make_shared<hive::HiveConfig>(
      std::make_shared<config::ConfigBase>(std::move(connectorConfig)));
  std::shared_ptr<config::ConfigBase> connectorSessionProperties =
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>());
  std::shared_ptr<dwio::common::WriterOptions> options =
      std::make_shared<dwrf::WriterOptions>();
  options->compressionKind = velox::common::CompressionKind_ZLIB;

  updateWriterOptionsFromHiveConfig(
      fileFormat, hiveConfig, connectorSessionProperties.get(), options);

  auto dwrfOptions = std::dynamic_pointer_cast<dwrf::WriterOptions>(options);
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(dwrf::Config::COMPRESSION.key),
      "1");
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(dwrf::Config::STRIPE_SIZE.key),
      std::to_string(100 * 1024 * 1024));
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(
          dwrf::Config::MAX_DICTIONARY_SIZE.key),
      std::to_string(128 * 1024 * 1024));
  ASSERT_EQ(
      dwrfOptions->config->get<bool>(
          dwrf::Config::INTEGER_DICTIONARY_ENCODING_ENABLED.key),
      true);
  ASSERT_EQ(
      dwrfOptions->config->get<bool>(
          dwrf::Config::STRING_DICTIONARY_ENCODING_ENABLED.key),
      false);
  ASSERT_EQ(
      dwrfOptions->config->get<bool>(
          dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS.key),
      true);
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(
          dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN.key),
      "512");
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(
          dwrf::Config::ZLIB_COMPRESSION_LEVEL.key),
      "1");
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(
          dwrf::Config::ZSTD_COMPRESSION_LEVEL.key),
      "1");
}

TEST_F(
    HiveConnectorUtilTest,
    updateWriterOptionsFromHiveConfigDWRFWithSessionProperties) {
  auto fileFormat = dwio::common::FileFormat::DWRF;
  std::unordered_map<std::string, std::string> connectorConfig = {
      {hive::HiveConfig::kOrcWriterMaxStripeSize, "100MB"},
      {hive::HiveConfig::kOrcWriterMaxDictionaryMemory, "128MB"},
      {hive::HiveConfig::kOrcWriterIntegerDictionaryEncodingEnabled, "true"},
      {hive::HiveConfig::kOrcWriterStringDictionaryEncodingEnabled, "false"},
      {hive::HiveConfig::kOrcWriterLinearStripeSizeHeuristics, "true"},
      {hive::HiveConfig::kOrcWriterMinCompressionSize, "512"},
      {hive::HiveConfig::kOrcWriterCompressionLevel, "1"}};
  auto hiveConfig = std::make_shared<hive::HiveConfig>(
      std::make_shared<config::ConfigBase>(std::move(connectorConfig)));

  std::unordered_map<std::string, std::string> sessionConfig = {
      {hive::HiveConfig::kOrcWriterMaxStripeSizeSession, "128MB"},
      {hive::HiveConfig::kOrcWriterMaxDictionaryMemorySession, "100MB"},
      {hive::HiveConfig::kOrcWriterIntegerDictionaryEncodingEnabledSession,
       "false"},
      {hive::HiveConfig::kOrcWriterStringDictionaryEncodingEnabledSession,
       "true"},
      {hive::HiveConfig::kOrcWriterLinearStripeSizeHeuristicsSession, "false"},
      {hive::HiveConfig::kOrcWriterMinCompressionSizeSession, "1024"},
      {hive::HiveConfig::kOrcWriterCompressionLevelSession, "2"}};

  std::shared_ptr<config::ConfigBase> connectorSessionProperties =
      std::make_shared<config::ConfigBase>(std::move(sessionConfig));
  std::shared_ptr<dwio::common::WriterOptions> options =
      std::make_shared<dwrf::WriterOptions>();
  options->compressionKind = velox::common::CompressionKind_ZLIB;

  updateWriterOptionsFromHiveConfig(
      fileFormat, hiveConfig, connectorSessionProperties.get(), options);

  auto dwrfOptions = std::dynamic_pointer_cast<dwrf::WriterOptions>(options);
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(dwrf::Config::COMPRESSION.key),
      "1");
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(dwrf::Config::STRIPE_SIZE.key),
      std::to_string(128 * 1024 * 1024));
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(
          dwrf::Config::MAX_DICTIONARY_SIZE.key),
      std::to_string(100 * 1024 * 1024));
  ASSERT_EQ(
      dwrfOptions->config->get<bool>(
          dwrf::Config::INTEGER_DICTIONARY_ENCODING_ENABLED.key),
      false);
  ASSERT_EQ(
      dwrfOptions->config->get<bool>(
          dwrf::Config::STRING_DICTIONARY_ENCODING_ENABLED.key),
      true);
  ASSERT_EQ(
      dwrfOptions->config->get<bool>(
          dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS.key),
      false);
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(
          dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN.key),
      "1024");
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(
          dwrf::Config::ZLIB_COMPRESSION_LEVEL.key),
      "2");
  ASSERT_EQ(
      dwrfOptions->config->get<std::string>(
          dwrf::Config::ZSTD_COMPRESSION_LEVEL.key),
      "2");
}

#ifdef VELOX_ENABLE_PARQUET
TEST_F(HiveConnectorUtilTest, updateWriterOptionsFromHiveConfigParquet) {
  auto fileFormat = dwio::common::FileFormat::PARQUET;
  std::unordered_map<std::string, std::string> connectorConfig = {
      {parquet::WriterOptions::kParquetSessionWriteTimestampUnit, "3"},
      {core::QueryConfig::kSessionTimezone, "UTC"}};
  auto hiveConfig = std::make_shared<hive::HiveConfig>(
      std::make_shared<config::ConfigBase>(std::move(connectorConfig)));
  std::shared_ptr<config::ConfigBase> connectorSessionProperties =
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>());
  std::shared_ptr<dwio::common::WriterOptions> options =
      std::make_shared<parquet::WriterOptions>();
  options->compressionKind = velox::common::CompressionKind_ZLIB;

  updateWriterOptionsFromHiveConfig(
      fileFormat, hiveConfig, connectorSessionProperties.get(), options);

  auto parquetOptions =
      std::dynamic_pointer_cast<parquet::WriterOptions>(options);
  ASSERT_EQ(
      parquetOptions->parquetWriteTimestampUnit.value(), TimestampUnit::kMilli);
  ASSERT_EQ(parquetOptions->parquetWriteTimestampTimeZone.value(), "UTC");
}
#endif

} // namespace facebook::velox::connector
