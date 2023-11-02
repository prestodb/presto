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

#include <folly/init/Init.h>
#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Config.h"
#include "velox/dwio/common/Options.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::connector::hive {
namespace {

using namespace facebook::velox::common;
using namespace facebook::velox::exec::test;

constexpr const char* kHiveConnectorId = "test-hive";

class HiveDataSinkTest : public exec::test::HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    Type::registerSerDe();
    HiveSortingColumn::registerSerDe();
    HiveBucketProperty::registerSerDe();

    rowType_ =
        ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
            {BIGINT(),
             INTEGER(),
             SMALLINT(),
             REAL(),
             DOUBLE(),
             VARCHAR(),
             BOOLEAN()});

    setupMemoryPools();

    spillExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(
        std::thread::hardware_concurrency());
  }

  std::unique_ptr<SpillConfig> getSpillConfig(
      const std::string spillPath,
      uint64_t writerFlushThreshold) {
    return std::make_unique<SpillConfig>(
        spillPath,
        0,
        0,
        0,
        spillExecutor_.get(),
        10,
        20,
        0,
        0,
        0,
        writerFlushThreshold,
        0,
        "none");
  }

  void setupMemoryPools() {
    connectorQueryCtx_.reset();
    connectorPool_.reset();
    opPool_.reset();
    root_.reset();

    root_ = memory::defaultMemoryManager().addRootPool(
        "HiveDataSinkTest", 1L << 30, exec::MemoryReclaimer::create());
    opPool_ = root_->addLeafChild("operator");
    connectorPool_ =
        root_->addAggregateChild("connector", exec::MemoryReclaimer::create());

    connectorQueryCtx_ = std::make_unique<connector::ConnectorQueryCtx>(
        opPool_.get(),
        connectorPool_.get(),
        connectorConfig_.get(),
        nullptr,
        nullptr,
        nullptr,
        "query.HiveDataSinkTest",
        "task.HiveDataSinkTest",
        "planNodeId.HiveDataSinkTest",
        0);
  }

  std::shared_ptr<connector::hive::HiveInsertTableHandle>
  createHiveInsertTableHandle(
      const RowTypePtr& outputRowType,
      const std::string& outputDirectoryPath,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& partitionedBy = {},
      const std::shared_ptr<connector::hive::HiveBucketProperty>&
          bucketProperty = nullptr) {
    return makeHiveInsertTableHandle(
        outputRowType->names(),
        outputRowType->children(),
        partitionedBy,
        bucketProperty,
        makeLocationHandle(
            outputDirectoryPath,
            std::nullopt,
            connector::hive::LocationHandle::TableType::kNew),
        fileFormat,
        CompressionKind::CompressionKind_ZSTD);
  }

  std::shared_ptr<HiveDataSink> createDataSink(
      const RowTypePtr& rowType,
      const std::string& outputDirectoryPath,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& partitionedBy = {},
      const std::shared_ptr<connector::hive::HiveBucketProperty>&
          bucketProperty = nullptr) {
    return std::make_shared<HiveDataSink>(
        rowType,
        createHiveInsertTableHandle(
            rowType,
            outputDirectoryPath,
            fileFormat,
            partitionedBy,
            bucketProperty),
        connectorQueryCtx_.get(),
        CommitStrategy::kNoCommit,
        connectorConfig_);
  }

  std::vector<std::string> listFiles(const std::string& dirPath) {
    std::vector<std::string> files;
    for (auto& dirEntry : fs::recursive_directory_iterator(dirPath)) {
      if (dirEntry.is_regular_file()) {
        files.push_back(dirEntry.path().string());
      }
    }
    return files;
  }

  void verifyWrittenData(const std::string& dirPath) {
    const std::vector<std::string> filePaths = listFiles(dirPath);
    ASSERT_EQ(filePaths.size(), 1);
    HiveConnectorTestBase::assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        {makeHiveConnectorSplit(filePaths[0])},
        fmt::format("SELECT * FROM tmp"));
  }

  void setConnectorConfig(
      std::unordered_map<std::string, std::string> connectorConfig) {
    connectorConfig_ =
        std::make_shared<core::MemConfig>(std::move(connectorConfig));
  }

  void setConnectorQueryContext(
      std::unique_ptr<ConnectorQueryCtx> connectorQueryCtx) {
    connectorQueryCtx_ = std::move(connectorQueryCtx);
  }

  const std::shared_ptr<memory::MemoryPool> pool_ =
      memory::addDefaultLeafMemoryPool();

  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::MemoryPool> opPool_;
  std::shared_ptr<memory::MemoryPool> connectorPool_;
  RowTypePtr rowType_;
  std::unique_ptr<ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<Config> connectorConfig_{std::make_unique<core::MemConfig>()};
  std::unique_ptr<folly::IOThreadPoolExecutor> spillExecutor_;
};

TEST_F(HiveDataSinkTest, hiveSortingColumn) {
  struct {
    std::string sortColumn;
    core::SortOrder sortOrder;
    bool badColumn;
    std::string exceptionString;
    std::string expectedToString;

    std::string debugString() const {
      return fmt::format(
          "sortColumn {} sortOrder {} badColumn {} exceptionString {} expectedToString {}",
          sortColumn,
          sortOrder.toString(),
          badColumn,
          exceptionString,
          expectedToString);
    }
  } testSettings[] = {
      {"a",
       core::SortOrder{true, true},
       false,
       "",
       "[COLUMN[a] ORDER[ASC NULLS FIRST]]"},
      {"a",
       core::SortOrder{false, false},
       false,
       "",
       "[COLUMN[a] ORDER[DESC NULLS LAST]]"},
      {"",
       core::SortOrder{true, true},
       true,
       "hive sort column must be set",
       ""},
      {"a",
       core::SortOrder{true, false},
       true,
       "Bad hive sort order: [COLUMN[a] ORDER[ASC NULLS LAST]]",
       ""},
      {"a",
       core::SortOrder{false, true},
       true,
       "Bad hive sort order: [COLUMN[a] ORDER[DESC NULLS FIRST]]",
       ""}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    if (testData.badColumn) {
      VELOX_ASSERT_THROW(
          HiveSortingColumn(testData.sortColumn, testData.sortOrder),
          testData.exceptionString);
      continue;
    }
    const HiveSortingColumn column(testData.sortColumn, testData.sortOrder);
    ASSERT_EQ(column.sortOrder(), testData.sortOrder);
    ASSERT_EQ(column.sortColumn(), testData.sortColumn);
    ASSERT_EQ(column.toString(), testData.expectedToString);
    auto obj = column.serialize();
    const auto deserializedColumn = HiveSortingColumn::deserialize(obj, pool());
    ASSERT_EQ(obj, deserializedColumn->serialize());
  }
}

TEST_F(HiveDataSinkTest, hiveBucketProperty) {
  const std::vector<std::string> columns = {"a", "b", "c"};
  const std::vector<TypePtr> types = {INTEGER(), VARCHAR(), BIGINT()};
  const std::vector<std::shared_ptr<const HiveSortingColumn>> sortedColumns = {
      std::make_shared<HiveSortingColumn>("d", core::SortOrder{false, false}),
      std::make_shared<HiveSortingColumn>("e", core::SortOrder{false, false}),
      std::make_shared<HiveSortingColumn>("f", core::SortOrder{true, true})};
  struct {
    HiveBucketProperty::Kind kind;
    std::vector<std::string> bucketedBy;
    std::vector<TypePtr> bucketedTypes;
    uint32_t bucketCount;
    std::vector<std::shared_ptr<const HiveSortingColumn>> sortedBy;
    bool badProperty;
    std::string exceptionString;
    std::string expectedToString;
  } testSettings[] = {
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0]},
       {types[0], types[1]},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {types[0]},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {},
       {types[0]},
       4,
       {},
       true,
       "Hive bucket columns must be set",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {types[0], types[1]},
       0,
       {},
       true,
       "Hive bucket count can't be zero",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {types[0], types[1]},
       4,
       {},
       false,
       "",
       "\nHiveBucketProperty[<PRESTO_NATIVE 4>\n"
       "\tBucket Columns:\n"
       "\t\ta\n"
       "\t\tb\n"
       "\tBucket Types:\n"
       "\t\tINTEGER\n"
       "\t\tVARCHAR\n"
       "]\n"},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0]},
       {types[0]},
       4,
       {},
       false,
       "",
       "\nHiveBucketProperty[<PRESTO_NATIVE 4>\n\tBucket Columns:\n\t\ta\n\tBucket Types:\n\t\tINTEGER\n]\n"},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {types[0], types[1]},
       4,
       {{sortedColumns[0]}},
       false,
       "",
       "\nHiveBucketProperty[<PRESTO_NATIVE 4>\n\tBucket Columns:\n\t\ta\n\t\tb\n\tBucket Types:\n\t\tINTEGER\n\t\tVARCHAR\n\tSortedBy Columns:\n\t\t[COLUMN[d] ORDER[DESC NULLS LAST]]\n]\n"},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0]},
       {types[0]},
       4,
       {{sortedColumns[0], sortedColumns[2]}},
       false,
       "",
       "\nHiveBucketProperty[<PRESTO_NATIVE 4>\n\tBucket Columns:\n\t\ta\n\tBucket Types:\n\t\tINTEGER\n\tSortedBy Columns:\n\t\t[COLUMN[d] ORDER[DESC NULLS LAST]]\n\t\t[COLUMN[f] ORDER[ASC NULLS FIRST]]\n]\n"},

      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0]},
       {types[0], types[1]},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {types[0]},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {},
       {types[0]},
       4,
       {},
       true,
       "Hive bucket columns must be set",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {types[0], types[1]},
       0,
       {},
       true,
       "Hive bucket count can't be zero",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {types[0], types[1]},
       4,
       {},
       false,
       "",
       "\nHiveBucketProperty[<HIVE_COMPATIBLE 4>\n"
       "\tBucket Columns:\n"
       "\t\ta\n"
       "\t\tb\n"
       "\tBucket Types:\n"
       "\t\tINTEGER\n"
       "\t\tVARCHAR\n"
       "]\n"},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0]},
       {types[0]},
       4,
       {},
       false,
       "",
       "\nHiveBucketProperty[<HIVE_COMPATIBLE 4>\n\tBucket Columns:\n\t\ta\n\tBucket Types:\n\t\tINTEGER\n]\n"},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {types[0], types[1]},
       4,
       {{sortedColumns[0]}},
       false,
       "",
       "\nHiveBucketProperty[<HIVE_COMPATIBLE 4>\n\tBucket Columns:\n\t\ta\n\t\tb\n\tBucket Types:\n\t\tINTEGER\n\t\tVARCHAR\n\tSortedBy Columns:\n\t\t[COLUMN[d] ORDER[DESC NULLS LAST]]\n]\n"},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0]},
       {types[0]},
       4,
       {{sortedColumns[0], sortedColumns[2]}},
       false,
       "",
       "\nHiveBucketProperty[<HIVE_COMPATIBLE 4>\n\tBucket Columns:\n\t\ta\n\tBucket Types:\n\t\tINTEGER\n\tSortedBy Columns:\n\t\t[COLUMN[d] ORDER[DESC NULLS LAST]]\n\t\t[COLUMN[f] ORDER[ASC NULLS FIRST]]\n]\n"},
  };
  for (const auto& testData : testSettings) {
    if (testData.badProperty) {
      VELOX_ASSERT_THROW(
          HiveBucketProperty(
              testData.kind,
              testData.bucketCount,
              testData.bucketedBy,
              testData.bucketedTypes,
              testData.sortedBy),
          testData.exceptionString);
      continue;
    }
    HiveBucketProperty hiveProperty(
        testData.kind,
        testData.bucketCount,
        testData.bucketedBy,
        testData.bucketedTypes,
        testData.sortedBy);
    ASSERT_EQ(hiveProperty.kind(), testData.kind);
    ASSERT_EQ(hiveProperty.sortedBy(), testData.sortedBy);
    ASSERT_EQ(hiveProperty.bucketedBy(), testData.bucketedBy);
    ASSERT_EQ(hiveProperty.bucketedTypes(), testData.bucketedTypes);
    ASSERT_EQ(hiveProperty.toString(), testData.expectedToString);

    auto obj = hiveProperty.serialize();
    const auto deserializedProperty =
        HiveBucketProperty::deserialize(obj, pool());
    ASSERT_EQ(obj, deserializedProperty->serialize());
  }
}

TEST_F(HiveDataSinkTest, basic) {
  const int numBatches = 10;
  const auto outputDirectory = TempDirectoryPath::create();
  auto dataSink = createDataSink(rowType_, outputDirectory->path);

  VectorFuzzer::Options options;
  options.vectorSize = 500;
  VectorFuzzer fuzzer(options, pool());
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numBatches; ++i) {
    vectors.push_back(fuzzer.fuzzRow(rowType_));
    dataSink->appendData(vectors.back());
  }
  const auto results = dataSink->close(true);
  ASSERT_EQ(results.size(), 1);

  createDuckDbTable(vectors);
  verifyWrittenData(outputDirectory->path);
}

TEST_F(HiveDataSinkTest, close) {
  for (bool empty : {true, false}) {
    SCOPED_TRACE(fmt::format("Data sink is empty: {}", empty));
    const auto outputDirectory = TempDirectoryPath::create();
    auto dataSink = createDataSink(rowType_, outputDirectory->path);

    std::vector<RowVectorPtr> vectors;
    VectorFuzzer::Options options;
    options.vectorSize = 1;
    VectorFuzzer fuzzer(options, pool());
    vectors.push_back(fuzzer.fuzzRow(rowType_));
    if (!empty) {
      dataSink->appendData(vectors.back());
      ASSERT_GT(dataSink->getCompletedBytes(), 0);
    } else {
      ASSERT_EQ(dataSink->getCompletedBytes(), 0);
    }
    const auto results = dataSink->close(true);
    // Can't append after close.
    VELOX_ASSERT_THROW(
        dataSink->appendData(vectors.back()), "Hive data sink has been closed");
    ASSERT_EQ(dataSink->close(true), results);
    VELOX_ASSERT_THROW(
        dataSink->close(false), "Can't abort a closed hive data sink");

    if (!empty) {
      ASSERT_EQ(results.size(), 1);
      ASSERT_GT(dataSink->getCompletedBytes(), 0);
      createDuckDbTable(vectors);
      verifyWrittenData(outputDirectory->path);
    } else {
      ASSERT_TRUE(results.empty());
      ASSERT_EQ(dataSink->getCompletedBytes(), 0);
    }
  }
}

TEST_F(HiveDataSinkTest, abort) {
  for (bool empty : {true, false}) {
    SCOPED_TRACE(fmt::format("Data sink is empty: {}", empty));
    const auto outputDirectory = TempDirectoryPath::create();
    auto dataSink = createDataSink(rowType_, outputDirectory->path);

    std::vector<RowVectorPtr> vectors;
    VectorFuzzer::Options options;
    options.vectorSize = 1;
    VectorFuzzer fuzzer(options, pool());
    vectors.push_back(fuzzer.fuzzRow(rowType_));
    int initialBytes = 0;
    if (!empty) {
      dataSink->appendData(vectors.back());
      initialBytes = dataSink->getCompletedBytes();
      ASSERT_GT(initialBytes, 0);
    } else {
      initialBytes = dataSink->getCompletedBytes();
      ASSERT_EQ(initialBytes, 0);
    }
    ASSERT_TRUE(dataSink->close(false).empty());
    // Can't close after abort.
    VELOX_ASSERT_THROW(
        dataSink->close(true), "Can't close an aborted hive data sink");
    ASSERT_TRUE(dataSink->close(false).empty());
    // Can't append after abort.
    VELOX_ASSERT_THROW(
        dataSink->appendData(vectors.back()),
        "Hive data sink hash been aborted");
  }
}

TEST_F(HiveDataSinkTest, memoryReclaim) {
  VectorFuzzer::Options options;
  options.vectorSize = 500;
  VectorFuzzer fuzzer(options, pool());
  std::vector<RowVectorPtr> vectors;
  const int numBatches{10};
  for (int i = 0; i < numBatches; ++i) {
    vectors.push_back(fuzzer.fuzzInputRow(rowType_));
  }

  struct {
    dwio::common::FileFormat format;
    bool sortWriter;
    bool writerSpillEnabled;
    uint64_t writerFlushThreshold;
    bool expectedWriterReclaimEnabled;
    bool expectedWriterReclaimed;

    std::string debugString() const {
      return fmt::format(
          "format: {}, sortWriter: {}, writerSpillEnabled: {}, writerFlushThreshold: {}, expectedWriterReclaimEnabled: {}, expectedWriterReclaimed: {}",
          format,
          sortWriter,
          writerSpillEnabled,
          succinctBytes(writerFlushThreshold),
          expectedWriterReclaimEnabled,
          expectedWriterReclaimed);
    }
  } testSettings[] = {
    {dwio::common::FileFormat::DWRF, true, true, 1 << 30, true, true},
    {dwio::common::FileFormat::DWRF, true, true, 1, true, true},
    {dwio::common::FileFormat::DWRF, true, false, 1 << 30, false, false},
    {dwio::common::FileFormat::DWRF, true, false, 1, false, false},
    {dwio::common::FileFormat::DWRF, false, true, 1 << 30, true, false},
    {dwio::common::FileFormat::DWRF, false, true, 1, true, true},
    {dwio::common::FileFormat::DWRF, false, false, 1 << 30, false, false},
    {dwio::common::FileFormat::DWRF, false, false, 1, false, false},
  // Add Parquet with https://github.com/facebookincubator/velox/issues/5560
#if 0
      {dwio::common::FileFormat::PARQUET, true, true, 1 << 30, false, false},
      {dwio::common::FileFormat::PARQUET, true, true, 1, false, false},
      {dwio::common::FileFormat::PARQUET, true, false, 1 << 30, false, false},
      {dwio::common::FileFormat::PARQUET, true, false, 1, false, false},
      {dwio::common::FileFormat::PARQUET, false, true, 1 << 30, false, false},
      {dwio::common::FileFormat::PARQUET, false, true, 1, false, false},
      {dwio::common::FileFormat::PARQUET, false, false, 1 << 30, false, false},
      {dwio::common::FileFormat::PARQUET, false, false, 1, false, false}
#endif
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemoryPools();

    const auto outputDirectory = TempDirectoryPath::create();
    std::shared_ptr<HiveBucketProperty> bucketProperty;
    std::vector<std::string> partitionBy;
    if (testData.sortWriter) {
      partitionBy = {"c6"};
      bucketProperty = std::make_shared<HiveBucketProperty>(
          HiveBucketProperty::Kind::kHiveCompatible,
          4,
          std::vector<std::string>{"c0"},
          std::vector<TypePtr>{BIGINT()},
          std::vector<std::shared_ptr<const HiveSortingColumn>>{
              std::make_shared<HiveSortingColumn>(
                  "c1", core::SortOrder{false, false})});
    }
    std::shared_ptr<TempDirectoryPath> spillDirectory;
    std::unique_ptr<SpillConfig> spillConfig;
    if (testData.writerSpillEnabled) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      spillConfig =
          getSpillConfig(spillDirectory->path, testData.writerFlushThreshold);
      auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
          opPool_.get(),
          connectorPool_.get(),
          connectorConfig_.get(),
          spillConfig.get(),
          nullptr,
          nullptr,
          "query.HiveDataSinkTest",
          "task.HiveDataSinkTest",
          "planNodeId.HiveDataSinkTest",
          0);
      setConnectorQueryContext(std::move(connectorQueryCtx));
    } else {
      auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
          opPool_.get(),
          connectorPool_.get(),
          connectorConfig_.get(),
          nullptr,
          nullptr,
          nullptr,
          "query.HiveDataSinkTest",
          "task.HiveDataSinkTest",
          "planNodeId.HiveDataSinkTest",
          0);
      setConnectorQueryContext(std::move(connectorQueryCtx));
    }

    auto dataSink = createDataSink(
        rowType_,
        outputDirectory->path,
        testData.format,
        partitionBy,
        bucketProperty);
    auto* hiveDataSink = static_cast<HiveDataSink*>(dataSink.get());
    ASSERT_EQ(
        hiveDataSink->canReclaim(), testData.expectedWriterReclaimEnabled);
    for (int i = 0; i < numBatches; ++i) {
      dataSink->appendData(vectors[i]);
    }
    memory::MemoryReclaimer::Stats stats;
    uint64_t reclaimableBytes;
    if (testData.expectedWriterReclaimed) {
      ASSERT_TRUE(root_->reclaimableBytes(reclaimableBytes));
      ASSERT_GT(reclaimableBytes, 0);
      ASSERT_GT(root_->reclaim(256L << 20, stats), 0);
      // We expect dwrf writer set numNonReclaimableAttempts counter.
      ASSERT_LE(stats.numNonReclaimableAttempts, 1);
    } else {
      ASSERT_FALSE(root_->reclaimableBytes(reclaimableBytes));
      ASSERT_EQ(reclaimableBytes, 0);
      ASSERT_EQ(root_->reclaim(256L << 20, stats), 0);
      if (testData.expectedWriterReclaimEnabled) {
        if (testData.sortWriter) {
          ASSERT_GE(stats.numNonReclaimableAttempts, 1);
        } else {
          ASSERT_EQ(stats.numNonReclaimableAttempts, 1);
        }
      } else {
        ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
      }
    }
    const auto results = dataSink->close(true);
    ASSERT_GE(results.size(), 1);
  }
}

TEST_F(HiveDataSinkTest, memoryReclaimAfterClose) {
  VectorFuzzer::Options options;
  options.vectorSize = 500;
  VectorFuzzer fuzzer(options, pool());
  std::vector<RowVectorPtr> vectors;
  const int numBatches{10};
  for (int i = 0; i < numBatches; ++i) {
    vectors.push_back(fuzzer.fuzzInputRow(rowType_));
  }

  struct {
    dwio::common::FileFormat format;
    bool sortWriter;
    bool writerSpillEnabled;
    bool close;
    bool expectedWriterReclaimEnabled;

    std::string debugString() const {
      return fmt::format(
          "format: {}, sortWriter: {}, writerSpillEnabled: {}, close: {}, expectedWriterReclaimEnabled: {}",
          format,
          sortWriter,
          writerSpillEnabled,
          close,
          expectedWriterReclaimEnabled);
    }
  } testSettings[] = {
      {dwio::common::FileFormat::DWRF, true, true, true, true},
      {dwio::common::FileFormat::DWRF, true, false, true, false},
      {dwio::common::FileFormat::DWRF, true, true, false, true},
      {dwio::common::FileFormat::DWRF, true, false, false, false},
      {dwio::common::FileFormat::DWRF, false, true, true, true},
      {dwio::common::FileFormat::DWRF, false, false, true, false},
      {dwio::common::FileFormat::DWRF, false, true, false, true},
      {dwio::common::FileFormat::DWRF, false, false, false, false}
      // Add parquet file format after fix
      // https://github.com/facebookincubator/velox/issues/5560
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::unordered_map<std::string, std::string> connectorConfig;
    // Always allow memory reclaim from the file writer/
    connectorConfig.emplace(
        "file_writer_flush_threshold_bytes", folly::to<std::string>(0));
    // Avoid internal the stripe flush while data write.
    connectorConfig.emplace("orc_optimized_writer_max_stripe_size", "1GB");
    connectorConfig.emplace(
        "orc_optimized_writer_max_dictionary_memory", "1GB");

    setConnectorConfig(connectorConfig);

    const auto outputDirectory = TempDirectoryPath::create();
    std::shared_ptr<HiveBucketProperty> bucketProperty;
    std::vector<std::string> partitionBy;
    if (testData.sortWriter) {
      partitionBy = {"c6"};
      bucketProperty = std::make_shared<HiveBucketProperty>(
          HiveBucketProperty::Kind::kHiveCompatible,
          4,
          std::vector<std::string>{"c0"},
          std::vector<TypePtr>{BIGINT()},
          std::vector<std::shared_ptr<const HiveSortingColumn>>{
              std::make_shared<HiveSortingColumn>(
                  "c1", core::SortOrder{false, false})});
    }
    std::shared_ptr<TempDirectoryPath> spillDirectory;
    std::unique_ptr<SpillConfig> spillConfig;
    if (testData.writerSpillEnabled) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      spillConfig = getSpillConfig(spillDirectory->path, 0);
      auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
          opPool_.get(),
          connectorPool_.get(),
          connectorConfig_.get(),
          spillConfig.get(),
          nullptr,
          nullptr,
          "query.HiveDataSinkTest",
          "task.HiveDataSinkTest",
          "planNodeId.HiveDataSinkTest",
          0);
      setConnectorQueryContext(std::move(connectorQueryCtx));
    } else {
      auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
          opPool_.get(),
          connectorPool_.get(),
          connectorConfig_.get(),
          nullptr,
          nullptr,
          nullptr,
          "query.HiveDataSinkTest",
          "task.HiveDataSinkTest",
          "planNodeId.HiveDataSinkTest",
          0);
      setConnectorQueryContext(std::move(connectorQueryCtx));
    }

    auto dataSink = createDataSink(
        rowType_,
        outputDirectory->path,
        testData.format,
        partitionBy,
        bucketProperty);
    auto* hiveDataSink = static_cast<HiveDataSink*>(dataSink.get());
    ASSERT_EQ(
        hiveDataSink->canReclaim(), testData.expectedWriterReclaimEnabled);

    for (int i = 0; i < numBatches; ++i) {
      dataSink->appendData(vectors[i]);
    }
    if (testData.close) {
      const auto results = dataSink->close(true);
      ASSERT_GE(results.size(), 1);
    } else {
      const auto results = dataSink->close(false);
      ASSERT_TRUE(results.empty());
    }

    memory::MemoryReclaimer::Stats stats;
    uint64_t reclaimableBytes;
    if (testData.expectedWriterReclaimEnabled) {
      ASSERT_TRUE(root_->reclaimableBytes(reclaimableBytes));
      if (testData.close) {
        // NOTE: file writer might not release all the memory on close
        // immediately.
        ASSERT_GE(reclaimableBytes, 0);
      } else {
        ASSERT_EQ(reclaimableBytes, 0);
      }
    } else {
      ASSERT_FALSE(root_->reclaimableBytes(reclaimableBytes));
      ASSERT_EQ(reclaimableBytes, 0);
    }
    ASSERT_EQ(root_->reclaim(1L << 30, stats), 0);
    if (testData.expectedWriterReclaimEnabled) {
      if (testData.sortWriter) {
        ASSERT_GE(stats.numNonReclaimableAttempts, 1);
      } else {
        ASSERT_EQ(stats.numNonReclaimableAttempts, 1);
      }
    } else {
      ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
    }
  }
}
} // namespace
} // namespace facebook::velox::connector::hive

// This main is needed for some tests on linux.
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // Signal handler required for ThreadDebugInfoTest
  facebook::velox::process::addDefaultFatalSignalHandler();
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
