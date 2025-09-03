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

#include "velox/exec/tests/utils/TableWriterTestBase.h"

namespace velox::exec::test {

TableWriterTestBase::TestParam::TestParam(
    FileFormat fileFormat,
    TestMode testMode,
    CommitStrategy commitStrategy,
    HiveBucketProperty::Kind bucketKind,
    bool bucketSort,
    bool multiDrivers,
    CompressionKind compressionKind,
    bool scaleWriter) {
  value = (scaleWriter ? 1ULL << 56 : 0) |
      static_cast<uint64_t>(compressionKind) << 48 |
      static_cast<uint64_t>(!!multiDrivers) << 40 |
      static_cast<uint64_t>(fileFormat) << 32 |
      static_cast<uint64_t>(testMode) << 24 |
      static_cast<uint64_t>(commitStrategy) << 16 |
      static_cast<uint64_t>(bucketKind) << 8 | !!bucketSort;
}

CompressionKind TableWriterTestBase::TestParam::compressionKind() const {
  return static_cast<facebook::velox::common::CompressionKind>(
      (value & ((1L << 56) - 1)) >> 48);
}

bool TableWriterTestBase::TestParam::multiDrivers() const {
  return (value & (1L << 40)) != 0;
}

FileFormat TableWriterTestBase::TestParam::fileFormat() const {
  return static_cast<FileFormat>((value & ((1L << 40) - 1)) >> 32);
}

TableWriterTestBase::TestMode TableWriterTestBase::TestParam::testMode() const {
  return static_cast<TestMode>((value & ((1L << 32) - 1)) >> 24);
}

CommitStrategy TableWriterTestBase::TestParam::commitStrategy() const {
  return static_cast<CommitStrategy>((value & ((1L << 24) - 1)) >> 16);
}

HiveBucketProperty::Kind TableWriterTestBase::TestParam::bucketKind() const {
  return static_cast<HiveBucketProperty::Kind>((value & ((1L << 16) - 1)) >> 8);
}

bool TableWriterTestBase::TestParam::bucketSort() const {
  return (value & ((1L << 8) - 1)) != 0;
}

bool TableWriterTestBase::TestParam::scaleWriter() const {
  return (value >> 56) != 0;
}

std::string TableWriterTestBase::TestParam::toString() const {
  return fmt::format(
      "FileFormat_{}_TestMode_{}_commitStrategy_{}_bucketKind_{}_bucketSort_{}_multiDrivers_{}_compression_{}_scaleWriter_{}",
      dwio::common::toString((fileFormat())),
      testModeString(testMode()),
      commitStrategyToString(commitStrategy()),
      HiveBucketProperty::kindString(bucketKind()),
      bucketSort(),
      multiDrivers(),
      compressionKindToString(compressionKind()),
      scaleWriter());
}

std::string TableWriterTestBase::testModeString(TestMode mode) {
  switch (mode) {
    case TestMode::kUnpartitioned:
      return "UNPARTITIONED";
    case TestMode::kPartitioned:
      return "PARTITIONED";
    case TestMode::kBucketed:
      return "BUCKETED";
    case TestMode::kOnlyBucketed:
      return "BUCKETED_WITHOUT_PARTITION";
  }
  VELOX_UNREACHABLE();
}

// static
core::ColumnStatsSpec TableWriterTestBase::generateColumnStatsSpec(
    const std::string& name,
    const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys,
    AggregationNode::Step step) {
  core::TypedExprPtr inputField =
      std::make_shared<const core::FieldAccessTypedExpr>(BIGINT(), name);
  auto callExpr =
      std::make_shared<const core::CallTypedExpr>(BIGINT(), "min", inputField);
  std::vector<std::string> aggregateNames = {"min"};
  std::vector<core::AggregationNode::Aggregate> aggregates = {
      core::AggregationNode::Aggregate{
          callExpr, {{BIGINT()}}, nullptr, {}, {}}};
  return core::ColumnStatsSpec{
      std::move(groupingKeys), step, aggregateNames, aggregates};
}

// static.
std::function<PlanNodePtr(std::string, PlanNodePtr)>
TableWriterTestBase::addTableWriter(
    const RowTypePtr& inputColumns,
    const std::vector<std::string>& tableColumnNames,
    const std::optional<core::ColumnStatsSpec>& columnStatsSpec,
    const std::shared_ptr<core::InsertTableHandle>& insertHandle,
    bool hasPartitioningScheme,
    connector::CommitStrategy commitStrategy) {
  return [=](core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    return std::make_shared<core::TableWriteNode>(
        nodeId,
        inputColumns,
        tableColumnNames,
        columnStatsSpec,
        insertHandle,
        hasPartitioningScheme,
        TableWriteTraits::outputType(columnStatsSpec),
        commitStrategy,
        std::move(source));
  };
}

// static.
RowTypePtr TableWriterTestBase::getNonPartitionsColumns(
    const std::vector<std::string>& partitionedKeys,
    const RowTypePtr& rowType) {
  std::vector<std::string> dataColumnNames;
  std::vector<TypePtr> dataColumnTypes;
  for (auto i = 0; i < rowType->size(); i++) {
    auto name = rowType->names()[i];
    if (std::find(partitionedKeys.cbegin(), partitionedKeys.cend(), name) ==
        partitionedKeys.cend()) {
      dataColumnNames.emplace_back(name);
      dataColumnTypes.emplace_back(rowType->findChild(name));
    }
  }
  return ROW(std::move(dataColumnNames), std::move(dataColumnTypes));
}

TableWriterTestBase::TableWriterTestBase(uint64_t testValue)
    : testParam_(static_cast<TestParam>(testValue)),
      fileFormat_(testParam_.fileFormat()),
      testMode_(testParam_.testMode()),
      numTableWriterCount_(
          testParam_.multiDrivers() ? kNumTableWriterCount : 1),
      numPartitionedTableWriterCount_(
          testParam_.multiDrivers() ? kNumPartitionedTableWriterCount : 1),
      commitStrategy_(testParam_.commitStrategy()),
      compressionKind_(testParam_.compressionKind()),
      scaleWriter_(testParam_.scaleWriter()) {
  LOG(INFO) << testParam_.toString();
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()});
  setDataTypes(rowType);
  if (testMode_ == TestMode::kPartitioned || testMode_ == TestMode::kBucketed) {
    const std::vector<std::string> partitionBy = {"c0", "c1"};
    setPartitionBy(partitionBy);
    numPartitionKeyValues_ = {4, 4};
  }
  if (testMode_ == TestMode::kBucketed ||
      testMode_ == TestMode::kOnlyBucketed) {
    std::vector<std::string> bucketedBy = {"c3", "c5"};
    std::vector<TypePtr> bucketedTypes = {REAL(), VARCHAR()};
    std::vector<std::shared_ptr<const HiveSortingColumn>> sortedBy;
    if (testParam_.bucketSort()) {
      // The sortedBy key shouldn't contain partitionBy key.
      sortedBy = {std::make_shared<const HiveSortingColumn>(
          "c4", core::SortOrder{true, true})};
      // The sortColumnIndices_ should represent the indices after removing
      // the partition keys.
      if (testMode_ == TestMode::kBucketed) {
        sortColumnIndices_ = {2};
      } else {
        sortColumnIndices_ = {4};
      }
      sortedFlags_ = {{true, true}};
    }
    bucketProperty_ = std::make_shared<HiveBucketProperty>(
        testParam_.bucketKind(), 4, bucketedBy, bucketedTypes, sortedBy);
  }
}

void TableWriterTestBase::SetUp() {
  HiveConnectorTestBase::SetUp();
}

std::shared_ptr<Task> TableWriterTestBase::assertQueryWithWriterConfigs(
    const core::PlanNodePtr& plan,
    std::vector<std::shared_ptr<TempFilePath>> filePaths,
    const std::string& duckDbSql,
    bool spillEnabled) {
  std::vector<Split> splits;
  for (const auto& filePath : filePaths) {
    splits.push_back(Split(makeHiveConnectorSplit(filePath->getPath())));
  }
  if (!spillEnabled) {
    return AssertQueryBuilder(plan, duckDbQueryRunner_)
        .maxDrivers(
            2 * std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
        .config(
            QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
        .config(
            QueryConfig::kTaskPartitionedWriterCount,
            std::to_string(numPartitionedTableWriterCount_))
        // Scale writer settings to trigger partition rebalancing.
        .config(QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio, "1.0")
        .config(
            QueryConfig::kScaleWriterMinProcessedBytesRebalanceThreshold, "0")
        .config(
            QueryConfig::
                kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
            "0")
        .splits(splits)
        .assertResults(duckDbSql);
  }
  const auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  return AssertQueryBuilder(plan, duckDbQueryRunner_)
      .spillDirectory(spillDirectory->getPath())
      .maxDrivers(
          2 * std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
      .config(
          QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
      .config(
          QueryConfig::kTaskPartitionedWriterCount,
          std::to_string(numPartitionedTableWriterCount_))
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(QueryConfig::kWriterSpillEnabled, "true")
      // Scale writer settings to trigger partition rebalancing.
      .config(QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio, "1.0")
      .config(QueryConfig::kScaleWriterMinProcessedBytesRebalanceThreshold, "0")
      .config(
          QueryConfig::kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
          "0")
      .splits(splits)
      .assertResults(duckDbSql);
}

std::shared_ptr<Task> TableWriterTestBase::assertQueryWithWriterConfigs(
    const core::PlanNodePtr& plan,
    const std::string& duckDbSql,
    bool enableSpill) {
  if (!enableSpill) {
    TestScopedSpillInjection scopedSpillInjection(100);
    return AssertQueryBuilder(plan, duckDbQueryRunner_)
        .maxDrivers(
            2 * std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
        .config(
            QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
        .config(
            QueryConfig::kTaskPartitionedWriterCount,
            std::to_string(numPartitionedTableWriterCount_))
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(QueryConfig::kWriterSpillEnabled, "true")
        // Scale writer settings to trigger partition rebalancing.
        .config(QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio, "1.0")
        .config(
            QueryConfig::kScaleWriterMinProcessedBytesRebalanceThreshold, "0")
        .config(
            QueryConfig::
                kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
            "0")
        .assertResults(duckDbSql);
  }
  const auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  return AssertQueryBuilder(plan, duckDbQueryRunner_)
      .spillDirectory(spillDirectory->getPath())
      .maxDrivers(
          2 * std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
      .config(
          QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
      .config(
          QueryConfig::kTaskPartitionedWriterCount,
          std::to_string(numPartitionedTableWriterCount_))
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(QueryConfig::kWriterSpillEnabled, "true")
      // Scale writer settings to trigger partition rebalancing.
      .config(QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio, "1.0")
      .config(QueryConfig::kScaleWriterMinProcessedBytesRebalanceThreshold, "0")
      .config(
          QueryConfig::kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
          "0")
      .assertResults(duckDbSql);
}

RowVectorPtr TableWriterTestBase::runQueryWithWriterConfigs(
    const core::PlanNodePtr& plan,
    bool spillEnabled) {
  if (!spillEnabled) {
    return AssertQueryBuilder(plan, duckDbQueryRunner_)
        .maxDrivers(
            2 * std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
        .config(
            QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
        .config(
            QueryConfig::kTaskPartitionedWriterCount,
            std::to_string(numPartitionedTableWriterCount_))
        // Scale writer settings to trigger partition rebalancing.
        .config(QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio, "1.0")
        .config(
            QueryConfig::kScaleWriterMinProcessedBytesRebalanceThreshold, "0")
        .config(
            QueryConfig::
                kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
            "0")
        .copyResults(pool());
  }
  const auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  return AssertQueryBuilder(plan, duckDbQueryRunner_)
      .spillDirectory(spillDirectory->getPath())
      .maxDrivers(
          2 * std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
      .config(
          QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
      .config(
          QueryConfig::kTaskPartitionedWriterCount,
          std::to_string(numPartitionedTableWriterCount_))
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(QueryConfig::kWriterSpillEnabled, "true")
      // Scale writer settings to trigger partition rebalancing.
      .config(QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio, "1.0")
      .config(QueryConfig::kScaleWriterMinProcessedBytesRebalanceThreshold, "0")
      .config(
          QueryConfig::kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
          "0")
      .copyResults(pool());
}

void TableWriterTestBase::setCommitStrategy(CommitStrategy commitStrategy) {
  commitStrategy_ = commitStrategy;
}

void TableWriterTestBase::setPartitionBy(
    const std::vector<std::string>& partitionBy) {
  partitionedBy_ = partitionBy;
  for (const auto& partitionColumn : partitionedBy_) {
    for (int i = 0; i < rowType_->size(); ++i) {
      if (rowType_->nameOf(i) == partitionColumn) {
        partitionChannels_.emplace_back(i);
        partitionTypes_.emplace_back(rowType_->childAt(i));
      }
    }
  }
}

void TableWriterTestBase::setBucketProperty(
    HiveBucketProperty::Kind kind,
    uint32_t bucketCount,
    const std::vector<std::string>& bucketedBy,
    const std::vector<TypePtr>& bucketedTypes,
    const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortedBy) {
  bucketProperty_ = std::make_shared<HiveBucketProperty>(
      kind, bucketCount, bucketedBy, bucketedTypes, sortedBy);
}

void TableWriterTestBase::setDataTypes(
    const RowTypePtr& inputType,
    const RowTypePtr& tableSchema) {
  rowType_ = inputType;
  if (tableSchema != nullptr) {
    setTableSchema(tableSchema);
  } else {
    setTableSchema(rowType_);
  }
}

void TableWriterTestBase::setTableSchema(const RowTypePtr& tableSchema) {
  tableSchema_ = tableSchema;
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
TableWriterTestBase::makeHiveConnectorSplits(
    const std::shared_ptr<TempDirectoryPath>& directoryPath) {
  return makeHiveConnectorSplits(directoryPath->getPath());
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
TableWriterTestBase::makeHiveConnectorSplits(const std::string& directoryPath) {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
    if (path.is_regular_file()) {
      splits.push_back(HiveConnectorTestBase::makeHiveConnectorSplits(
          path.path().string(), 1, fileFormat_)[0]);
    }
  }
  return splits;
}

// Lists and returns all the regular files from a given directory
// recursively.
std::vector<std::string> TableWriterTestBase::listAllFiles(
    const std::string& directoryPath) {
  std::vector<std::string> files;
  for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
    if (path.is_regular_file()) {
      files.push_back(path.path().filename());
    }
  }
  return files;
}

// Builds and returns the hive splits from the list of files with one split
// per each file.
std::vector<std::shared_ptr<connector::ConnectorSplit>>
TableWriterTestBase::makeHiveConnectorSplits(
    const std::vector<std::filesystem::path>& filePaths) {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  for (const auto& filePath : filePaths) {
    splits.push_back(HiveConnectorTestBase::makeHiveConnectorSplits(
        filePath.string(), 1, fileFormat_)[0]);
  }
  return splits;
}

std::vector<RowVectorPtr> TableWriterTestBase::makeVectors(
    int32_t numVectors,
    int32_t rowsPerVector) {
  auto rowVectors =
      HiveConnectorTestBase::makeVectors(rowType_, numVectors, rowsPerVector);
  if (testMode_ == TestMode::kUnpartitioned ||
      testMode_ == TestMode::kOnlyBucketed) {
    return rowVectors;
  }
  // In case of partitioned table write test case, we ensure the number of
  // unique partition key values are capped.
  for (auto& rowVector : rowVectors) {
    auto c0PartitionVector =
        makeFlatVector<int64_t>(rowsPerVector, [&](auto /*unused*/) {
          return folly::Random().rand32() % numPartitionKeyValues_[0];
        });
    auto c1PartitionVector =
        makeFlatVector<int32_t>(rowsPerVector, [&](auto /*unused*/) {
          return folly::Random().rand32() % numPartitionKeyValues_[1];
        });
    rowVector->childAt(0) = c0PartitionVector;
    rowVector->childAt(1) = c1PartitionVector;
  }
  return rowVectors;
}

RowVectorPtr TableWriterTestBase::makeConstantVector(size_t size) {
  return makeRowVector(
      rowType_->names(),
      {makeConstant((int64_t)123'456, size),
       makeConstant((int32_t)321, size),
       makeConstant((int16_t)12'345, size),
       makeConstant(variant(TypeKind::REAL), size),
       makeConstant((double)1'234.01, size),
       makeConstant(variant(TypeKind::VARCHAR), size)});
}

std::vector<RowVectorPtr> TableWriterTestBase::makeBatches(
    vector_size_t numBatches,
    std::function<RowVectorPtr(int32_t)> makeVector) {
  std::vector<RowVectorPtr> batches;
  batches.reserve(numBatches);
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(makeVector(i));
  }
  return batches;
}

std::set<std::string> TableWriterTestBase::getLeafSubdirectories(
    const std::string& directoryPath) {
  std::set<std::string> subdirectories;
  for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
    if (path.is_regular_file()) {
      subdirectories.emplace(path.path().parent_path().string());
    }
  }
  return subdirectories;
}

std::vector<std::string> TableWriterTestBase::getRecursiveFiles(
    const std::string& directoryPath) {
  std::vector<std::string> files;
  for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
    if (path.is_regular_file()) {
      files.push_back(path.path().string());
    }
  }
  return files;
}

uint32_t TableWriterTestBase::countRecursiveFiles(
    const std::string& directoryPath) {
  return getRecursiveFiles(directoryPath).size();
}

// Helper method to return InsertTableHandle.
std::shared_ptr<core::InsertTableHandle>
TableWriterTestBase::createInsertTableHandle(
    const RowTypePtr& outputRowType,
    const connector::hive::LocationHandle::TableType& outputTableType,
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionedBy,
    const std::shared_ptr<HiveBucketProperty> bucketProperty,
    const std::optional<CompressionKind> compressionKind) {
  return std::make_shared<core::InsertTableHandle>(
      kHiveConnectorId,
      makeHiveInsertTableHandle(
          outputRowType->names(),
          outputRowType->children(),
          partitionedBy,
          bucketProperty,
          makeLocationHandle(
              outputDirectoryPath, std::nullopt, outputTableType),
          fileFormat_,
          compressionKind));
}

// Returns a table insert plan node.
PlanNodePtr TableWriterTestBase::createInsertPlan(
    PlanBuilder& inputPlan,
    const RowTypePtr& outputRowType,
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionedBy,
    std::shared_ptr<HiveBucketProperty> bucketProperty,
    const std::optional<CompressionKind> compressionKind,
    int numTableWriters,
    const connector::hive::LocationHandle::TableType& outputTableType,
    const CommitStrategy& outputCommitStrategy,
    bool aggregateResult,
    const std::optional<ColumnStatsSpec>& columnStatsSpec) {
  return createInsertPlan(
      inputPlan,
      inputPlan.planNode()->outputType(),
      outputRowType,
      outputDirectoryPath,
      partitionedBy,
      std::move(bucketProperty),
      compressionKind,
      numTableWriters,
      outputTableType,
      outputCommitStrategy,
      aggregateResult,
      columnStatsSpec);
}

PlanNodePtr TableWriterTestBase::createInsertPlan(
    PlanBuilder& inputPlan,
    const RowTypePtr& inputRowType,
    const RowTypePtr& tableRowType,
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionedBy,
    std::shared_ptr<HiveBucketProperty> bucketProperty,
    const std::optional<CompressionKind> compressionKind,
    int numTableWriters,
    const connector::hive::LocationHandle::TableType& outputTableType,
    const CommitStrategy& outputCommitStrategy,
    bool aggregateResult,
    const std::optional<ColumnStatsSpec>& columnStatsSpec) {
  if (numTableWriters == 1) {
    return createInsertPlanWithSingleWriter(
        inputPlan,
        inputRowType,
        tableRowType,
        outputDirectoryPath,
        partitionedBy,
        bucketProperty,
        compressionKind,
        outputTableType,
        outputCommitStrategy,
        aggregateResult,
        columnStatsSpec);
  } else if (bucketProperty_ == nullptr) {
    return createInsertPlanWithForNonBucketedTable(
        inputPlan,
        inputRowType,
        tableRowType,
        outputDirectoryPath,
        partitionedBy,
        compressionKind,
        outputTableType,
        outputCommitStrategy,
        aggregateResult,
        columnStatsSpec);
  } else {
    return createInsertPlanForBucketTable(
        inputPlan,
        inputRowType,
        tableRowType,
        outputDirectoryPath,
        partitionedBy,
        bucketProperty,
        compressionKind,
        outputTableType,
        outputCommitStrategy,
        aggregateResult,
        columnStatsSpec);
  }
}

PlanNodePtr TableWriterTestBase::createInsertPlanWithSingleWriter(
    PlanBuilder& inputPlan,
    const RowTypePtr& inputRowType,
    const RowTypePtr& tableRowType,
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionedBy,
    std::shared_ptr<HiveBucketProperty> bucketProperty,
    const std::optional<CompressionKind> compressionKind,
    const connector::hive::LocationHandle::TableType& outputTableType,
    const CommitStrategy& outputCommitStrategy,
    bool aggregateResult,
    std::optional<ColumnStatsSpec> columnStatsSpec) {
  const bool addScaleWriterExchange =
      scaleWriter_ && (bucketProperty != nullptr);
  auto insertPlan = inputPlan;
  if (addScaleWriterExchange) {
    if (!partitionedBy.empty()) {
      insertPlan.scaleWriterlocalPartition(
          inputColumnNames(partitionedBy, tableRowType, inputRowType));
    } else {
      insertPlan.scaleWriterlocalPartitionRoundRobin();
    }
  }
  insertPlan
      .addNode(addTableWriter(
          inputRowType,
          tableRowType->names(),
          columnStatsSpec,
          createInsertTableHandle(
              tableRowType,
              outputTableType,
              outputDirectoryPath,
              partitionedBy,
              bucketProperty,
              compressionKind),
          false,
          outputCommitStrategy))
      .capturePlanNodeId(tableWriteNodeId_);
  if (aggregateResult) {
    insertPlan.project({TableWriteTraits::rowCountColumnName()})
        .singleAggregation(
            {},
            {fmt::format("sum({})", TableWriteTraits::rowCountColumnName())});
  }
  return insertPlan.planNode();
}

PlanNodePtr TableWriterTestBase::createInsertPlanForBucketTable(
    PlanBuilder& inputPlan,
    const RowTypePtr& inputRowType,
    const RowTypePtr& tableRowType,
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionedBy,
    std::shared_ptr<HiveBucketProperty> bucketProperty,
    const std::optional<CompressionKind> compressionKind,
    const connector::hive::LocationHandle::TableType& outputTableType,
    const CommitStrategy& outputCommitStrategy,
    bool aggregateResult,
    std::optional<ColumnStatsSpec> columnStatsSpec) {
  // Since we might do column rename, so generate bucket property based on
  // the data type from 'inputPlan'.
  std::vector<std::string> bucketColumns;
  bucketColumns.reserve(bucketProperty->bucketedBy().size());
  for (int i = 0; i < bucketProperty->bucketedBy().size(); ++i) {
    bucketColumns.push_back(inputRowType->names()[tableRowType->getChildIdx(
        bucketProperty->bucketedBy()[i])]);
  }
  auto localPartitionBucketProperty = std::make_shared<HiveBucketProperty>(
      bucketProperty->kind(),
      bucketProperty->bucketCount(),
      bucketColumns,
      bucketProperty->bucketedTypes(),
      bucketProperty->sortedBy());
  auto insertPlan =
      inputPlan.localPartitionByBucket(localPartitionBucketProperty)
          .addNode(addTableWriter(
              inputRowType,
              tableRowType->names(),
              std::nullopt,
              createInsertTableHandle(
                  tableRowType,
                  outputTableType,
                  outputDirectoryPath,
                  partitionedBy,
                  bucketProperty,
                  compressionKind),
              false,
              outputCommitStrategy))
          .capturePlanNodeId(tableWriteNodeId_)
          .localPartition({})
          .tableWriteMerge();
  if (aggregateResult) {
    insertPlan.project({TableWriteTraits::rowCountColumnName()})
        .singleAggregation(
            {},
            {fmt::format("sum({})", TableWriteTraits::rowCountColumnName())});
  }
  return insertPlan.planNode();
}

// static
std::vector<std::string> TableWriterTestBase::inputColumnNames(
    const std::vector<std::string>& tableColumnNames,
    const RowTypePtr& tableRowType,
    const RowTypePtr& inputRowType) {
  std::vector<std::string> inputNames;
  inputNames.reserve(tableColumnNames.size());
  for (const auto& tableColumnName : tableColumnNames) {
    const auto columnIdx = tableRowType->getChildIdx(tableColumnName);
    inputNames.push_back(inputRowType->nameOf(columnIdx));
  }
  return inputNames;
}

PlanNodePtr TableWriterTestBase::createInsertPlanWithForNonBucketedTable(
    PlanBuilder& inputPlan,
    const RowTypePtr& inputRowType,
    const RowTypePtr& tableRowType,
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionedBy,
    const std::optional<CompressionKind> compressionKind,
    const connector::hive::LocationHandle::TableType& outputTableType,
    const CommitStrategy& outputCommitStrategy,
    bool aggregateResult,
    std::optional<ColumnStatsSpec> columnStatsSpec) {
  auto insertPlan = inputPlan;
  if (scaleWriter_) {
    if (!partitionedBy.empty()) {
      insertPlan.scaleWriterlocalPartition(
          inputColumnNames(partitionedBy, tableRowType, inputRowType));
    } else {
      insertPlan.scaleWriterlocalPartitionRoundRobin();
    }
  }
  insertPlan
      .addNode(addTableWriter(
          inputRowType,
          tableRowType->names(),
          std::nullopt,
          createInsertTableHandle(
              tableRowType,
              outputTableType,
              outputDirectoryPath,
              partitionedBy,
              nullptr,
              compressionKind),
          false,
          outputCommitStrategy))
      .capturePlanNodeId(tableWriteNodeId_)
      .localPartition(std::vector<std::string>{})
      .tableWriteMerge();
  if (aggregateResult) {
    insertPlan.project({TableWriteTraits::rowCountColumnName()})
        .singleAggregation(
            {},
            {fmt::format("sum({})", TableWriteTraits::rowCountColumnName())});
  }
  return insertPlan.planNode();
}

std::string TableWriterTestBase::partitionNameToPredicate(
    const std::string& partitionName,
    const std::vector<TypePtr>& partitionTypes) {
  std::vector<std::string> conjuncts;
  std::vector<std::string> partitionKeyValues;
  folly::split('/', partitionName, partitionKeyValues);
  VELOX_CHECK_EQ(partitionKeyValues.size(), partitionTypes.size());
  for (auto i = 0; i < partitionKeyValues.size(); ++i) {
    if (partitionTypes[i]->isVarchar() || partitionTypes[i]->isVarbinary() ||
        partitionTypes[i]->isDate()) {
      conjuncts.push_back(partitionKeyValues[i]
                              .replace(partitionKeyValues[i].find("="), 1, "='")
                              .append("'"));
    } else {
      conjuncts.push_back(partitionKeyValues[i]);
    }
  }
  return folly::join(" AND ", conjuncts);
}

std::string TableWriterTestBase::partitionNameToPredicate(
    const std::vector<std::string>& partitionDirNames) {
  std::vector<std::string> conjuncts;
  VELOX_CHECK_EQ(partitionDirNames.size(), partitionTypes_.size());
  std::vector<std::string> partitionKeyValues = partitionDirNames;
  for (auto i = 0; i < partitionDirNames.size(); ++i) {
    if (partitionTypes_[i]->isVarchar() || partitionTypes_[i]->isVarbinary() ||
        partitionTypes_[i]->isDate()) {
      conjuncts.push_back(partitionKeyValues[i]
                              .replace(partitionKeyValues[i].find("="), 1, "='")
                              .append("'"));
    } else {
      conjuncts.push_back(partitionDirNames[i]);
    }
  }
  return folly::join(" AND ", conjuncts);
}

void TableWriterTestBase::verifyUnbucketedFilePath(
    const std::filesystem::path& filePath,
    const std::string& targetDir) {
  ASSERT_EQ(filePath.parent_path().string(), targetDir);
  if (commitStrategy_ == CommitStrategy::kNoCommit) {
    ASSERT_TRUE(RE2::FullMatch(
        filePath.filename().string(),
        fmt::format(
            "test_cursor.+_[0-{}]_{}_.+",
            numTableWriterCount_ - 1,
            tableWriteNodeId_)))
        << filePath.filename().string();
  } else {
    ASSERT_TRUE(RE2::FullMatch(
        filePath.filename().string(),
        fmt::format(
            ".tmp.velox.test_cursor.+_[0-{}]_{}_.+",
            numTableWriterCount_ - 1,
            tableWriteNodeId_)))
        << filePath.filename().string();
  }
}

void TableWriterTestBase::verifyPartitionedFilePath(
    const std::filesystem::path& filePath,
    const std::string& targetDir) {
  verifyPartitionedDirPath(filePath.parent_path(), targetDir);
  verifyUnbucketedFilePath(filePath, filePath.parent_path().string());
}

void TableWriterTestBase::verifyBucketedFileName(
    const std::filesystem::path& filePath) {
  if (commitStrategy_ == CommitStrategy::kNoCommit) {
    if (fileFormat_ == FileFormat::PARQUET) {
      ASSERT_TRUE(RE2::FullMatch(
          filePath.filename().string(),
          "0[0-9]+_0_TaskCursorQuery_[0-9]+\\.parquet$"))
          << filePath.filename().string();
    } else {
      ASSERT_TRUE(RE2::FullMatch(
          filePath.filename().string(), "0[0-9]+_0_TaskCursorQuery_[0-9]+"))
          << filePath.filename().string();
    }
  } else {
    if (fileFormat_ == FileFormat::PARQUET) {
      ASSERT_TRUE(RE2::FullMatch(
          filePath.filename().string(),
          ".tmp.velox.0[0-9]+_0_TaskCursorQuery_[0-9]+_.+\\.parquet$"))
          << filePath.filename().string();
    } else {
      ASSERT_TRUE(RE2::FullMatch(
          filePath.filename().string(),
          ".tmp.velox.0[0-9]+_0_TaskCursorQuery_[0-9]+_.+"))
          << filePath.filename().string();
    }
  }
}

void TableWriterTestBase::verifyBucketedFilePath(
    const std::filesystem::path& filePath,
    const std::string& targetDir) {
  verifyPartitionedDirPath(filePath, targetDir);
  verifyBucketedFileName(filePath);
}

void TableWriterTestBase::verifyPartitionedDirPath(
    const std::filesystem::path& dirPath,
    const std::string& targetDir) {
  std::string regex(targetDir);
  bool matched{false};
  for (int i = 0; i < partitionedBy_.size(); ++i) {
    regex = fmt::format("{}/{}=.+", regex, partitionedBy_[i]);
    if (RE2::FullMatch(dirPath.string(), regex)) {
      matched = true;
      break;
    }
  }
  ASSERT_TRUE(matched) << dirPath;
}

uint32_t TableWriterTestBase::parseBucketId(const std::string& bucketFileName) {
  uint32_t bucketId;
  if (commitStrategy_ == CommitStrategy::kNoCommit) {
    VELOX_CHECK(RE2::FullMatch(bucketFileName, "(\\d+)_.+", &bucketId));
  } else {
    VELOX_CHECK(
        RE2::FullMatch(bucketFileName, ".tmp.velox.(\\d+)_.+", &bucketId));
  }
  return bucketId;
}

// Returns the list of partition directory names in the given directory
// path.
std::vector<std::string> TableWriterTestBase::getPartitionDirNames(
    const std::filesystem::path& dirPath) {
  std::vector<std::string> dirNames;
  auto nextPath = dirPath;
  for (int i = 0; i < partitionedBy_.size(); ++i) {
    dirNames.push_back(nextPath.filename().string());
    nextPath = nextPath.parent_path();
  }
  return dirNames;
}

void TableWriterTestBase::verifyPartitionedFilesData(
    const std::vector<std::filesystem::path>& filePaths,
    const std::filesystem::path& dirPath) {
  HiveConnectorTestBase::assertQuery(
      PlanBuilder().tableScan(rowType_).planNode(),
      {makeHiveConnectorSplits(filePaths)},
      fmt::format(
          "SELECT c2, c3, c4, c5 FROM tmp WHERE {}",
          partitionNameToPredicate(getPartitionDirNames(dirPath))));
}

std::unique_ptr<core::PartitionFunction> TableWriterTestBase::getBucketFunction(
    const RowTypePtr& outputType) {
  const auto& bucketedBy = bucketProperty_->bucketedBy();
  std::vector<column_index_t> bucketedByChannels;
  bucketedByChannels.reserve(bucketedBy.size());
  for (auto i = 0; i < bucketedBy.size(); ++i) {
    const auto& bucketColumn = bucketedBy[i];
    for (column_index_t columnChannel = 0; columnChannel < outputType->size();
         ++columnChannel) {
      if (outputType->nameOf(columnChannel) == bucketColumn) {
        bucketedByChannels.push_back(columnChannel);
        break;
      }
    }
  }
  VELOX_USER_CHECK_EQ(bucketedByChannels.size(), bucketedBy.size());
  return std::make_unique<HivePartitionFunction>(
      bucketProperty_->bucketCount(), bucketedByChannels);
}

void TableWriterTestBase::verifyBucketedFileData(
    const std::filesystem::path& filePath,
    const RowTypePtr& outputFileType) {
  const std::vector<std::filesystem::path> filePaths = {filePath};
  // Read data from bucketed file on disk into 'rowVector'.
  core::PlanNodeId scanNodeId;
  auto plan = PlanBuilder()
                  .tableScan(outputFileType, {}, "", outputFileType)
                  .capturePlanNodeId(scanNodeId)
                  .planNode();
  const auto resultVector =
      AssertQueryBuilder(plan)
          .splits(scanNodeId, makeHiveConnectorSplits(filePaths))
          .copyResults(pool_.get());
  // Parse the bucket id encoded in bucketed file name.
  const uint32_t expectedBucketId = parseBucketId(filePath.filename().string());
  // Compute the bucket id from read result by applying hash partition on
  // bucketed columns in read result, and we expect they all match the one
  // encoded in file name.
  auto bucketFunction = getBucketFunction(outputFileType);
  std::vector<uint32_t> bucketIds;
  bucketIds.reserve(resultVector->size());
  bucketFunction->partition(*resultVector, bucketIds);
  for (const auto bucketId : bucketIds) {
    ASSERT_EQ(expectedBucketId, bucketId);
  }
  if (!testParam_.bucketSort()) {
    return;
  }
  // Verifies the sorting behavior
  for (int i = 0; i < resultVector->size() - 1; ++i) {
    for (int j = 0; j < sortColumnIndices_.size(); ++j) {
      auto compareResult =
          resultVector->childAt(sortColumnIndices_.at(j))
              ->compare(
                  resultVector->childAt(sortColumnIndices_.at(j))
                      ->wrappedVector(),
                  i,
                  i + 1,
                  sortedFlags_[j]);
      if (compareResult.has_value()) {
        if (compareResult.value() < 0) {
          break;
        }
        ASSERT_EQ(compareResult.value(), 0);
      }
    }
  }
}

void TableWriterTestBase::verifyTableWriterOutput(
    const std::string& targetDir,
    const RowTypePtr& bucketCheckFileType,
    bool verifyPartitionedData,
    bool verifyBucketedData) {
  SCOPED_TRACE(testParam_.toString());
  std::vector<std::filesystem::path> filePaths;
  std::vector<std::filesystem::path> dirPaths;
  for (auto& path : fs::recursive_directory_iterator(targetDir)) {
    if (path.is_regular_file()) {
      filePaths.push_back(path.path());
    } else {
      dirPaths.push_back(path.path());
    }
  }
  if (testMode_ == TestMode::kUnpartitioned) {
    ASSERT_EQ(dirPaths.size(), 0);
    ASSERT_LE(filePaths.size(), numTableWriterCount_);
    verifyUnbucketedFilePath(filePaths[0], targetDir);
    return;
  } else if (testMode_ == TestMode::kOnlyBucketed) {
    ASSERT_EQ(dirPaths.size(), 0);
    for (const auto& filePath : filePaths) {
      ASSERT_EQ(filePath.parent_path().string(), targetDir);
      verifyBucketedFileName(filePath);
      if (verifyBucketedData) {
        verifyBucketedFileData(filePath, bucketCheckFileType);
      }
    }
    return;
  }
  // Validation for both partitioned with and without buckets.
  ASSERT_EQ(numPartitionKeyValues_.size(), 2);
  const auto totalPartitions =
      numPartitionKeyValues_[0] * numPartitionKeyValues_[1];
  ASSERT_LE(dirPaths.size(), totalPartitions + numPartitionKeyValues_[0]);
  int32_t numLeafDir{0};
  for (const auto& dirPath : dirPaths) {
    verifyPartitionedDirPath(dirPath, targetDir);
    if (dirPath.parent_path().string() != targetDir) {
      ++numLeafDir;
    }
  }
  if (testMode_ == TestMode::kPartitioned) {
    // We expect only one file under each directory without dynamic writer
    // support.
    ASSERT_GE(numLeafDir * numTableWriterCount_, filePaths.size());
    for (const auto& filePath : filePaths) {
      verifyPartitionedFilePath(filePath, targetDir);
      if (verifyPartitionedData) {
        verifyPartitionedFilesData({filePath}, filePath.parent_path());
      }
    }
    return;
  }
  ASSERT_GE(numLeafDir * bucketProperty_->bucketCount(), filePaths.size());
  std::unordered_map<std::string, std::vector<std::filesystem::path>>
      bucketFilesPerPartition;
  for (const auto& filePath : filePaths) {
    bucketFilesPerPartition[filePath.parent_path().string()].push_back(
        filePath);
    verifyBucketedFilePath(filePath, targetDir);
    if (verifyBucketedData) {
      verifyBucketedFileData(filePath, bucketCheckFileType);
    }
  }
  if (verifyPartitionedData) {
    for (const auto& entry : bucketFilesPerPartition) {
      verifyPartitionedFilesData(entry.second, entry.second[0].parent_path());
    }
  }
}

int TableWriterTestBase::getNumWriters() {
  return bucketProperty_ != nullptr ? numPartitionedTableWriterCount_
                                    : numTableWriterCount_;
}
} // namespace velox::exec::test
