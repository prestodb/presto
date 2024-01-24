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
#include "folly/dynamic.h"
#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <re2/re2.h>
#include <string>
#include "velox/dwio/common/Options.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::common;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::common::hll;

enum class TestMode {
  kUnpartitioned,
  kPartitioned,
  kBucketed,
};

std::string testModeString(TestMode mode) {
  switch (mode) {
    case TestMode::kUnpartitioned:
      return "UNPARTITIONED";
    case TestMode::kPartitioned:
      return "PARTITIONED";
    case TestMode::kBucketed:
      return "BUCKETED";
  }
}

static std::shared_ptr<core::AggregationNode> generateAggregationNode(
    const std::string& name,
    const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys,
    AggregationNode::Step step,
    const PlanNodePtr& source) {
  core::TypedExprPtr inputField =
      std::make_shared<const core::FieldAccessTypedExpr>(BIGINT(), name);
  auto callExpr = std::make_shared<const core::CallTypedExpr>(
      BIGINT(), std::vector<core::TypedExprPtr>{inputField}, "min");
  std::vector<std::string> aggregateNames = {"min"};
  std::vector<core::AggregationNode::Aggregate> aggregates = {
      core::AggregationNode::Aggregate{
          callExpr, {{BIGINT()}}, nullptr, {}, {}}};
  return std::make_shared<core::AggregationNode>(
      core::PlanNodeId(),
      step,
      groupingKeys,
      std::vector<core::FieldAccessTypedExprPtr>{},
      aggregateNames,
      aggregates,
      false, // ignoreNullKeys
      source);
}

std::function<PlanNodePtr(std::string, PlanNodePtr)> addTableWriter(
    const RowTypePtr& inputColumns,
    const std::vector<std::string>& tableColumnNames,
    const std::shared_ptr<core::AggregationNode>& aggregationNode,
    const std::shared_ptr<core::InsertTableHandle>& insertHandle,
    bool hasPartitioningScheme,
    connector::CommitStrategy commitStrategy =
        connector::CommitStrategy::kNoCommit) {
  return [=](core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    return std::make_shared<core::TableWriteNode>(
        nodeId,
        inputColumns,
        tableColumnNames,
        aggregationNode,
        insertHandle,
        hasPartitioningScheme,
        TableWriteTraits::outputType(aggregationNode),
        commitStrategy,
        std::move(source));
  };
}

RowTypePtr getNonPartitionsColumns(
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

FOLLY_ALWAYS_INLINE std::ostream& operator<<(std::ostream& os, TestMode mode) {
  os << testModeString(mode);
  return os;
}

// NOTE: google parameterized test framework can't handle complex test
// parameters properly. So we encode the different test parameters into one
// integer value.
struct TestParam {
  uint64_t value;

  explicit TestParam(uint64_t _value) : value(_value) {}

  TestParam(
      FileFormat fileFormat,
      TestMode testMode,
      CommitStrategy commitStrategy,
      HiveBucketProperty::Kind bucketKind,
      bool bucketSort,
      bool multiDrivers,
      CompressionKind compressionKind) {
    value = static_cast<uint64_t>(compressionKind) << 48 |
        static_cast<uint64_t>(!!multiDrivers) << 40 |
        static_cast<uint64_t>(fileFormat) << 32 |
        static_cast<uint64_t>(testMode) << 24 |
        static_cast<uint64_t>(commitStrategy) << 16 |
        static_cast<uint64_t>(bucketKind) << 8 | !!bucketSort;
  }

  CompressionKind compressionKind() const {
    return static_cast<facebook::velox::common::CompressionKind>(
        (value & ((1L << 56) - 1)) >> 48);
  }

  bool multiDrivers() const {
    return (value >> 40) != 0;
  }

  FileFormat fileFormat() const {
    return static_cast<FileFormat>((value & ((1L << 40) - 1)) >> 32);
  }

  TestMode testMode() const {
    return static_cast<TestMode>((value & ((1L << 32) - 1)) >> 24);
  }

  CommitStrategy commitStrategy() const {
    return static_cast<CommitStrategy>((value & ((1L << 24) - 1)) >> 16);
  }

  HiveBucketProperty::Kind bucketKind() const {
    return static_cast<HiveBucketProperty::Kind>(
        (value & ((1L << 16) - 1)) >> 8);
  }

  bool bucketSort() const {
    return (value & ((1L << 8) - 1)) != 0;
  }

  std::string toString() const {
    return fmt::format(
        "FileFormat[{}] TestMode[{}] commitStrategy[{}] bucketKind[{}] bucketSort[{}] multiDrivers[{}] compression[{}]",
        dwio::common::toString((fileFormat())),
        testModeString(testMode()),
        commitStrategyToString(commitStrategy()),
        HiveBucketProperty::kindString(bucketKind()),
        bucketSort(),
        multiDrivers(),
        compressionKindToString(compressionKind()));
  }
};

class TableWriteTest : public HiveConnectorTestBase {
 protected:
  explicit TableWriteTest(uint64_t testValue)
      : testParam_(static_cast<TestParam>(testValue)),
        fileFormat_(testParam_.fileFormat()),
        testMode_(testParam_.testMode()),
        numTableWriterCount_(
            testParam_.multiDrivers() ? kNumTableWriterCount : 1),
        numPartitionedTableWriterCount_(
            testParam_.multiDrivers() ? kNumPartitionedTableWriterCount : 1),
        commitStrategy_(testParam_.commitStrategy()),
        compressionKind_(testParam_.compressionKind()) {
    LOG(INFO) << testParam_.toString();

    auto rowType =
        ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
            {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()});
    setDataTypes(rowType);
    if (testMode_ != TestMode::kUnpartitioned) {
      const std::vector<std::string> partitionBy = {"c0", "c1"};
      setPartitionBy(partitionBy);
      numPartitionKeyValues_ = {4, 4};
    }
    if (testMode_ == TestMode::kBucketed) {
      std::vector<std::string> bucketedBy = {"c3", "c5"};
      std::vector<TypePtr> bucketedTypes = {REAL(), VARCHAR()};
      std::vector<std::shared_ptr<const HiveSortingColumn>> sortedBy;
      if (testParam_.bucketSort()) {
        // The sortedBy key shouldn't contain partitionBy key.
        sortedBy = {std::make_shared<const HiveSortingColumn>(
            "c4", core::SortOrder{true, true})};
        // The sortColumnIndices_ should represent the indices after removing
        // the partition keys.
        sortColumnIndices_ = {2};
        sortedFlags_ = {{true, true}};
      }
      bucketProperty_ = std::make_shared<HiveBucketProperty>(
          testParam_.bucketKind(), 4, bucketedBy, bucketedTypes, sortedBy);
    }
  }

  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  void TearDown() override {
    HiveConnectorTestBase::TearDown();
  }

  std::shared_ptr<Task> assertQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      std::vector<std::shared_ptr<TempFilePath>> filePaths,
      const std::string& duckDbSql,
      bool spillEnabled = false) {
    std::vector<Split> splits;
    for (const auto& filePath : filePaths) {
      splits.push_back(exec::Split(makeHiveConnectorSplit(filePath->path)));
    }
    if (!spillEnabled) {
      return AssertQueryBuilder(plan, duckDbQueryRunner_)
          .maxDrivers(
              2 *
              std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
          .config(
              QueryConfig::kTaskWriterCount,
              std::to_string(numTableWriterCount_))
          .config(
              QueryConfig::kTaskPartitionedWriterCount,
              std::to_string(numPartitionedTableWriterCount_))
          .splits(splits)
          .assertResults(duckDbSql);
    }
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    return AssertQueryBuilder(plan, duckDbQueryRunner_)
        .spillDirectory(spillDirectory->path)
        .maxDrivers(
            2 * std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
        .config(
            QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
        .config(
            QueryConfig::kTaskPartitionedWriterCount,
            std::to_string(numPartitionedTableWriterCount_))
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(QueryConfig::kWriterSpillEnabled, "true")
        .config(QueryConfig::kTestingSpillPct, "100")
        .splits(splits)
        .assertResults(duckDbSql);
  }

  std::shared_ptr<Task> assertQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      const std::string& duckDbSql,
      bool enableSpill = false) {
    if (!enableSpill) {
      return AssertQueryBuilder(plan, duckDbQueryRunner_)
          .maxDrivers(
              2 *
              std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
          .config(
              QueryConfig::kTaskWriterCount,
              std::to_string(numTableWriterCount_))
          .config(
              QueryConfig::kTaskPartitionedWriterCount,
              std::to_string(numPartitionedTableWriterCount_))
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(QueryConfig::kWriterSpillEnabled, "true")
          .config(QueryConfig::kTestingSpillPct, "100")
          .assertResults(duckDbSql);
    }

    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    return AssertQueryBuilder(plan, duckDbQueryRunner_)
        .spillDirectory(spillDirectory->path)
        .maxDrivers(
            2 * std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
        .config(
            QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
        .config(
            QueryConfig::kTaskPartitionedWriterCount,
            std::to_string(numPartitionedTableWriterCount_))
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(QueryConfig::kWriterSpillEnabled, "true")
        .config(QueryConfig::kTestingSpillPct, "100")
        .assertResults(duckDbSql);
  }

  RowVectorPtr runQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      bool spillEnabled = false) {
    if (!spillEnabled) {
      return AssertQueryBuilder(plan, duckDbQueryRunner_)
          .maxDrivers(
              2 *
              std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
          .config(
              QueryConfig::kTaskWriterCount,
              std::to_string(numTableWriterCount_))
          .config(
              QueryConfig::kTaskPartitionedWriterCount,
              std::to_string(numPartitionedTableWriterCount_))
          .copyResults(pool());
    }

    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    return AssertQueryBuilder(plan, duckDbQueryRunner_)
        .spillDirectory(spillDirectory->path)
        .maxDrivers(
            2 * std::max(kNumTableWriterCount, kNumPartitionedTableWriterCount))
        .config(
            QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
        .config(
            QueryConfig::kTaskPartitionedWriterCount,
            std::to_string(numPartitionedTableWriterCount_))
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(QueryConfig::kWriterSpillEnabled, "true")
        .config(QueryConfig::kTestingSpillPct, "100")
        .copyResults(pool());
  }

  void setCommitStrategy(CommitStrategy commitStrategy) {
    commitStrategy_ = commitStrategy;
  }

  void setPartitionBy(const std::vector<std::string>& partitionBy) {
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

  void setBucketProperty(
      HiveBucketProperty::Kind kind,
      uint32_t bucketCount,
      const std::vector<std::string>& bucketedBy,
      const std::vector<TypePtr>& bucketedTypes,
      const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortedBy =
          {}) {
    bucketProperty_ = std::make_shared<HiveBucketProperty>(
        kind, bucketCount, bucketedBy, bucketedTypes, sortedBy);
  }

  void setDataTypes(
      const RowTypePtr& inputType,
      const RowTypePtr& tableSchema = nullptr) {
    rowType_ = inputType;
    if (tableSchema != nullptr) {
      setTableSchema(tableSchema);
    } else {
      setTableSchema(rowType_);
    }
  }

  void setTableSchema(const RowTypePtr& tableSchema) {
    tableSchema_ = tableSchema;
  }

  std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeHiveConnectorSplits(
      const std::shared_ptr<TempDirectoryPath>& directoryPath) {
    return makeHiveConnectorSplits(directoryPath->path);
  }

  std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeHiveConnectorSplits(const std::string& directoryPath) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;

    for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
      if (path.is_regular_file()) {
        splits.push_back(HiveConnectorTestBase::makeHiveConnectorSplits(
            path.path().string(), 1, fileFormat_)[0]);
      }
    }

    return splits;
  }

  // Lists and returns all the regular files from a given directory recursively.
  std::vector<std::string> listAllFiles(const std::string& directoryPath) {
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
  makeHiveConnectorSplits(const std::vector<std::filesystem::path>& filePaths) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    for (const auto& filePath : filePaths) {
      splits.push_back(HiveConnectorTestBase::makeHiveConnectorSplits(
          filePath.string(), 1, fileFormat_)[0]);
    }
    return splits;
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t numVectors,
      int32_t rowsPerVector) {
    auto rowVectors =
        HiveConnectorTestBase::makeVectors(rowType_, numVectors, rowsPerVector);
    if (testMode_ == TestMode::kUnpartitioned) {
      return rowVectors;
    }
    // In case of partitioned table write test case, we ensure the number of
    // unique partition key values are capped.
    for (auto& rowVecotr : rowVectors) {
      auto c0PartitionVector =
          makeFlatVector<int64_t>(rowsPerVector, [&](auto /*unused*/) {
            return folly::Random().rand32() % numPartitionKeyValues_[0];
          });
      auto c1PartitionVector =
          makeFlatVector<int32_t>(rowsPerVector, [&](auto /*unused*/) {
            return folly::Random().rand32() % numPartitionKeyValues_[1];
          });
      rowVecotr->childAt(0) = c0PartitionVector;
      rowVecotr->childAt(1) = c1PartitionVector;
    }
    return rowVectors;
  }

  RowVectorPtr makeConstantVector(size_t size) {
    return makeRowVector(
        rowType_->names(),
        {makeConstant((int64_t)123'456, size),
         makeConstant((int32_t)321, size),
         makeConstant((int16_t)12'345, size),
         makeConstant(variant(TypeKind::REAL), size),
         makeConstant((double)1'234.01, size),
         makeConstant(variant(TypeKind::VARCHAR), size)});
  }

  std::vector<RowVectorPtr> makeBatches(
      vector_size_t numBatches,
      std::function<RowVectorPtr(int32_t)> makeVector) {
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);
    for (int32_t i = 0; i < numBatches; ++i) {
      batches.push_back(makeVector(i));
    }
    return batches;
  }

  std::set<std::string> getLeafSubdirectories(
      const std::string& directoryPath) {
    std::set<std::string> subdirectories;
    for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
      if (path.is_regular_file()) {
        subdirectories.emplace(path.path().parent_path().string());
      }
    }
    return subdirectories;
  }

  std::vector<std::string> getRecursiveFiles(const std::string& directoryPath) {
    std::vector<std::string> files;
    for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
      if (path.is_regular_file()) {
        files.push_back(path.path().string());
      }
    }
    return files;
  }

  uint32_t countRecursiveFiles(const std::string& directoryPath) {
    return getRecursiveFiles(directoryPath).size();
  }

  // Helper method to return InsertTableHandle.
  std::shared_ptr<core::InsertTableHandle> createInsertTableHandle(
      const RowTypePtr& outputRowType,
      const connector::hive::LocationHandle::TableType& outputTableType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy,
      const std::shared_ptr<HiveBucketProperty> bucketProperty,
      const std::optional<CompressionKind> compressionKind = {}) {
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
  PlanNodePtr createInsertPlan(
      PlanBuilder& inputPlan,
      const RowTypePtr& outputRowType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy = {},
      std::shared_ptr<HiveBucketProperty> bucketProperty = {},
      const std::optional<CompressionKind> compressionKind = {},
      int numTableWriters = 1,
      const connector::hive::LocationHandle::TableType& outputTableType =
          connector::hive::LocationHandle::TableType::kNew,
      const CommitStrategy& outputCommitStrategy = CommitStrategy::kNoCommit,
      bool aggregateResult = true,
      std::shared_ptr<core::AggregationNode> aggregationNode = nullptr) {
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
        aggregationNode);
  }

  PlanNodePtr createInsertPlan(
      PlanBuilder& inputPlan,
      const RowTypePtr& inputRowType,
      const RowTypePtr& tableRowType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy = {},
      std::shared_ptr<HiveBucketProperty> bucketProperty = {},
      const std::optional<CompressionKind> compressionKind = {},
      int numTableWriters = 1,
      const connector::hive::LocationHandle::TableType& outputTableType =
          connector::hive::LocationHandle::TableType::kNew,
      const CommitStrategy& outputCommitStrategy = CommitStrategy::kNoCommit,
      bool aggregateResult = true,
      std::shared_ptr<core::AggregationNode> aggregationNode = nullptr) {
    if (numTableWriters == 1) {
      auto insertPlan = inputPlan
                            .addNode(addTableWriter(
                                inputRowType,
                                tableRowType->names(),
                                aggregationNode,
                                createInsertTableHandle(
                                    tableRowType,
                                    outputTableType,
                                    outputDirectoryPath,
                                    partitionedBy,
                                    bucketProperty,
                                    compressionKind),
                                bucketProperty != nullptr,
                                outputCommitStrategy))
                            .capturePlanNodeId(tableWriteNodeId_);
      if (aggregateResult) {
        insertPlan.project({TableWriteTraits::rowCountColumnName()})
            .singleAggregation(
                {},
                {fmt::format(
                    "sum({})", TableWriteTraits::rowCountColumnName())});
      }
      return insertPlan.planNode();
    } else if (bucketProperty_ == nullptr) {
      auto insertPlan = inputPlan.localPartitionRoundRobin()
                            .addNode(addTableWriter(
                                inputRowType,
                                tableRowType->names(),
                                nullptr,
                                createInsertTableHandle(
                                    tableRowType,
                                    outputTableType,
                                    outputDirectoryPath,
                                    partitionedBy,
                                    bucketProperty,
                                    compressionKind),
                                bucketProperty != nullptr,
                                outputCommitStrategy))
                            .capturePlanNodeId(tableWriteNodeId_)
                            .localPartition(std::vector<std::string>{})
                            .tableWriteMerge();
      if (aggregateResult) {
        insertPlan.project({TableWriteTraits::rowCountColumnName()})
            .singleAggregation(
                {},
                {fmt::format(
                    "sum({})", TableWriteTraits::rowCountColumnName())});
      }
      return insertPlan.planNode();
    } else {
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
                  nullptr,
                  createInsertTableHandle(
                      tableRowType,
                      outputTableType,
                      outputDirectoryPath,
                      partitionedBy,
                      bucketProperty,
                      compressionKind),
                  bucketProperty != nullptr,
                  outputCommitStrategy))
              .capturePlanNodeId(tableWriteNodeId_)
              .localPartition({})
              .tableWriteMerge();
      if (aggregateResult) {
        insertPlan.project({TableWriteTraits::rowCountColumnName()})
            .singleAggregation(
                {},
                {fmt::format(
                    "sum({})", TableWriteTraits::rowCountColumnName())});
      }
      return insertPlan.planNode();
    }
  }

  RowVectorPtr makePartitionsVector(
      RowVectorPtr input,
      const std::vector<column_index_t>& partitionChannels) {
    std::vector<VectorPtr> partitions;
    std::vector<std::string> partitonKeyNames;
    std::vector<TypePtr> partitionKeyTypes;

    RowTypePtr inputType = asRowType(input->type());
    for (column_index_t channel : partitionChannels) {
      partitions.push_back(input->childAt(channel));
      partitonKeyNames.push_back(inputType->nameOf(channel));
      partitionKeyTypes.push_back(inputType->childAt(channel));
    }

    return std::make_shared<RowVector>(
        pool(),
        ROW(std::move(partitonKeyNames), std::move(partitionKeyTypes)),
        nullptr,
        input->size(),
        partitions);
  }

  // Parameter partitionName is string formatted in the Hive style
  // key1=value1/key2=value2/... Parameter partitionTypes are types of partition
  // keys in the same order as in partitionName.The return value is a SQL
  // predicate with values single quoted for string and date and not quoted for
  // other supported types, ex., key1='value1' AND key2=value2 AND ...
  std::string partitionNameToPredicate(
      const std::string& partitionName,
      const std::vector<TypePtr>& partitionTypes) {
    std::vector<std::string> conjuncts;

    std::vector<std::string> partitionKeyValues;
    folly::split('/', partitionName, partitionKeyValues);
    VELOX_CHECK_EQ(partitionKeyValues.size(), partitionTypes.size());

    for (auto i = 0; i < partitionKeyValues.size(); ++i) {
      if (partitionTypes[i]->isVarchar() || partitionTypes[i]->isVarbinary() ||
          partitionTypes[i]->isDate()) {
        conjuncts.push_back(
            partitionKeyValues[i]
                .replace(partitionKeyValues[i].find("="), 1, "='")
                .append("'"));
      } else {
        conjuncts.push_back(partitionKeyValues[i]);
      }
    }

    return folly::join(" AND ", conjuncts);
  }

  std::string partitionNameToPredicate(
      const std::vector<std::string>& partitionDirNames) {
    std::vector<std::string> conjuncts;
    VELOX_CHECK_EQ(partitionDirNames.size(), partitionTypes_.size());

    std::vector<std::string> partitionKeyValues = partitionDirNames;
    for (auto i = 0; i < partitionDirNames.size(); ++i) {
      if (partitionTypes_[i]->isVarchar() ||
          partitionTypes_[i]->isVarbinary() || partitionTypes_[i]->isDate()) {
        conjuncts.push_back(
            partitionKeyValues[i]
                .replace(partitionKeyValues[i].find("="), 1, "='")
                .append("'"));
      } else {
        conjuncts.push_back(partitionDirNames[i]);
      }
    }
    return folly::join(" AND ", conjuncts);
  }

  // Verifies if a unbucketed file name is encoded properly based on the
  // used commit strategy.
  void verifyUnbucketedFilePath(
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

  // Verifies if a partitioned file path (directory and file name) is encoded
  // properly.
  void verifyPartitionedFilePath(
      const std::filesystem::path& filePath,
      const std::string& targetDir) {
    verifyPartitionedDirPath(filePath.parent_path(), targetDir);
    verifyUnbucketedFilePath(filePath, filePath.parent_path().string());
  }

  // Verifies if a bucketed file path (directory and file name) is encoded
  // properly.
  void verifyBucketedFilePath(
      const std::filesystem::path& filePath,
      const std::string& targetDir) {
    verifyPartitionedDirPath(filePath, targetDir);
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

  // Verifies if the given partitioned table directory (names) are encoded
  // properly based on the used partitioned keys.
  void verifyPartitionedDirPath(
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

  // Parses and returns the bucket id encoded in the bucketed file name.
  uint32_t parseBucketId(const std::string& bucketFileName) {
    uint32_t bucketId;
    if (commitStrategy_ == CommitStrategy::kNoCommit) {
      VELOX_CHECK(RE2::FullMatch(bucketFileName, "(\\d+)_.+", &bucketId));
    } else {
      VELOX_CHECK(
          RE2::FullMatch(bucketFileName, ".tmp.velox.(\\d+)_.+", &bucketId));
    }
    return bucketId;
  }

  // Returns the list of partition directory names in the given directory path.
  std::vector<std::string> getPartitionDirNames(
      const std::filesystem::path& dirPath) {
    std::vector<std::string> dirNames;
    auto nextPath = dirPath;
    for (int i = 0; i < partitionedBy_.size(); ++i) {
      dirNames.push_back(nextPath.filename().string());
      nextPath = nextPath.parent_path();
    }
    return dirNames;
  }

  // Verifies the partitioned file data on disk by comparing with the same set
  // of data read from duckbd.
  void verifyPartitionedFilesData(
      const std::vector<std::filesystem::path>& filePaths,
      const std::filesystem::path& dirPath) {
    HiveConnectorTestBase::assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        {makeHiveConnectorSplits(filePaths)},
        fmt::format(
            "SELECT c2, c3, c4, c5 FROM tmp WHERE {}",
            partitionNameToPredicate(getPartitionDirNames(dirPath))));
  }

  // Gets the hash function used by the production code to build bucket id.
  std::unique_ptr<core::PartitionFunction> getBucketFunction(
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

  // Verifies the bucketed file data by checking if the bucket id of each read
  // row is the same as the one encoded in the corresponding bucketed file name.
  void verifyBucketedFileData(
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
    const uint32_t expectedBucketId =
        parseBucketId(filePath.filename().string());

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

  // Verifies the file layout and data produced by a table writer.
  void verifyTableWriterOutput(
      const std::string& targetDir,
      const RowTypePtr& bucketCheckFileType,
      bool verifyPartitionedData = true,
      bool verifyBucketedData = true) {
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
    }
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

  int getNumWriters() {
    return bucketProperty_ != nullptr ? numPartitionedTableWriterCount_
                                      : numTableWriterCount_;
  }

  static inline int kNumTableWriterCount = 4;
  static inline int kNumPartitionedTableWriterCount = 2;

  const TestParam testParam_;
  const FileFormat fileFormat_;
  const TestMode testMode_;
  // Returns all available table types to test insert without any
  // partitions (used in "immutablePartitions" set of tests).
  const std::vector<connector::hive::LocationHandle::TableType> tableTypes_ = {
      // Velox does not currently support TEMPORARY table type.
      // Once supported, it should be added to this list.
      connector::hive::LocationHandle::TableType::kNew,
      connector::hive::LocationHandle::TableType::kExisting};
  const int numTableWriterCount_;
  const int numPartitionedTableWriterCount_;

  RowTypePtr rowType_;
  RowTypePtr tableSchema_;
  CommitStrategy commitStrategy_;
  std::optional<CompressionKind> compressionKind_;
  std::vector<std::string> partitionedBy_;
  std::vector<TypePtr> partitionTypes_;
  std::vector<column_index_t> partitionChannels_;
  std::vector<uint32_t> numPartitionKeyValues_;
  std::vector<column_index_t> sortColumnIndices_;
  std::vector<CompareFlags> sortedFlags_;
  std::shared_ptr<HiveBucketProperty> bucketProperty_{nullptr};
  core::PlanNodeId tableWriteNodeId_;
};

class BasicTableWriteTest : public HiveConnectorTestBase {};

TEST_F(BasicTableWriteTest, roundTrip) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row * 2; }, nullEvery(7)),
  });

  auto sourceFilePath = TempFilePath::create();
  writeToFile(sourceFilePath->path, data);

  auto targetDirectoryPath = TempDirectoryPath::create();

  auto rowType = asRowType(data->type());
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .tableWrite(targetDirectoryPath->path)
                  .planNode();

  auto results = AssertQueryBuilder(plan)
                     .split(makeHiveConnectorSplit(sourceFilePath->path))
                     .copyResults(pool());
  ASSERT_EQ(2, results->size());

  // First column has number of rows written in the first row and nulls in other
  // rows.
  auto rowCount = results->childAt(TableWriteTraits::kRowCountChannel)
                      ->as<FlatVector<int64_t>>();
  ASSERT_FALSE(rowCount->isNullAt(0));
  ASSERT_EQ(size, rowCount->valueAt(0));
  ASSERT_TRUE(rowCount->isNullAt(1));

  // Second column contains details about written files.
  auto details = results->childAt(TableWriteTraits::kFragmentChannel)
                     ->as<FlatVector<StringView>>();
  ASSERT_TRUE(details->isNullAt(0));
  ASSERT_FALSE(details->isNullAt(1));
  folly::dynamic obj = folly::parseJson(details->valueAt(1));

  ASSERT_EQ(size, obj["rowCount"].asInt());
  auto fileWriteInfos = obj["fileWriteInfos"];
  ASSERT_EQ(1, fileWriteInfos.size());

  auto writeFileName = fileWriteInfos[0]["writeFileName"].asString();

  // Read from 'writeFileName' and verify the data matches the original.
  plan = PlanBuilder().tableScan(rowType).planNode();

  auto copy = AssertQueryBuilder(plan)
                  .split(makeHiveConnectorSplit(fmt::format(
                      "{}/{}", targetDirectoryPath->path, writeFileName)))
                  .copyResults(pool());
  assertEqualResults({data}, {copy});
}

class PartitionedTableWriterTest
    : public TableWriteTest,
      public testing::WithParamInterface<uint64_t> {
 public:
  PartitionedTableWriterTest() : TableWriteTest(GetParam()) {}

  static std::vector<uint64_t> getTestParams() {
    std::vector<uint64_t> testParams;
    const std::vector<bool> multiDriverOptions = {false, true};
    std::vector<FileFormat> fileFormats = {FileFormat::DWRF};
    if (hasWriterFactory(FileFormat::PARQUET)) {
      fileFormats.push_back(FileFormat::PARQUET);
    }
    for (bool multiDrivers : multiDriverOptions) {
      for (FileFormat fileFormat : fileFormats) {
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kPartitioned,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kPartitioned,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kPrestoNative,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kPrestoNative,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
      }
    }
    return testParams;
  }
};

class UnpartitionedTableWriterTest
    : public TableWriteTest,
      public testing::WithParamInterface<uint64_t> {
 public:
  UnpartitionedTableWriterTest() : TableWriteTest(GetParam()) {}

  static std::vector<uint64_t> getTestParams() {
    std::vector<uint64_t> testParams;
    const std::vector<bool> multiDriverOptions = {false, true};
    std::vector<FileFormat> fileFormats = {FileFormat::DWRF};
    if (hasWriterFactory(FileFormat::PARQUET)) {
      fileFormats.push_back(FileFormat::PARQUET);
    }
    for (bool multiDrivers : multiDriverOptions) {
      for (FileFormat fileFormat : fileFormats) {
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kUnpartitioned,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_NONE}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kUnpartitioned,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_NONE}
                                 .value);
      }
    }
    return testParams;
  }
};

class BucketedTableOnlyWriteTest
    : public TableWriteTest,
      public testing::WithParamInterface<uint64_t> {
 public:
  BucketedTableOnlyWriteTest() : TableWriteTest(GetParam()) {}

  static std::vector<uint64_t> getTestParams() {
    std::vector<uint64_t> testParams;
    const std::vector<bool> multiDriverOptions = {false, true};
    std::vector<FileFormat> fileFormats = {FileFormat::DWRF};
    if (hasWriterFactory(FileFormat::PARQUET)) {
      fileFormats.push_back(FileFormat::PARQUET);
    }
    for (bool multiDrivers : multiDriverOptions) {
      for (FileFormat fileFormat : fileFormats) {
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            true,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            true,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kPrestoNative,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kPrestoNative,
            true,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kPrestoNative,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kPrestoNative,
            true,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
      }
    }
    return testParams;
  }
};

class BucketSortOnlyTableWriterTest
    : public TableWriteTest,
      public testing::WithParamInterface<uint64_t> {
 public:
  BucketSortOnlyTableWriterTest() : TableWriteTest(GetParam()) {}

  static std::vector<uint64_t> getTestParams() {
    std::vector<uint64_t> testParams;
    const std::vector<bool> multiDriverOptions = {false, true};
    // Add Parquet with https://github.com/facebookincubator/velox/issues/5560
    std::vector<FileFormat> fileFormats = {FileFormat::DWRF};
    for (bool multiDrivers : multiDriverOptions) {
      for (FileFormat fileFormat : fileFormats) {
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            true,
            multiDrivers,
            facebook::velox::common::CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            true,
            multiDrivers,
            facebook::velox::common::CompressionKind_NONE}
                                 .value);
      }
    }
    return testParams;
  }
};

class PartitionedWithoutBucketTableWriterTest
    : public TableWriteTest,
      public testing::WithParamInterface<uint64_t> {
 public:
  PartitionedWithoutBucketTableWriterTest() : TableWriteTest(GetParam()) {}

  static std::vector<uint64_t> getTestParams() {
    std::vector<uint64_t> testParams;
    const std::vector<bool> multiDriverOptions = {false, true};
    std::vector<FileFormat> fileFormats = {FileFormat::DWRF};
    if (hasWriterFactory(FileFormat::PARQUET)) {
      fileFormats.push_back(FileFormat::PARQUET);
    }
    for (bool multiDrivers : multiDriverOptions) {
      for (FileFormat fileFormat : fileFormats) {
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kPartitioned,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kPartitioned,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            true,
            CompressionKind_ZSTD}
                                 .value);
      }
    }
    return testParams;
  }
};

class AllTableWriterTest : public TableWriteTest,
                           public testing::WithParamInterface<uint64_t> {
 public:
  AllTableWriterTest() : TableWriteTest(GetParam()) {}

  static std::vector<uint64_t> getTestParams() {
    std::vector<uint64_t> testParams;
    const std::vector<bool> multiDriverOptions = {false, true};
    std::vector<FileFormat> fileFormats = {FileFormat::DWRF};
    if (hasWriterFactory(FileFormat::PARQUET)) {
      fileFormats.push_back(FileFormat::PARQUET);
    }
    for (bool multiDrivers : multiDriverOptions) {
      for (FileFormat fileFormat : fileFormats) {
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kUnpartitioned,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kUnpartitioned,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kPartitioned,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kPartitioned,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kHiveCompatible,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kNoCommit,
            HiveBucketProperty::Kind::kPrestoNative,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
        testParams.push_back(TestParam{
            fileFormat,
            TestMode::kBucketed,
            CommitStrategy::kTaskCommit,
            HiveBucketProperty::Kind::kPrestoNative,
            false,
            multiDrivers,
            CompressionKind_ZSTD}
                                 .value);
      }
    }
    return testParams;
  }
};

// Runs a pipeline with read + filter + project (with substr) + write.
TEST_P(AllTableWriterTest, scanFilterProjectWrite) {
  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 500);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();

  auto planBuilder = PlanBuilder();
  auto project = planBuilder.tableScan(rowType_).filter("c2 <> 0").project(
      {"c0", "c1", "c3", "c5", "c2 + c3", "substr(c5, 1, 1)"});

  auto intputTypes = project.planNode()->outputType()->children();
  std::vector<std::string> tableColumnNames = {
      "c0", "c1", "c3", "c5", "c2_plus_c3", "substr_c5"};
  const auto outputType =
      ROW(std::move(tableColumnNames), std::move(intputTypes));

  auto plan = createInsertPlan(
      project,
      outputType,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  assertQueryWithWriterConfigs(
      plan, filePaths, "SELECT count(*) FROM tmp WHERE c2 <> 0");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query.
  if (partitionedBy_.size() > 0) {
    auto newOutputType = getNonPartitionsColumns(partitionedBy_, outputType);
    assertQuery(
        PlanBuilder().tableScan(newOutputType).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT c3, c5, c2 + c3, substr(c5, 1, 1) FROM tmp WHERE c2 <> 0");
    verifyTableWriterOutput(outputDirectory->path, newOutputType, false);
  } else {
    assertQuery(
        PlanBuilder().tableScan(outputType).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT c0, c1, c3, c5, c2 + c3, substr(c5, 1, 1) FROM tmp WHERE c2 <> 0");
    verifyTableWriterOutput(outputDirectory->path, outputType, false);
  }
}

TEST_P(AllTableWriterTest, renameAndReorderColumns) {
  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 500);
  for (int i = 0; i < filePaths.size(); ++i) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();

  if (testMode_ != TestMode::kUnpartitioned) {
    const std::vector<std::string> partitionBy = {"x", "y"};
    setPartitionBy(partitionBy);
  }
  if (testMode_ == TestMode::kBucketed) {
    setBucketProperty(
        bucketProperty_->kind(),
        bucketProperty_->bucketCount(),
        {"z", "v"},
        {REAL(), VARCHAR()},
        {});
  }

  auto inputRowType =
      ROW({"c2", "c5", "c4", "c1", "c0", "c3"},
          {SMALLINT(), VARCHAR(), DOUBLE(), INTEGER(), BIGINT(), REAL()});

  setTableSchema(
      ROW({"u", "v", "w", "x", "y", "z"},
          {SMALLINT(), VARCHAR(), DOUBLE(), INTEGER(), BIGINT(), REAL()}));

  auto plan = createInsertPlan(
      PlanBuilder().tableScan(rowType_),
      inputRowType,
      tableSchema_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  assertQueryWithWriterConfigs(plan, filePaths, "SELECT count(*) FROM tmp");

  if (partitionedBy_.size() > 0) {
    auto newOutputType = getNonPartitionsColumns(partitionedBy_, tableSchema_);
    HiveConnectorTestBase::assertQuery(
        PlanBuilder().tableScan(newOutputType).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT c2, c5, c4, c3 FROM tmp");

    verifyTableWriterOutput(outputDirectory->path, newOutputType, false);
  } else {
    HiveConnectorTestBase::assertQuery(
        PlanBuilder().tableScan(tableSchema_).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT c2, c5, c4, c1, c0, c3 FROM tmp");

    verifyTableWriterOutput(outputDirectory->path, tableSchema_, false);
  }
}

// Runs a pipeline with read + write.
TEST_P(AllTableWriterTest, directReadWrite) {
  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 200);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = createInsertPlan(
      PlanBuilder().tableScan(rowType_),
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query.

  if (partitionedBy_.size() > 0) {
    auto newOutputType = getNonPartitionsColumns(partitionedBy_, tableSchema_);
    assertQuery(
        PlanBuilder().tableScan(newOutputType).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT c2, c3, c4, c5 FROM tmp");
    rowType_ = newOutputType;
    verifyTableWriterOutput(outputDirectory->path, rowType_);
  } else {
    assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT * FROM tmp");

    verifyTableWriterOutput(outputDirectory->path, rowType_);
  }
}

// Tests writing constant vectors.
TEST_P(AllTableWriterTest, constantVectors) {
  vector_size_t size = 1'000;

  // Make constant vectors of various types with null and non-null values.
  auto vector = makeConstantVector(size);

  createDuckDbTable({vector});

  auto outputDirectory = TempDirectoryPath::create();
  auto op = createInsertPlan(
      PlanBuilder().values({vector}),
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  assertQuery(op, fmt::format("SELECT {}", size));

  if (partitionedBy_.size() > 0) {
    auto newOutputType = getNonPartitionsColumns(partitionedBy_, tableSchema_);
    assertQuery(
        PlanBuilder().tableScan(newOutputType).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT c2, c3, c4, c5 FROM tmp");
    rowType_ = newOutputType;
    verifyTableWriterOutput(outputDirectory->path, rowType_);
  } else {
    assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT * FROM tmp");

    verifyTableWriterOutput(outputDirectory->path, rowType_);
  }
}

TEST_P(AllTableWriterTest, emptyInput) {
  auto outputDirectory = TempDirectoryPath::create();
  auto vector = makeConstantVector(0);
  auto op = createInsertPlan(
      PlanBuilder().values({vector}),
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  assertQuery(op, "SELECT 0");
}

TEST_P(AllTableWriterTest, commitStrategies) {
  auto filePaths = makeFilePaths(5);
  auto vectors = makeVectors(filePaths.size(), 100);

  createDuckDbTable(vectors);

  // Test the kTaskCommit commit strategy writing to one dot-prefixed
  // temporary file.
  {
    SCOPED_TRACE(CommitStrategy::kTaskCommit);
    auto outputDirectory = TempDirectoryPath::create();
    auto plan = createInsertPlan(
        PlanBuilder().values(vectors),
        rowType_,
        outputDirectory->path,
        partitionedBy_,
        bucketProperty_,
        compressionKind_,
        getNumWriters(),
        connector::hive::LocationHandle::TableType::kNew,
        commitStrategy_);

    assertQuery(plan, "SELECT count(*) FROM tmp");

    if (partitionedBy_.size() > 0) {
      auto newOutputType =
          getNonPartitionsColumns(partitionedBy_, tableSchema_);
      assertQuery(
          PlanBuilder().tableScan(newOutputType).planNode(),
          makeHiveConnectorSplits(outputDirectory),
          "SELECT c2, c3, c4, c5 FROM tmp");
      auto originalRowType = rowType_;
      rowType_ = newOutputType;
      verifyTableWriterOutput(outputDirectory->path, rowType_);
      rowType_ = originalRowType;
    } else {
      assertQuery(
          PlanBuilder().tableScan(rowType_).planNode(),
          makeHiveConnectorSplits(outputDirectory),
          "SELECT * FROM tmp");
      verifyTableWriterOutput(outputDirectory->path, rowType_);
    }
  }
  // Test kNoCommit commit strategy writing to non-temporary files.
  {
    SCOPED_TRACE(CommitStrategy::kNoCommit);
    auto outputDirectory = TempDirectoryPath::create();
    setCommitStrategy(CommitStrategy::kNoCommit);
    auto plan = createInsertPlan(
        PlanBuilder().values(vectors),
        rowType_,
        outputDirectory->path,
        partitionedBy_,
        bucketProperty_,
        compressionKind_,
        getNumWriters(),
        connector::hive::LocationHandle::TableType::kNew,
        commitStrategy_);

    assertQuery(plan, "SELECT count(*) FROM tmp");

    if (partitionedBy_.size() > 0) {
      auto newOutputType =
          getNonPartitionsColumns(partitionedBy_, tableSchema_);
      assertQuery(
          PlanBuilder().tableScan(newOutputType).planNode(),
          makeHiveConnectorSplits(outputDirectory),
          "SELECT c2, c3, c4, c5 FROM tmp");
      rowType_ = newOutputType;
      verifyTableWriterOutput(outputDirectory->path, rowType_);
    } else {
      assertQuery(
          PlanBuilder().tableScan(rowType_).planNode(),
          makeHiveConnectorSplits(outputDirectory),
          "SELECT * FROM tmp");
      verifyTableWriterOutput(outputDirectory->path, rowType_);
    }
  }
}

TEST_P(PartitionedTableWriterTest, specialPartitionName) {
  const int32_t numPartitions = 50;
  const int32_t numBatches = 2;

  const auto rowType =
      ROW({"c0", "p0", "p1", "c1", "c3", "c5"},
          {INTEGER(), INTEGER(), VARCHAR(), BIGINT(), REAL(), VARCHAR()});
  const std::vector<std::string> partitionKeys = {"p0", "p1"};
  const std::vector<TypePtr> partitionTypes = {INTEGER(), VARCHAR()};

  const std::vector charsToEscape = {
      '"',
      '#',
      '%',
      '\'',
      '*',
      '/',
      ':',
      '=',
      '?',
      '\\',
      '\x7F',
      '{',
      '[',
      ']',
      '^'};
  ASSERT_GE(numPartitions, charsToEscape.size());
  std::vector<RowVectorPtr> vectors = makeBatches(numBatches, [&](auto) {
    return makeRowVector(
        rowType->names(),
        {
            makeFlatVector<int32_t>(
                numPartitions, [&](auto row) { return row + 100; }),
            makeFlatVector<int32_t>(
                numPartitions, [&](auto row) { return row; }),
            makeFlatVector<StringView>(
                numPartitions,
                [&](auto row) {
                  // special character
                  return StringView::makeInline(
                      fmt::format("str_{}{}", row, charsToEscape.at(row % 15)));
                }),
            makeFlatVector<int64_t>(
                numPartitions, [&](auto row) { return row + 1000; }),
            makeFlatVector<float>(
                numPartitions, [&](auto row) { return row + 33.23; }),
            makeFlatVector<StringView>(
                numPartitions,
                [&](auto row) {
                  return StringView::makeInline(
                      fmt::format("bucket_{}", row * 3));
                }),
        });
  });
  createDuckDbTable(vectors);

  auto inputFilePaths = makeFilePaths(numBatches);
  for (int i = 0; i < numBatches; i++) {
    writeToFile(inputFilePaths[i]->path, vectors[i]);
  }

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = createInsertPlan(
      PlanBuilder().tableScan(rowType),
      rowType,
      outputDirectory->path,
      partitionKeys,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  auto task = assertQuery(plan, inputFilePaths, "SELECT count(*) FROM tmp");

  std::set<std::string> actualPartitionDirectories =
      getLeafSubdirectories(outputDirectory->path);

  std::set<std::string> expectedPartitionDirectories;
  const std::vector<std::string> expectedCharsAfterEscape = {
      "%22",
      "%23",
      "%25",
      "%27",
      "%2A",
      "%2F",
      "%3A",
      "%3D",
      "%3F",
      "%5C",
      "%7F",
      "%7B",
      "%5B",
      "%5D",
      "%5E"};
  for (auto i = 0; i < numPartitions; ++i) {
    // url encoded
    auto partitionName = fmt::format(
        "p0={}/p1=str_{}{}", i, i, expectedCharsAfterEscape.at(i % 15));
    expectedPartitionDirectories.emplace(
        fs::path(outputDirectory->path) / partitionName);
  }
  EXPECT_EQ(actualPartitionDirectories, expectedPartitionDirectories);
}

TEST_P(PartitionedTableWriterTest, multiplePartitions) {
  int32_t numPartitions = 50;
  int32_t numBatches = 2;

  auto rowType =
      ROW({"c0", "p0", "p1", "c1", "c3", "c5"},
          {INTEGER(), INTEGER(), VARCHAR(), BIGINT(), REAL(), VARCHAR()});
  std::vector<std::string> partitionKeys = {"p0", "p1"};
  std::vector<TypePtr> partitionTypes = {INTEGER(), VARCHAR()};

  std::vector<RowVectorPtr> vectors = makeBatches(numBatches, [&](auto) {
    return makeRowVector(
        rowType->names(),
        {
            makeFlatVector<int32_t>(
                numPartitions, [&](auto row) { return row + 100; }),
            makeFlatVector<int32_t>(
                numPartitions, [&](auto row) { return row; }),
            makeFlatVector<StringView>(
                numPartitions,
                [&](auto row) {
                  return StringView::makeInline(fmt::format("str_{}", row));
                }),
            makeFlatVector<int64_t>(
                numPartitions, [&](auto row) { return row + 1000; }),
            makeFlatVector<float>(
                numPartitions, [&](auto row) { return row + 33.23; }),
            makeFlatVector<StringView>(
                numPartitions,
                [&](auto row) {
                  return StringView::makeInline(
                      fmt::format("bucket_{}", row * 3));
                }),
        });
  });
  createDuckDbTable(vectors);

  auto inputFilePaths = makeFilePaths(numBatches);
  for (int i = 0; i < numBatches; i++) {
    writeToFile(inputFilePaths[i]->path, vectors[i]);
  }

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = createInsertPlan(
      PlanBuilder().tableScan(rowType),
      rowType,
      outputDirectory->path,
      partitionKeys,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  auto task = assertQuery(plan, inputFilePaths, "SELECT count(*) FROM tmp");

  // Verify that there is one partition directory for each partition.
  std::set<std::string> actualPartitionDirectories =
      getLeafSubdirectories(outputDirectory->path);

  std::set<std::string> expectedPartitionDirectories;
  std::set<std::string> partitionNames;
  for (auto i = 0; i < numPartitions; i++) {
    auto partitionName = fmt::format("p0={}/p1=str_{}", i, i);
    partitionNames.emplace(partitionName);
    expectedPartitionDirectories.emplace(
        fs::path(outputDirectory->path) / partitionName);
  }
  EXPECT_EQ(actualPartitionDirectories, expectedPartitionDirectories);

  // Verify distribution of records in partition directories.
  auto iterPartitionDirectory = actualPartitionDirectories.begin();
  auto iterPartitionName = partitionNames.begin();
  auto newOutputType = getNonPartitionsColumns(partitionKeys, rowType);
  while (iterPartitionDirectory != actualPartitionDirectories.end()) {
    assertQuery(
        PlanBuilder().tableScan(newOutputType).planNode(),
        makeHiveConnectorSplits(*iterPartitionDirectory),
        fmt::format(
            "SELECT c0, c1, c3, c5 FROM tmp WHERE {}",
            partitionNameToPredicate(*iterPartitionName, partitionTypes)));
    // In case of unbucketed partitioned table, one single file is written to
    // each partition directory for Hive connector.
    if (testMode_ == TestMode::kPartitioned) {
      ASSERT_EQ(countRecursiveFiles(*iterPartitionDirectory), 1);
    } else {
      ASSERT_GE(countRecursiveFiles(*iterPartitionDirectory), 1);
    }

    ++iterPartitionDirectory;
    ++iterPartitionName;
  }
}

TEST_P(PartitionedTableWriterTest, singlePartition) {
  const int32_t numBatches = 2;
  auto rowType =
      ROW({"c0", "p0", "c3", "c5"}, {VARCHAR(), BIGINT(), REAL(), VARCHAR()});
  std::vector<std::string> partitionKeys = {"p0"};

  // Partition vector is constant vector.
  std::vector<RowVectorPtr> vectors = makeBatches(numBatches, [&](auto) {
    return makeRowVector(
        rowType->names(),
        {makeFlatVector<StringView>(
             1'000,
             [&](auto row) {
               return StringView::makeInline(fmt::format("str_{}", row));
             }),
         makeConstant((int64_t)365, 1'000),
         makeFlatVector<float>(1'000, [&](auto row) { return row + 33.23; }),
         makeFlatVector<StringView>(1'000, [&](auto row) {
           return StringView::makeInline(fmt::format("bucket_{}", row * 3));
         })});
  });
  createDuckDbTable(vectors);

  auto inputFilePaths = makeFilePaths(numBatches);
  for (int i = 0; i < numBatches; i++) {
    writeToFile(inputFilePaths[i]->path, vectors[i]);
  }

  auto outputDirectory = TempDirectoryPath::create();
  const int numWriters = getNumWriters();
  auto plan = createInsertPlan(
      PlanBuilder().tableScan(rowType),
      rowType,
      outputDirectory->path,
      partitionKeys,
      bucketProperty_,
      compressionKind_,
      numWriters,
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  auto task = assertQueryWithWriterConfigs(
      plan, inputFilePaths, "SELECT count(*) FROM tmp");

  std::set<std::string> partitionDirectories =
      getLeafSubdirectories(outputDirectory->path);

  // Verify only a single partition directory is created.
  ASSERT_EQ(partitionDirectories.size(), 1);
  EXPECT_EQ(
      *partitionDirectories.begin(),
      fs::path(outputDirectory->path) / "p0=365");

  // Verify all data is written to the single partition directory.
  auto newOutputType = getNonPartitionsColumns(partitionKeys, rowType);
  assertQuery(
      PlanBuilder().tableScan(newOutputType).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT c0, c3, c5 FROM tmp");

  // In case of unbucketed partitioned table, one single file is written to
  // each partition directory for Hive connector.
  if (testMode_ == TestMode::kPartitioned) {
    ASSERT_LE(countRecursiveFiles(*partitionDirectories.begin()), numWriters);
  } else {
    ASSERT_GE(countRecursiveFiles(*partitionDirectories.begin()), numWriters);
  }
}

TEST_P(PartitionedWithoutBucketTableWriterTest, fromSinglePartitionToMultiple) {
  const int32_t numBatches = 1;
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), BIGINT()});
  setDataTypes(rowType);
  std::vector<std::string> partitionKeys = {"c0"};

  // Partition vector is constant vector.
  std::vector<RowVectorPtr> vectors;
  // The initial vector has the same partition key value;
  vectors.push_back(makeRowVector(
      rowType->names(),
      {makeFlatVector<int64_t>(1'000, [&](auto /*unused*/) { return 1; }),
       makeFlatVector<int64_t>(1'000, [&](auto row) { return row + 1; })}));
  // The second vector has different partition key value.
  vectors.push_back(makeRowVector(
      rowType->names(),
      {makeFlatVector<int64_t>(1'000, [&](auto row) { return row * 234 % 30; }),
       makeFlatVector<int64_t>(1'000, [&](auto row) { return row + 1; })}));
  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = createInsertPlan(
      PlanBuilder().values(vectors),
      rowType,
      outputDirectory->path,
      partitionKeys,
      nullptr,
      compressionKind_,
      numTableWriterCount_);

  assertQueryWithWriterConfigs(plan, "SELECT count(*) FROM tmp");

  auto newOutputType = getNonPartitionsColumns(partitionKeys, rowType);
  assertQuery(
      PlanBuilder().tableScan(newOutputType).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT c1 FROM tmp");
}

TEST_P(PartitionedTableWriterTest, maxPartitions) {
  SCOPED_TRACE(testParam_.toString());
  const int32_t maxPartitions = 100;
  const int32_t numPartitions =
      testMode_ == TestMode::kBucketed ? 1 : maxPartitions + 1;
  if (testMode_ == TestMode::kBucketed) {
    setBucketProperty(
        testParam_.bucketKind(),
        1000,
        bucketProperty_->bucketedBy(),
        bucketProperty_->bucketedTypes(),
        bucketProperty_->sortedBy());
  }

  auto rowType = ROW({"p0", "c3", "c5"}, {BIGINT(), REAL(), VARCHAR()});
  std::vector<std::string> partitionKeys = {"p0"};

  RowVectorPtr vector;
  if (testMode_ == TestMode::kPartitioned) {
    vector = makeRowVector(
        rowType->names(),
        {makeFlatVector<int64_t>(numPartitions, [&](auto row) { return row; }),
         makeFlatVector<float>(
             numPartitions, [&](auto row) { return row + 33.23; }),
         makeFlatVector<StringView>(numPartitions, [&](auto row) {
           return StringView::makeInline(fmt::format("bucket_{}", row * 3));
         })});
  } else {
    vector = makeRowVector(
        rowType->names(),
        {makeFlatVector<int64_t>(4'000, [&](auto /*unused*/) { return 0; }),
         makeFlatVector<float>(4'000, [&](auto row) { return row + 33.23; }),
         makeFlatVector<StringView>(4'000, [&](auto row) {
           return StringView::makeInline(fmt::format("bucket_{}", row * 3));
         })});
  };

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = createInsertPlan(
      PlanBuilder().values({vector}),
      rowType,
      outputDirectory->path,
      partitionKeys,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  if (testMode_ == TestMode::kPartitioned) {
    VELOX_ASSERT_THROW(
        AssertQueryBuilder(plan)
            .connectorSessionProperty(
                kHiveConnectorId,
                HiveConfig::kMaxPartitionsPerWritersSession,
                folly::to<std::string>(maxPartitions))
            .copyResults(pool()),
        fmt::format(
            "Exceeded limit of {} distinct partitions.", maxPartitions));
  } else {
    VELOX_ASSERT_THROW(
        AssertQueryBuilder(plan)
            .connectorSessionProperty(
                kHiveConnectorId,
                HiveConfig::kMaxPartitionsPerWritersSession,
                folly::to<std::string>(maxPartitions))
            .copyResults(pool()),
        "Exceeded open writer limit");
  }
}

// Test TableWriter does not create a file if input is empty.
TEST_P(AllTableWriterTest, writeNoFile) {
  auto outputDirectory = TempDirectoryPath::create();
  auto plan = createInsertPlan(
      PlanBuilder().tableScan(rowType_).filter("false"),
      rowType_,
      outputDirectory->path);

  auto execute = [&](const std::shared_ptr<const core::PlanNode>& plan,
                     std::shared_ptr<core::QueryCtx> queryCtx) {
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = queryCtx;
    readCursor(params, [&](Task* task) { task->noMoreSplits("0"); });
  };

  execute(plan, std::make_shared<core::QueryCtx>(executor_.get()));
  ASSERT_TRUE(fs::is_empty(outputDirectory->path));
}

TEST_P(UnpartitionedTableWriterTest, differentCompression) {
  std::vector<CompressionKind> compressions{
      CompressionKind_NONE,
      CompressionKind_ZLIB,
      CompressionKind_SNAPPY,
      CompressionKind_LZO,
      CompressionKind_ZSTD,
      CompressionKind_LZ4,
      CompressionKind_GZIP,
      CompressionKind_MAX};

  for (auto compressionKind : compressions) {
    auto input = makeVectors(10, 10);
    auto outputDirectory = TempDirectoryPath::create();
    if (compressionKind == CompressionKind_MAX) {
      VELOX_ASSERT_THROW(
          createInsertPlan(
              PlanBuilder().values(input),
              rowType_,
              outputDirectory->path,
              {},
              nullptr,
              compressionKind,
              numTableWriterCount_,
              connector::hive::LocationHandle::TableType::kNew),
          "Unsupported compression type: CompressionKind_MAX");
      return;
    }
    auto plan = createInsertPlan(
        PlanBuilder().values(input),
        rowType_,
        outputDirectory->path,
        {},
        nullptr,
        compressionKind,
        numTableWriterCount_,
        connector::hive::LocationHandle::TableType::kNew);

    // currently we don't support any compression in PARQUET format
    if (fileFormat_ == FileFormat::PARQUET &&
        compressionKind != CompressionKind_NONE) {
      continue;
    }
    if (compressionKind == CompressionKind_NONE ||
        compressionKind == CompressionKind_ZLIB ||
        compressionKind == CompressionKind_ZSTD) {
      auto result = AssertQueryBuilder(plan)
                        .config(
                            QueryConfig::kTaskWriterCount,
                            std::to_string(numTableWriterCount_))
                        .copyResults(pool());
      assertEqualResults(
          {makeRowVector({makeConstant<int64_t>(100, 1)})}, {result});
    } else {
      VELOX_ASSERT_THROW(
          AssertQueryBuilder(plan)
              .config(
                  QueryConfig::kTaskWriterCount,
                  std::to_string(numTableWriterCount_))
              .copyResults(pool()),
          "Unsupported compression type:");
    }
  }
}

TEST_P(UnpartitionedTableWriterTest, runtimeStatsCheck) {
  // The runtime stats test only applies for dwrf file format.
  if (fileFormat_ != dwio::common::FileFormat::DWRF) {
    return;
  }
  struct {
    int numInputVectors;
    std::string maxStripeSize;
    int expectedNumStripes;

    std::string debugString() const {
      return fmt::format(
          "numInputVectors: {}, maxStripeSize: {}, expectedNumStripes: {}",
          numInputVectors,
          maxStripeSize,
          expectedNumStripes);
    }
  } testSettings[] = {
      {10, "1GB", 1},
      {1, "1GB", 1},
      {2, "1GB", 1},
      {10, "1B", 10},
      {2, "1B", 2},
      {1, "1B", 1}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto rowType = ROW({"c0", "c1"}, {VARCHAR(), BIGINT()});

    VectorFuzzer::Options options;
    options.nullRatio = 0.0;
    options.vectorSize = 1;
    options.stringLength = 1L << 20;
    VectorFuzzer fuzzer(options, pool());

    std::vector<RowVectorPtr> vectors;
    for (int i = 0; i < testData.numInputVectors; ++i) {
      vectors.push_back(fuzzer.fuzzInputRow(rowType));
    }

    createDuckDbTable(vectors);

    auto outputDirectory = TempDirectoryPath::create();
    auto plan = createInsertPlan(
        PlanBuilder().values(vectors),
        rowType,
        outputDirectory->path,
        {},
        nullptr,
        compressionKind_,
        1,
        connector::hive::LocationHandle::TableType::kNew);
    const std::shared_ptr<Task> task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(QueryConfig::kTaskWriterCount, std::to_string(1))
            .connectorSessionProperty(
                kHiveConnectorId,
                HiveConfig::kOrcWriterMaxStripeSizeSession,
                testData.maxStripeSize)
            .assertResults("SELECT count(*) FROM tmp");
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    if (testData.maxStripeSize == "1GB") {
      ASSERT_GT(
          stats[1].memoryStats.peakTotalMemoryReservation,
          testData.numInputVectors * options.stringLength);
    }
    ASSERT_EQ(
        stats[1].runtimeStats["stripeSize"].count, testData.expectedNumStripes);
    ASSERT_EQ(stats[1].runtimeStats["numWrittenFiles"].sum, 1);
    ASSERT_EQ(stats[1].runtimeStats["numWrittenFiles"].count, 1);
  }
}

TEST_P(UnpartitionedTableWriterTest, immutableSettings) {
  struct {
    connector::hive::LocationHandle::TableType dataType;
    bool immutablePartitionsEnabled;
    bool expectedInsertSuccees;

    std::string debugString() const {
      return fmt::format(
          "dataType:{}, immutablePartitionsEnabled:{}, operationSuccess:{}",
          dataType,
          immutablePartitionsEnabled,
          expectedInsertSuccees);
    }
  } testSettings[] = {
      {connector::hive::LocationHandle::TableType::kNew, true, true},
      {connector::hive::LocationHandle::TableType::kNew, false, true},
      {connector::hive::LocationHandle::TableType::kExisting, true, false},
      {connector::hive::LocationHandle::TableType::kExisting, false, true}};

  for (auto testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::unordered_map<std::string, std::string> propFromFile{
        {"hive.immutable-partitions",
         testData.immutablePartitionsEnabled ? "true" : "false"}};
    std::shared_ptr<const Config> config{
        std::make_shared<core::MemConfig>(propFromFile)};
    resetHiveConnector(config);

    auto input = makeVectors(10, 10);
    auto outputDirectory = TempDirectoryPath::create();
    auto plan = createInsertPlan(
        PlanBuilder().values(input),
        rowType_,
        outputDirectory->path,
        {},
        nullptr,
        CompressionKind_NONE,
        numTableWriterCount_,
        testData.dataType);

    if (!testData.expectedInsertSuccees) {
      VELOX_ASSERT_THROW(
          AssertQueryBuilder(plan).copyResults(pool()),
          "Unpartitioned Hive tables are immutable.");
    } else {
      auto result = AssertQueryBuilder(plan)
                        .config(
                            QueryConfig::kTaskWriterCount,
                            std::to_string(numTableWriterCount_))
                        .copyResults(pool());
      assertEqualResults(
          {makeRowVector({makeConstant<int64_t>(100, 1)})}, {result});
    }
  }
}

TEST_P(BucketedTableOnlyWriteTest, bucketCountLimit) {
  SCOPED_TRACE(testParam_.toString());
  auto input = makeVectors(1, 100);
  createDuckDbTable(input);
  struct {
    uint32_t bucketCount;
    bool expectedError;

    std::string debugString() const {
      return fmt::format(
          "bucketCount:{} expectedError:{}", bucketCount, expectedError);
    }
  } testSettings[] = {
      {1, false},
      {3, false},
      {HiveDataSink::maxBucketCount() - 1, false},
      {HiveDataSink::maxBucketCount(), true},
      {HiveDataSink::maxBucketCount() + 1, true},
      {HiveDataSink::maxBucketCount() * 2, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto outputDirectory = TempDirectoryPath::create();
    setBucketProperty(
        bucketProperty_->kind(),
        testData.bucketCount,
        bucketProperty_->bucketedBy(),
        bucketProperty_->bucketedTypes(),
        bucketProperty_->sortedBy());
    auto plan = createInsertPlan(
        PlanBuilder().values({input}),
        rowType_,
        outputDirectory->path,
        partitionedBy_,
        bucketProperty_,
        compressionKind_,
        getNumWriters(),
        connector::hive::LocationHandle::TableType::kNew,
        commitStrategy_);
    if (testData.expectedError) {
      VELOX_ASSERT_THROW(
          AssertQueryBuilder(plan)
              .connectorSessionProperty(
                  kHiveConnectorId,
                  HiveConfig::kMaxPartitionsPerWritersSession,
                  // Make sure we have a sufficient large writer limit.
                  folly::to<std::string>(testData.bucketCount * 2))
              .copyResults(pool()),
          "bucketCount exceeds the limit");
    } else {
      assertQueryWithWriterConfigs(plan, "SELECT count(*) FROM tmp");

      if (partitionedBy_.size() > 0) {
        auto newOutputType =
            getNonPartitionsColumns(partitionedBy_, tableSchema_);
        assertQuery(
            PlanBuilder().tableScan(newOutputType).planNode(),
            makeHiveConnectorSplits(outputDirectory),
            "SELECT c2, c3, c4, c5 FROM tmp");
        auto originalRowType = rowType_;
        rowType_ = newOutputType;
        verifyTableWriterOutput(outputDirectory->path, rowType_);
        rowType_ = originalRowType;
      } else {
        assertQuery(
            PlanBuilder().tableScan(rowType_).planNode(),
            makeHiveConnectorSplits(outputDirectory),
            "SELECT * FROM tmp");
        verifyTableWriterOutput(outputDirectory->path, rowType_);
      }
    }
  }
}

TEST_P(BucketedTableOnlyWriteTest, mismatchedBucketTypes) {
  SCOPED_TRACE(testParam_.toString());
  auto input = makeVectors(1, 100);
  createDuckDbTable(input);
  auto outputDirectory = TempDirectoryPath::create();
  std::vector<TypePtr> badBucketedBy = bucketProperty_->bucketedTypes();
  const auto oldType = badBucketedBy[0];
  badBucketedBy[0] = VARCHAR();
  setBucketProperty(
      bucketProperty_->kind(),
      bucketProperty_->bucketCount(),
      bucketProperty_->bucketedBy(),
      badBucketedBy,
      bucketProperty_->sortedBy());
  auto plan = createInsertPlan(
      PlanBuilder().values({input}),
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      fmt::format(
          "Input column {} type {} doesn't match bucket type {}",
          bucketProperty_->bucketedBy()[0],
          oldType->toString(),
          bucketProperty_->bucketedTypes()[0]));
}

TEST_P(AllTableWriterTest, tableWriteOutputCheck) {
  SCOPED_TRACE(testParam_.toString());
  if (!testParam_.multiDrivers() ||
      testParam_.testMode() != TestMode::kUnpartitioned) {
    return;
  }
  auto input = makeVectors(10, 100);
  createDuckDbTable(input);
  auto outputDirectory = TempDirectoryPath::create();
  auto plan = createInsertPlan(
      PlanBuilder().values({input}),
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_,
      false);

  auto result = runQueryWithWriterConfigs(plan);
  auto writtenRowVector = result->childAt(TableWriteTraits::kRowCountChannel)
                              ->asFlatVector<int64_t>();
  auto fragmentVector = result->childAt(TableWriteTraits::kFragmentChannel)
                            ->asFlatVector<StringView>();
  auto commitContextVector = result->childAt(TableWriteTraits::kContextChannel)
                                 ->asFlatVector<StringView>();
  const int64_t expectedRows = 10 * 100;
  std::vector<std::string> writeFiles;
  int64_t numRows{0};
  for (int i = 0; i < result->size(); ++i) {
    if (testParam_.multiDrivers()) {
      ASSERT_FALSE(commitContextVector->isNullAt(i));
      if (!fragmentVector->isNullAt(i)) {
        ASSERT_TRUE(writtenRowVector->isNullAt(i));
      }
    } else {
      if (i == 0) {
        ASSERT_TRUE(fragmentVector->isNullAt(i));
      } else {
        ASSERT_TRUE(writtenRowVector->isNullAt(i));
        ASSERT_FALSE(fragmentVector->isNullAt(i));
      }
      ASSERT_FALSE(commitContextVector->isNullAt(i));
    }
    if (!fragmentVector->isNullAt(i)) {
      ASSERT_FALSE(fragmentVector->isNullAt(i));
      folly::dynamic obj = folly::parseJson(fragmentVector->valueAt(i));
      if (testMode_ == TestMode::kUnpartitioned) {
        ASSERT_EQ(obj["targetPath"], outputDirectory->path);
        ASSERT_EQ(obj["writePath"], outputDirectory->path);
      } else {
        std::string partitionDirRe;
        for (const auto& partitionBy : partitionedBy_) {
          partitionDirRe += fmt::format("/{}=.+", partitionBy);
        }
        ASSERT_TRUE(RE2::FullMatch(
            obj["targetPath"].asString(),
            fmt::format("{}{}", outputDirectory->path, partitionDirRe)))
            << obj["targetPath"].asString();
        ASSERT_TRUE(RE2::FullMatch(
            obj["writePath"].asString(),
            fmt::format("{}{}", outputDirectory->path, partitionDirRe)))
            << obj["writePath"].asString();
      }
      numRows += obj["rowCount"].asInt();
      ASSERT_EQ(obj["updateMode"].asString(), "NEW");

      ASSERT_TRUE(obj["fileWriteInfos"].isArray());
      ASSERT_EQ(obj["fileWriteInfos"].size(), 1);
      folly::dynamic writerInfoObj = obj["fileWriteInfos"][0];
      const std::string writeFileName =
          writerInfoObj["writeFileName"].asString();
      writeFiles.push_back(writeFileName);
      const std::string targetFileName =
          writerInfoObj["targetFileName"].asString();
      const std::string writeFileFullPath =
          obj["writePath"].asString() + "/" + writeFileName;
      std::filesystem::path path{writeFileFullPath};
      const auto actualFileSize = fs::file_size(path);
      ASSERT_EQ(obj["onDiskDataSizeInBytes"].asInt(), actualFileSize);
      ASSERT_EQ(writerInfoObj["fileSize"], actualFileSize);
      if (commitStrategy_ == CommitStrategy::kNoCommit) {
        ASSERT_EQ(writeFileName, targetFileName);
      } else {
        const std::string kParquetSuffix = ".parquet";
        if (folly::StringPiece(targetFileName).endsWith(kParquetSuffix)) {
          // Remove the .parquet suffix.
          auto trimmedFilename = targetFileName.substr(
              0, targetFileName.size() - kParquetSuffix.size());
          ASSERT_TRUE(writeFileName.find(trimmedFilename) != std::string::npos);
        } else {
          ASSERT_TRUE(writeFileName.find(targetFileName) != std::string::npos);
        }
      }
    }
    if (!commitContextVector->isNullAt(i)) {
      ASSERT_TRUE(RE2::FullMatch(
          commitContextVector->valueAt(i).getString(),
          fmt::format(".*{}.*", commitStrategyToString(commitStrategy_))))
          << commitContextVector->valueAt(i);
    }
  }
  ASSERT_EQ(numRows, expectedRows);
  if (testMode_ == TestMode::kUnpartitioned) {
    ASSERT_GT(writeFiles.size(), 0);
    ASSERT_LE(writeFiles.size(), numTableWriterCount_);
  }
  auto diskFiles = listAllFiles(outputDirectory->path);
  std::sort(diskFiles.begin(), diskFiles.end());
  std::sort(writeFiles.begin(), writeFiles.end());
  ASSERT_EQ(diskFiles, writeFiles)
      << "\nwrite files: " << folly::join(",", writeFiles)
      << "\ndisk files: " << folly::join(",", diskFiles);
  // Verify the utilities provided by table writer traits.
  ASSERT_EQ(TableWriteTraits::getRowCount(result), 10 * 100);
  auto obj = TableWriteTraits::getTableCommitContext(result);
  ASSERT_EQ(
      obj[TableWriteTraits::kCommitStrategyContextKey],
      commitStrategyToString(commitStrategy_));
  ASSERT_EQ(obj[TableWriteTraits::klastPageContextKey], true);
  ASSERT_EQ(obj[TableWriteTraits::kLifeSpanContextKey], "TaskWide");
}

TEST_P(AllTableWriterTest, columnStatsDataTypes) {
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"},
          {BIGINT(),
           INTEGER(),
           SMALLINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           BOOLEAN(),
           MAP(DATE(), BIGINT()),
           ARRAY(BIGINT())});
  setDataTypes(rowType);
  std::vector<RowVectorPtr> input;
  input.push_back(makeRowVector(
      rowType_->names(),
      {
          makeFlatVector<int64_t>(1'000, [&](auto row) { return 1; }),
          makeFlatVector<int32_t>(1'000, [&](auto row) { return 1; }),
          makeFlatVector<int16_t>(1'000, [&](auto row) { return row; }),
          makeFlatVector<float>(1'000, [&](auto row) { return row + 33.23; }),
          makeFlatVector<double>(1'000, [&](auto row) { return row + 33.23; }),
          makeFlatVector<StringView>(
              1'000,
              [&](auto row) {
                return StringView(std::to_string(row).c_str());
              }),
          makeFlatVector<bool>(1'000, [&](auto row) { return true; }),
          makeMapVector<int32_t, int64_t>(
              1'000,
              [](auto /*row*/) { return 5; },
              [](auto row) { return row; },
              [](auto row) { return row * 3; }),
          makeArrayVector<int64_t>(
              1'000,
              [](auto /*row*/) { return 5; },
              [](auto row) { return row * 3; }),
      }));
  createDuckDbTable(input);
  auto outputDirectory = TempDirectoryPath::create();

  std::vector<FieldAccessTypedExprPtr> groupingKeyFields;
  for (int i = 0; i < partitionedBy_.size(); ++i) {
    groupingKeyFields.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
        partitionTypes_.at(i), partitionedBy_.at(i)));
  }

  // aggregation node
  core::TypedExprPtr intInputField =
      std::make_shared<const core::FieldAccessTypedExpr>(SMALLINT(), "c2");
  auto minCallExpr = std::make_shared<const core::CallTypedExpr>(
      SMALLINT(), std::vector<core::TypedExprPtr>{intInputField}, "min");
  auto maxCallExpr = std::make_shared<const core::CallTypedExpr>(
      SMALLINT(), std::vector<core::TypedExprPtr>{intInputField}, "max");
  auto distinctCountCallExpr = std::make_shared<const core::CallTypedExpr>(
      VARCHAR(),
      std::vector<core::TypedExprPtr>{intInputField},
      "approx_distinct");

  core::TypedExprPtr strInputField =
      std::make_shared<const core::FieldAccessTypedExpr>(VARCHAR(), "c5");
  auto maxDataSizeCallExpr = std::make_shared<const core::CallTypedExpr>(
      BIGINT(),
      std::vector<core::TypedExprPtr>{strInputField},
      "max_data_size_for_stats");
  auto sumDataSizeCallExpr = std::make_shared<const core::CallTypedExpr>(
      BIGINT(),
      std::vector<core::TypedExprPtr>{strInputField},
      "sum_data_size_for_stats");

  core::TypedExprPtr boolInputField =
      std::make_shared<const core::FieldAccessTypedExpr>(BOOLEAN(), "c6");
  auto countCallExpr = std::make_shared<const core::CallTypedExpr>(
      BIGINT(), std::vector<core::TypedExprPtr>{boolInputField}, "count");
  auto countIfCallExpr = std::make_shared<const core::CallTypedExpr>(
      BIGINT(), std::vector<core::TypedExprPtr>{boolInputField}, "count_if");

  core::TypedExprPtr mapInputField =
      std::make_shared<const core::FieldAccessTypedExpr>(
          MAP(DATE(), BIGINT()), "c7");
  auto countMapCallExpr = std::make_shared<const core::CallTypedExpr>(
      BIGINT(), std::vector<core::TypedExprPtr>{mapInputField}, "count");
  auto sumDataSizeMapCallExpr = std::make_shared<const core::CallTypedExpr>(
      BIGINT(),
      std::vector<core::TypedExprPtr>{mapInputField},
      "sum_data_size_for_stats");

  core::TypedExprPtr arrayInputField =
      std::make_shared<const core::FieldAccessTypedExpr>(
          MAP(DATE(), BIGINT()), "c7");
  auto countArrayCallExpr = std::make_shared<const core::CallTypedExpr>(
      BIGINT(), std::vector<core::TypedExprPtr>{mapInputField}, "count");
  auto sumDataSizeArrayCallExpr = std::make_shared<const core::CallTypedExpr>(
      BIGINT(),
      std::vector<core::TypedExprPtr>{mapInputField},
      "sum_data_size_for_stats");

  const std::vector<std::string> aggregateNames = {
      "min",
      "max",
      "approx_distinct",
      "max_data_size_for_stats",
      "sum_data_size_for_stats",
      "count",
      "count_if",
      "count",
      "sum_data_size_for_stats",
      "count",
      "sum_data_size_for_stats",
  };

  auto makeAggregate = [](const auto& callExpr) {
    std::vector<TypePtr> rawInputTypes;
    for (const auto& input : callExpr->inputs()) {
      rawInputTypes.push_back(input->type());
    }
    return core::AggregationNode::Aggregate{
        callExpr,
        rawInputTypes,
        nullptr, // mask
        {}, // sortingKeys
        {} // sortingOrders
    };
  };

  std::vector<core::AggregationNode::Aggregate> aggregates = {
      makeAggregate(minCallExpr),
      makeAggregate(maxCallExpr),
      makeAggregate(distinctCountCallExpr),
      makeAggregate(maxDataSizeCallExpr),
      makeAggregate(sumDataSizeCallExpr),
      makeAggregate(countCallExpr),
      makeAggregate(countIfCallExpr),
      makeAggregate(countMapCallExpr),
      makeAggregate(sumDataSizeMapCallExpr),
      makeAggregate(countArrayCallExpr),
      makeAggregate(sumDataSizeArrayCallExpr),
  };
  const auto aggregationNode = std::make_shared<core::AggregationNode>(
      core::PlanNodeId(),
      core::AggregationNode::Step::kPartial,
      groupingKeyFields,
      std::vector<core::FieldAccessTypedExprPtr>{},
      aggregateNames,
      aggregates,
      false, // ignoreNullKeys
      PlanBuilder().values({input}).planNode());

  auto plan = PlanBuilder()
                  .values({input})
                  .addNode(addTableWriter(
                      rowType_,
                      rowType_->names(),
                      aggregationNode,
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType_->names(),
                              rowType_->children(),
                              partitionedBy_,
                              nullptr,
                              makeLocationHandle(outputDirectory->path))),
                      false,
                      CommitStrategy::kNoCommit))
                  .planNode();

  // the result is in format of : row/fragments/context/[partition]/[stats]
  int nextColumnStatsIndex = 3 + partitionedBy_.size();
  const RowVectorPtr result = AssertQueryBuilder(plan).copyResults(pool());
  auto minStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int16_t>();
  ASSERT_EQ(minStatsVector->valueAt(0), 0);
  const auto maxStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int16_t>();
  ASSERT_EQ(maxStatsVector->valueAt(0), 999);
  const auto distinctCountStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<StringView>();
  HashStringAllocator allocator{pool_.get()};
  DenseHll denseHll{
      std::string(distinctCountStatsVector->valueAt(0)).c_str(), &allocator};
  ASSERT_EQ(denseHll.cardinality(), 1000);
  const auto maxDataSizeStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int64_t>();
  ASSERT_EQ(maxDataSizeStatsVector->valueAt(0), 7);
  const auto sumDataSizeStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int64_t>();
  ASSERT_EQ(sumDataSizeStatsVector->valueAt(0), 6890);
  const auto countStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int64_t>();
  ASSERT_EQ(countStatsVector->valueAt(0), 1000);
  const auto countIfStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int64_t>();
  ASSERT_EQ(countStatsVector->valueAt(0), 1000);
  const auto countMapStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int64_t>();
  ASSERT_EQ(countMapStatsVector->valueAt(0), 1000);
  const auto sumDataSizeMapStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int64_t>();
  ASSERT_EQ(sumDataSizeMapStatsVector->valueAt(0), 64000);
  const auto countArrayStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int64_t>();
  ASSERT_EQ(countArrayStatsVector->valueAt(0), 1000);
  const auto sumDataSizeArrayStatsVector =
      result->childAt(nextColumnStatsIndex++)->asFlatVector<int64_t>();
  ASSERT_EQ(sumDataSizeArrayStatsVector->valueAt(0), 64000);
}

TEST_P(AllTableWriterTest, columnStats) {
  auto input = makeVectors(1, 100);
  createDuckDbTable(input);
  auto outputDirectory = TempDirectoryPath::create();

  // 1. standard columns
  std::vector<std::string> output = {
      "numWrittenRows", "fragment", "tableCommitContext"};
  std::vector<TypePtr> types = {BIGINT(), VARBINARY(), VARBINARY()};
  std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
  // 2. partition columns
  for (int i = 0; i < partitionedBy_.size(); i++) {
    groupingKeys.emplace_back(
        std::make_shared<const core::FieldAccessTypedExpr>(
            partitionTypes_.at(i), partitionedBy_.at(i)));
    output.emplace_back(partitionedBy_.at(i));
    types.emplace_back(partitionTypes_.at(i));
  }
  // 3. stats columns
  output.emplace_back("min");
  types.emplace_back(BIGINT());
  const auto writerOutputType = ROW(std::move(output), std::move(types));

  // aggregation node
  auto aggregationNode = generateAggregationNode(
      "c0",
      groupingKeys,
      core::AggregationNode::Step::kPartial,
      PlanBuilder().values({input}).planNode());

  auto plan = PlanBuilder()
                  .values({input})
                  .addNode(addTableWriter(
                      rowType_,
                      rowType_->names(),
                      aggregationNode,
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType_->names(),
                              rowType_->children(),
                              partitionedBy_,
                              bucketProperty_,
                              makeLocationHandle(outputDirectory->path))),
                      false,
                      commitStrategy_))
                  .planNode();

  auto result = AssertQueryBuilder(plan).copyResults(pool());
  auto rowVector = result->childAt(0)->asFlatVector<int64_t>();
  auto fragmentVector = result->childAt(1)->asFlatVector<StringView>();
  auto commitContextVector = result->childAt(2)->asFlatVector<StringView>();
  auto columnStatsVector =
      result->childAt(3 + partitionedBy_.size())->asFlatVector<int64_t>();

  const int64_t expectedRows = 10 * 100;
  std::vector<std::string> writeFiles;
  int64_t numRows{0};

  // For partitioned, expected result is as follows:
  // Row     Fragment           Context       partition           c1_min_value
  // null    null                x            partition1          0
  // null    null                x            partition2          10
  // null    null                x            partition3          15
  // count   null                x            null                null
  // null    partition1_update   x            null                null
  // null    partition1_update   x            null                null
  // null    partition2_update   x            null                null
  // null    partition2_update   x            null                null
  // null    partition3_update   x            null                null
  //
  // Note that we can have multiple same partition_update, they're for different
  // files, but for stats, we would only have one record for each partition
  //
  // For unpartitioned, expected result is:
  // Row     Fragment           Context       partition           c1_min_value
  // null    null                x                                0
  // count   null                x            null                null
  // null    update              x            null                null

  int countRow = 0;
  while (!columnStatsVector->isNullAt(countRow)) {
    countRow++;
  }
  for (int i = 0; i < result->size(); ++i) {
    if (i < countRow) {
      ASSERT_FALSE(columnStatsVector->isNullAt(i));
      ASSERT_TRUE(rowVector->isNullAt(i));
      ASSERT_TRUE(fragmentVector->isNullAt(i));
    } else if (i == countRow) {
      ASSERT_TRUE(columnStatsVector->isNullAt(i));
      ASSERT_FALSE(rowVector->isNullAt(i));
      ASSERT_TRUE(fragmentVector->isNullAt(i));
    } else {
      ASSERT_TRUE(columnStatsVector->isNullAt(i));
      ASSERT_TRUE(rowVector->isNullAt(i));
      ASSERT_FALSE(fragmentVector->isNullAt(i));
    }
  }
}

TEST_P(AllTableWriterTest, columnStatsWithTableWriteMerge) {
  auto input = makeVectors(1, 100);
  createDuckDbTable(input);
  auto outputDirectory = TempDirectoryPath::create();

  // 1. standard columns
  std::vector<std::string> output = {
      "numWrittenRows", "fragment", "tableCommitContext"};
  std::vector<TypePtr> types = {BIGINT(), VARBINARY(), VARBINARY()};
  std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
  // 2. partition columns
  for (int i = 0; i < partitionedBy_.size(); i++) {
    groupingKeys.emplace_back(
        std::make_shared<const core::FieldAccessTypedExpr>(
            partitionTypes_.at(i), partitionedBy_.at(i)));
    output.emplace_back(partitionedBy_.at(i));
    types.emplace_back(partitionTypes_.at(i));
  }
  // 3. stats columns
  output.emplace_back("min");
  types.emplace_back(BIGINT());
  const auto writerOutputType = ROW(std::move(output), std::move(types));

  // aggregation node
  auto aggregationNode = generateAggregationNode(
      "c0",
      groupingKeys,
      core::AggregationNode::Step::kPartial,
      PlanBuilder().values({input}).planNode());

  auto tableWriterPlan = PlanBuilder().values({input}).addNode(addTableWriter(
      rowType_,
      rowType_->names(),
      aggregationNode,
      std::make_shared<core::InsertTableHandle>(
          kHiveConnectorId,
          makeHiveInsertTableHandle(
              rowType_->names(),
              rowType_->children(),
              partitionedBy_,
              bucketProperty_,
              makeLocationHandle(outputDirectory->path))),
      false,
      commitStrategy_));

  auto mergeAggregationNode = generateAggregationNode(
      "min",
      groupingKeys,
      core::AggregationNode::Step::kIntermediate,
      std::move(tableWriterPlan.planNode()));

  auto finalPlan = tableWriterPlan.capturePlanNodeId(tableWriteNodeId_)
                       .localPartition(std::vector<std::string>{})
                       .tableWriteMerge(std::move(mergeAggregationNode))
                       .planNode();

  auto result = AssertQueryBuilder(finalPlan).copyResults(pool());
  auto rowVector = result->childAt(0)->asFlatVector<int64_t>();
  auto fragmentVector = result->childAt(1)->asFlatVector<StringView>();
  auto commitContextVector = result->childAt(2)->asFlatVector<StringView>();
  auto columnStatsVector =
      result->childAt(3 + partitionedBy_.size())->asFlatVector<int64_t>();

  const int64_t expectedRows = 10 * 100;
  std::vector<std::string> writeFiles;
  int64_t numRows{0};

  // For partitioned, expected result is as follows:
  // Row     Fragment           Context       partition           c1_min_value
  // null    null                x            partition1          0
  // null    null                x            partition2          10
  // null    null                x            partition3          15
  // count   null                x            null                null
  // null    partition1_update   x            null                null
  // null    partition1_update   x            null                null
  // null    partition2_update   x            null                null
  // null    partition2_update   x            null                null
  // null    partition3_update   x            null                null
  //
  // Note that we can have multiple same partition_update, they're for different
  // files, but for stats, we would only have one record for each partition
  //
  // For unpartitioned, expected result is:
  // Row     Fragment           Context       partition           c1_min_value
  // null    null                x                                0
  // count   null                x            null                null
  // null    update              x            null                null

  int statsRow = 0;
  while (columnStatsVector->isNullAt(statsRow) && statsRow < result->size()) {
    ++statsRow;
  }
  for (int i = 1; i < result->size(); ++i) {
    if (i < statsRow) {
      ASSERT_TRUE(rowVector->isNullAt(i));
      ASSERT_FALSE(fragmentVector->isNullAt(i));
      ASSERT_TRUE(columnStatsVector->isNullAt(i));
    } else if (i < result->size() - 1) {
      ASSERT_TRUE(rowVector->isNullAt(i));
      ASSERT_TRUE(fragmentVector->isNullAt(i));
      ASSERT_FALSE(columnStatsVector->isNullAt(i));
    } else {
      ASSERT_FALSE(rowVector->isNullAt(i));
      ASSERT_TRUE(fragmentVector->isNullAt(i));
      ASSERT_TRUE(columnStatsVector->isNullAt(i));
    }
  }
}

// TODO: add partitioned table write update mode tests and more failure tests.

TEST_P(AllTableWriterTest, tableWriterStats) {
  const int32_t numBatches = 2;
  auto rowType =
      ROW({"c0", "p0", "c3", "c5"}, {VARCHAR(), BIGINT(), REAL(), VARCHAR()});
  std::vector<std::string> partitionKeys = {"p0"};

  VectorFuzzer::Options options;
  options.vectorSize = 1000;
  VectorFuzzer fuzzer(options, pool());
  // Partition vector is constant vector.
  std::vector<RowVectorPtr> vectors = makeBatches(numBatches, [&](auto) {
    return makeRowVector(
        rowType->names(),
        {fuzzer.fuzzFlat(VARCHAR()),
         fuzzer.fuzzConstant(BIGINT()),
         fuzzer.fuzzFlat(REAL()),
         fuzzer.fuzzFlat(VARCHAR())});
  });
  createDuckDbTable(vectors);

  auto inputFilePaths = makeFilePaths(numBatches);
  for (int i = 0; i < numBatches; i++) {
    writeToFile(inputFilePaths[i]->path, vectors[i]);
  }

  auto outputDirectory = TempDirectoryPath::create();
  const int numWriters = getNumWriters();
  auto plan = createInsertPlan(
      PlanBuilder().tableScan(rowType),
      rowType,
      outputDirectory->path,
      partitionKeys,
      bucketProperty_,
      compressionKind_,
      numWriters,
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  auto task = assertQueryWithWriterConfigs(
      plan, inputFilePaths, "SELECT count(*) FROM tmp");

  // Each batch would create a new partition, numWrittenFiles is same as
  // partition num when not bucketed. When bucketed, it's partitionNum *
  // bucketNum, bucket number is 4
  const int numWrittenFiles =
      bucketProperty_ == nullptr ? numBatches : numBatches * 4;
  // The size of bytes (ORC_MAGIC_LEN) written when the DWRF writer
  // initializes a file.
  const int32_t ORC_HEADER_LEN{3};
  const auto fixedWrittenBytes =
      numWrittenFiles * (fileFormat_ == FileFormat::DWRF ? ORC_HEADER_LEN : 0);

  auto planStats = exec::toPlanStats(task->taskStats());
  auto& stats = planStats.at(tableWriteNodeId_);
  ASSERT_GT(stats.physicalWrittenBytes, fixedWrittenBytes);
  ASSERT_GT(
      stats.operatorStats.at("TableWrite")->physicalWrittenBytes,
      fixedWrittenBytes);
  ASSERT_EQ(
      stats.operatorStats.at("TableWrite")
          ->customStats.at("numWrittenFiles")
          .sum,
      numWrittenFiles);
}

DEBUG_ONLY_TEST_P(
    UnpartitionedTableWriterTest,
    fileWriterFlushErrorOnDriverClose) {
  VectorFuzzer::Options options;
  const int batchSize = 1000;
  options.vectorSize = batchSize;
  VectorFuzzer fuzzer(options, pool());
  const int numBatches = 10;
  std::vector<RowVectorPtr> vectors;
  int numRows{0};
  for (int i = 0; i < numBatches; ++i) {
    numRows += batchSize;
    vectors.push_back(fuzzer.fuzzRow(rowType_));
  }
  std::atomic<int> writeInputs{0};
  std::atomic<bool> triggerWriterOOM{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (op->operatorType() != "TableWrite") {
          return;
        }
        if (++writeInputs != 3) {
          return;
        }
        op->testingOperatorCtx()->task()->requestAbort();
        triggerWriterOOM = true;
      }));
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
        const std::string dictPoolRe(".*dictionary");
        const std::string generalPoolRe(".*general");
        const std::string compressionPoolRe(".*compression");
        if (!RE2::FullMatch(pool->name(), dictPoolRe) &&
            !RE2::FullMatch(pool->name(), generalPoolRe) &&
            !RE2::FullMatch(pool->name(), compressionPoolRe)) {
          return;
        }
        if (!triggerWriterOOM) {
          return;
        }
        VELOX_MEM_POOL_CAP_EXCEEDED("Inject write OOM");
      }));

  auto outputDirectory = TempDirectoryPath::create();
  auto op = createInsertPlan(
      PlanBuilder().values(vectors),
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  VELOX_ASSERT_THROW(
      assertQuery(op, fmt::format("SELECT {}", numRows)),
      "Aborted for external error");
}

DEBUG_ONLY_TEST_P(UnpartitionedTableWriterTest, dataSinkAbortError) {
  if (fileFormat_ != FileFormat::DWRF) {
    // NOTE: only test on dwrf writer format as we inject write error in dwrf
    // writer.
    return;
  }
  VectorFuzzer::Options options;
  const int batchSize = 100;
  options.vectorSize = batchSize;
  VectorFuzzer fuzzer(options, pool());
  auto vector = fuzzer.fuzzInputRow(rowType_);

  std::atomic<bool> triggerWriterErrorOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::dwrf::Writer::write",
      std::function<void(dwrf::Writer*)>([&](dwrf::Writer* /*unused*/) {
        if (!triggerWriterErrorOnce.exchange(false)) {
          return;
        }
        VELOX_FAIL("inject writer error");
      }));

  std::atomic<bool> triggerAbortErrorOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::connector::hive::HiveDataSink::closeInternal",
      std::function<void(const HiveDataSink*)>(
          [&](const HiveDataSink* /*unused*/) {
            if (!triggerAbortErrorOnce.exchange(false)) {
              return;
            }
            VELOX_FAIL("inject abort error");
          }));

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .values({vector})
                  .tableWrite(outputDirectory->path, fileFormat_)
                  .planNode();
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()), "inject writer error");
  ASSERT_FALSE(triggerWriterErrorOnce);
  ASSERT_FALSE(triggerAbortErrorOnce);
}

TEST_P(BucketSortOnlyTableWriterTest, sortWriterSpill) {
  SCOPED_TRACE(testParam_.toString());

  const auto vectors = makeVectors(5, 500);
  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto op = createInsertPlan(
      PlanBuilder().values(vectors),
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  const auto spillStats = globalSpillStats();
  auto task =
      assertQueryWithWriterConfigs(op, fmt::format("SELECT {}", 5 * 500), true);
  if (partitionedBy_.size() > 0) {
    rowType_ = getNonPartitionsColumns(partitionedBy_, rowType_);
    verifyTableWriterOutput(outputDirectory->path, rowType_);
  } else {
    verifyTableWriterOutput(outputDirectory->path, rowType_);
  }

  const auto updatedSpillStats = globalSpillStats();
  ASSERT_GT(updatedSpillStats.spilledBytes, spillStats.spilledBytes);
  ASSERT_GT(updatedSpillStats.spilledPartitions, spillStats.spilledPartitions);
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& stats = taskStats.at(tableWriteNodeId_);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledBytes, 0);
  // One spilled partition per each written files.
  const int numWrittenFiles = stats.customStats["numWrittenFiles"].sum;
  ASSERT_GE(stats.spilledPartitions, numWrittenFiles);
  ASSERT_GT(stats.customStats["spillRuns"].sum, 0);
  ASSERT_GT(stats.customStats["spillFillTime"].sum, 0);
  ASSERT_GT(stats.customStats["spillSortTime"].sum, 0);
  ASSERT_GT(stats.customStats["spillSerializationTime"].sum, 0);
  ASSERT_GT(stats.customStats["spillFlushTime"].sum, 0);
  ASSERT_GT(stats.customStats["spillDiskWrites"].sum, 0);
  ASSERT_GT(stats.customStats["spillWriteTime"].sum, 0);
}

DEBUG_ONLY_TEST_P(BucketSortOnlyTableWriterTest, outputBatchRows) {
  struct {
    uint32_t maxOutputRows;
    std::string maxOutputBytes;
    int expectedOutputCount;

    // TODO: add output size check with spilling enabled
    std::string debugString() const {
      return fmt::format(
          "maxOutputRows: {}, maxOutputBytes: {}, expectedOutputCount: {}",
          maxOutputRows,
          maxOutputBytes,
          expectedOutputCount);
    }
  } testSettings[] = {// we have 4 buckets thus 4 writers.
                      {10000, "1000kB", 4},
                      // when maxOutputRows = 1, 1000 rows triggers 1000 writes
                      {1, "1kB", 1000},
                      // estimatedRowSize is ~62bytes, when maxOutputSize = 62 *
                      // 100, 1000 rows triggers ~10 writes
                      {10000, "6200B", 12}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::atomic_int outputCount{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::dwrf::Writer::write",
        std::function<void(dwrf::Writer*)>(
            [&](dwrf::Writer* /*unused*/) { ++outputCount; }));

    auto rowType =
        ROW({"c0", "p0", "c1", "c3", "c4", "c5"},
            {VARCHAR(), BIGINT(), INTEGER(), REAL(), DOUBLE(), VARCHAR()});
    std::vector<std::string> partitionKeys = {"p0"};

    // Partition vector is constant vector.
    std::vector<RowVectorPtr> vectors = makeBatches(1, [&](auto) {
      return makeRowVector(
          rowType->names(),
          {makeFlatVector<StringView>(
               1'000,
               [&](auto row) {
                 return StringView::makeInline(fmt::format("str_{}", row));
               }),
           makeConstant((int64_t)365, 1'000),
           makeConstant((int32_t)365, 1'000),
           makeFlatVector<float>(1'000, [&](auto row) { return row + 33.23; }),
           makeFlatVector<double>(1'000, [&](auto row) { return row + 33.23; }),
           makeFlatVector<StringView>(1'000, [&](auto row) {
             return StringView::makeInline(fmt::format("bucket_{}", row * 3));
           })});
    });
    createDuckDbTable(vectors);

    auto outputDirectory = TempDirectoryPath::create();
    auto plan = createInsertPlan(
        PlanBuilder().values({vectors}),
        rowType,
        outputDirectory->path,
        partitionKeys,
        bucketProperty_,
        compressionKind_,
        1,
        connector::hive::LocationHandle::TableType::kNew,
        commitStrategy_);
    const std::shared_ptr<Task> task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(QueryConfig::kTaskWriterCount, std::to_string(1))
            .connectorSessionProperty(
                kHiveConnectorId,
                HiveConfig::kSortWriterMaxOutputRowsSession,
                folly::to<std::string>(testData.maxOutputRows))
            .connectorSessionProperty(
                kHiveConnectorId,
                HiveConfig::kSortWriterMaxOutputBytesSession,
                folly::to<std::string>(testData.maxOutputBytes))
            .assertResults("SELECT count(*) FROM tmp");
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    ASSERT_EQ(outputCount, testData.expectedOutputCount);
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableWriterTest,
    UnpartitionedTableWriterTest,
    testing::ValuesIn(UnpartitionedTableWriterTest::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableWriterTest,
    PartitionedTableWriterTest,
    testing::ValuesIn(PartitionedTableWriterTest::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableWriterTest,
    BucketedTableOnlyWriteTest,
    testing::ValuesIn(BucketedTableOnlyWriteTest::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableWriterTest,
    AllTableWriterTest,
    testing::ValuesIn(AllTableWriterTest::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableWriterTest,
    PartitionedWithoutBucketTableWriterTest,
    testing::ValuesIn(
        PartitionedWithoutBucketTableWriterTest::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableWriterTest,
    BucketSortOnlyTableWriterTest,
    testing::ValuesIn(BucketSortOnlyTableWriterTest::getTestParams()));
