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

#include "velox/experimental/cudf/connectors/parquet/ParquetConfig.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetConnector.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetConnectorSplit.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetDataSource.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetTableHandle.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/tests/utils/CudfPlanBuilder.h"
#include "velox/experimental/cudf/tests/utils/ParquetConnectorTestBase.h"

#include "folly/dynamic.h"
#include "velox/common/base/Fs.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <re2/re2.h>

#include <string>

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::common;
using namespace facebook::velox::connector;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::test;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::dwio::common;

using namespace facebook::velox::cudf_velox;
using namespace facebook::velox::cudf_velox::exec;
using namespace facebook::velox::cudf_velox::exec::test;

constexpr uint64_t kQueryMemoryCapacity = 512 * MB;

namespace {

static std::shared_ptr<core::AggregationNode> generateColumnStatsSpec(
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

} // namespace

enum class TestMode {
  kUnpartitioned,
};

std::string testModeString(TestMode mode) {
  switch (mode) {
    case TestMode::kUnpartitioned:
      return "UNPARTITIONED";
  }
  VELOX_UNREACHABLE();
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

  explicit TestParam(uint64_t value) : value(value) {}

  TestParam(
      FileFormat fileFormat,
      TestMode testMode,
      CommitStrategy commitStrategy,
      bool multiDrivers,
      CompressionKind compressionKind) {
    value = static_cast<uint64_t>(compressionKind) << 32 |
        static_cast<uint64_t>(static_cast<bool>(multiDrivers)) << 24 |
        static_cast<uint64_t>(fileFormat) << 16 |
        static_cast<uint64_t>(testMode) << 8 |
        static_cast<uint64_t>(commitStrategy);
  }

  CompressionKind compressionKind() const {
    return static_cast<CompressionKind>((value & ((1L << 40) - 1)) >> 32);
  }

  bool multiDrivers() const {
    return (value >> 24) != 0;
  }

  FileFormat fileFormat() const {
    return static_cast<FileFormat>((value & ((1L << 24) - 1)) >> 16);
  }

  TestMode testMode() const {
    return static_cast<TestMode>((value & ((1L << 16) - 1)) >> 8);
  }

  CommitStrategy commitStrategy() const {
    return static_cast<CommitStrategy>((value & ((1L << 8) - 1)));
  }

  std::string toString() const {
    return fmt::format(
        "FileFormat[{}] TestMode[{}] commitStrategy[{}] multiDrivers[{}] compression[{}]",
        dwio::common::toString((fileFormat())),
        testModeString(testMode()),
        commitStrategyToString(commitStrategy()),
        multiDrivers(),
        compressionKindToString(compressionKind()));
  }
};

class TableWriteTest : public ParquetConnectorTestBase {
 protected:
  explicit TableWriteTest(uint64_t testValue)
      : testParam_(static_cast<TestParam>(testValue)),
        fileFormat_(dwio::common::FileFormat::PARQUET),
        testMode_(testParam_.testMode()),
        numTableWriterCount_(
            testParam_.multiDrivers() ? kNumTableWriterCount : 1),
        commitStrategy_(testParam_.commitStrategy()),
        compressionKind_(testParam_.compressionKind()) {
    LOG(INFO) << testParam_.toString();
    if (cudfDebugEnabled()) {
      std::cout << testParam_.toString() << std::endl;
    }

    auto rowType =
        ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
            {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()});
    setDataTypes(rowType);
  }

  void SetUp() override {
    ParquetConnectorTestBase::SetUp();
  }

  std::shared_ptr<Task> assertQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      std::vector<std::shared_ptr<TempFilePath>> filePaths,
      const std::string& duckDbSql,
      bool spillEnabled = false) {
    std::vector<Split> splits;
    for (const auto& filePath : filePaths) {
      splits.push_back(facebook::velox::exec::Split(
          makeParquetConnectorSplit(filePath->getPath())));
    }
    if (!spillEnabled) {
      return AssertQueryBuilder(plan, duckDbQueryRunner_)
          .maxDrivers(2 * kNumTableWriterCount)
          .config(
              QueryConfig::kTaskWriterCount,
              std::to_string(numTableWriterCount_))
          .splits(splits)
          .assertResults(duckDbSql);
    }
  }

  std::shared_ptr<Task> assertQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      const std::string& duckDbSql,
      bool enableSpill = false) {
    if (!enableSpill) {
      TestScopedSpillInjection scopedSpillInjection(100);
      return AssertQueryBuilder(plan, duckDbQueryRunner_)
          .maxDrivers(2 * kNumTableWriterCount)
          .config(
              QueryConfig::kTaskWriterCount,
              std::to_string(numTableWriterCount_))
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
  }

  RowVectorPtr runQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      bool spillEnabled = false) {
    if (!spillEnabled) {
      return AssertQueryBuilder(plan, duckDbQueryRunner_)
          .maxDrivers(2 * kNumTableWriterCount)
          .config(
              QueryConfig::kTaskWriterCount,
              std::to_string(numTableWriterCount_))
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
  }

  void setCommitStrategy(CommitStrategy commitStrategy) {
    commitStrategy_ = commitStrategy;
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

  std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
  makeParquetConnectorSplits(
      const std::shared_ptr<TempDirectoryPath>& directoryPath) {
    return makeParquetConnectorSplits(directoryPath->getPath());
  }

  std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
  makeParquetConnectorSplits(const std::string& directoryPath) {
    std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
        splits;

    for (auto& path : fs::recursive_directory_iterator(directoryPath)) {
      if (path.is_regular_file()) {
        splits.push_back(ParquetConnectorTestBase::makeParquetConnectorSplits(
            path.path().string(), 1)[0]);
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

  // Builds and returns the parquet splits from the list of files with one split
  // per each file.
  std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
  makeParquetConnectorSplits(
      const std::vector<std::filesystem::path>& filePaths) {
    std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
        splits;
    for (const auto& filePath : filePaths) {
      splits.push_back(ParquetConnectorTestBase::makeParquetConnectorSplits(
          filePath.string(), 1)[0]);
    }
    return splits;
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t numVectors,
      int32_t rowsPerVector) {
    return ParquetConnectorTestBase::makeVectors(
        rowType_, numVectors, rowsPerVector);
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
      const cudf_velox::connector::parquet::LocationHandle::TableType&
          outputTableType,
      const std::string& outputDirectoryPath,
      const std::optional<CompressionKind> compressionKind = {}) {
    return std::make_shared<core::InsertTableHandle>(
        kParquetConnectorId,
        makeParquetInsertTableHandle(
            outputRowType->names(),
            outputRowType->children(),
            makeLocationHandle(outputDirectoryPath, outputTableType),
            compressionKind));
  }

  // Returns a table insert plan node.
  PlanNodePtr createInsertPlan(
      PlanBuilder& inputPlan,
      const RowTypePtr& outputRowType,
      const std::string& outputDirectoryPath,
      const std::optional<CompressionKind> compressionKind = {},
      int numTableWriters = 1,
      const cudf_velox::connector::parquet::LocationHandle::TableType&
          outputTableType =
              cudf_velox::connector::parquet::LocationHandle::TableType::kNew,
      const CommitStrategy& outputCommitStrategy = CommitStrategy::kNoCommit,
      bool aggregateResult = true,
      std::shared_ptr<core::AggregationNode> aggregationNode = nullptr) {
    return createInsertPlan(
        inputPlan,
        inputPlan.planNode()->outputType(),
        outputRowType,
        outputDirectoryPath,
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
      const std::optional<CompressionKind> compressionKind = {},
      int numTableWriters = 1,
      const cudf_velox::connector::parquet::LocationHandle::TableType&
          outputTableType =
              cudf_velox::connector::parquet::LocationHandle::TableType::kNew,
      const CommitStrategy& outputCommitStrategy = CommitStrategy::kNoCommit,
      bool aggregateResult = true,
      std::shared_ptr<core::AggregationNode> aggregationNode = nullptr) {
    VELOX_CHECK(
        numTableWriters == 1, "Multiple CudfTableWriters not yet supported");
    return createInsertPlanWithSingleWriter(
        inputPlan,
        inputRowType,
        tableRowType,
        outputDirectoryPath,
        compressionKind,
        outputTableType,
        outputCommitStrategy,
        aggregateResult,
        aggregationNode);
  }

  PlanNodePtr createInsertPlanWithSingleWriter(
      PlanBuilder& inputPlan,
      const RowTypePtr& inputRowType,
      const RowTypePtr& tableRowType,
      const std::string& outputDirectoryPath,
      const std::optional<CompressionKind> compressionKind,
      const cudf_velox::connector::parquet::LocationHandle::TableType&
          outputTableType,
      const CommitStrategy& outputCommitStrategy,
      bool aggregateResult,
      std::shared_ptr<core::AggregationNode> aggregationNode) {
    const bool addScaleWriterExchange = false;
    auto insertPlan = inputPlan;
    insertPlan
        .addNode(addCudfTableWriter(
            inputRowType,
            tableRowType->names(),
            aggregationNode,
            createInsertTableHandle(
                tableRowType,
                outputTableType,
                outputDirectoryPath,
                compressionKind),
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

  // Return the corresponding column names in 'inputRowType' of
  // 'tableColumnNames' from 'tableRowType'.
  static std::vector<std::string> inputColumnNames(
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

  // Parameter partitionName is string formatted in the Parquet style
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
  }

  int getNumWriters() {
    return numTableWriterCount_;
  }

  static inline int kNumTableWriterCount = 1;

  const TestParam testParam_;
  const FileFormat fileFormat_ = FileFormat::PARQUET;
  const TestMode testMode_;
  const int numTableWriterCount_;

  RowTypePtr rowType_;
  RowTypePtr tableSchema_;
  CommitStrategy commitStrategy_;
  std::optional<CompressionKind> compressionKind_;
  std::vector<column_index_t> sortColumnIndices_;
  std::vector<CompareFlags> sortedFlags_;
  core::PlanNodeId tableWriteNodeId_;
};

class BasicTableWriteTest : public ParquetConnectorTestBase {};

TEST_F(BasicTableWriteTest, roundTrip) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row * 2; }, nullEvery(7)),
  });

  auto sourceFilePath = TempFilePath::create();
  writeToFile(sourceFilePath->getPath(), data);

  auto targetDirectoryPath = TempDirectoryPath::create();

  auto rowType = asRowType(data->type());
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(rowType)
                  .tableHandle(ParquetConnectorTestBase::makeTableHandle())
                  .endTableScan()
                  .addNode(cudfTableWrite(targetDirectoryPath->getPath()))
                  .planNode();

  auto results =
      AssertQueryBuilder(plan)
          .split(makeParquetConnectorSplit(sourceFilePath->getPath()))
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
  plan = PlanBuilder()
             .startTableScan()
             .outputType(rowType)
             .tableHandle(ParquetConnectorTestBase::makeTableHandle())
             .endTableScan()
             .planNode();

  auto copy = AssertQueryBuilder(plan)
                  .split(makeParquetConnectorSplit(fmt::format(
                      "{}/{}", targetDirectoryPath->getPath(), writeFileName)))
                  .copyResults(pool());
  assertEqualResults({data}, {copy});
}

TEST_F(BasicTableWriteTest, targetFileName) {
  constexpr const char* kFileName = "test.parquet";
  auto data = makeRowVector({makeFlatVector<int64_t>(10, folly::identity)});
  auto directory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .values({data})
                  .addNode(cudfTableWrite(
                      directory->getPath(),
                      dwio::common::FileFormat::PARQUET,
                      {},
                      nullptr,
                      kFileName))
                  .planNode();

  auto results = AssertQueryBuilder(plan).copyResults(pool());
  auto* details = results->childAt(TableWriteTraits::kFragmentChannel)
                      ->asUnchecked<SimpleVector<StringView>>();
  auto detail = folly::parseJson(details->valueAt(1));
  auto fileWriteInfos = detail["fileWriteInfos"];
  ASSERT_EQ(1, fileWriteInfos.size());
  ASSERT_EQ(fileWriteInfos[0]["writeFileName"].asString(), kFileName);
  plan = PlanBuilder()
             .startTableScan()
             .outputType(asRowType(data->type()))
             .tableHandle(ParquetConnectorTestBase::makeTableHandle())
             .endTableScan()
             .planNode();
  AssertQueryBuilder(plan)
      .split(makeParquetConnectorSplit(
          fmt::format("{}/{}", directory->getPath(), kFileName)))
      .assertResults(data);
}

class UnpartitionedTableWriterTest
    : public TableWriteTest,
      public testing::WithParamInterface<uint64_t> {
 public:
  UnpartitionedTableWriterTest() : TableWriteTest(GetParam()) {}

  static std::vector<uint64_t> getTestParams() {
    std::vector<uint64_t> testParams;
    const auto multiDriverOptions = std::vector<bool>{false, true};
    for (bool multiDrivers : multiDriverOptions) {
      testParams.push_back(TestParam{
          FileFormat::PARQUET,
          TestMode::kUnpartitioned,
          CommitStrategy::kNoCommit,
          multiDrivers,
          CompressionKind_NONE}
                               .value);
      testParams.push_back(TestParam{
          FileFormat::PARQUET,
          TestMode::kUnpartitioned,
          CommitStrategy::kTaskCommit,
          multiDrivers,
          CompressionKind_NONE}
                               .value);
    }
    return testParams;
  }
};

TEST_P(UnpartitionedTableWriterTest, differentCompression) {
  std::vector<CompressionKind> compressions{
      CompressionKind_NONE,
      CompressionKind_SNAPPY,
      CompressionKind_ZSTD,
      CompressionKind_LZ4,
      CompressionKind_MAX};

  for (auto compressionKind : compressions) {
    auto input = makeVectors(10, 10);
    auto outputDirectory = TempDirectoryPath::create();
    if (compressionKind == CompressionKind_MAX) {
      VELOX_ASSERT_THROW(
          createInsertPlan(
              PlanBuilder().values(input),
              rowType_,
              outputDirectory->getPath(),
              compressionKind,
              numTableWriterCount_,
              cudf_velox::connector::parquet::LocationHandle::TableType::kNew),
          "Unsupported compression type: CompressionKind_MAX");
      return;
    }
    auto plan = createInsertPlan(
        PlanBuilder().values(input),
        rowType_,
        outputDirectory->getPath(),
        compressionKind,
        numTableWriterCount_,
        cudf_velox::connector::parquet::LocationHandle::TableType::kNew);

    auto result = AssertQueryBuilder(plan)
                      .config(
                          QueryConfig::kTaskWriterCount,
                          std::to_string(numTableWriterCount_))
                      .copyResults(pool());
    assertEqualResults(
        {makeRowVector({makeConstant<int64_t>(100, 1)})}, {result});
  }
}

// Test not really needed as we always write a TableType::kNew table in Parquet
// DataSink
TEST_P(UnpartitionedTableWriterTest, immutableSettings) {
  struct {
    cudf_velox::connector::parquet::LocationHandle::TableType dataType;
    bool immutableFilesEnabled;
    bool expectedInsertSuccees;

    std::string debugString() const {
      return fmt::format(
          "dataType:{}, immutableFilesEnabled:{}, operationSuccess:{}",
          dataType,
          immutableFilesEnabled,
          expectedInsertSuccees);
    }
  } testSettings[] = {
      {cudf_velox::connector::parquet::LocationHandle::TableType::kNew,
       true,
       true},
      {cudf_velox::connector::parquet::LocationHandle::TableType::kNew,
       false,
       true}};

  for (auto testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    std::unordered_map<std::string, std::string> propFromFile{
        {"parquet.immutable-files",
         testData.immutableFilesEnabled ? "true" : "false"}};
    std::shared_ptr<const config::ConfigBase> config{
        std::make_shared<config::ConfigBase>(std::move(propFromFile))};
    resetParquetConnector(config);

    auto input = makeVectors(10, 10);
    auto outputDirectory = TempDirectoryPath::create();
    auto plan = createInsertPlan(
        PlanBuilder().values(input),
        rowType_,
        outputDirectory->getPath(),
        CompressionKind_NONE,
        numTableWriterCount_,
        testData.dataType);

    if (!testData.expectedInsertSuccees) {
      VELOX_ASSERT_THROW(
          AssertQueryBuilder(plan).copyResults(pool()),
          "Parquet tables are immutable.");
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

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableWriterTest,
    UnpartitionedTableWriterTest,
    testing::ValuesIn(UnpartitionedTableWriterTest::getTestParams()));
