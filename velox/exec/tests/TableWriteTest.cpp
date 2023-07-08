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
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <re2/re2.h>

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::dwio::common;

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
      bool bucketSort) {
    value = static_cast<uint64_t>(fileFormat) << 48 |
        static_cast<uint64_t>(testMode) << 40 |
        static_cast<uint64_t>(commitStrategy) << 32 |
        static_cast<uint64_t>(bucketKind) << 24 | !!bucketSort;
  }

  FileFormat fileFormat() const {
    return static_cast<FileFormat>(value >> 48);
  }

  TestMode testMode() const {
    return static_cast<TestMode>((value & ((1L << 48) - 1)) >> 40);
  }

  CommitStrategy commitStrategy() const {
    return static_cast<CommitStrategy>((value & ((1L << 40) - 1)) >> 32);
  }

  HiveBucketProperty::Kind bucketKind() const {
    return static_cast<HiveBucketProperty::Kind>(
        (value & ((1L << 32) - 1)) >> 24);
  }

  bool bucketSort() const {
    return (value & ((1L << 24) - 1)) != 0;
  }

  std::string toString() const {
    return fmt::format(
        "FileFormat[{}] TestMode[{}] commitStrategy[{}] bucketKind[{}] bucketSort[{}]",
        fileFormat(),
        testMode(),
        commitStrategy(),
        bucketKind(),
        bucketSort());
  }
};

class TableWriteTest : public HiveConnectorTestBase {
 protected:
  explicit TableWriteTest(uint64_t testValue)
      : testParam_(static_cast<TestParam>(testValue)),
        fileFormat_(testParam_.fileFormat()),
        testMode_(testParam_.testMode()),
        commitStrategy_(testParam_.commitStrategy()) {
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
        sortedBy = {
            std::make_shared<const HiveSortingColumn>(
                "c4", core::SortOrder{true, true}),
            std::make_shared<const HiveSortingColumn>(
                "c1", core::SortOrder{false, false})};
      }
      bucketProperty_ = std::make_shared<HiveBucketProperty>(
          testParam_.bucketKind(), 4, bucketedBy, bucketedTypes, sortedBy);
    }
  }

  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
  }

  void TearDown() {
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
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
      const std::shared_ptr<HiveBucketProperty> bucketProperty) {
    return std::make_shared<core::InsertTableHandle>(
        kHiveConnectorId,
        makeHiveInsertTableHandle(
            outputRowType->names(),
            outputRowType->children(),
            partitionedBy,
            bucketProperty,
            makeLocationHandle(
                outputDirectoryPath, std::nullopt, outputTableType),
            fileFormat_));
  }

  // Returns a table insert plan node.
  PlanNodePtr createInsertPlan(
      PlanBuilder& inputPlan,
      const RowTypePtr& outputRowType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy = {},
      std::shared_ptr<HiveBucketProperty> bucketProperty = {},
      const connector::hive::LocationHandle::TableType& outputTableType =
          connector::hive::LocationHandle::TableType::kNew,
      const CommitStrategy& outputCommitStrategy = CommitStrategy::kNoCommit) {
    return inputPlan
        .tableWrite(
            outputRowType->names(),
            createInsertTableHandle(
                outputRowType,
                outputTableType,
                outputDirectoryPath,
                partitionedBy,
                bucketProperty),
            nullptr,
            outputCommitStrategy,
            "rows")
        .project({"rows"})
        .planNode();
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
    folly::split("/", partitionName, partitionKeyValues);
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
      ASSERT_TRUE(
          RE2::FullMatch(filePath.filename().string(), "test_cursor.+_0_.+"))
          << filePath.filename().string();
    } else {
      ASSERT_TRUE(RE2::FullMatch(
          filePath.filename().string(), ".tmp.velox.test_cursor.+_0_0_.+"))
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
      ASSERT_TRUE(RE2::FullMatch(
          filePath.filename().string(), "0[0-9]+_0_TaskCursorQuery_[0-9]+"))
          << filePath.filename().string();
    } else {
      ASSERT_TRUE(RE2::FullMatch(
          filePath.filename().string(),
          ".tmp.velox.0[0-9]+_0_TaskCursorQuery_[0-9]+_.+"))
          << filePath.filename().string();
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
    assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        {makeHiveConnectorSplits(filePaths)},
        fmt::format(
            "SELECT * FROM tmp WHERE {}",
            partitionNameToPredicate(getPartitionDirNames(dirPath))));
  }

  // Gets the hash function used by the production code to build bucket id.
  std::unique_ptr<core::PartitionFunction> getBucketFunction() {
    const auto& bucketedBy = bucketProperty_->bucketedBy();
    std::vector<column_index_t> bucketedByChannels;
    bucketedByChannels.reserve(bucketedBy.size());
    for (int32_t i = 0; i < bucketedBy.size(); ++i) {
      const auto& bucketColumn = bucketedBy[i];
      for (column_index_t columnChannel = 0;
           columnChannel < tableSchema_->size();
           ++columnChannel) {
        if (tableSchema_->nameOf(columnChannel) == bucketColumn) {
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
  void verifyBucketedFileData(const std::filesystem::path& filePath) {
    const std::vector<std::filesystem::path> filePaths = {filePath};

    // Read data from bucketed file on disk into 'rowVector'.
    core::PlanNodeId scanNodeId;
    auto plan = PlanBuilder()
                    .tableScan(tableSchema_)
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
    auto bucketFunction = getBucketFunction();
    std::vector<uint32_t> bucketIds;
    bucketIds.reserve(resultVector->size());
    bucketFunction->partition(*resultVector, bucketIds);
    for (const auto bucketId : bucketIds) {
      ASSERT_EQ(expectedBucketId, bucketId);
    }
  }

  // Verifies the file layout and data produced by a table writer.
  void verifyTableWriterOutput(
      const std::string& targetDir,
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
      ASSERT_EQ(filePaths.size(), 1);
      verifyUnbucketedFilePath(filePaths[0], targetDir);
      return;
    }
    ASSERT_EQ(numPartitionKeyValues_.size(), 2);
    const int32_t totalPartitions =
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
      ASSERT_EQ(numLeafDir, filePaths.size());
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
        verifyBucketedFileData(filePath);
      }
    }
    if (verifyPartitionedData) {
      for (const auto& entry : bucketFilesPerPartition) {
        verifyPartitionedFilesData(entry.second, entry.second[0].parent_path());
      }
    }
  }

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

  RowTypePtr rowType_;
  RowTypePtr tableSchema_;
  CommitStrategy commitStrategy_;
  std::vector<std::string> partitionedBy_;
  std::vector<TypePtr> partitionTypes_;
  std::vector<column_index_t> partitionChannels_;
  std::vector<uint32_t> numPartitionKeyValues_;
  std::shared_ptr<HiveBucketProperty> bucketProperty_{nullptr};
};

class PartitionedTableWriterTest
    : public TableWriteTest,
      public testing::WithParamInterface<uint64_t> {
 public:
  PartitionedTableWriterTest() : TableWriteTest(GetParam()) {}

  static std::vector<uint64_t> getTestParams() {
    std::vector<uint64_t> testParams;
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kPartitioned,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kPartitioned,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kPrestoNative,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kPrestoNative,
        false}
                             .value);
    if (hasWriterFactory(FileFormat::PARQUET)) {
      testParams.push_back(TestParam{
          FileFormat::PARQUET,
          TestMode::kPartitioned,
          CommitStrategy::kNoCommit,
          HiveBucketProperty::Kind::kHiveCompatible,
          false}
                               .value);
      testParams.push_back(TestParam{
          FileFormat::PARQUET,
          TestMode::kPartitioned,
          CommitStrategy::kTaskCommit,
          HiveBucketProperty::Kind::kHiveCompatible,
          false}
                               .value);
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
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kUnpartitioned,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kUnpartitioned,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    if (hasWriterFactory(FileFormat::PARQUET)) {
      testParams.push_back(TestParam{
          FileFormat::PARQUET,
          TestMode::kUnpartitioned,
          CommitStrategy::kNoCommit,
          HiveBucketProperty::Kind::kHiveCompatible,
          false}
                               .value);
      testParams.push_back(TestParam{
          FileFormat::PARQUET,
          TestMode::kUnpartitioned,
          CommitStrategy::kTaskCommit,
          HiveBucketProperty::Kind::kHiveCompatible,
          false}
                               .value);
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
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kPrestoNative,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kPrestoNative,
        false}
                             .value);
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
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kPartitioned,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kPartitioned,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    return testParams;
  }
};

class AllTableWriterTest : public TableWriteTest,
                           public testing::WithParamInterface<uint64_t> {
 public:
  AllTableWriterTest() : TableWriteTest(GetParam()) {}

  static std::vector<uint64_t> getTestParams() {
    std::vector<uint64_t> testParams;
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kUnpartitioned,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kUnpartitioned,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kPartitioned,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kPartitioned,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kHiveCompatible,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kNoCommit,
        HiveBucketProperty::Kind::kPrestoNative,
        false}
                             .value);
    testParams.push_back(TestParam{
        FileFormat::DWRF,
        TestMode::kBucketed,
        CommitStrategy::kTaskCommit,
        HiveBucketProperty::Kind::kPrestoNative,
        false}
                             .value);
    // TODO: add to test bucket sort after it is supported.
    if (hasWriterFactory(FileFormat::PARQUET)) {
      testParams.push_back(TestParam{
          FileFormat::PARQUET,
          TestMode::kUnpartitioned,
          CommitStrategy::kNoCommit,
          HiveBucketProperty::Kind::kHiveCompatible,
          false}
                               .value);
      testParams.push_back(TestParam{
          FileFormat::PARQUET,
          TestMode::kUnpartitioned,
          CommitStrategy::kTaskCommit,
          HiveBucketProperty::Kind::kHiveCompatible,
          false}
                               .value);
    }
    return testParams;
  }
};

// Runs a pipeline with read + filter + project (with substr) + write.
TEST_P(AllTableWriterTest, scanFilterProjectWrite) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(filePaths.size(), 1000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();

  auto planBuilder = PlanBuilder();
  auto project =
      planBuilder.tableScan(rowType_)
          .filter("c2 <> 0")
          .project({"c0", "c1", "c3", "c5", "c2 + c3", "substr(c5, 1, 1)"})
          .planNode();

  auto types = project->outputType()->children();
  std::vector<std::string> tableColumnNames = {
      "c0", "c1", "c3", "c5", "c2_plus_c3", "substr_c5"};
  auto plan = planBuilder
                  .tableWrite(
                      tableColumnNames,
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              tableColumnNames,
                              types,
                              partitionedBy_,
                              bucketProperty_,
                              makeLocationHandle(outputDirectory->path),
                              fileFormat_)),
                      nullptr,
                      commitStrategy_,
                      "rows")
                  .project({"rows"})
                  .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp WHERE c2 <> 0");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query.

  assertQuery(
      PlanBuilder()
          .tableScan(ROW(std::move(tableColumnNames), std::move(types)))
          .planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT c0, c1, c3, c5, c2 + c3, substr(c5, 1, 1) FROM tmp WHERE c2 <> 0");

  verifyTableWriterOutput(outputDirectory->path, false);
}

TEST_P(AllTableWriterTest, renameAndReorderColumns) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto inputRowType =
      ROW({"c5", "c4", "c1", "c0", "c3"},
          {VARCHAR(), DOUBLE(), INTEGER(), BIGINT(), REAL()});
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
  setTableSchema(
      ROW({"v", "w", "x", "y", "z"},
          {VARCHAR(), DOUBLE(), INTEGER(), BIGINT(), REAL()}));
  auto plan = PlanBuilder()
                  .tableScan(rowType_)
                  .tableWrite(
                      inputRowType,
                      tableSchema_->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              tableSchema_->names(),
                              inputRowType->children(),
                              partitionedBy_,
                              bucketProperty_,
                              makeLocationHandle(outputDirectory->path),
                              fileFormat_)),
                      nullptr,
                      commitStrategy_,
                      "rows")
                  .project({"rows"})
                  .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  assertQuery(
      PlanBuilder().tableScan(tableSchema_).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT c5, c4, c1, c0, c3 FROM tmp");

  verifyTableWriterOutput(outputDirectory->path, false);
}

// Runs a pipeline with read + write.
TEST_P(AllTableWriterTest, directReadWrite) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(filePaths.size(), 1000);
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
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query.

  assertQuery(
      PlanBuilder().tableScan(rowType_).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");

  verifyTableWriterOutput(outputDirectory->path);
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
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  assertQuery(op, fmt::format("SELECT {}", size));

  assertQuery(
      PlanBuilder().tableScan(rowType_).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");

  verifyTableWriterOutput(outputDirectory->path);
}

TEST_P(AllTableWriterTest, commitStrategies) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(filePaths.size(), 1000);

  createDuckDbTable(vectors);

  // Test the kTaskCommit commit strategy writing to one dot-prefixed temporary
  // file.
  {
    SCOPED_TRACE(CommitStrategy::kTaskCommit);
    auto outputDirectory = TempDirectoryPath::create();
    auto plan = createInsertPlan(
        PlanBuilder().values(vectors),
        rowType_,
        outputDirectory->path,
        partitionedBy_,
        bucketProperty_,
        connector::hive::LocationHandle::TableType::kNew,
        commitStrategy_);

    assertQuery(plan, "SELECT count(*) FROM tmp");

    assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT * FROM tmp");
    verifyTableWriterOutput(outputDirectory->path);
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
        connector::hive::LocationHandle::TableType::kNew,
        commitStrategy_);

    assertQuery(plan, "SELECT count(*) FROM tmp");

    assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT * FROM tmp");
    verifyTableWriterOutput(outputDirectory->path);
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
  while (iterPartitionDirectory != actualPartitionDirectories.end()) {
    assertQuery(
        PlanBuilder().tableScan(rowType).planNode(),
        makeHiveConnectorSplits(*iterPartitionDirectory),
        fmt::format(
            "SELECT * FROM tmp WHERE {}",
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
  auto plan = createInsertPlan(
      PlanBuilder().tableScan(rowType),
      rowType,
      outputDirectory->path,
      partitionKeys,
      bucketProperty_,
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  auto task = assertQuery(plan, inputFilePaths, "SELECT count(*) FROM tmp");

  std::set<std::string> partitionDirectories =
      getLeafSubdirectories(outputDirectory->path);

  // Verify only a single partition directory is created.
  ASSERT_EQ(partitionDirectories.size(), 1);
  EXPECT_EQ(
      *partitionDirectories.begin(),
      fs::path(outputDirectory->path) / "p0=365");

  // Verify all data is written to the single partition directory.
  assertQuery(
      PlanBuilder().tableScan(rowType).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");

  // In case of unbucketed partitioned table, one single file is written to
  // each partition directory for Hive connector.
  if (testMode_ == TestMode::kPartitioned) {
    ASSERT_EQ(countRecursiveFiles(*partitionDirectories.begin()), 1);
  } else {
    ASSERT_GE(countRecursiveFiles(*partitionDirectories.begin()), 1);
  }
}

TEST_P(PartitionedWithoutBucketTableWriterTest, fromSinglePartitionToMultiple) {
  const int32_t numBatches = 1;
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), BIGINT()});
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
      partitionKeys);

  assertQuery(plan, "SELECT count(*) FROM tmp");

  assertQuery(
      PlanBuilder().tableScan(rowType).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");
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
      connector::hive::LocationHandle::TableType::kNew,
      commitStrategy_);

  if (testMode_ == TestMode::kPartitioned) {
    VELOX_ASSERT_THROW(
        AssertQueryBuilder(plan)
            .connectorConfig(
                kHiveConnectorId,
                HiveConfig::kMaxPartitionsPerWriters,
                folly::to<std::string>(maxPartitions))
            .copyResults(pool()),
        fmt::format(
            "Exceeded limit of {} distinct partitions.", maxPartitions));
  } else {
    VELOX_ASSERT_THROW(
        AssertQueryBuilder(plan)
            .connectorConfig(
                kHiveConnectorId,
                HiveConfig::kMaxPartitionsPerWriters,
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

TEST_P(UnpartitionedTableWriterTest, createAndInsertIntoUnpartitionedTable) {
  // When table type is NEW, we always return UpdateMode::kNew. In this case
  // no exception is expected because we are trying to insert rows into a new
  // table.
  for (auto immutablePartitionsEnabled : {"true", "false"}) {
    auto input = makeVectors(10, 10);
    auto outputDirectory = TempDirectoryPath::create();
    auto plan = createInsertPlan(
        PlanBuilder().values(input),
        rowType_,
        outputDirectory->path,
        {},
        nullptr,
        connector::hive::LocationHandle::TableType::kNew);

    auto result = AssertQueryBuilder(plan)
                      .connectorConfig(
                          kHiveConnectorId,
                          HiveConfig::kImmutablePartitions,
                          immutablePartitionsEnabled)
                      .copyResults(pool());

    assertEqualResults(
        {makeRowVector({makeConstant<int64_t>(100, 1)})}, {result});
  }
}

TEST_P(
    UnpartitionedTableWriterTest,
    appendToAnExistingUnpartitionedTableNotAllowed) {
  // When table type is EXISTING and "immutable_partitions" config is set to
  // true, inserts into such unpartitioned tables are not allowed.
  //
  // We assert that an error is thrown in this case.

  auto input = makeVectors(10, 10);
  auto outputDirectory = TempDirectoryPath::create();
  auto plan = createInsertPlan(
      PlanBuilder().values(input),
      rowType_,
      outputDirectory->path,
      {},
      nullptr,
      connector::hive::LocationHandle::TableType::kExisting);

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .connectorConfig(
              kHiveConnectorId, HiveConfig::kImmutablePartitions, "true")
          .copyResults(pool()),
      "Unpartitioned Hive tables are immutable.");
}

TEST_P(UnpartitionedTableWriterTest, appendToAnExistingUnpartitionedTable) {
  // This test uses the default value "false" for the "immutable_partitions"
  // config allowing writes to an existing unpartitioned table.
  //
  // The test inserts data vector by vector and checks the intermediate results
  // as well as the final result.

  auto kRowsPerVector = 100;
  auto input = makeVectors(10, kRowsPerVector);

  createDuckDbTable(input);

  for (auto tableType : tableTypes_) {
    auto outputDirectory = TempDirectoryPath::create();
    auto numRows = 0;

    for (auto rowVector : input) {
      numRows += kRowsPerVector;
      auto plan = createInsertPlan(
          PlanBuilder().values({rowVector}),
          rowType_,
          outputDirectory->path,
          {},
          nullptr,
          tableType);
      assertQuery(plan, fmt::format("SELECT {}", kRowsPerVector));
      assertQuery(
          PlanBuilder()
              .tableScan(rowType_)
              .singleAggregation({}, {"count(*)"})
              .planNode(),
          makeHiveConnectorSplits(outputDirectory),
          fmt::format("SELECT {}", numRows));
    }

    assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT * FROM tmp");
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
        connector::hive::LocationHandle::TableType::kNew,
        commitStrategy_);
    if (testData.expectedError) {
      VELOX_ASSERT_THROW(
          AssertQueryBuilder(plan)
              .connectorConfig(
                  kHiveConnectorId,
                  HiveConfig::kMaxPartitionsPerWriters,
                  // Make sure we have a sufficient large writer limit.
                  folly::to<std::string>(testData.bucketCount * 2))
              .copyResults(pool()),
          "bucketCount exceeds the limit");
    } else {
      assertQuery(plan, "SELECT count(*) FROM tmp");

      assertQuery(
          PlanBuilder().tableScan(rowType_).planNode(),
          makeHiveConnectorSplits(outputDirectory),
          "SELECT * FROM tmp");
      verifyTableWriterOutput(outputDirectory->path);
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
  auto input = makeVectors(10, 100);
  createDuckDbTable(input);
  auto outputDirectory = TempDirectoryPath::create();
  const auto writerOutputType =
      ROW({"numWrittenRows", "fragment", "tableCommitContext"},
          {BIGINT(), VARBINARY(), VARBINARY()});
  auto plan = PlanBuilder()
                  .values({input})
                  .tableWrite(
                      rowType_,
                      rowType_->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType_->names(),
                              rowType_->children(),
                              partitionedBy_,
                              bucketProperty_,
                              makeLocationHandle(outputDirectory->path),
                              fileFormat_)),
                      nullptr,
                      commitStrategy_,
                      writerOutputType)
                  .planNode();

  auto result = AssertQueryBuilder(plan).copyResults(pool());
  auto writtenRowVector = result->childAt(TableWriterTraits::kRowCountChannel)
                              ->asFlatVector<int64_t>();
  auto fragmentVector = result->childAt(TableWriterTraits::kFragmentChannel)
                            ->asFlatVector<StringView>();
  auto commitContextVector = result->childAt(TableWriterTraits::kContextChannel)
                                 ->asFlatVector<StringView>();
  const int64_t expectedRows = 10 * 100;
  std::vector<std::string> writeFiles;
  int64_t numRows{0};
  for (int i = 0; i < result->size(); ++i) {
    if (i == 0) {
      ASSERT_TRUE(fragmentVector->isNullAt(i));
    } else {
      ASSERT_TRUE(writtenRowVector->isNullAt(i));
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
      if (commitStrategy_ == CommitStrategy::kNoCommit) {
        ASSERT_EQ(writeFileName, targetFileName);
      } else {
        ASSERT_TRUE(writeFileName.find(targetFileName) != std::string::npos);
      }
    }
    ASSERT_FALSE(commitContextVector->isNullAt(i));
    ASSERT_TRUE(RE2::FullMatch(
        commitContextVector->valueAt(i).getString(),
        fmt::format(".*{}.*", commitStrategy_)))
        << commitContextVector->valueAt(i);
  }
  ASSERT_EQ(numRows, expectedRows);
  if (testMode_ == TestMode::kUnpartitioned) {
    ASSERT_EQ(writeFiles.size(), 1);
  }
  auto diskFiles = listAllFiles(outputDirectory->path);
  std::sort(diskFiles.begin(), diskFiles.end());
  std::sort(writeFiles.begin(), writeFiles.end());
  ASSERT_EQ(diskFiles, writeFiles)
      << "\nwrite files: " << folly::join(",", writeFiles)
      << "\ndisk files: " << folly::join(",", diskFiles);
  // Verify the utilities provided by table writer traits.
  ASSERT_EQ(TableWriterTraits::getRowCount(result), 10 * 100);
  auto obj = TableWriterTraits::getTableCommitContext(result);
  ASSERT_EQ(
      obj[TableWriterTraits::kCommitStrategyContextKey],
      commitStrategyToString(commitStrategy_));
  ASSERT_EQ(obj[TableWriterTraits::klastPageContextKey], true);
  ASSERT_EQ(obj[TableWriterTraits::kLifeSpanContextKey], "TaskWide");
}

// TODO: add partitioned table write update mode tests and more failure tests.

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
