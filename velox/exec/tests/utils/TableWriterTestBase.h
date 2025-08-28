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
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <re2/re2.h>
#include <string>
#include "folly/experimental/EventCount.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"

namespace velox::exec::test {
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

class TableWriterTestBase : public HiveConnectorTestBase {
 public:
  enum class TestMode {
    kUnpartitioned,
    kPartitioned,
    kBucketed,
    kOnlyBucketed,
  };

  static std::string testModeString(TestMode mode);

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
        CompressionKind compressionKind,
        bool scaleWriter);

    CompressionKind compressionKind() const;

    bool multiDrivers() const;

    FileFormat fileFormat() const;

    TestMode testMode() const;

    CommitStrategy commitStrategy() const;

    HiveBucketProperty::Kind bucketKind() const;

    bool bucketSort() const;

    bool scaleWriter() const;

    std::string toString() const;
  };

 protected:
  explicit TableWriterTestBase(uint64_t testValue);

  void SetUp() override;

  static std::function<PlanNodePtr(std::string, PlanNodePtr)> addTableWriter(
      const RowTypePtr& inputColumns,
      const std::vector<std::string>& tableColumnNames,
      const std::optional<core::ColumnStatsSpec>& columnStatsSpec,
      const std::shared_ptr<core::InsertTableHandle>& insertHandle,
      bool hasPartitioningScheme,
      connector::CommitStrategy commitStrategy =
          connector::CommitStrategy::kNoCommit);

  static RowTypePtr getNonPartitionsColumns(
      const std::vector<std::string>& partitionedKeys,
      const RowTypePtr& rowType);

  static core::ColumnStatsSpec generateColumnStatsSpec(
      const std::string& name,
      const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys,
      AggregationNode::Step step);

  std::shared_ptr<Task> assertQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      std::vector<std::shared_ptr<TempFilePath>> filePaths,
      const std::string& duckDbSql,
      bool spillEnabled = false);

  std::shared_ptr<Task> assertQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      const std::string& duckDbSql,
      bool enableSpill = false);

  RowVectorPtr runQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      bool spillEnabled = false);

  void setCommitStrategy(CommitStrategy commitStrategy);

  void setPartitionBy(const std::vector<std::string>& partitionBy);

  void setBucketProperty(
      HiveBucketProperty::Kind kind,
      uint32_t bucketCount,
      const std::vector<std::string>& bucketedBy,
      const std::vector<TypePtr>& bucketedTypes,
      const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortedBy =
          {});

  void setDataTypes(
      const RowTypePtr& inputType,
      const RowTypePtr& tableSchema = nullptr);

  void setTableSchema(const RowTypePtr& tableSchema);

  std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeHiveConnectorSplits(
      const std::shared_ptr<TempDirectoryPath>& directoryPath);

  std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeHiveConnectorSplits(const std::string& directoryPath);

  // Lists and returns all the regular files from a given directory recursively.
  std::vector<std::string> listAllFiles(const std::string& directoryPath);

  // Builds and returns the hive splits from the list of files with one split
  // per each file.
  std::vector<std::shared_ptr<connector::ConnectorSplit>>
  makeHiveConnectorSplits(const std::vector<std::filesystem::path>& filePaths);

  std::vector<RowVectorPtr> makeVectors(
      int32_t numVectors,
      int32_t rowsPerVector);

  RowVectorPtr makeConstantVector(size_t size);

  std::vector<RowVectorPtr> makeBatches(
      vector_size_t numBatches,
      std::function<RowVectorPtr(int32_t)> makeVector);

  std::set<std::string> getLeafSubdirectories(const std::string& directoryPath);

  std::vector<std::string> getRecursiveFiles(const std::string& directoryPath);

  uint32_t countRecursiveFiles(const std::string& directoryPath);

  // Helper method to return InsertTableHandle.
  std::shared_ptr<core::InsertTableHandle> createInsertTableHandle(
      const RowTypePtr& outputRowType,
      const connector::hive::LocationHandle::TableType& outputTableType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy,
      const std::shared_ptr<HiveBucketProperty> bucketProperty,
      const std::optional<CompressionKind> compressionKind = {});

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
      const std::optional<ColumnStatsSpec>& columnStatsSpec = std::nullopt);

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
      const std::optional<ColumnStatsSpec>& columnStatsSpec = std::nullopt);

  PlanNodePtr createInsertPlanWithSingleWriter(
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
      std::optional<ColumnStatsSpec> columnStatsSpec);

  PlanNodePtr createInsertPlanForBucketTable(
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
      std::optional<ColumnStatsSpec> columnStatsSpec);

  // Return the corresponding column names in 'inputRowType' of
  // 'tableColumnNames' from 'tableRowType'.
  static std::vector<std::string> inputColumnNames(
      const std::vector<std::string>& tableColumnNames,
      const RowTypePtr& tableRowType,
      const RowTypePtr& inputRowType);

  PlanNodePtr createInsertPlanWithForNonBucketedTable(
      PlanBuilder& inputPlan,
      const RowTypePtr& inputRowType,
      const RowTypePtr& tableRowType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy,
      const std::optional<CompressionKind> compressionKind,
      const connector::hive::LocationHandle::TableType& outputTableType,
      const CommitStrategy& outputCommitStrategy,
      bool aggregateResult,
      std::optional<ColumnStatsSpec> columnStatsSpec);

  // Parameter partitionName is string formatted in the Hive style
  // key1=value1/key2=value2/... Parameter partitionTypes are types of partition
  // keys in the same order as in partitionName.The return value is a SQL
  // predicate with values single quoted for string and date and not quoted for
  // other supported types, ex., key1='value1' AND key2=value2 AND ...
  std::string partitionNameToPredicate(
      const std::string& partitionName,
      const std::vector<TypePtr>& partitionTypes);

  std::string partitionNameToPredicate(
      const std::vector<std::string>& partitionDirNames);

  // Verifies if a unbucketed file name is encoded properly based on the
  // used commit strategy.
  void verifyUnbucketedFilePath(
      const std::filesystem::path& filePath,
      const std::string& targetDir);

  // Verifies if a partitioned file path (directory and file name) is encoded
  // properly.
  void verifyPartitionedFilePath(
      const std::filesystem::path& filePath,
      const std::string& targetDir);

  // Verifies if the bucket file name is encoded properly.
  void verifyBucketedFileName(const std::filesystem::path& filePath);

  // Verifies if a bucketed file path (directory and file name) is encoded
  // properly.
  void verifyBucketedFilePath(
      const std::filesystem::path& filePath,
      const std::string& targetDir);

  // Verifies if the given partitioned table directory (names) are encoded
  // properly based on the used partitioned keys.
  void verifyPartitionedDirPath(
      const std::filesystem::path& dirPath,
      const std::string& targetDir);

  // Parses and returns the bucket id encoded in the bucketed file name.
  uint32_t parseBucketId(const std::string& bucketFileName);

  // Returns the list of partition directory names in the given directory path.
  std::vector<std::string> getPartitionDirNames(
      const std::filesystem::path& dirPath);

  // Verifies the partitioned file data on disk by comparing with the same set
  // of data read from duckbd.
  void verifyPartitionedFilesData(
      const std::vector<std::filesystem::path>& filePaths,
      const std::filesystem::path& dirPath);

  // Gets the hash function used by the production code to build bucket id.
  std::unique_ptr<core::PartitionFunction> getBucketFunction(
      const RowTypePtr& outputType);

  // Verifies the bucketed file data by checking if the bucket id of each read
  // row is the same as the one encoded in the corresponding bucketed file name.
  void verifyBucketedFileData(
      const std::filesystem::path& filePath,
      const RowTypePtr& outputFileType);

  // Verifies the file layout and data produced by a table writer.
  void verifyTableWriterOutput(
      const std::string& targetDir,
      const RowTypePtr& bucketCheckFileType,
      bool verifyPartitionedData = true,
      bool verifyBucketedData = true);

  int getNumWriters();

 protected:
  static inline int kNumTableWriterCount = 4;
  static inline int kNumPartitionedTableWriterCount = 2;

  const TestParam testParam_;
  const FileFormat fileFormat_;
  const TestMode testMode_;
  const int numTableWriterCount_;
  const int numPartitionedTableWriterCount_;
  const std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator_{
      std::make_shared<core::PlanNodeIdGenerator>()};

  RowTypePtr rowType_;
  RowTypePtr tableSchema_;
  CommitStrategy commitStrategy_;
  std::optional<CompressionKind> compressionKind_;
  bool scaleWriter_;
  std::vector<std::string> partitionedBy_;
  std::vector<TypePtr> partitionTypes_;
  std::vector<column_index_t> partitionChannels_;
  std::vector<uint32_t> numPartitionKeyValues_;
  std::vector<column_index_t> sortColumnIndices_;
  std::vector<CompareFlags> sortedFlags_;
  std::shared_ptr<HiveBucketProperty> bucketProperty_{nullptr};
  core::PlanNodeId tableWriteNodeId_;
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    TableWriterTestBase::TestMode mode) {
  os << TableWriterTestBase::testModeString(mode);
  return os;
}
} // namespace velox::exec::test
