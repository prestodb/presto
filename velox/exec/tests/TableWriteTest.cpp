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
#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HivePartitionUtil.h"
#include "velox/dwio/common/DataSink.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <regex>

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;

class TableWriteTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
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
        splits.push_back(makeHiveConnectorSplit(path.path().string()));
      }
    }

    return splits;
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

    for (auto i = 0; i < partitionKeyValues.size(); i++) {
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

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
};

// Runs a pipeline with read + filter + project (with substr) + write.
TEST_F(TableWriteTest, scanFilterProjectWrite) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType_, filePaths.size(), 1000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto planBuilder = PlanBuilder();
  auto project = planBuilder.tableScan(rowType_)
                     .filter("c0 <> 0")
                     .project({"c0", "c1", "c1 + c2", "substr(c5, 1, 1)"})
                     .planNode();

  auto types = project->outputType()->children();
  std::vector<std::string> tableColumnNames = {
      "c0", "c1", "c1_plus_c2", "substr_c5"};
  auto plan = planBuilder
                  .tableWrite(
                      tableColumnNames,
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              tableColumnNames,
                              types,
                              {},
                              makeLocationHandle(outputDirectory->path))),
                      CommitStrategy::kNoCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp WHERE c0 <> 0");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query.

  assertQuery(
      PlanBuilder()
          .tableScan(ROW(std::move(tableColumnNames), std::move(types)))
          .planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT c0, c1, c1 + c2, substr(c5, 1, 1) FROM tmp WHERE c0 <> 0");
}

TEST_F(TableWriteTest, renameAndReorderColumns) {
  auto rowType =
      ROW({"a", "b", "c", "d"}, {BIGINT(), INTEGER(), DOUBLE(), VARCHAR()});
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType, filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto inputRowType = ROW({"d", "c", "b"}, {VARCHAR(), DOUBLE(), INTEGER()});
  std::vector<std::string> tableColumnNames = {"x", "y", "z"};
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .tableWrite(
                      inputRowType,
                      tableColumnNames,
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              tableColumnNames,
                              inputRowType->children(),
                              {},
                              makeLocationHandle(outputDirectory->path))),
                      CommitStrategy::kNoCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  assertQuery(
      PlanBuilder()
          .tableScan(ROW(
              std::move(tableColumnNames), {{VARCHAR(), DOUBLE(), INTEGER()}}))
          .planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT d, c, b FROM tmp");
}

// Runs a pipeline with read + write.
TEST_F(TableWriteTest, directReadWrite) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType_, filePaths.size(), 1000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .tableScan(rowType_)
                  .tableWrite(
                      rowType_->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType_->names(),
                              rowType_->children(),
                              {},
                              makeLocationHandle(outputDirectory->path))),
                      CommitStrategy::kNoCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query.

  assertQuery(
      PlanBuilder().tableScan(rowType_).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");
}

// Tests writing constant vectors.
TEST_F(TableWriteTest, constantVectors) {
  vector_size_t size = 1'000;

  // Make constant vectors of various types with null and non-null values.
  std::string somewhatLongString = "Somewhat long string";
  auto vector = makeRowVector({
      makeConstant((int64_t)123'456, size),
      makeConstant(variant(TypeKind::BIGINT), size),
      makeConstant((int32_t)12'345, size),
      makeConstant(variant(TypeKind::INTEGER), size),
      makeConstant((int16_t)1'234, size),
      makeConstant(variant(TypeKind::SMALLINT), size),
      makeConstant((int8_t)123, size),
      makeConstant(variant(TypeKind::TINYINT), size),
      makeConstant(true, size),
      makeConstant(false, size),
      makeConstant(variant(TypeKind::BOOLEAN), size),
      makeConstant(somewhatLongString.c_str(), size),
      makeConstant(variant(TypeKind::VARCHAR), size),
  });
  auto rowType = std::dynamic_pointer_cast<const RowType>(vector->type());

  createDuckDbTable({vector});

  auto outputDirectory = TempDirectoryPath::create();
  auto op = PlanBuilder()
                .values({vector})
                .tableWrite(
                    rowType->names(),
                    std::make_shared<core::InsertTableHandle>(
                        kHiveConnectorId,
                        makeHiveInsertTableHandle(
                            rowType->names(),
                            rowType->children(),
                            {},
                            makeLocationHandle(outputDirectory->path))),
                    CommitStrategy::kNoCommit,
                    "rows")
                .project({"rows"})
                .planNode();

  assertQuery(op, fmt::format("SELECT {}", size));

  assertQuery(
      PlanBuilder().tableScan(rowType).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");
}

TEST_F(TableWriteTest, commitStrategies) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType_, filePaths.size(), 1000);

  createDuckDbTable(vectors);

  // Test the kTaskCommit commit strategy writing to one dot-prefixed temporary
  // file.
  {
    auto outputDirectory = TempDirectoryPath::create();
    auto plan = PlanBuilder()
                    .values(vectors)
                    .tableWrite(
                        rowType_->names(),
                        std::make_shared<core::InsertTableHandle>(
                            kHiveConnectorId,
                            makeHiveInsertTableHandle(
                                rowType_->names(),
                                rowType_->children(),
                                {},
                                makeLocationHandle(outputDirectory->path))),
                        CommitStrategy::kTaskCommit,
                        "rows")
                    .project({"rows"})
                    .planNode();

    assertQuery(plan, "SELECT count(*) FROM tmp");

    auto outputFiles = getRecursiveFiles(outputDirectory->path);
    EXPECT_EQ(outputFiles.size(), 1);
    EXPECT_EQ(fs::path(outputFiles[0]).filename().string()[0], '.');
    assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT * FROM tmp");
  }
  // Test kNoCommit commit strategy writing to non-temporary files.
  {
    auto outputDirectory = TempDirectoryPath::create();
    auto plan = PlanBuilder()
                    .values(vectors)
                    .tableWrite(
                        rowType_->names(),
                        std::make_shared<core::InsertTableHandle>(
                            kHiveConnectorId,
                            makeHiveInsertTableHandle(
                                rowType_->names(),
                                rowType_->children(),
                                {},
                                makeLocationHandle(outputDirectory->path))),
                        CommitStrategy::kNoCommit,
                        "rows")
                    .project({"rows"})
                    .planNode();

    assertQuery(plan, "SELECT count(*) FROM tmp");

    auto outputFiles = getRecursiveFiles(outputDirectory->path);
    EXPECT_EQ(outputFiles.size(), 1);
    EXPECT_NE(fs::path(outputFiles[0]).filename().string()[0], '.');
    assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        makeHiveConnectorSplits(outputDirectory),
        "SELECT * FROM tmp");
  }
}

TEST_F(TableWriteTest, multiplePartitions) {
  int32_t numPartitions = 50;
  int32_t numBatches = 2;

  auto rowType = ROW(
      {"c0", "p0", "p1", "c1"}, {INTEGER(), INTEGER(), VARCHAR(), BIGINT()});
  std::vector<column_index_t> partitionChannels = {1, 2};
  std::vector<std::string> partitionKeys = {"p0", "p1"};
  std::vector<TypePtr> partitionTypes = {INTEGER(), VARCHAR()};

  std::vector<RowVectorPtr> vectors = makeBatches(numBatches, [&](auto) {
    return makeRowVector(
        rowType->names(),
        {makeFlatVector<int32_t>(
             numPartitions, [&](auto row) { return row + 100; }),
         makeFlatVector<int32_t>(numPartitions, [&](auto row) { return row; }),
         makeFlatVector<StringView>(
             numPartitions,
             [&](auto row) { return StringView(fmt::format("str_{}", row)); }),
         makeFlatVector<int64_t>(
             numPartitions, [&](auto row) { return row + 1000; })});
  });
  createDuckDbTable(vectors);

  auto inputFilePaths = makeFilePaths(numBatches);
  for (int i = 0; i < numBatches; i++) {
    writeToFile(inputFilePaths[i]->path, vectors[i]);
  }

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .tableWrite(
                      rowType->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType->names(),
                              rowType->children(),
                              partitionKeys,
                              makeLocationHandle(outputDirectory->path))),
                      CommitStrategy::kNoCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

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
    // One single file is written to each partition directory for Hive
    // connector.
    EXPECT_EQ(countRecursiveFiles(*iterPartitionDirectory), 1);

    ++iterPartitionDirectory;
    ++iterPartitionName;
  }
}

TEST_F(TableWriteTest, singlePartition) {
  int32_t numBatches = 2;

  auto rowType = ROW({"c0", "p0"}, {VARCHAR(), BIGINT()});
  std::vector<std::string> partitionKeys = {"p0"};

  // Partition vector is constant vector.
  std::vector<RowVectorPtr> vectors = makeBatches(numBatches, [&](auto) {
    return makeRowVector(
        rowType->names(),
        {makeFlatVector<StringView>(
             1'000,
             [&](auto row) { return StringView(fmt::format("str_{}", row)); }),
         makeConstant((int64_t)365, 1'000)});
  });
  createDuckDbTable(vectors);

  auto inputFilePaths = makeFilePaths(numBatches);
  for (int i = 0; i < numBatches; i++) {
    writeToFile(inputFilePaths[i]->path, vectors[i]);
  }

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .tableWrite(
                      rowType->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType->names(),
                              rowType->children(),
                              partitionKeys,
                              makeLocationHandle(outputDirectory->path))),
                      CommitStrategy::kNoCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

  auto task = assertQuery(plan, inputFilePaths, "SELECT count(*) FROM tmp");

  std::set<std::string> partitionDirectories =
      getLeafSubdirectories(outputDirectory->path);

  // Verify only a single partition directory is created.
  EXPECT_EQ(partitionDirectories.size(), 1);
  EXPECT_EQ(
      *partitionDirectories.begin(),
      fs::path(outputDirectory->path) / "p0=365");

  // Verify all data is written to the single partition directory.
  assertQuery(
      PlanBuilder().tableScan(rowType).planNode(),
      makeHiveConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");

  // Verify that one single file is written to the single partition directory
  // for Hive connector.
  EXPECT_EQ(countRecursiveFiles(*partitionDirectories.begin()), 1);
}

TEST_F(TableWriteTest, maxPartitions) {
  int32_t maxPartitions = 1'000;
  int32_t numPartitions = maxPartitions + 1;

  auto rowType = ROW({"p0"}, {BIGINT()});
  std::vector<std::string> partitionKeys = {"p0"};

  auto vector = makeRowVector(
      rowType->names(),
      {makeFlatVector<int64_t>(numPartitions, [&](auto row) { return row; })});

  auto outputDirectory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .values({vector})
                  .tableWrite(
                      rowType->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType->names(),
                              rowType->children(),
                              partitionKeys,
                              makeLocationHandle(outputDirectory->path))),
                      CommitStrategy::kNoCommit,
                      "rows")
                  .project({"rows"})
                  .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .connectorConfig(
              kHiveConnectorId,
              HiveConfig::kMaxPartitionsPerWriters,
              folly::to<std::string>(maxPartitions))
          .copyResults(pool()),
      fmt::format("Exceeded limit of {} distinct partitions.", maxPartitions));
}

// Test TableWriter does not create a file if input is empty.
TEST_F(TableWriteTest, writeNoFile) {
  auto outputDirectory = TempDirectoryPath::create();
  auto plan = PlanBuilder()
                  .tableScan(rowType_)
                  .filter("false")
                  .tableWrite(
                      rowType_->names(),
                      std::make_shared<core::InsertTableHandle>(
                          kHiveConnectorId,
                          makeHiveInsertTableHandle(
                              rowType_->names(),
                              rowType_->children(),
                              {},
                              makeLocationHandle(outputDirectory->path))),
                      CommitStrategy::kNoCommit,
                      "rows")
                  .planNode();

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
