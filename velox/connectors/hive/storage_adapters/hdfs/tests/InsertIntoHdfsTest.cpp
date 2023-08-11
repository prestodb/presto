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

#include <folly/Singleton.h>
#include "gtest/gtest.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/tests/HdfsMiniCluster.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::test;

class InsertIntoHdfsTest : public HiveConnectorTestBase {
 public:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    if (miniCluster == nullptr) {
      miniCluster = std::make_shared<filesystems::test::HdfsMiniCluster>();
      miniCluster->start();
    }
  }

  void TearDown() override {
    HiveConnectorTestBase::TearDown();
    miniCluster->stop();
  }

  void setDataTypes(const RowTypePtr& inputType) {
    rowType_ = inputType;
  }

  static std::shared_ptr<common::ScanSpec> makeScanSpec(
      const RowTypePtr& rowType) {
    auto scanSpec = std::make_shared<common::ScanSpec>("");
    scanSpec->addAllChildFields(*rowType);
    return scanSpec;
  }

  std::unique_ptr<RowReader> createReader(
      const RowTypePtr& rowType,
      const std::shared_ptr<ReadFile>& readFile) {
    dwio::common::ReaderOptions readerOpts{pool_.get()};
    auto input =
        std::make_unique<BufferedInput>(readFile, readerOpts.getMemoryPool());

    std::unique_ptr<Reader> reader =
        std::make_unique<facebook::velox::parquet::ParquetReader>(
            std::move(input), readerOpts);

    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
            rowType, rowType->names()));

    auto scanSpec = makeScanSpec(rowType);
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    return rowReader;
  }

  template <typename T>
  VectorPtr rangeVector(size_t size, T start) {
    std::vector<T> vals(size);
    for (size_t i = 0; i < size; ++i) {
      vals[i] = start + static_cast<T>(i);
    }
    return vectorMaker_->flatVector(vals);
  }

  static std::shared_ptr<filesystems::test::HdfsMiniCluster> miniCluster;
  RowTypePtr rowType_;
  std::unique_ptr<VectorMaker> vectorMaker_{
      std::make_unique<VectorMaker>(pool_.get())};
};

std::shared_ptr<filesystems::test::HdfsMiniCluster>
    InsertIntoHdfsTest::miniCluster = nullptr;

std::shared_ptr<core::InsertTableHandle> createInsertTableHandle(
    const RowTypePtr& outputRowType,
    const connector::hive::LocationHandle::TableType& outputTableType,
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionedBy,
    const std::shared_ptr<HiveBucketProperty>& bucketProperty) {
  return std::make_shared<core::InsertTableHandle>(
      kHiveConnectorId,
      HiveConnectorTestBase::makeHiveInsertTableHandle(
          outputRowType->names(),
          outputRowType->children(),
          partitionedBy,
          bucketProperty,
          HiveConnectorTestBase::makeLocationHandle(
              outputDirectoryPath, std::nullopt, outputTableType),
          FileFormat::PARQUET));
}

PlanNodePtr createInsertPlan(
    PlanBuilder& inputPlan,
    const RowTypePtr& outputRowType,
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionedBy = {},
    const std::shared_ptr<HiveBucketProperty>& bucketProperty = {},
    const connector::hive::LocationHandle::TableType& outputTableType =
        connector::hive::LocationHandle::TableType::kNew,
    const CommitStrategy& outputCommitStrategy = CommitStrategy::kNoCommit) {
  auto insertPlan = inputPlan.tableWrite(
      inputPlan.planNode()->outputType(),
      outputRowType->names(),
      nullptr,
      createInsertTableHandle(
          outputRowType,
          outputTableType,
          outputDirectoryPath,
          partitionedBy,
          bucketProperty),
      bucketProperty != nullptr,
      outputCommitStrategy);
  return insertPlan.planNode();
}

void assertEqualVectorPart(
    const VectorPtr& expected,
    const VectorPtr& actual,
    vector_size_t offset) {
  ASSERT_GE(expected->size(), actual->size() + offset);
  ASSERT_EQ(expected->typeKind(), actual->typeKind());
  for (vector_size_t i = 0; i < actual->size(); i++) {
    ASSERT_TRUE(expected->equalValueAt(actual.get(), i + offset, i))
        << "at " << (i + offset) << ": expected "
        << expected->toString(i + offset) << ", but got "
        << actual->toString(i);
  }
}

void assertReadExpected(
    const std::shared_ptr<const RowType>& outputType,
    dwio::common::RowReader& reader,
    const RowVectorPtr& expected,
    memory::MemoryPool& memoryPool) {
  vector_size_t total = 0;
  VectorPtr result = BaseVector::create(outputType, 0, &memoryPool);

  while (total < expected->size()) {
    auto part = reader.next(1000, result);
    if (part > 0) {
      assertEqualVectorPart(expected, result, total);
      total += result->size();
    } else {
      break;
    }
  }
  EXPECT_EQ(total, expected->size());
  EXPECT_EQ(reader.next(1000, result), 0);
}

TEST_F(InsertIntoHdfsTest, insertIntoHdfsTest) {
  folly::SingletonVault::singleton()->registrationComplete();
  const int64_t expectedRows = 10 * 100;
  setDataTypes(ROW(
      {"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()}));

  auto input = vectorMaker_->rowVector(
      {rangeVector<int64_t>(expectedRows, 1),
       rangeVector<int32_t>(expectedRows, 1),
       rangeVector<int16_t>(expectedRows, 1),
       rangeVector<double>(expectedRows, 1)});

  auto outputDirectory = "hdfs://localhost:7878/";
  // INSERT INTO hdfs with one writer
  auto plan = createInsertPlan(
      PlanBuilder().values({input}), rowType_, outputDirectory);

  auto result =
      AssertQueryBuilder(plan, duckDbQueryRunner_).copyResults(pool());

  auto fragmentVector = result->childAt(TableWriteTraits::kFragmentChannel)
                            ->asFlatVector<StringView>();

  std::vector<std::string> writeFiles;
  int64_t numRows{0};
  int64_t fileSize{0};
  for (int i = 0; i < result->size(); ++i) {
    if (!fragmentVector->isNullAt(i)) {
      ASSERT_FALSE(fragmentVector->isNullAt(i));
      folly::dynamic obj = folly::parseJson(fragmentVector->valueAt(i));
      ASSERT_EQ(obj["targetPath"], outputDirectory);
      ASSERT_EQ(obj["writePath"], outputDirectory);
      numRows += obj["rowCount"].asInt();

      folly::dynamic writerInfoObj = obj["fileWriteInfos"][0];
      const std::string writeFileName =
          writerInfoObj["writeFileName"].asString();
      const std::string writeFileFullPath =
          obj["writePath"].asString() + "/" + writeFileName;
      writeFiles.push_back(writeFileFullPath);
      fileSize += writerInfoObj["fileSize"].asInt();
    }
  }

  ASSERT_EQ(numRows, expectedRows);
  ASSERT_EQ(writeFiles.size(), 1);

  // Verify that the data is written to hdfs
  auto fileSystem = filesystems::getFileSystem(writeFiles[0], nullptr);
  auto* hdfsFileSystem =
      dynamic_cast<filesystems::HdfsFileSystem*>(fileSystem.get());
  ASSERT_TRUE(hdfsFileSystem != nullptr);
  auto fileForRead = hdfsFileSystem->openFileForRead(writeFiles[0]);
  auto hdfsFileSize = fileForRead->size();
  ASSERT_EQ(fileSize, hdfsFileSize);

  auto rowReader = createReader(rowType_, std::move(fileForRead));
  assertReadExpected(rowType_, *rowReader, input, *pool_);
}
