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

#include <folly/init/Init.h>

#include "gtest/gtest.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/tests/MinioServer.h"
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
using namespace facebook::velox::filesystems;

class S3InsertTest : public testing::Test, public VectorTestBase {
 public:
  static constexpr char const* kMinioConnectionString{"127.0.0.1:7000"};
  /// We use static initialization because we want a single version of the
  /// Minio server running.
  /// Each test must use a unique bucket to avoid concurrency issues.
  static void SetUpTestSuite() {
    minioServer_ = std::make_shared<MinioServer>(kMinioConnectionString);
    minioServer_->start();

    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(3);
    filesystems::registerS3FileSystem();
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                minioServer_->hiveConfig(),
                ioExecutor_.get());
    connector::registerConnector(hiveConnector);
  }

  static void TearDownTestSuite() {
    filesystems::finalizeS3FileSystem();
    unregisterConnector(kHiveConnectorId);
    minioServer_->stop();
    minioServer_ = nullptr;
  }

  static std::shared_ptr<MinioServer> minioServer_;
  static std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
};
std::shared_ptr<MinioServer> S3InsertTest::minioServer_ = nullptr;
std::unique_ptr<folly::IOThreadPoolExecutor> S3InsertTest::ioExecutor_ =
    nullptr;

PlanNodePtr createInsertPlan(
    PlanBuilder& inputPlan,
    const RowTypePtr& outputRowType,
    const std::string_view& outputDirectoryPath,
    const std::vector<std::string>& partitionedBy = {},
    const std::shared_ptr<HiveBucketProperty>& bucketProperty = {},
    const connector::hive::LocationHandle::TableType& outputTableType =
        connector::hive::LocationHandle::TableType::kNew,
    const CommitStrategy& outputCommitStrategy = CommitStrategy::kNoCommit) {
  auto insertTableHandle = std::make_shared<core::InsertTableHandle>(
      kHiveConnectorId,
      HiveConnectorTestBase::makeHiveInsertTableHandle(
          outputRowType->names(),
          outputRowType->children(),
          partitionedBy,
          bucketProperty,
          HiveConnectorTestBase::makeLocationHandle(
              outputDirectoryPath.data(), std::nullopt, outputTableType),
          FileFormat::PARQUET));

  auto insertPlan = inputPlan.tableWrite(
      inputPlan.planNode()->outputType(),
      outputRowType->names(),
      nullptr,
      insertTableHandle,
      bucketProperty != nullptr,
      outputCommitStrategy);
  return insertPlan.planNode();
}

TEST_F(S3InsertTest, s3InsertTest) {
  const int64_t kExpectedRows = 1'000;
  const std::string_view kOutputDirectory{"s3://writedata/"};

  auto rowType = ROW(
      {"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()});

  auto input = makeRowVector(
      {makeFlatVector<int64_t>(kExpectedRows, [](auto row) { return row; }),
       makeFlatVector<int32_t>(kExpectedRows, [](auto row) { return row; }),
       makeFlatVector<int16_t>(kExpectedRows, [](auto row) { return row; }),
       makeFlatVector<double>(kExpectedRows, [](auto row) { return row; })});

  minioServer_->addBucket("writedata");

  // Insert into s3 with one writer.
  auto plan = createInsertPlan(
      PlanBuilder().values({input}), rowType, kOutputDirectory);

  // Execute the write plan.
  auto result = AssertQueryBuilder(plan).copyResults(pool());

  // Get the fragment from the TableWriter output.
  auto fragmentVector = result->childAt(TableWriteTraits::kFragmentChannel)
                            ->asFlatVector<StringView>();

  ASSERT(fragmentVector);

  // The fragment contains data provided by the DataSink#finish.
  // This includes the target filename, rowCount, etc...
  // Extract the filename, row counts, filesize.
  std::vector<std::string> writeFiles;
  int64_t numRows{0};
  int64_t writeFileSize{0};
  for (int i = 0; i < result->size(); ++i) {
    if (!fragmentVector->isNullAt(i)) {
      folly::dynamic obj = folly::parseJson(fragmentVector->valueAt(i));
      ASSERT_EQ(obj["targetPath"], kOutputDirectory);
      ASSERT_EQ(obj["writePath"], kOutputDirectory);
      numRows += obj["rowCount"].asInt();

      folly::dynamic writerInfoObj = obj["fileWriteInfos"][0];
      const std::string writeFileName =
          writerInfoObj["writeFileName"].asString();
      const std::string writeFileFullPath =
          obj["writePath"].asString() + "/" + writeFileName;
      writeFiles.push_back(writeFileFullPath);
      writeFileSize += writerInfoObj["fileSize"].asInt();
    }
  }

  ASSERT_EQ(numRows, kExpectedRows);
  ASSERT_EQ(writeFiles.size(), 1);

  // Verify that the data is written to S3 correctly by scanning the file.
  auto tableScan = PlanBuilder(pool_.get()).tableScan(rowType).planNode();
  CursorParameters params;
  params.planNode = tableScan;
  const int numSplitsPerFile = 1;
  bool noMoreSplits = false;
  auto addSplits = [&](exec::Task* task) {
    if (!noMoreSplits) {
      auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
          writeFiles[0], numSplitsPerFile, dwio::common::FileFormat::PARQUET);
      for (const auto& split : splits) {
        task->addSplit("0", exec::Split(split));
      }
      task->noMoreSplits("0");
    }
    noMoreSplits = true;
  };
  auto scanResult = readCursor(params, addSplits);
  assertEqualResults(scanResult.second, {input});
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
