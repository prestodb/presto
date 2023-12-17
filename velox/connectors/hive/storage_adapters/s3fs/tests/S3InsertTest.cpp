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
#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/tests/MinioServer.h"
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
    facebook::velox::memory::MemoryManager::initialize({});
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
  auto plan =
      PlanBuilder()
          .values({input})
          .tableWrite(
              kOutputDirectory.data(), dwio::common::FileFormat::PARQUET)
          .planNode();

  // Execute the write plan.
  auto results = AssertQueryBuilder(plan).copyResults(pool());

  // First column has number of rows written in the first row and nulls in other
  // rows.
  auto rowCount = results->childAt(TableWriteTraits::kRowCountChannel)
                      ->as<FlatVector<int64_t>>();
  ASSERT_FALSE(rowCount->isNullAt(0));
  ASSERT_EQ(kExpectedRows, rowCount->valueAt(0));
  ASSERT_TRUE(rowCount->isNullAt(1));

  // Second column contains details about written files.
  auto details = results->childAt(TableWriteTraits::kFragmentChannel)
                     ->as<FlatVector<StringView>>();
  ASSERT_TRUE(details->isNullAt(0));
  ASSERT_FALSE(details->isNullAt(1));
  folly::dynamic obj = folly::parseJson(details->valueAt(1));

  ASSERT_EQ(kExpectedRows, obj["rowCount"].asInt());
  auto fileWriteInfos = obj["fileWriteInfos"];
  ASSERT_EQ(1, fileWriteInfos.size());

  auto writeFileName = fileWriteInfos[0]["writeFileName"].asString();

  // Read from 'writeFileName' and verify the data matches the original.
  plan = PlanBuilder().tableScan(rowType).planNode();

  auto splits = HiveConnectorTestBase::makeHiveConnectorSplits(
      fmt::format("{}/{}", kOutputDirectory, writeFileName),
      1,
      dwio::common::FileFormat::PARQUET);
  auto copy = AssertQueryBuilder(plan).split(splits[0]).copyResults(pool());
  assertEqualResults({input}, {copy});
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
