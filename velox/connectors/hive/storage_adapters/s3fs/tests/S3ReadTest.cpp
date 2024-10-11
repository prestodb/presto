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
#include "velox/connectors/hive/storage_adapters/s3fs/tests/S3Test.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox {
namespace {

class S3ReadTest : public S3Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    S3Test::SetUp();
    filesystems::registerS3FileSystem();
    connector::registerConnectorFactory(
        std::make_shared<connector::hive::HiveConnectorFactory>());
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, minioServer_->hiveConfig());
    connector::registerConnector(hiveConnector);
    parquet::registerParquetReaderFactory();
  }

  void TearDown() override {
    parquet::unregisterParquetReaderFactory();
    filesystems::finalizeS3FileSystem();
    connector::unregisterConnectorFactory(
        connector::hive::HiveConnectorFactory::kHiveConnectorName);
    connector::unregisterConnector(kHiveConnectorId);
    S3Test::TearDown();
  }
};
} // namespace

TEST_F(S3ReadTest, s3ReadTest) {
  const auto sourceFile = test::getDataFilePath(
      "velox/connectors/hive/storage_adapters/s3fs/tests",
      "../../../../../dwio/parquet/tests/examples/int.parquet");
  const char* bucketName = "data";
  const auto destinationFile = S3Test::localPath(bucketName) + "/int.parquet";
  minioServer_->addBucket(bucketName);
  std::ifstream src(sourceFile, std::ios::binary);
  std::ofstream dest(destinationFile, std::ios::binary);
  // Copy source file to destination bucket.
  dest << src.rdbuf();
  ASSERT_GT(dest.tellp(), 0) << "Unable to copy from source " << sourceFile;
  dest.close();

  // Read the parquet file via the S3 bucket.
  const auto readDirectory{s3URI(bucketName)};
  auto rowType = ROW({"int", "bigint"}, {INTEGER(), BIGINT()});
  auto plan = PlanBuilder().tableScan(rowType).planNode();
  auto split = HiveConnectorSplitBuilder(
                   fmt::format("{}/{}", readDirectory, "int.parquet"))
                   .fileFormat(dwio::common::FileFormat::PARQUET)
                   .build();
  auto copy = AssertQueryBuilder(plan).split(split).copyResults(pool());

  // expectedResults is the data in int.parquet file.
  const int64_t kExpectedRows = 10;
  auto expectedResults = makeRowVector(
      {makeFlatVector<int32_t>(
           kExpectedRows, [](auto row) { return row + 100; }),
       makeFlatVector<int64_t>(
           kExpectedRows, [](auto row) { return row + 1000; })});
  assertEqualResults({expectedResults}, {copy});
}
} // namespace facebook::velox

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
