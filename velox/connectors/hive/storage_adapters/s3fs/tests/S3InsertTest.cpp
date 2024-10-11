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
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox {
namespace {

class S3InsertTest : public S3Test {
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
            ->newConnector(
                ::exec::test::kHiveConnectorId,
                minioServer_->hiveConfig(),
                ioExecutor_.get());
    connector::registerConnector(hiveConnector);
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
  }

  void TearDown() override {
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
    connector::unregisterConnectorFactory(
        connector::hive::HiveConnectorFactory::kHiveConnectorName);
    connector::unregisterConnector(::exec::test::kHiveConnectorId);
    S3Test::TearDown();
    filesystems::finalizeS3FileSystem();
  }
};
} // namespace

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
  auto rowCount = results->childAt(exec::TableWriteTraits::kRowCountChannel)
                      ->as<FlatVector<int64_t>>();
  ASSERT_FALSE(rowCount->isNullAt(0));
  ASSERT_EQ(kExpectedRows, rowCount->valueAt(0));
  ASSERT_TRUE(rowCount->isNullAt(1));

  // Second column contains details about written files.
  auto details = results->childAt(exec::TableWriteTraits::kFragmentChannel)
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

  auto filePath = fmt::format("{}{}", kOutputDirectory, writeFileName);
  const int64_t fileSize = fileWriteInfos[0]["fileSize"].asInt();
  auto split = HiveConnectorSplitBuilder(filePath)
                   .fileFormat(dwio::common::FileFormat::PARQUET)
                   .length(fileSize)
                   .build();
  auto copy = AssertQueryBuilder(plan).split(split).copyResults(pool());
  assertEqualResults({input}, {copy});
}
} // namespace facebook::velox

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
