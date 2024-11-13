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
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/connectors/hive/storage_adapters/s3fs/tests/S3Test.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

static const std::string_view kConnectorId1 = "test-hive1";
static const std::string_view kConnectorId2 = "test-hive2";
static const std::string_view kBucketName = "writedata";

using namespace facebook::velox::exec::test;

namespace facebook::velox {
namespace {

class S3MultipleEndpoints : public S3Test, public ::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
  static void TearDownTestCase() {
    filesystems::finalizeS3FileSystem();
  }

  void SetUp() override {
    S3Test::SetUp();
    minioSecondServer_ = std::make_unique<MinioServer>();
    minioSecondServer_->start();
    minioServer_->addBucket(kBucketName.data());
    minioSecondServer_->addBucket(kBucketName.data());

    filesystems::registerS3FileSystem();
    connector::registerConnectorFactory(
        std::make_shared<connector::hive::HiveConnectorFactory>());
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
  }

  void registerConnectors(
      std::string_view connectorId1,
      std::string_view connectorId2,
      const std::unordered_map<std::string, std::string> config1Override = {},
      const std::unordered_map<std::string, std::string> config2Override = {}) {
    auto hiveConnector1 =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                std::string(connectorId1),
                minioServer_->hiveConfig(config1Override),
                ioExecutor_.get());
    auto hiveConnector2 =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                std::string(connectorId2),
                minioSecondServer_->hiveConfig(config2Override),
                ioExecutor_.get());
    connector::registerConnector(hiveConnector1);
    connector::registerConnector(hiveConnector2);
  }

  void TearDown() override {
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
    connector::unregisterConnectorFactory(
        connector::hive::HiveConnectorFactory::kHiveConnectorName);
    S3Test::TearDown();
  }

  folly::dynamic writeData(
      const RowVectorPtr input,
      const std::string& outputDirectory,
      const std::string& connectorId) {
    auto plan = PlanBuilder()
                    .values({input})
                    .tableWrite(
                        outputDirectory.data(),
                        {},
                        0,
                        {},
                        {},
                        dwio::common::FileFormat::PARQUET,
                        {},
                        connectorId)
                    .planNode();
    // Execute the write plan.
    auto results = AssertQueryBuilder(plan).copyResults(pool());
    // Second column contains details about written files.
    auto details = results->childAt(exec::TableWriteTraits::kFragmentChannel)
                       ->as<FlatVector<StringView>>();
    folly::dynamic obj = folly::parseJson(details->valueAt(1));
    return obj["fileWriteInfos"];
  }

  std::shared_ptr<connector::hive::HiveConnectorSplit> createSplit(
      folly::dynamic tableWriteInfo,
      std::string outputDirectory,
      std::string connectorId) {
    auto writeFileName = tableWriteInfo[0]["writeFileName"].asString();
    auto filePath = fmt::format("{}{}", outputDirectory, writeFileName);
    const int64_t fileSize = tableWriteInfo[0]["fileSize"].asInt();

    return HiveConnectorSplitBuilder(filePath)
        .connectorId(connectorId)
        .fileFormat(dwio::common::FileFormat::PARQUET)
        .length(fileSize)
        .build();
  }

  void testJoin(
      int numRows,
      std::string_view outputDirectory,
      std::string_view connectorId1,
      std::string_view connectorId2) {
    auto rowType1 = ROW(
        {"a0", "a1", "a2", "a3"}, {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()});
    auto rowType2 = ROW(
        {"b0", "b1", "b2", "b3"}, {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()});

    auto input1 = makeRowVector(
        rowType1->names(),
        {makeFlatVector<int64_t>(numRows, [](auto row) { return row; }),
         makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
         makeFlatVector<int16_t>(numRows, [](auto row) { return row; }),
         makeFlatVector<double>(numRows, [](auto row) { return row; })});
    auto input2 = makeRowVector(rowType2->names(), input1->children());

    // Insert input data into both tables.
    auto table1WriteInfo =
        writeData(input1, outputDirectory.data(), std::string(connectorId1));
    auto table2WriteInfo =
        writeData(input2, outputDirectory.data(), std::string(connectorId2));

    // Inner Join both the tables.
    core::PlanNodeId scan1, scan2;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto table1Scan = PlanBuilder(planNodeIdGenerator, pool())
                          .startTableScan()
                          .tableName("hive_table1")
                          .outputType(rowType1)
                          .connectorId(std::string(connectorId1))
                          .endTableScan()
                          .capturePlanNodeId(scan1)
                          .planNode();
    auto join =
        PlanBuilder(planNodeIdGenerator, pool())
            .startTableScan()
            .tableName("hive_table1")
            .outputType(rowType2)
            .connectorId(std::string(connectorId2))
            .endTableScan()
            .capturePlanNodeId(scan2)
            .hashJoin({"b0"}, {"a0"}, table1Scan, "", {"a0", "a1", "a2", "a3"})
            .planNode();

    auto split1 = createSplit(
        table1WriteInfo, outputDirectory.data(), std::string(connectorId1));
    auto split2 = createSplit(
        table2WriteInfo, outputDirectory.data(), std::string(connectorId2));
    auto results = AssertQueryBuilder(join)
                       .split(scan1, split1)
                       .split(scan2, split2)
                       .copyResults(pool());
    assertEqualResults({input1}, {results});
  }

  std::unique_ptr<MinioServer> minioSecondServer_;
};
} // namespace

TEST_F(S3MultipleEndpoints, baseEndpoints) {
  const int64_t kExpectedRows = 1'000;
  const auto outputDirectory{filesystems::s3URI(kBucketName, "")};

  registerConnectors(kConnectorId1, kConnectorId2);

  testJoin(kExpectedRows, outputDirectory, kConnectorId1, kConnectorId2);

  connector::unregisterConnector(std::string(kConnectorId1));
  connector::unregisterConnector(std::string(kConnectorId2));
}

TEST_F(S3MultipleEndpoints, bucketEndpoints) {
  const int64_t kExpectedRows = 1'000;
  const auto outputDirectory{filesystems::s3URI(kBucketName, "")};

  auto configOverride = [](std::shared_ptr<const config::ConfigBase> config) {
    return std::unordered_map<std::string, std::string>{
        {"hive.s3.bucket.writedata.endpoint",
         config->get<std::string>("hive.s3.endpoint").value()},
        {"hive.s3.bucket.writedata.aws-access-key",
         config->get<std::string>("hive.s3.aws-access-key").value()},
        {"hive.s3.bucket.writedata.aws-secret-key",
         config->get<std::string>("hive.s3.aws-secret-key").value()},
        {"hive.s3.endpoint", "fail"},
        {"hive.s3.aws-access-key", "fail"},
        {"hive.s3.aws-secret-key", "fail"},
    };
  };
  auto config1 = configOverride(minioServer_->hiveConfig());
  auto config2 = configOverride(minioSecondServer_->hiveConfig());
  registerConnectors(kConnectorId1, kConnectorId2, config1, config2);

  testJoin(kExpectedRows, outputDirectory, kConnectorId1, kConnectorId2);

  connector::unregisterConnector(std::string(kConnectorId1));
  connector::unregisterConnector(std::string(kConnectorId2));
}

} // namespace facebook::velox

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
