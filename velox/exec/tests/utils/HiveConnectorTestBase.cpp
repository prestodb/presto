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

#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/common/base/tests/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::velox::exec::test {

HiveConnectorTestBase::HiveConnectorTestBase() {
  filesystems::registerLocalFileSystem();
}

void HiveConnectorTestBase::SetUp() {
  OperatorTestBase::SetUp();
  executor_ = std::make_unique<folly::IOThreadPoolExecutor>(3);
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, nullptr, executor_.get());
  connector::registerConnector(hiveConnector);
  dwrf::registerDwrfReaderFactory();
}

void HiveConnectorTestBase::TearDown() {
  if (executor_) {
    executor_->join();
  }
  dwrf::unregisterDwrfReaderFactory();
  connector::unregisterConnector(kHiveConnectorId);
  OperatorTestBase::TearDown();
}

void HiveConnectorTestBase::writeToFile(
    const std::string& filePath,
    RowVectorPtr vector) {
  writeToFile(filePath, std::vector{vector});
}

void HiveConnectorTestBase::writeToFile(
    const std::string& filePath,
    const std::vector<RowVectorPtr>& vectors,
    std::shared_ptr<dwrf::Config> config) {
  static const auto kWriter = "HiveConnectorTestBase.Writer";

  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = vectors[0]->type();
  auto sink =
      std::make_unique<facebook::velox::dwio::common::FileSink>(filePath);
  facebook::velox::dwrf::Writer writer{
      options,
      std::move(sink),
      pool_->addChild(kWriter, std::numeric_limits<int64_t>::max())};

  for (size_t i = 0; i < vectors.size(); ++i) {
    writer.write(vectors[i]);
  }
  writer.close();
}

std::vector<RowVectorPtr> HiveConnectorTestBase::makeVectors(
    const RowTypePtr& rowType,
    int32_t numVectors,
    int32_t rowsPerVector) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < numVectors; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        velox::test::BatchMaker::createBatch(rowType, rowsPerVector, *pool_));
    vectors.push_back(vector);
  }
  return vectors;
}

std::shared_ptr<exec::Task> HiveConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(
      plan, makeHiveConnectorSplits(filePaths), duckDbSql);
}

std::vector<std::shared_ptr<TempFilePath>> HiveConnectorTestBase::makeFilePaths(
    int count) {
  std::vector<std::shared_ptr<TempFilePath>> filePaths;

  filePaths.reserve(count);
  for (auto i = 0; i < count; ++i) {
    filePaths.emplace_back(TempFilePath::create());
  }
  return filePaths;
}

std::vector<std::shared_ptr<connector::hive::HiveConnectorSplit>>
HiveConnectorTestBase::makeHiveConnectorSplits(
    const std::string& filePath,
    uint32_t splitCount,
    dwio::common::FileFormat format) {
  const int fileSize = fs::file_size(filePath);
  // Take the upper bound.
  const int splitSize = std::ceil((fileSize) / splitCount);
  std::vector<std::shared_ptr<connector::hive::HiveConnectorSplit>> splits;

  // Add all the splits.
  for (int i = 0; i < splitCount; i++) {
    auto split = HiveConnectorSplitBuilder(filePath)
                     .fileFormat(format)
                     .start(i * splitSize)
                     .length(splitSize)
                     .build();
    splits.push_back(std::move(split));
  }
  return splits;
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
HiveConnectorTestBase::makeHiveConnectorSplits(
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths) {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  for (auto filePath : filePaths) {
    splits.push_back(makeHiveConnectorSplit(filePath->path));
  }
  return splits;
}

std::shared_ptr<connector::ConnectorSplit>
HiveConnectorTestBase::makeHiveConnectorSplit(
    const std::string& filePath,
    uint64_t start,
    uint64_t length) {
  return HiveConnectorSplitBuilder(filePath)
      .start(start)
      .length(length)
      .build();
}

std::shared_ptr<connector::hive::HiveColumnHandle>
HiveConnectorTestBase::regularColumn(
    const std::string& name,
    const TypePtr& type) {
  return std::make_shared<connector::hive::HiveColumnHandle>(
      name, connector::hive::HiveColumnHandle::ColumnType::kRegular, type);
}

std::shared_ptr<connector::hive::HiveColumnHandle>
HiveConnectorTestBase::synthesizedColumn(
    const std::string& name,
    const TypePtr& type) {
  return std::make_shared<connector::hive::HiveColumnHandle>(
      name, connector::hive::HiveColumnHandle::ColumnType::kSynthesized, type);
}

std::shared_ptr<connector::hive::HiveColumnHandle>
HiveConnectorTestBase::partitionKey(
    const std::string& name,
    const TypePtr& type) {
  return std::make_shared<connector::hive::HiveColumnHandle>(
      name, connector::hive::HiveColumnHandle::ColumnType::kPartitionKey, type);
}

} // namespace facebook::velox::exec::test
