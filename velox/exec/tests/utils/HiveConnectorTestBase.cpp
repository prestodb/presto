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
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

namespace facebook::velox::exec::test {

HiveConnectorTestBase::HiveConnectorTestBase() {
  filesystems::registerLocalFileSystem();
}

void HiveConnectorTestBase::SetUp() {
  OperatorTestBase::SetUp();
  if (useAsyncCache_) {
    executor_ = std::make_unique<folly::IOThreadPoolExecutor>(3);
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, nullptr, nullptr, executor_.get());
    connector::registerConnector(hiveConnector);
  } else {
    auto dataCache = std::make_unique<SimpleLRUDataCache>(1UL << 30);
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, nullptr, std::move(dataCache));
    connector::registerConnector(hiveConnector);
  }
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
    const std::string& name,
    RowVectorPtr vector) {
  writeToFile(filePath, name, std::vector{vector});
}

void HiveConnectorTestBase::writeToFile(
    const std::string& filePath,
    const std::string& name,
    const std::vector<RowVectorPtr>& vectors,
    std::shared_ptr<dwrf::Config> config) {
  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = vectors[0]->type();
  auto sink =
      std::make_unique<facebook::velox::dwio::common::FileSink>(filePath);
  facebook::velox::dwrf::Writer writer{
      options,
      std::move(sink),
      pool_->addChild(name, std::numeric_limits<int64_t>::max())};

  for (size_t i = 0; i < vectors.size(); ++i) {
    writer.write(vectors[i]);
  }
  writer.close();
}

std::vector<RowVectorPtr> HiveConnectorTestBase::makeVectors(
    const std::shared_ptr<const RowType>& rowType,
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
    const std::shared_ptr<const core::PlanNode>& plan,
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(
      plan, makeHiveSplits(filePaths), duckDbSql);
}

std::shared_ptr<exec::Task> HiveConnectorTestBase::assertQuery(
    const std::shared_ptr<const core::PlanNode>& plan,
    const std::unordered_map<
        core::PlanNodeId,
        std::vector<std::shared_ptr<TempFilePath>>>& filePaths,
    const std::string& duckDbSql) {
  bool noMoreSplits = false;
  return test::assertQuery(
      plan,
      [&](auto* task) {
        if (!noMoreSplits) {
          for (const auto& entry : filePaths) {
            auto planNodeId = entry.first;
            for (auto file : entry.second) {
              addSplit(task, planNodeId, makeHiveSplit(file->path));
            }
            task->noMoreSplits(planNodeId);
          }
          noMoreSplits = true;
        }
      },
      duckDbSql,
      duckDbQueryRunner_);
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
    auto split = std::make_shared<connector::hive::HiveConnectorSplit>(
        kHiveConnectorId, "file:" + filePath, format, i * splitSize, splitSize);
    splits.push_back(std::move(split));
  }
  return splits;
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
HiveConnectorTestBase::makeHiveSplits(
    const std::vector<std::shared_ptr<TempFilePath>>& filePaths) {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  for (auto filePath : filePaths) {
    splits.push_back(makeHiveConnectorSplit(filePath->path));
  }
  return splits;
}

std::shared_ptr<connector::hive::HiveConnectorSplit>
HiveConnectorTestBase::makeHiveConnectorSplit(
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKeys,
    uint64_t start,
    uint64_t length) {
  return std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId,
      "file:" + filePath,
      dwio::common::FileFormat::ORC,
      start,
      length,
      partitionKeys);
}

exec::Split HiveConnectorTestBase::makeHiveSplit(
    const std::string& filePath,
    uint64_t start,
    uint64_t length) {
  return exec::Split(std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId,
      "file:" + filePath,
      dwio::common::FileFormat::ORC,
      start,
      length));
}

exec::Split HiveConnectorTestBase::makeHiveSplitWithGroup(
    const std::string& filePath,
    int32_t groupId) {
  auto split = HiveConnectorTestBase::makeHiveSplit(filePath);
  split.groupId = groupId;
  return split;
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

void HiveConnectorTestBase::addConnectorSplit(
    Task* task,
    const core::PlanNodeId& planNodeId,
    const std::shared_ptr<connector::ConnectorSplit>& connectorSplit) {
  addSplit(task, planNodeId, exec::Split(folly::copy(connectorSplit), -1));
}

void HiveConnectorTestBase::addSplit(
    Task* task,
    const core::PlanNodeId& planNodeId,
    exec::Split&& split) {
  task->addSplit(planNodeId, std::move(split));
}

} // namespace facebook::velox::exec::test
