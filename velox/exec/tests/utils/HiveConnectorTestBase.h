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
#pragma once
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

#include <folly/executors/IOThreadPoolExecutor.h>

namespace facebook::velox::exec::test {

static const std::string kHiveConnectorId = "test-hive";

using ColumnHandleMap =
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>;

class HiveConnectorTestBase : public OperatorTestBase {
 public:
  HiveConnectorTestBase();
  void SetUp() override;
  void TearDown() override;

  void writeToFile(const std::string& filePath, RowVectorPtr vector);

  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors,
      std::shared_ptr<dwrf::Config> config =
          std::make_shared<facebook::velox::dwrf::Config>());

  std::vector<RowVectorPtr> makeVectors(
      const RowTypePtr& rowType,
      int32_t numVectors,
      int32_t rowsPerVector);

  std::shared_ptr<exec::Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, duckDbSql);
  }

  /// Assumes plan has a single TableScan node.
  std::shared_ptr<exec::Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql);

  static std::vector<std::shared_ptr<TempFilePath>> makeFilePaths(int count);

  static std::vector<std::shared_ptr<connector::ConnectorSplit>> makeHiveSplits(
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths);

  static std::shared_ptr<connector::ConnectorSplit> makeHiveConnectorSplit(
      const std::string& filePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max()) {
    return makeHiveConnectorSplit(filePath, {}, start, length);
  }

  /// Split file at path 'filePath' into 'splitCount' splits.
  static std::vector<std::shared_ptr<connector::hive::HiveConnectorSplit>>
  makeHiveConnectorSplits(
      const std::string& filePath,
      uint32_t splitCount,
      dwio::common::FileFormat format);

  static std::shared_ptr<connector::ConnectorSplit> makeHiveConnectorSplit(
      const std::string& filePath,
      const std::unordered_map<std::string, std::optional<std::string>>&
          partitionKeys,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max());

  static exec::Split makeHiveSplit(
      const std::string& filePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max());

  static exec::Split makeHiveSplitWithGroup(
      const std::string& filePath,
      int32_t groupId);

  static std::shared_ptr<connector::hive::HiveTableHandle> makeTableHandle(
      common::test::SubfieldFilters subfieldFilters = {},
      const std::shared_ptr<const core::ITypedExpr>& remainingFilter = nullptr,
      const std::string& tableName = "hive_table") {
    return std::make_shared<connector::hive::HiveTableHandle>(
        tableName, true, std::move(subfieldFilters), remainingFilter);
  }

  static std::shared_ptr<connector::hive::HiveColumnHandle> regularColumn(
      const std::string& name,
      const TypePtr& type);

  static std::shared_ptr<connector::hive::HiveColumnHandle> partitionKey(
      const std::string& name,
      const TypePtr& type);

  static std::shared_ptr<connector::hive::HiveColumnHandle> synthesizedColumn(
      const std::string& name,
      const TypePtr& type);

  static ColumnHandleMap allRegularColumns(const RowTypePtr& rowType) {
    ColumnHandleMap assignments;
    assignments.reserve(rowType->size());
    for (uint32_t i = 0; i < rowType->size(); ++i) {
      const auto& name = rowType->nameOf(i);
      assignments[name] = regularColumn(name, rowType->childAt(i));
    }
    return assignments;
  }

  memory::MappedMemory* mappedMemory() {
    return memory::MappedMemory::getInstance();
  }

  SimpleLRUDataCache* dataCache;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
};

} // namespace facebook::velox::exec::test
