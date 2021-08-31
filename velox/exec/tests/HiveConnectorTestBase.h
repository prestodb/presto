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
#include "velox/exec/tests/OperatorTestBase.h"
#include "velox/exec/tests/TempFilePath.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

namespace facebook::velox::exec::test {

static const std::string kHiveConnectorId = "test-hive";

using ColumnHandleMap =
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>;

// A dummy cache that always misses. This class is for testing purpose only.
class DummyDataCache : public DataCache {
 public:
  bool put(std::string_view /* key */, std::string_view /* value */) override {
    putCount++;
    return false;
  }

  bool get(std::string_view /* key */, uint64_t /* size */, void* /* buf */)
      override {
    getCount++;
    return false;
  }

  bool get(std::string_view /* key */, std::string* /* value */) override {
    getCount++;
    return false;
  }

  int64_t currentSize() const final {
    return 0;
  }

  int64_t maxSize() const final {
    return 0;
  }

  size_t putCount{0};
  size_t getCount{0};
};

class HiveConnectorTestBase : public OperatorTestBase {
 public:
  void SetUp() override;

  void TearDown() override;

  void writeToFile(
      const std::string& filePath,
      const std::string& name,
      RowVectorPtr vector);

  void writeToFile(
      const std::string& filePath,
      const std::string& name,
      const std::vector<RowVectorPtr>& vectors);

  std::vector<RowVectorPtr> makeVectors(
      const std::shared_ptr<const RowType>& rowType,
      int32_t numVectors,
      int32_t rowsPerVector);

  std::shared_ptr<exec::Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::string& duckDbSql) {
    return assertQuery(
        plan, std::vector<std::shared_ptr<TempFilePath>>(), duckDbSql);
  }

  std::shared_ptr<exec::Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql);

  std::shared_ptr<exec::Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::unordered_map<int, std::vector<std::shared_ptr<TempFilePath>>>&
          filePaths,
      const std::string& duckDbSql);

  static std::vector<std::shared_ptr<TempFilePath>> makeFilePaths(int count);

  static std::vector<std::shared_ptr<connector::ConnectorSplit>> makeHiveSplits(
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths);

  static std::shared_ptr<connector::hive::HiveConnectorSplit>
  makeHiveConnectorSplit(
      const std::string& filePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max());

  static exec::Split makeHiveSplit(
      const std::string& filePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max());

  static std::shared_ptr<connector::hive::HiveTableHandle> makeTableHandle(
      common::test::SubfieldFilters subfieldFilters,
      const std::shared_ptr<const core::ITypedExpr>& remainingFilter =
          nullptr) {
    return std::make_shared<connector::hive::HiveTableHandle>(
        true, std::move(subfieldFilters), remainingFilter);
  }

  static std::shared_ptr<connector::hive::HiveColumnHandle> regularColumn(
      const std::string& name);

  static std::shared_ptr<connector::hive::HiveColumnHandle> partitionKey(
      const std::string& name);

  static std::shared_ptr<connector::hive::HiveColumnHandle> synthesizedColumn(
      const std::string& name);

  static ColumnHandleMap allRegularColumns(
      const std::shared_ptr<const RowType>& rowType) {
    ColumnHandleMap assignments;
    assignments.reserve(rowType->size());
    for (auto& name : rowType->names()) {
      assignments[name] = regularColumn(name);
    }
    return assignments;
  }

  static void addConnectorSplit(
      Task* task,
      const core::PlanNodeId& planNodeId,
      const std::shared_ptr<connector::ConnectorSplit>& connectorSplit);

  static void
  addSplit(Task* task, const core::PlanNodeId& planNodeId, exec::Split&& split);

  DummyDataCache* dataCache;
};

} // namespace facebook::velox::exec::test
