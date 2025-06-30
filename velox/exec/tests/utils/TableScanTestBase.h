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

#include <fmt/ranges.h>

#include <folly/experimental/EventCount.h>
#include <folly/synchronization/Baton.h>
#include <folly/synchronization/Latch.h>
#include "velox/connectors/hive/FileHandle.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec::test {

class TableScanTestBase : public HiveConnectorTestBase {
 protected:
  void SetUp() override;

  static void SetUpTestCase();

  void verifyCacheStats(
      const FileHandleCacheStats& cacheStats,
      size_t curSize,
      size_t numHits,
      size_t numLookups);

  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      const RowTypePtr& rowType = nullptr);

  exec::Split makeHiveSplit(const std::string& path, int64_t splitWeight = 0);

  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::shared_ptr<connector::ConnectorSplit>& hiveSplit,
      const std::string& duckDbSql);

  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const exec::Split&& split,
      const std::string& duckDbSql);

  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql);

  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& duckDbSql,
      const int32_t numPrefetchSplit);

  // Run query with spill enabled.
  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths,
      const std::string& spillDirectory,
      const std::string& duckDbSql);

  core::PlanNodePtr tableScanNode();

  core::PlanNodePtr tableScanNode(const RowTypePtr& outputType);

  static PlanNodeStats getTableScanStats(const std::shared_ptr<Task>& task);

  static std::unordered_map<std::string, RuntimeMetric>
  getTableScanRuntimeStats(const std::shared_ptr<Task>& task);

  static int64_t getSkippedStridesStat(const std::shared_ptr<Task>& task);

  static int64_t getSkippedSplitsStat(const std::shared_ptr<Task>& task);

  static void waitForFinishedDrivers(
      const std::shared_ptr<Task>& task,
      uint32_t n);

  void testPartitionedTableImpl(
      const std::string& filePath,
      const TypePtr& partitionType,
      const std::optional<std::string>& partitionValue);

  void testPartitionedTable(
      const std::string& filePath,
      const TypePtr& partitionType,
      const std::optional<std::string>& partitionValue);

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           INTEGER(),
           SMALLINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           TINYINT()})};
};

} // namespace facebook::velox::exec::test
