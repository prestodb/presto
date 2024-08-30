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

#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::velox::exec::test {

class AssertQueryBuilder {
 public:
  AssertQueryBuilder(
      const core::PlanNodePtr& plan,
      DuckDbQueryRunner& duckDbQueryRunner);

  explicit AssertQueryBuilder(DuckDbQueryRunner& duckDbQueryRunner);

  explicit AssertQueryBuilder(const core::PlanNodePtr& plan);

  /// Set the plan.
  AssertQueryBuilder& plan(const core::PlanNodePtr& plan);

  /// Change requested number of drivers. Default is 1.
  AssertQueryBuilder& maxDrivers(int32_t maxDrivers);

  /// Change task's 'destination', the partition number assigned to the task.
  /// Default is 0.
  AssertQueryBuilder& destination(int32_t destination);

  /// Use serial execution mode to execute the Velox plan.
  /// Default is false.
  AssertQueryBuilder& serialExecution(bool serial);

  /// Set configuration property. May be called multiple times to set multiple
  /// properties.
  AssertQueryBuilder& config(const std::string& key, const std::string& value);

  template <typename T>
  typename std::enable_if<std::is_arithmetic<T>::value, AssertQueryBuilder&>::
      type
      config(const std::string& key, const T& value) {
    return config(key, std::to_string(value));
  }

  /// Set multiple configuration properties.
  AssertQueryBuilder& configs(
      const std::unordered_map<std::string, std::string>& values);

  /// Set connector-specific configuration session property. May be called
  /// multiple times to set multiple properties for one or multiple connectors.
  AssertQueryBuilder& connectorSessionProperty(
      const std::string& connectorId,
      const std::string& key,
      const std::string& value);

  // Methods to add splits.

  /// Add a single split for the specified plan node.
  AssertQueryBuilder& split(const core::PlanNodeId& planNodeId, Split split);

  /// Add a single split to the only leaf plan node. Throws if there are
  /// multiple leaf nodes.
  AssertQueryBuilder& split(Split split);

  /// Add multiple splits for the specified plan node.
  AssertQueryBuilder& splits(
      const core::PlanNodeId& planNodeId,
      std::vector<Split> splits);

  /// Add multiple splits to the only leaf plan node. Throws if there are
  /// multiple leaf nodes.
  AssertQueryBuilder& splits(std::vector<Split> splits);

  /// Add a single connector split to the only leaf plan node. Throws if there
  /// are multiple leaf nodes.
  AssertQueryBuilder& split(
      const std::shared_ptr<connector::ConnectorSplit>& connectorSplit);

  /// Add a single connector split for the specified plan node.
  AssertQueryBuilder& split(
      const core::PlanNodeId& planNodeId,
      const std::shared_ptr<connector::ConnectorSplit>& connectorSplit);

  /// Add multiple connector splits for the specified plan node.
  AssertQueryBuilder& splits(
      const core::PlanNodeId& planNodeId,
      const std::vector<std::shared_ptr<connector::ConnectorSplit>>&
          connectorSplits);

  /// Add multiple connector splits to the only leaf plan node. Throws if there
  /// are multiple leaf nodes.
  AssertQueryBuilder& splits(
      const std::vector<std::shared_ptr<connector::ConnectorSplit>>&
          connectorSplits);

  /// Sets the QueryCtx.
  AssertQueryBuilder& queryCtx(const std::shared_ptr<core::QueryCtx>& ctx) {
    params_.queryCtx = ctx;
    return *this;
  }

  /// Spilling directory, if not empty, then the task's spilling directory would
  /// be built from it.
  AssertQueryBuilder& spillDirectory(const std::string& dir) {
    params_.spillDirectory = dir;
    return *this;
  }

  /// Methods to configure the group execution mode.
  AssertQueryBuilder& executionStrategy(
      core::ExecutionStrategy executionStrategy) {
    params_.executionStrategy = executionStrategy;
    return *this;
  }

  AssertQueryBuilder& numSplitGroups(int numSplitGroups) {
    params_.numSplitGroups = numSplitGroups;
    return *this;
  }

  AssertQueryBuilder& numConcurrentSplitGroups(
      int32_t numConcurrentSplitGroups) {
    params_.numConcurrentSplitGroups = numConcurrentSplitGroups;
    return *this;
  }

  AssertQueryBuilder& groupedExecutionLeafNodeIds(
      const std::unordered_set<core::PlanNodeId>& groupedExecutionLeafNodeIds) {
    params_.groupedExecutionLeafNodeIds = groupedExecutionLeafNodeIds;
    return *this;
  }

  /// Methods to run the query and verify the results.

  /// Run the query and verify results against DuckDB. Requires
  /// duckDbQueryRunner to be provided in the constructor.
  std::shared_ptr<Task> assertResults(
      const std::string& duckDbSql,
      const std::optional<std::vector<uint32_t>>& sortingKeys = std::nullopt);

  /// Run the query and compare results with 'expected'.
  std::shared_ptr<Task> assertResults(const RowVectorPtr& expected);

  /// Run the query and compare results with 'expected'.
  std::shared_ptr<Task> assertResults(
      const std::vector<RowVectorPtr>& expected);

  /// Run the query and test that it returns no results (empty result set).
  std::shared_ptr<Task> assertEmptyResults();

  /// Run the query and ensure it returns batches of `expectedType`, and exactly
  /// `numRows`.
  std::shared_ptr<Task> assertTypeAndNumRows(
      const TypePtr& expectedType,
      vector_size_t expectedNumRows);

  /// Run the query and collect all results into a single vector. Throws if
  /// query returns empty result.
  RowVectorPtr copyResults(memory::MemoryPool* pool);

  /// Similar to above method and also returns the task.
  RowVectorPtr copyResults(
      memory::MemoryPool* pool,
      std::shared_ptr<Task>& task);

 private:
  std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>>
  readCursor();

  static std::unique_ptr<folly::Executor> newExecutor() {
    return std::make_unique<folly::CPUThreadPoolExecutor>(
        std::thread::hardware_concurrency());
  }

  // Used by the created task as the default driver executor.
  std::unique_ptr<folly::Executor> executor_{newExecutor()};
  DuckDbQueryRunner* const duckDbQueryRunner_;
  CursorParameters params_;
  std::unordered_map<std::string, std::string> configs_;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      connectorSessionProperties_;
  std::unordered_map<core::PlanNodeId, std::vector<Split>> splits_;
};

} // namespace facebook::velox::exec::test
