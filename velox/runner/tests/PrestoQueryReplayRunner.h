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

#include "velox/runner/LocalRunner.h"

namespace facebook::velox::runner {

extern const int32_t kDefaultWidth;
extern const int32_t kDefaultMaxDrivers;
extern const int32_t kWaitTimeoutUs;

typedef std::string (*TaskPrefixExtractor)(const std::string&);
using ConnectorSplitPtr = std::shared_ptr<connector::ConnectorSplit>;

class PrestoQueryReplayRunner {
 public:
  enum class Status { kSuccess, kUnsupported, kError };

  /// Create a QueryReplayRunner with the given memory pool.
  /// @param taskPrefixExtractor A function that extracts the task prefix from a
  /// task id contained in the serialized plan fragment.
  /// @param width The number of workers for each stage except the gathering
  /// stages.
  /// @param maxDrivers The maximum number of drivers for each worker.
  explicit PrestoQueryReplayRunner(
      memory::MemoryPool* pool,
      TaskPrefixExtractor taskPrefixExtractor,
      int32_t width = kDefaultWidth,
      int32_t maxDrivers = kDefaultMaxDrivers,
      const std::unordered_map<std::string, std::string>& config = {},
      const std::unordered_map<std::string, std::string>& hiveConfig = {});

  /// Runs a query with the given serialized plan fragments and returns a pair
  /// of the results and execution status. The result is std::nullopt if the
  /// status is not kSuccess. The serialized plan fragments should have the same
  /// query id as 'queryId'.
  std::pair<std::optional<std::vector<RowVectorPtr>>, Status> run(
      const std::string& queryId,
      const std::vector<std::string>& serializedPlanFragments,
      const std::vector<std::string>& serializedConnectorSplits);

 private:
  std::shared_ptr<core::QueryCtx> makeQueryCtx(
      const std::string& queryId,
      const std::shared_ptr<memory::MemoryPool>& rootPool);

  // For each jsonRecord of the logged plan fragment, extract the task prefix
  // from the logged task id through taskPrefixExtractor_. Return a list of
  // extracted task prefixes in the same order as the jsonRecords.
  std::vector<std::string> getTaskPrefixes(
      const std::vector<folly::dynamic>& jsonRecords);

  /// Deserialize a list of plan fragments into a MultiFragmentPlanPtr. If any
  /// of the plan fragment is unsupported, return a nullptr.
  MultiFragmentPlanPtr deserializeSupportedPlan(
      const std::string& queryId,
      const std::vector<std::string>& serializedPlanFragments);

  std::unordered_map<core::PlanNodeId, std::vector<ConnectorSplitPtr>>
  deserializeConnectorSplits(const std::vector<std::string>& serializedSplits);

  memory::MemoryPool* pool_{nullptr};
  TaskPrefixExtractor taskPrefixExtractor_;
  int32_t width_;
  int32_t maxDrivers_;

  const std::unordered_map<std::string, std::string> config_;
  const std::unordered_map<std::string, std::string> hiveConfig_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};

} // namespace facebook::velox::runner
