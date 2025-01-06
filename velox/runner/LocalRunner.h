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

#include "velox/connectors/Connector.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/Exchange.h"
#include "velox/runner/MultiFragmentPlan.h"
#include "velox/runner/Runner.h"

namespace facebook::velox::runner {

/// Testing proxy for a split source managed by a system with full metadata
/// access.
class SimpleSplitSource : public SplitSource {
 public:
  explicit SimpleSplitSource(
      std::vector<std::shared_ptr<connector::ConnectorSplit>> splits)
      : splits_(std::move(splits)) {}

  virtual std::vector<SplitAndGroup> getSplits(uint64_t targetBytes) override;

 private:
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits_;
  int32_t splitIdx_{0};
};

/// Testing proxy for a split source factory that uses connector metadata to
/// enumerate splits. This takes a precomputed split list for each scan.
class SimpleSplitSourceFactory : public SplitSourceFactory {
 public:
  explicit SimpleSplitSourceFactory(
      std::unordered_map<
          core::PlanNodeId,
          std::vector<std::shared_ptr<connector::ConnectorSplit>>> nodeSplitMap)
      : nodeSplitMap_(std::move(nodeSplitMap)) {}

  std::shared_ptr<SplitSource> splitSourceForScan(
      const core::TableScanNode& scan) override;

 private:
  std::unordered_map<
      core::PlanNodeId,
      std::vector<std::shared_ptr<connector::ConnectorSplit>>>
      nodeSplitMap_;
};

/// Runner for in-process execution of a distributed plan.
class LocalRunner : public Runner,
                    public std::enable_shared_from_this<LocalRunner> {
 public:
  LocalRunner(
      MultiFragmentPlanPtr plan,
      std::shared_ptr<core::QueryCtx> queryCtx,
      std::shared_ptr<SplitSourceFactory> splitSourceFactory)
      : plan_(std::move(plan)),
        fragments_(plan_->fragments()),
        options_(plan_->options()),
        splitSourceFactory_(std::move(splitSourceFactory)) {
    params_.queryCtx = std::move(queryCtx);
  }

  RowVectorPtr next() override;

  std::vector<exec::TaskStats> stats() const override;

  void abort() override;

  void waitForCompletion(int32_t maxWaitMicros) override;

  State state() const override {
    return state_;
  }

 private:
  void start();

  // Creates all stages except for the single worker final consumer stage.
  std::vector<std::shared_ptr<exec::RemoteConnectorSplit>> makeStages();
  std::shared_ptr<SplitSource> splitSourceForScan(
      const core::TableScanNode& scan);

  // Serializes 'cursor_' and 'error_'.
  mutable std::mutex mutex_;

  const MultiFragmentPlanPtr plan_;
  const std::vector<ExecutableFragment> fragments_;
  const MultiFragmentPlan::Options& options_;

  exec::CursorParameters params_;

  tsan_atomic<State> state_{State::kInitialized};

  std::unique_ptr<exec::TaskCursor> cursor_;
  std::vector<std::vector<std::shared_ptr<exec::Task>>> stages_;
  std::exception_ptr error_;
  std::shared_ptr<SplitSourceFactory> splitSourceFactory_;
};

} // namespace facebook::velox::runner
