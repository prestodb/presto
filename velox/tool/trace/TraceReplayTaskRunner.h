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

#include <folly/executors/IOThreadPoolExecutor.h>
#include "velox/exec/Cursor.h"

namespace facebook::velox::tool::trace {

class TraceReplayTaskRunner {
 public:
  explicit TraceReplayTaskRunner(
      core::PlanNodePtr plan,
      std::shared_ptr<core::QueryCtx> queryCtx) {
    cursorParams_.planNode = std::move(plan);
    cursorParams_.queryCtx = std::move(queryCtx);
    VELOX_CHECK_NOT_NULL(cursorParams_.planNode);
    VELOX_CHECK_NOT_NULL(cursorParams_.queryCtx);
  }

  /// Run the replaying task. Return the copied results if 'copyResults' is
  /// true or return null.
  std::pair<std::shared_ptr<exec::Task>, RowVectorPtr> run(
      bool copyResults = false);

  /// Max number of drivers. Default is 1.
  TraceReplayTaskRunner& maxDrivers(int32_t maxDrivers);

  /// Spilling directory, if not empty, then the task's spilling directory would
  /// be built from it.
  TraceReplayTaskRunner& spillDirectory(const std::string& dir);

  /// Splits of this task.
  TraceReplayTaskRunner& splits(
      const core::PlanNodeId& planNodeId,
      std::vector<exec::Split> splits);

 private:
  void addSplits(exec::Task* task);

  std::shared_ptr<RowVector> copy(const std::vector<RowVectorPtr>& results);

  exec::CursorParameters cursorParams_;
  std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splits_;
  bool noMoreSplits_ = false;
};
} // namespace facebook::velox::tool::trace
