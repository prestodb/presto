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

#include <unordered_set>

namespace facebook::velox::exec {

struct OperatorStats;

/// Stores execution stats per pipeline.
struct PipelineStats {
  // Cumulative OperatorStats for finished Drivers. The subscript is the
  // operator id, which is the initial ordinal position of the
  // operator in the DriverFactory.
  std::vector<OperatorStats> operatorStats;

  // True if contains the source node for the task.
  bool inputPipeline;

  // True if contains the sync node for the task.
  bool outputPipeline;

  PipelineStats(bool _inputPipeline, bool _outputPipeline)
      : inputPipeline{_inputPipeline}, outputPipeline{_outputPipeline} {}
};

/// Stores execution stats per task.
struct TaskStats {
  int32_t numTotalSplits{0};
  int32_t numFinishedSplits{0};
  int32_t numRunningSplits{0};
  int32_t numQueuedSplits{0};
  std::unordered_set<int32_t> completedSplitGroups;

  // The subscript is given by each Operator's
  // DriverCtx::pipelineId. This is a sum total reflecting fully
  // processed Splits for Drivers of this pipeline.
  std::vector<PipelineStats> pipelineStats;

  // Epoch time (ms) when task starts to run
  uint64_t executionStartTimeMs{0};

  // Epoch time (ms) when last split is processed. For some tasks there might be
  // some additional time to send buffered results before the task finishes.
  uint64_t executionEndTimeMs{0};

  // Epoch time (ms) when first split is fetched from the task by an operator.
  uint64_t firstSplitStartTimeMs{0};

  // Epoch time (ms) when last split is fetched from the task by an operator.
  uint64_t lastSplitStartTimeMs{0};

  // Epoch time (ms) when the task completed, e.g. all splits were processed and
  // results have been consumed.
  uint64_t endTimeMs{0};
};
} // namespace facebook::velox::exec
