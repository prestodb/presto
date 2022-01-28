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
#include <limits>
#include <unordered_set>
#include <vector>

namespace facebook::velox::exec {

class Driver;
class JoinBridge;
class MergeSource;

/// Corresponds to Presto TaskState, needed for reporting query completion.
enum TaskState { kRunning, kFinished, kCanceled, kAborted, kFailed };

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

struct BarrierState {
  int32_t numRequested;
  std::vector<std::shared_ptr<Driver>> drivers;
  std::vector<VeloxPromise<bool>> promises;
};

/// Structure to accumulate splits for distribution.
struct SplitsStore {
  /// Arrived (added), but not distributed yet, splits.
  std::deque<exec::Split> splits;
  /// Signal, that no more splits will arrive.
  bool noMoreSplits{false};
  /// Blocking promises given out when out of splits to distribute.
  std::vector<VeloxPromise<bool>> splitPromises;
};

/// Structure contains the current info on splits.
struct SplitsState {
  /// For ungrouped (usual) execution, we store splits here.
  /// Also, we use 'noMoreSplits' member here in grouped execution as well.
  SplitsStore splitsStore;

  /// Keep the max added split's sequence id to deduplicate incoming splits.
  long maxSequenceId{std::numeric_limits<long>::min()};

  /// Ensure we always have allocated entries when accessing group stores.
  std::vector<SplitsStore>& groupSplitsStores(uint32_t numGroups) {
    if (groupSplitsStores_.size() == numGroups) {
      return groupSplitsStores_;
    }
    // This member will be called only once and resize from 0 to 'numGroups'.
    // Use swap, as promises are not copyable and 'resize' wouldn't compile.
    std::vector<SplitsStore> tmp{numGroups};
    groupSplitsStores_.swap(tmp);
    return groupSplitsStores_;
  }

  /// We need these due to having promises in the structure.
  SplitsState() = default;
  SplitsState(SplitsState const&) = delete;
  SplitsState& operator=(SplitsState const&) = delete;

 private:
  /// For grouped execution we store splits in this vector, separately for
  /// each group.
  std::vector<SplitsStore> groupSplitsStores_;
};

/// Stores local exchange sources with the memory manager.
struct LocalExchange {
  std::unique_ptr<LocalExchangeMemoryManager> memoryManager;
  std::vector<std::shared_ptr<LocalExchangeSource>> sources;
};

/// Stores inter-operator state (exchange, bridges) for split groups.
struct SplitGroupState {
  /// Map from the plan node id of the join to the corresponding JoinBridge.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<JoinBridge>> bridges;

  std::vector<std::shared_ptr<MergeSource>> localMergeSources;

  std::unordered_map<core::PlanNodeId, std::shared_ptr<MergeJoinSource>>
      mergeJoinSources;

  /// Map of local exchanges keyed on LocalPartition plan node ID.
  std::unordered_map<core::PlanNodeId, LocalExchange> localExchanges;

  /// Drivers created and still running for this split group.
  /// The split group is finished when this numbers reaches zero.
  uint32_t activeDrivers{0};

  /// Clears the state.
  void clear() {
    bridges.clear();
    localMergeSources.clear();
    mergeJoinSources.clear();
    localExchanges.clear();
  }
};

} // namespace facebook::velox::exec
