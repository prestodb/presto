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
class LocalExchangeMemoryManager;
class LocalExchangeSource;
class MergeSource;
class MergeJoinSource;
class Split;

/// Corresponds to Presto TaskState, needed for reporting query completion.
enum TaskState { kRunning, kFinished, kCanceled, kAborted, kFailed };

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

/// Structure contains the current info on splits for a particular plan node.
struct SplitsState {
  /// Plan node-wide 'no more splits'.
  bool noMoreSplits{false};

  /// Keep the max added split's sequence id to deduplicate incoming splits.
  long maxSequenceId{std::numeric_limits<long>::min()};

  /// Map split group id -> split store.
  std::unordered_map<uint32_t, SplitsStore> groupSplitsStores;

  /// We need these due to having promises in the structure.
  SplitsState() = default;
  SplitsState(SplitsState const&) = delete;
  SplitsState& operator=(SplitsState const&) = delete;
};

/// Stores local exchange queues with the memory manager.
struct LocalExchangeState {
  std::shared_ptr<LocalExchangeMemoryManager> memoryManager;
  std::vector<std::shared_ptr<LocalExchangeQueue>> queues;
};

/// Stores inter-operator state (exchange, bridges) for split groups.
struct SplitGroupState {
  /// Map from the plan node id of the join to the corresponding JoinBridge.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<JoinBridge>> bridges;

  // Holds states for Task::allPeersFinished.
  std::unordered_map<core::PlanNodeId, BarrierState> barriers;

  /// Map of merge sources keyed on LocalMergeNode plan node ID.
  std::
      unordered_map<core::PlanNodeId, std::vector<std::shared_ptr<MergeSource>>>
          localMergeSources;

  /// Map of merge join sources keyed on MergeJoinNode plan node ID.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<MergeJoinSource>>
      mergeJoinSources;

  /// Map of local exchanges keyed on LocalPartition plan node ID.
  std::unordered_map<core::PlanNodeId, LocalExchangeState> localExchanges;

  /// Drivers created and still running for this split group.
  /// The split group is finished when this numbers reaches zero.
  uint32_t activeDrivers{0};

  /// Clears the state.
  void clear() {
    bridges.clear();
    barriers.clear();
    localMergeSources.clear();
    mergeJoinSources.clear();
    localExchanges.clear();
  }
};

} // namespace facebook::velox::exec
