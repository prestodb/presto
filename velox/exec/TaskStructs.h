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

#include "velox/common/base/SkewedPartitionBalancer.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/LocalPartition.h"
#include "velox/exec/ScaledScanController.h"
#include "velox/exec/Split.h"
#include "velox/exec/TaskStats.h"

#include <folly/CPortability.h>

#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

namespace facebook::velox::exec {

class Driver;
class JoinBridge;
class LocalExchangeMemoryManager;
class MergeSource;
class MergeJoinSource;

/// Corresponds to Presto TaskState, needed for reporting query completion.
enum class TaskState : int {
  kRunning = 0,
  kFinished = 1,
  kCanceled = 2,
  kAborted = 3,
  kFailed = 4
};

std::string taskStateString(TaskState state);

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    TaskState state) {
  os << taskStateString(state);
  return os;
}

struct BarrierState {
  int32_t numRequested;
  std::vector<std::shared_ptr<Driver>> drivers;
  /// Promises given to non-last peer drivers that the last driver will collect
  /// all hashtables from the peers and assembles them into one (HashBuilder
  /// operator does that). After the last drier done its work, the promises are
  /// fulfilled and the non-last drivers can continue.
  std::vector<ContinuePromise> allPeersFinishedPromises;
};

using ConnectorSplitPreloadFunc =
    std::function<void(const std::shared_ptr<connector::ConnectorSplit>&)>;

/// A split store that either can accumulate splits through addSplit() or
/// generating splits from its own source.
///
/// This object is not multi-thread safe.
class SplitsStore {
 public:
  explicit SplitsStore(bool remoteSplit) : remoteSplit_(remoteSplit) {}

  virtual ~SplitsStore() = default;

  /// Add a barrier split to this store.  Some of the waiters previously waiting
  /// on the ContinueFuture after the nextSplit() call should be notified by the
  /// caller of this function by setting values on the `promises` output
  /// parameter.
  ///
  /// `promises` should be set by caller (potentially outside a lock), to notify
  /// any waiters on the splits.
  virtual void requestBarrier(std::vector<ContinuePromise>& promises) = 0;

  /// Return true when split is set or there is no more splits; false when
  /// caller should retry when the future is fulfilled.
  virtual bool nextSplit(
      Split& split,
      ContinueFuture& future,
      int maxPreloadSplits,
      const ConnectorSplitPreloadFunc& preload) = 0;

  /// Return whether all splits has been consumed and there will be no more
  /// splits.
  virtual bool allSplitsConsumed() const = 0;

  /// Add new split into this store.  Some of the waiters previously waiting on
  /// the ContinueFuture after the nextSplit() call should be notified by the
  /// caller of this function by setting values on the `promises` output
  /// parameter.
  ///
  /// `promises` should be set by caller (potentially outside a lock), to notify
  /// any waiters on the splits.
  void addSplit(Split split, std::vector<ContinuePromise>& promises);

  /// Return the number of waiters waiting for new splits.
  int numWaiters() const {
    return promises_.size();
  }

  /// Signal there will not be any more splits added to this store.
  std::vector<ContinuePromise> noMoreSplits() {
    noMoreSplits_ = true;
    return std::move(promises_);
  }

  void setTaskStats(TaskStats& taskStats) {
    taskStats_ = &taskStats;
  }

  void setPreloadingSplits(
      folly::F14FastSet<std::shared_ptr<connector::ConnectorSplit>>&
          preloadingSplits) {
    preloadingSplits_ = &preloadingSplits;
  }

 protected:
  Split getSplit(
      int maxPreloadSplits,
      const ConnectorSplitPreloadFunc& preload);

  ContinueFuture makeFuture();

  const bool remoteSplit_;
  TaskStats* taskStats_{};
  folly::F14FastSet<std::shared_ptr<connector::ConnectorSplit>>*
      preloadingSplits_{};

  // Arrived (added), but not distributed yet, splits.
  std::deque<Split> splits_;

  // Signal, that no more splits will arrive.
  bool noMoreSplits_{false};

  // Blocking promises given out when out of splits to distribute.
  std::vector<ContinuePromise> promises_;
};

/// Structure contains the current info on splits for a particular plan node.
struct SplitsState {
  /// True if the source node is a table scan.
  bool sourceIsTableScan{false};

  /// Plan node-wide 'no more splits'.
  bool noMoreSplits{false};

  /// Keep the max added split's sequence id to deduplicate incoming splits.
  long maxSequenceId{std::numeric_limits<long>::min()};

  /// Map split group id -> split store.
  std::unordered_map<uint32_t, std::unique_ptr<SplitsStore>> groupSplitsStores;

  /// We need these due to having promises in the structure.
  SplitsState() = default;
  SplitsState(SplitsState const&) = delete;
  SplitsState& operator=(SplitsState const&) = delete;
};

/// Stores local exchange queues with the memory manager.
struct LocalExchangeState {
  std::shared_ptr<LocalExchangeMemoryManager> memoryManager;
  std::shared_ptr<LocalExchangeVectorPool> vectorPool;
  std::vector<std::shared_ptr<LocalExchangeQueue>> queues;
  std::shared_ptr<common::SkewedPartitionRebalancer>
      scaleWriterPartitionBalancer;
};

/// Stores inter-operator state (exchange, bridges) for split groups.
struct SplitGroupState {
  /// Map from the plan node id of the join to the corresponding JoinBridge.
  /// This map will contain only HashJoinBridge and NestedLoopJoinBridge.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<JoinBridge>> bridges;
  /// This map will contain all other custom bridges.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<JoinBridge>>
      customBridges;
  /// Holds states for Task::allPeersFinished.
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

  /// Map of scaled scan controllers keyed on TableScan plan node ID.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<ScaledScanController>>
      scaledScanControllers;

  /// Drivers created and still running for this split group.
  /// The split group is finished when this numbers reaches zero.
  uint32_t numRunningDrivers{0};

  /// The number of completed drivers in the output pipeline. When all drivers
  /// in the output pipeline finish, the remaining running pipelines should stop
  /// processing and transition to finished state as well. This happens when
  /// there is a downstream operator that finishes before receiving all input,
  /// e.g. Limit.
  uint32_t numFinishedOutputDrivers{0};

  // True if the state contains structures used for connecting ungrouped
  // execution pipeline with grouped execution pipeline. In that case we don't
  // want to clean up some of these structures.
  bool mixedExecutionMode{false};

  /// Clears the state.
  void clear() {
    if (!mixedExecutionMode) {
      bridges.clear();
      customBridges.clear();
      barriers.clear();
    }
    localMergeSources.clear();
    mergeJoinSources.clear();
    localExchanges.clear();
  }
};

} // namespace facebook::velox::exec

template <>
struct fmt::formatter<facebook::velox::exec::TaskState>
    : formatter<std::string> {
  auto format(facebook::velox::exec::TaskState state, format_context& ctx)
      const {
    return formatter<std::string>::format(
        facebook::velox::exec::taskStateString(state), ctx);
  }
};
