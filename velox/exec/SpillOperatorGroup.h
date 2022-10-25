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

#include <folly/container/F14Set.h>
#include <stdint.h>
#include <functional>
#include "velox/core/PlanNode.h"

namespace facebook::velox::exec {

class Operator;

/// Used to coordinate the disk spill operation among multiple operator
/// instances of the same plan node within a task (split group). Take hash
/// build for example, when any one of the build operator needs to spill, then
/// all its peer operators' drivers will stop execution for spill, and one of
/// them will be selected (the last one reaches to group spill barrier) as the
/// coordinator to run spill on all the operators, and then resume all the
/// driver executions after that.
///
/// NOTE: as for now, only hash build operators needs to group spill
/// coordination.
class SpillOperatorGroup {
 public:
  /// Define the internal execution state of a spill group. The valid state
  /// transition is depicted as below:
  ///
  ///       kInit --->  kRunning  --->  kStopped
  ///                      ^               |
  ///                      |               v
  ///                      +---------------+
  ///
  enum class State {
    kInit = 0,
    kRunning = 1,
    kStopped = 2,
  };
  static std::string stateName(State state);

  SpillOperatorGroup(
      const std::string taskId,
      const uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId)
      : taskId_(taskId),
        splitGroupId_(splitGroupId),
        planNodeId_(planNodeId),
        state_(State::kInit) {}

  State state();

  using SpillRunner = std::function<void(const std::vector<Operator*>& ops)>;

  /// Invoked by an operator on construction to add to this spill group.
  /// 'spillRunner' is the operator specific callback to run spill on the
  /// group of operators together.
  ///
  /// NOTE: the group of operators are instantiated from the same plan node in a
  /// task pipeline, such as all the build operators for a hash join node.
  void addOperator(Operator& op, SpillRunner spillRunner);

  /// Invoked to indicate a driver won't trigger any more spill request and
  /// it will also not involve in the future group spill operation. The function
  /// will put this 'op' into 'stoppedOperators_'. This function will also help
  /// to check if the group spill is ready to run if there is a pending one.
  void operatorStopped(const Operator& op);

  /// Invoked by the task framework to start this spilling group, and the
  /// operator needs to add to a group before it is started.
  void start();

  /// Invoked to restart this spill group to support recursive spilling. For
  /// example, the hassh build operator might need to trigger recursive spilling
  /// while building the hash table from a previously spilled partition.
  ///
  /// NOTE: a spill group will be stopped after all the operators call
  /// 'operatorStopped()' to indicates that there will be no more spill request
  /// from that operator. For example, a hash build operator will call
  /// 'operatorStopped()' after finishing build the hash table as there will be
  /// no more memory consumption after that.
  void restart();

  /// Indicates if this group needs spill or not.
  bool needSpill() const {
    return needSpill_;
  }

  /// Invoked to request a new spill operation on the group. The function
  /// returns true if it needs to wait for spill to run, otherwise the spill has
  /// been inline executed and returns false.
  bool requestSpill(Operator& op, ContinueFuture& future);

  /// Invoked to check if 'op' needs to wait for any pending group spill to run.
  /// The function returns true if it needs to wait, otherwise it returns false.
  /// The latter case is either because there is no pending spill or 'op' is the
  /// last one that reaches to the spill barrier and has inline executed the
  /// spill for the group.
  bool waitSpill(Operator& op, ContinueFuture& future);

 private:
  void checkStoppedStateLocked() const;

  // Return the number of non-stopped operators which includes both running and
  // waiting ones.
  uint32_t numActiveOperatorsLocked() const {
    return operators_.size() - stoppedOperators_.size();
  }

  // Invoked to run spill runner callback on all the spill operators, and after
  // that the function will also resume the waiting operators' executions by
  // fulfilling 'promises'.
  void runSpill(std::vector<ContinuePromise>& promises);

  bool waitSpillLocked(
      Operator& op,
      std::vector<ContinuePromise>& promises,
      ContinueFuture& future);

  std::string toStringLocked() const;

  const std::string taskId_;
  const uint32_t splitGroupId_;
  const core::PlanNodeId planNodeId_;

  std::mutex mutex_;
  State state_;

  // The participating spill operators which won't change after starts the
  // group.
  std::vector<Operator*> operators_;
  // Invoked to run spill on all 'operators_' no matter it has stopped or not.
  SpillRunner spillRunner_;

  // Indicates if the group needs to spill or not.
  std::atomic<bool> needSpill_{false};

  // Counts the number of operators to wait for the pending spill to run. Once
  // the counter matches the number of non-stopped operators, then the last
  // wait operator will act as the coordinator to run the spill for all the
  // operators.
  int32_t numWaitingOperators_{0};

  // The promises created for each waiting spill operator except the last one.
  std::vector<ContinuePromise> promises_;

  // The set of stopped operators which will not trigger any more spill
  // operation such as hash build operators which have finished the table build.
  //
  // NOTE: once all the operators in the group have been stopped, then the group
  // will transit to STOPPED state until it has been restarted. The set will be
  // also been cleared by the group restart. A spill group will be restarted to
  // support recursive spill operation as required by hash join build.
  folly::F14FastSet<const Operator*> stoppedOperators_;
};

inline std::ostream& operator<<(
    std::ostream& os,
    SpillOperatorGroup::State state) {
  os << SpillOperatorGroup::stateName(state);
  return os;
}

} // namespace facebook::velox::exec
