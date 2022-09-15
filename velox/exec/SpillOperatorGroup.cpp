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

#include "velox/exec/SpillOperatorGroup.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

SpillOperatorGroup::State SpillOperatorGroup::state() {
  std::lock_guard<std::mutex> l(mutex_);
  return state_;
}

std::string SpillOperatorGroup::stateName(State state) {
  switch (state) {
    case State::INIT:
      return "INIT";
    case State::RUNNING:
      return "RUNNING";
    case State::STOPPED:
      return "STOPPED";
    default:
      return fmt::format("UNKNOWN STATE: {}", static_cast<int>(state));
  }
}

void SpillOperatorGroup::addOperator(
    Operator& op,
    SpillOperatorGroup::SpillRunner runner) {
  VELOX_CHECK_NOT_NULL(runner);
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK_EQ(
      state_,
      State::INIT,
      "Can only add an operator before group starts: {}",
      toStringLocked());
  operators_.push_back(&op);
  // Set 'spillRunner_' to the callback provided by the first added operator.
  if (spillRunner_ == nullptr) {
    spillRunner_ = std::move(runner);
  }
}

void SpillOperatorGroup::operatorStopped(const Operator& op) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_EQ(
        state_,
        State::RUNNING,
        "Can only stop an operator when group is running: {}",
        toStringLocked());
    VELOX_CHECK(!stoppedOperators_.contains(&op));
    stoppedOperators_.insert(&op);
    VELOX_CHECK_LE(stoppedOperators_.size(), operators_.size());
    if (stoppedOperators_.size() == operators_.size()) {
      state_ = State::STOPPED;
      checkStoppedStateLocked();
      return;
    }
    if (!needSpill_) {
      return;
    }
    // NOTE: the stopping operator doesn't need to wait for spill to run.
    if (numWaitingOperators_ < numActiveOperatorsLocked()) {
      return;
    }
    promises = std::move(promises_);
  }
  runSpill(promises);
}

void SpillOperatorGroup::start() {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK_EQ(
      state_, State::INIT, "Can only start a group once: {}", toStringLocked());
  state_ = State::RUNNING;
}

void SpillOperatorGroup::restart() {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK_EQ(
      state_,
      State::STOPPED,
      "Can only restart a stopped group: {}",
      toStringLocked());
  checkStoppedStateLocked();
  stoppedOperators_.clear();
  state_ = State::RUNNING;
}

bool SpillOperatorGroup::requestSpill(Operator& op, ContinueFuture& future) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_EQ(
        state_,
        State::RUNNING,
        "Can only request spill when group is running: {}",
        toStringLocked());
    VELOX_CHECK_LT(numWaitingOperators_, numActiveOperatorsLocked());
    VELOX_CHECK_LT(stoppedOperators_.size(), operators_.size());
    needSpill_ = true;
    if (waitSpillLocked(op, promises, future)) {
      VELOX_CHECK(future.valid());
      return true;
    }
  }
  runSpill(promises);
  return false;
}

bool SpillOperatorGroup::waitSpill(Operator& op, ContinueFuture& future) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_EQ(
        state_,
        State::RUNNING,
        "Can only wait spill when group is running: {}",
        toStringLocked());
    VELOX_CHECK_LT(stoppedOperators_.size(), operators_.size());
    if (!needSpill_) {
      VELOX_CHECK_EQ(numWaitingOperators_, 0);
      return false;
    }
    if (waitSpillLocked(op, promises, future)) {
      return true;
    }
  }
  runSpill(promises);
  return false;
}

bool SpillOperatorGroup::waitSpillLocked(
    Operator& op,
    std::vector<ContinuePromise>& promises,
    ContinueFuture& future) {
  VELOX_CHECK_LT(numWaitingOperators_, numActiveOperatorsLocked());
  if (++numWaitingOperators_ == numActiveOperatorsLocked()) {
    promises = std::move(promises_);
    return false;
  }
  promises_.emplace_back(ContinuePromise(fmt::format(
      "SpillOperatorGroup::waitSpillLocked {}/{}/{}/{}",
      taskId_,
      planNodeId_,
      splitGroupId_,
      op.stats().operatorId)));
  future = promises_.back().getSemiFuture();
  return true;
}

void SpillOperatorGroup::runSpill(std::vector<ContinuePromise>& promises) {
  VELOX_CHECK(needSpill_);
  spillRunner_(operators_);
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_EQ(
        state_,
        State::RUNNING,
        "Can only run spill when group is running: {}",
        toStringLocked());
    needSpill_ = false;
    numWaitingOperators_ = 0;
  }
  for (auto& promise : promises) {
    promise.setValue();
  }
}

void SpillOperatorGroup::checkStoppedStateLocked() const {
  CHECK_EQ(state_, State::STOPPED);
  VELOX_CHECK_EQ(stoppedOperators_.size(), operators_.size());
  // Reset this coordinator state if all the participated operators have
  // finished memory intensive operations and won't trigger any more spill.
  VELOX_CHECK(!needSpill_);
  VELOX_CHECK_EQ(numWaitingOperators_, 0);
  VELOX_CHECK(promises_.empty());
}

std::string SpillOperatorGroup::toStringLocked() const {
  return fmt::format(
      "[STATE:{}, NUM_OPS:{}, NUM_WAITING_OPS:{}, NUM_STOPPED_OPS:{}]",
      stateName(state_),
      operators_.size(),
      numWaitingOperators_,
      stoppedOperators_.size());
}

} // namespace facebook::velox::exec
