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

#include "velox/exec/HashJoinBridge.h"

namespace facebook::velox::exec {

void HashJoinBridge::start() {
  std::lock_guard<std::mutex> l(mutex_);
  started_ = true;
  VELOX_CHECK_GT(numBuilders_, 0);
}

void HashJoinBridge::addBuilder() {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(!started_);
  ++numBuilders_;
}

bool HashJoinBridge::setHashTable(
    std::unique_ptr<BaseHashTable> table,
    SpillPartitionSet spillPartitionSet) {
  VELOX_CHECK_NOT_NULL(table, "setHashTable called with null table");

  auto spillPartitionIdSet = toSpillPartitionIdSet(spillPartitionSet);

  bool hasSpillData;
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(started_);
    VELOX_CHECK(!buildResult_.has_value());
    VELOX_CHECK(restoringSpillShards_.empty());

    if (restoringSpillPartitionId_.has_value()) {
      for (const auto& id : spillPartitionIdSet) {
        VELOX_DCHECK_LT(
            restoringSpillPartitionId_->partitionBitOffset(),
            id.partitionBitOffset());
      }
    }

    for (auto& partitionEntry : spillPartitionSet) {
      const auto id = partitionEntry.first;
      VELOX_CHECK_EQ(spillPartitionSets_.count(id), 0);
      spillPartitionSets_.emplace(id, std::move(partitionEntry.second));
    }
    buildResult_ = HashBuildResult(
        std::move(table),
        std::move(restoringSpillPartitionId_),
        std::move(spillPartitionIdSet));
    restoringSpillPartitionId_.reset();

    hasSpillData = !spillPartitionSets_.empty();
    promises = std::move(promises_);
  }
  notify(std::move(promises));
  return hasSpillData;
}

void HashJoinBridge::setAntiJoinHasNullKeys() {
  std::vector<ContinuePromise> promises;
  SpillPartitionSet spillPartitions;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(started_);
    VELOX_CHECK(!buildResult_.has_value());
    VELOX_CHECK(restoringSpillShards_.empty());

    buildResult_ = HashBuildResult{};
    restoringSpillPartitionId_.reset();
    spillPartitions.swap(spillPartitionSets_);
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<HashJoinBridge::HashBuildResult> HashJoinBridge::tableOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(started_);
  VELOX_CHECK(!cancelled_, "Getting hash table after join is aborted");
  VELOX_CHECK(
      !buildResult_.has_value() ||
      (!restoringSpillPartitionId_.has_value() &&
       restoringSpillShards_.empty()));

  if (buildResult_.has_value()) {
    return buildResult_.value();
  }
  promises_.emplace_back("HashJoinBridge::tableOrFuture");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

bool HashJoinBridge::probeFinished() {
  std::vector<ContinuePromise> promises;
  bool hasSpillInput = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(started_);
    VELOX_CHECK(buildResult_.has_value());
    VELOX_CHECK(
        !restoringSpillPartitionId_.has_value() &&
        restoringSpillShards_.empty());
    VELOX_CHECK_GT(numBuilders_, 0);

    // NOTE: we are clearing the hash table as it has been fully processed and
    // not needed anymore. We'll wait for the HashBuild operator to build a new
    // table from the next spill partition now.
    buildResult_.reset();

    if (!spillPartitionSets_.empty()) {
      hasSpillInput = true;
      restoringSpillPartitionId_ = spillPartitionSets_.begin()->first;
      restoringSpillShards_ =
          spillPartitionSets_.begin()->second->split(numBuilders_);
      VELOX_CHECK_EQ(restoringSpillShards_.size(), numBuilders_);
      spillPartitionSets_.erase(spillPartitionSets_.begin());
      promises = std::move(promises_);
    } else {
      VELOX_CHECK(promises_.empty());
    }
  }
  notify(std::move(promises));
  return hasSpillInput;
}

std::optional<HashJoinBridge::SpillInput> HashJoinBridge::spillInputOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(started_);
  VELOX_CHECK(!cancelled_, "Getting spill input after join is aborted");
  VELOX_DCHECK(
      !restoringSpillPartitionId_.has_value() || !buildResult_.has_value());

  if (!restoringSpillPartitionId_.has_value()) {
    if (spillPartitionSets_.empty()) {
      return HashJoinBridge::SpillInput{};
    } else {
      promises_.emplace_back("HashJoinBridge::spillInputOrFuture");
      *future = promises_.back().getSemiFuture();
      return std::nullopt;
    }
  }
  VELOX_CHECK(!restoringSpillShards_.empty());
  auto spillShard = std::move(restoringSpillShards_.back());
  restoringSpillShards_.pop_back();
  return SpillInput(std::move(spillShard));
}

} // namespace facebook::velox::exec
