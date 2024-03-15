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
namespace {
static const char* kSpillProbedFlagColumnName = "__probedFlag";
}

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

void HashJoinBridge::setHashTable(
    std::unique_ptr<BaseHashTable> table,
    SpillPartitionSet spillPartitionSet,
    bool hasNullKeys) {
  VELOX_CHECK_NOT_NULL(table, "setHashTable called with null table");

  auto spillPartitionIdSet = toSpillPartitionIdSet(spillPartitionSet);

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
        std::move(spillPartitionIdSet),
        hasNullKeys);
    restoringSpillPartitionId_.reset();
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

void HashJoinBridge::setSpilledHashTable(SpillPartitionSet spillPartitionSet) {
  VELOX_CHECK(
      !spillPartitionSet.empty(), "Spilled table partitions can't be empty");
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(started_);
  VELOX_CHECK(buildResult_.has_value());
  VELOX_CHECK(restoringSpillShards_.empty());
  VELOX_CHECK(!restoringSpillPartitionId_.has_value());

  for (auto& partitionEntry : spillPartitionSet) {
    const auto id = partitionEntry.first;
    VELOX_CHECK_EQ(spillPartitionSets_.count(id), 0);
    spillPartitionSets_.emplace(id, std::move(partitionEntry.second));
  }
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
    }
    promises = std::move(promises_);
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

  // If 'buildResult_' is set, then the probe side is under processing. The
  // build shall just wait.
  if (buildResult_.has_value()) {
    VELOX_CHECK(!restoringSpillPartitionId_.has_value());
    promises_.emplace_back("HashJoinBridge::spillInputOrFuture");
    *future = promises_.back().getSemiFuture();
    return std::nullopt;
  }

  // If 'restoringSpillPartitionId_' is not set after probe side is done, then
  // the join processing is all done.
  if (!restoringSpillPartitionId_.has_value()) {
    VELOX_CHECK(spillPartitionSets_.empty());
    VELOX_CHECK(restoringSpillShards_.empty());
    return HashJoinBridge::SpillInput{};
  }

  VELOX_CHECK(!restoringSpillShards_.empty());
  auto spillShard = std::move(restoringSpillShards_.back());
  restoringSpillShards_.pop_back();
  return SpillInput(std::move(spillShard));
}

bool isLeftNullAwareJoinWithFilter(
    const std::shared_ptr<const core::HashJoinNode>& joinNode) {
  return (joinNode->isAntiJoin() || joinNode->isLeftSemiProjectJoin() ||
          joinNode->isLeftSemiFilterJoin()) &&
      joinNode->isNullAware() && (joinNode->filter() != nullptr);
}

uint64_t HashJoinMemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  // The flags to track if we have reclaimed from both build and probe operators
  // under a hash join node.
  bool hasReclaimedFromBuild{false};
  bool hasReclaimedFromProbe{false};
  uint64_t reclaimedBytes{0};
  pool->visitChildren([&](memory::MemoryPool* child) {
    VELOX_CHECK_EQ(child->kind(), memory::MemoryPool::Kind::kLeaf);
    const bool isBuild = isHashBuildMemoryPool(*child);
    if (isBuild) {
      if (!hasReclaimedFromBuild) {
        // We just need to reclaim from any one of the hash build operator.
        hasReclaimedFromBuild = true;
        reclaimedBytes = child->reclaim(targetBytes, maxWaitMs, stats);
      }
      return !hasReclaimedFromProbe;
    }

    if (!hasReclaimedFromProbe) {
      // The same as build operator, we only need to reclaim from any one of the
      // hash probe operator.
      hasReclaimedFromProbe = true;
      reclaimedBytes = child->reclaim(targetBytes, maxWaitMs, stats);
    }
    return !hasReclaimedFromBuild;
  });
  return reclaimedBytes;
}

bool isHashBuildMemoryPool(const memory::MemoryPool& pool) {
  return folly::StringPiece(pool.name()).endsWith("HashBuild");
}

bool isHashProbeMemoryPool(const memory::MemoryPool& pool) {
  return folly::StringPiece(pool.name()).endsWith("HashProbe");
}

bool needRightSideJoin(core::JoinType joinType) {
  return isRightJoin(joinType) || isFullJoin(joinType) ||
      isRightSemiFilterJoin(joinType) || isRightSemiProjectJoin(joinType);
}

RowTypePtr hashJoinTableSpillType(
    const RowTypePtr& tableType,
    core::JoinType joinType) {
  if (!needRightSideJoin(joinType)) {
    return tableType;
  }
  auto names = tableType->names();
  names.push_back(kSpillProbedFlagColumnName);
  auto types = tableType->children();
  types.push_back(BOOLEAN());
  return ROW(std::move(names), std::move(types));
}

bool isHashJoinTableSpillType(
    const RowTypePtr& spillType,
    core::JoinType joinType) {
  if (!needRightSideJoin(joinType)) {
    return true;
  }
  const column_index_t probedColumnChannel = spillType->size() - 1;
  if (!spillType->childAt(probedColumnChannel)->isBoolean()) {
    return false;
  }
  return spillType->nameOf(probedColumnChannel) == kSpillProbedFlagColumnName;
}
} // namespace facebook::velox::exec
