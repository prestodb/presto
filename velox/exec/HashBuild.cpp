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

#include "velox/exec/HashBuild.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

void HashJoinBridge::setHashTable(std::unique_ptr<BaseHashTable> table) {
  VELOX_CHECK(table, "setHashTable called with null table");

  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(!table_, "setHashTable may be called only once");
    // Ownership becomes shared.
    table_.reset(table.release());
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

void HashJoinBridge::setAntiJoinHasNullKeys() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(
        !table_,
        "Only one of setAntiJoinHasNullKeys or setHashTable may be called");

    antiJoinHasNullKeys_ = true;
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<HashJoinBridge::HashBuildResult> HashJoinBridge::tableOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(
      !cancelled_, "Getting hash table after the build side is aborted");
  if (table_ || antiJoinHasNullKeys_) {
    return HashBuildResult{table_, antiJoinHasNullKeys_};
  }
  promises_.emplace_back("HashJoinBridge::tableOrFuture");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

HashBuild::HashBuild(
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::shared_ptr<const core::HashJoinNode> joinNode)
    : Operator(driverCtx, nullptr, operatorId, joinNode->id(), "HashBuild"),
      joinType_{joinNode->joinType()},
      mappedMemory_(operatorCtx_->mappedMemory()) {
  auto type = joinNode->sources()[1]->outputType();

  auto numKeys = joinNode->rightKeys().size();
  keyChannels_.reserve(numKeys);
  folly::F14FastSet<ChannelIndex> keyChannelSet;
  keyChannelSet.reserve(numKeys);
  std::vector<std::unique_ptr<VectorHasher>> keyHashers;
  keyHashers.reserve(numKeys);
  for (auto& key : joinNode->rightKeys()) {
    auto channel = exprToChannel(key.get(), type);
    keyChannelSet.emplace(channel);
    keyChannels_.emplace_back(channel);
    keyHashers.emplace_back(
        std::make_unique<VectorHasher>(type->childAt(channel), channel));
  }

  // Identify the non-key build side columns and make a decoder for each.
  auto numDependents = type->size() - numKeys;
  dependentChannels_.reserve(numDependents);
  decoders_.reserve(numDependents);
  std::vector<TypePtr> dependentTypes;
  dependentTypes.reserve(numDependents);
  for (auto i = 0; i < type->size(); ++i) {
    if (keyChannelSet.find(i) == keyChannelSet.end()) {
      dependentTypes.emplace_back(type->childAt(i));
      dependentChannels_.emplace_back(i);
      decoders_.emplace_back(std::make_unique<DecodedVector>());
    }
  }

  if (joinNode->isRightJoin() || joinNode->isFullJoin()) {
    // Do not ignore null keys.
    table_ = HashTable<false>::createForJoin(
        std::move(keyHashers),
        dependentTypes,
        true, // allowDuplicates
        true, // hasProbedFlag
        mappedMemory_);
  } else {
    // Semi and anti join only needs to know whether there is a match. Hence, no
    // need to store entries with duplicate keys.
    const bool allowDuplicates =
        !joinNode->isSemiJoin() && !joinNode->isAntiJoin();

    table_ = HashTable<true>::createForJoin(
        std::move(keyHashers),
        dependentTypes,
        allowDuplicates,
        false, // hasProbedFlag
        mappedMemory_);
  }
  analyzeKeys_ = table_->hashMode() != BaseHashTable::HashMode::kHash;
}

void HashBuild::addInput(RowVectorPtr input) {
  activeRows_.resize(input->size());
  activeRows_.setAll();
  if (!isRightJoin(joinType_) && !isFullJoin(joinType_)) {
    deselectRowsWithNulls(
        *input, keyChannels_, activeRows_, *operatorCtx_->execCtx());
  }

  if (joinType_ == core::JoinType::kAnti) {
    // Anti join returns no rows if build side has nulls in join keys. Hence, we
    // can stop processing on first null.
    if (activeRows_.countSelected() < input->size()) {
      antiJoinHasNullKeys_ = true;
      noMoreInput();
      return;
    }
  }

  if (analyzeKeys_ && hashes_.size() < activeRows_.size()) {
    hashes_.resize(activeRows_.size());
  }

  auto& hashers = table_->hashers();

  // As long as analyzeKeys is true, we keep running the keys through
  // the Vectorhashers so that we get a possible mapping of the keys
  // to small ints for array or normalized key. When mayUseValueIds is
  // false for the first time we stop. We do not retain the value ids
  // since the final ones will only be known after all data is
  // received.
  for (auto& hasher : hashers) {
    // TODO: Load only for active rows, except if right/full outer join.
    if (analyzeKeys_) {
      hasher->computeValueIds(
          *input->loadedChildAt(hasher->channel()), activeRows_, hashes_);
      analyzeKeys_ = hasher->mayUseValueIds();
    } else {
      hasher->decode(*input->loadedChildAt(hasher->channel()), activeRows_);
    }
  }
  for (auto i = 0; i < dependentChannels_.size(); ++i) {
    decoders_[i]->decode(
        *input->loadedChildAt(dependentChannels_[i]), activeRows_);
  }
  auto rows = table_->rows();
  auto nextOffset = rows->nextOffset();
  activeRows_.applyToSelected([&](auto rowIndex) {
    char* newRow = rows->newRow();
    if (nextOffset) {
      *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
    }
    // Store the columns for each row in sequence. At probe time
    // strings of the row will probably be in consecutive places, so
    // reading one will prime the cache for the next.
    for (auto i = 0; i < hashers.size(); ++i) {
      rows->store(hashers[i]->decodedVector(), rowIndex, newRow, i);
    }
    for (auto i = 0; i < dependentChannels_.size(); ++i) {
      rows->store(*decoders_[i], rowIndex, newRow, i + hashers.size());
    }
  });
}

void HashBuild::noMoreInput() {
  if (noMoreInput_) {
    return;
  }

  Operator::noMoreInput();
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<Driver>> peers;
  // The last Driver to hit HashBuild::finish gathers the data from
  // all build Drivers and hands it over to the probe side. At this
  // point all build Drivers are continued and will free their
  // state. allPeersFinished is true only for the last Driver of the
  // build pipeline.
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    return;
  }

  std::vector<std::unique_ptr<BaseHashTable>> otherTables;
  otherTables.reserve(peers.size());

  if (!antiJoinHasNullKeys_) {
    for (auto& peer : peers) {
      auto op = peer->findOperator(planNodeId());
      HashBuild* build = dynamic_cast<HashBuild*>(op);
      VELOX_CHECK(build);
      if (build->antiJoinHasNullKeys_) {
        antiJoinHasNullKeys_ = true;
        break;
      }
      otherTables.push_back(std::move(build->table_));
    }
  }

  // Realize the promises so that the other Drivers (which were not
  // the last to finish) can continue from the barrier and finish.
  peers.clear();
  for (auto& promise : promises) {
    promise.setValue(true);
  }

  if (antiJoinHasNullKeys_) {
    operatorCtx_->task()
        ->getHashJoinBridge(
            operatorCtx_->driverCtx()->splitGroupId, planNodeId())
        ->setAntiJoinHasNullKeys();
  } else {
    table_->prepareJoinTable(std::move(otherTables));

    addRuntimeStats();

    operatorCtx_->task()
        ->getHashJoinBridge(
            operatorCtx_->driverCtx()->splitGroupId, planNodeId())
        ->setHashTable(std::move(table_));
  }
}

void HashBuild::addRuntimeStats() {
  // Report range sizes and number of distinct values for the join keys.
  const auto& hashers = table_->hashers();
  uint64_t asRange;
  uint64_t asDistinct;
  for (auto i = 0; i < hashers.size(); i++) {
    hashers[i]->cardinality(asRange, asDistinct);
    if (asRange != VectorHasher::kRangeTooLarge) {
      stats_.addRuntimeStat(
          fmt::format("rangeKey{}", i), RuntimeCounter(asRange));
    }
    if (asDistinct != VectorHasher::kRangeTooLarge) {
      stats_.addRuntimeStat(
          fmt::format("distinctKey{}", i), RuntimeCounter(asDistinct));
    }
  }
}

BlockingReason HashBuild::isBlocked(ContinueFuture* future) {
  if (!future_.valid()) {
    return BlockingReason::kNotBlocked;
  }
  *future = std::move(future_);
  return BlockingReason::kWaitForJoinBuild;
}

bool HashBuild::isFinished() {
  return !future_.valid() && noMoreInput_;
}

} // namespace facebook::velox::exec
