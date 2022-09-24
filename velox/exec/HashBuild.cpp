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

HashBuild::HashBuild(
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::shared_ptr<const core::HashJoinNode> joinNode)
    : Operator(driverCtx, nullptr, operatorId, joinNode->id(), "HashBuild"),
      joinNode_(std::move(joinNode)),
      joinType_(joinNode_->joinType()),
      joinBridge_(operatorCtx_->task()->getHashJoinBridgeLocked(
          operatorCtx_->driverCtx()->splitGroupId,
          planNodeId())),
      mappedMemory_(operatorCtx_->mappedMemory()) {
  VELOX_CHECK_NOT_NULL(joinBridge_);
  joinBridge_->addBuilder();

  auto outputType = joinNode_->sources()[1]->outputType();

  auto numKeys = joinNode_->rightKeys().size();
  keyChannels_.reserve(numKeys);
  folly::F14FastSet<column_index_t> keyChannelSet;
  keyChannelSet.reserve(numKeys);
  std::vector<std::string> names;
  names.reserve(outputType->size());
  std::vector<TypePtr> types;
  types.reserve(outputType->size());

  for (auto& key : joinNode_->rightKeys()) {
    auto channel = exprToChannel(key.get(), outputType);
    keyChannelSet.emplace(channel);
    keyChannels_.emplace_back(channel);
    names.emplace_back(outputType->nameOf(channel));
    types.emplace_back(outputType->childAt(channel));
  }

  // Identify the non-key build side columns and make a decoder for each.
  const auto numDependents = outputType->size() - numKeys;
  dependentChannels_.reserve(numDependents);
  decoders_.reserve(numDependents);
  for (auto i = 0; i < outputType->size(); ++i) {
    if (keyChannelSet.find(i) == keyChannelSet.end()) {
      dependentChannels_.emplace_back(i);
      decoders_.emplace_back(std::make_unique<DecodedVector>());
      names.emplace_back(outputType->nameOf(i));
      types.emplace_back(outputType->childAt(i));
    }
  }

  tableType_ = ROW(std::move(names), std::move(types));
  setupTable();
}

void HashBuild::setupTable() {
  VELOX_CHECK_NULL(table_);

  const auto numKeys = keyChannels_.size();
  std::vector<std::unique_ptr<VectorHasher>> keyHashers;
  keyHashers.reserve(numKeys);
  for (vector_size_t i = 0; i < numKeys; ++i) {
    keyHashers.emplace_back(std::make_unique<VectorHasher>(
        tableType_->childAt(i), keyChannels_[i]));
  }

  const auto numDependents = tableType_->size() - numKeys;
  std::vector<TypePtr> dependentTypes;
  dependentTypes.reserve(numDependents);
  for (int i = numKeys; i < tableType_->size(); ++i) {
    dependentTypes.emplace_back(tableType_->childAt(i));
  }
  if (joinNode_->isRightJoin() || joinNode_->isFullJoin()) {
    // Do not ignore null keys.
    table_ = HashTable<false>::createForJoin(
        std::move(keyHashers),
        dependentTypes,
        true, // allowDuplicates
        true, // hasProbedFlag
        mappedMemory_);
  } else {
    // (Left) semi and anti join with no extra filter only needs to know whether
    // there is a match. Hence, no need to store entries with duplicate keys.
    const bool dropDuplicates = !joinNode_->filter() &&
        (joinNode_->isLeftSemiJoin() || joinNode_->isNullAwareAntiJoin());
    // Right semi join needs to tag build rows that were probed.
    const bool needProbedFlag = joinNode_->isRightSemiJoin();
    // Ignore null keys
    table_ = HashTable<true>::createForJoin(
        std::move(keyHashers),
        dependentTypes,
        !dropDuplicates, // allowDuplicates
        needProbedFlag, // hasProbedFlag
        mappedMemory_);
  }
  analyzeKeys_ = table_->hashMode() != BaseHashTable::HashMode::kHash;
}

void HashBuild::addInput(RowVectorPtr input) {
  activeRows_.resize(input->size());
  activeRows_.setAll();

  auto& hashers = table_->hashers();

  for (auto i = 0; i < hashers.size(); ++i) {
    auto key = input->childAt(hashers[i]->channel())->loadedVector();
    hashers[i]->decode(*key, activeRows_);
  }

  if (!isRightJoin(joinType_) && !isFullJoin(joinType_)) {
    deselectRowsWithNulls(hashers, activeRows_);
  }

  if (joinType_ == core::JoinType::kNullAwareAnti) {
    // Null-aware anti join returns no rows if build side has nulls in join
    // keys. Hence, we can stop processing on first null.
    if (activeRows_.countSelected() < input->size()) {
      antiJoinHasNullKeys_ = true;
      noMoreInput();
      return;
    }
  }

  if (analyzeKeys_ && hashes_.size() < activeRows_.size()) {
    hashes_.resize(activeRows_.size());
  }

  // As long as analyzeKeys is true, we keep running the keys through
  // the Vectorhashers so that we get a possible mapping of the keys
  // to small ints for array or normalized key. When mayUseValueIds is
  // false for the first time we stop. We do not retain the value ids
  // since the final ones will only be known after all data is
  // received.
  for (auto& hasher : hashers) {
    // TODO: Load only for active rows, except if right/full outer join.
    if (analyzeKeys_) {
      hasher->computeValueIds(activeRows_, hashes_);
      analyzeKeys_ = hasher->mayUseValueIds();
    }
  }
  for (auto i = 0; i < dependentChannels_.size(); ++i) {
    decoders_[i]->decode(
        *input->childAt(dependentChannels_[i])->loadedVector(), activeRows_);
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
    promise.setValue();
  }

  if (antiJoinHasNullKeys_) {
    joinBridge_->setAntiJoinHasNullKeys();
  } else {
    bool hasOthers = !otherTables.empty();
    table_->prepareJoinTable(
        std::move(otherTables),
        hasOthers ? operatorCtx_->task()->queryCtx()->executor() : nullptr);

    addRuntimeStats();
    joinBridge_->setHashTable(std::move(table_), {});
  }
}

void HashBuild::addRuntimeStats() {
  // Report range sizes and number of distinct values for the join keys.
  const auto& hashers = table_->hashers();
  uint64_t asRange;
  uint64_t asDistinct;
  for (auto i = 0; i < hashers.size(); i++) {
    hashers[i]->cardinality(0, asRange, asDistinct);
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
