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
#include "velox/exec/fuzzer/JoinMaker.h"

#include "velox/exec/OperatorUtils.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"

namespace facebook::velox::exec {
namespace {
// Get the batches of data to use in the join from `joinSource` given
// `inputType`.
const std::vector<RowVectorPtr>& getBatches(
    const std::shared_ptr<JoinSource>& joinSource,
    const JoinMaker::InputType inputType) {
  switch (inputType) {
    case JoinMaker::InputType::ENCODED:
      return joinSource->batches();
    case JoinMaker::InputType::FLAT:
      return joinSource->flatBatches();
    default:
      VELOX_UNREACHABLE();
  }
}

// Returns a PlanBuilder that reads from `joinSource` using a Values node,
// respecting `inputType`, and applying any necessary projections.
test::PlanBuilder makeJoinSourcePlan(
    const std::shared_ptr<JoinSource>& joinSource,
    const JoinMaker::InputType inputType,
    const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator) {
  return test::PlanBuilder(planNodeIdGenerator)
      .values(getBatches(joinSource, inputType))
      .projectExpressions(joinSource->projections());
}

// Breaks the batches from joinSource (respecting `inputType`) into up to 4
// partitions. And returns PlanNodes to read each of those partitions, applying
// any necessary projections.
std::vector<core::PlanNodePtr> makeSourcesForPartitionedJoinPlan(
    const std::shared_ptr<JoinSource> joinSource,
    const JoinMaker::InputType inputType,
    const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator) {
  const auto& batches = getBatches(joinSource, inputType);
  auto numSources = std::min<size_t>(4, batches.size());
  std::vector<std::vector<RowVectorPtr>> sourceInputs(numSources);
  for (auto i = 0; i < batches.size(); ++i) {
    sourceInputs[i % numSources].push_back(batches[i]);
  }

  std::vector<core::PlanNodePtr> sourceNodes;
  for (const auto& sourceInput : sourceInputs) {
    sourceNodes.push_back(test::PlanBuilder(planNodeIdGenerator)
                              .values(sourceInput)
                              .projectExpressions(joinSource->projections())
                              .planNode());
  }

  return sourceNodes;
}

// Returns a PlanBuilder that breaks `joinSource` into partitions, reads those
// and adds a localPartition step to apply `partitionStrategy`.
test::PlanBuilder makePartitionedJoinSourcePlan(
    const JoinMaker::PartitionStrategy partitionStrategy,
    const std::shared_ptr<JoinSource>& joinSource,
    const JoinMaker::InputType inputType,
    const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator) {
  switch (partitionStrategy) {
    case JoinMaker::PartitionStrategy::NONE:
      VELOX_FAIL(
          "makePartitionedJoinSource called with a partitionStrategy of NONE.");
    case JoinMaker::PartitionStrategy::HASH:
      return test::PlanBuilder(planNodeIdGenerator)
          .localPartition(
              joinSource->keys(),
              makeSourcesForPartitionedJoinPlan(
                  joinSource, inputType, planNodeIdGenerator));
    case JoinMaker::PartitionStrategy::ROUND_ROBIN:
      return test::PlanBuilder(planNodeIdGenerator)
          .localPartitionRoundRobin(makeSourcesForPartitionedJoinPlan(
              joinSource, inputType, planNodeIdGenerator));
    default:
      VELOX_UNREACHABLE();
  }
}

// Returns a PlanBuilder that reads from `joinSource` using a TableScan node,
// respecting `inputType` and converting it to splits, and applying any
// necessary projections.
// `tableScanId` is populated with the PlanNodeId for the TableScan.
test::PlanBuilder makeTableScanJoinSourcePlan(
    const std::shared_ptr<JoinSource>& joinSource,
    const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator,
    core::PlanNodeId& tableScanId) {
  return test::PlanBuilder(planNodeIdGenerator)
      .tableScan(joinSource->outputType())
      .capturePlanNodeId(tableScanId)
      .projectExpressions(joinSource->projections());
}

// Returns the reversed JoinType if the JoinType can be reversed, otherwise
// returns std::nullopt.
std::optional<core::JoinType> tryFlipJoinType(core::JoinType joinType) {
  switch (joinType) {
    case core::JoinType::kInner:
      return joinType;
    case core::JoinType::kLeft:
      return core::JoinType::kRight;
    case core::JoinType::kRight:
      return core::JoinType::kLeft;
    case core::JoinType::kFull:
      return joinType;
    case core::JoinType::kLeftSemiFilter:
      return core::JoinType::kRightSemiFilter;
    case core::JoinType::kLeftSemiProject:
      return core::JoinType::kRightSemiProject;
    case core::JoinType::kRightSemiFilter:
      return core::JoinType::kLeftSemiFilter;
    case core::JoinType::kRightSemiProject:
      return core::JoinType::kLeftSemiProject;
    default:
      return std::nullopt;
  }
}

// Returns an equality join filter between probeKeys and buildKeys.
std::string makeJoinFilter(
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys) {
  const auto numKeys = probeKeys.size();
  std::string filter;
  VELOX_CHECK_EQ(numKeys, buildKeys.size());
  for (auto i = 0; i < numKeys; ++i) {
    if (i > 0) {
      filter += " AND ";
    }
    filter += fmt::format("{} = {}", probeKeys[i], buildKeys[i]);
  }
  return filter;
}
} // namespace

const std::vector<RowVectorPtr>& JoinSource::flatBatches() const {
  if (!flatBatches_.has_value()) {
    flatBatches_ = flatten(batches_);
  }

  return *flatBatches_;
}

const std::vector<Split>& JoinSource::splits() const {
  if (!splits_.has_value()) {
    splits_ = test::makeSplits(batches_, splitsDir_, writerPool_);
  }

  return *splits_;
}

const std::vector<Split>& JoinSource::groupedSplits(
    const int32_t numGroups) const {
  auto it = groupedSplits_.find(numGroups);

  if (it != groupedSplits_.end()) {
    return it->second;
  }

  const std::vector<std::vector<RowVectorPtr>> inputVectorsByGroup =
      splitInputByGroup(numGroups);

  std::vector<exec::Split> splitsWithGroup;
  for (int32_t groupId = 0; groupId < numGroups; ++groupId) {
    for (auto i = 0; i < inputVectorsByGroup[groupId].size(); ++i) {
      const std::string filePath = fmt::format(
          "{}/grouped[{}].{}.{}", splitsDir_, groupId, numGroups, i);
      test::writeToFile(
          filePath, inputVectorsByGroup[groupId][i], writerPool_.get());
      splitsWithGroup.emplace_back(test::makeConnectorSplit(filePath), groupId);
    }
    splitsWithGroup.emplace_back(nullptr, groupId);
  }

  return groupedSplits_.insert(std::make_pair(numGroups, splitsWithGroup))
      .first->second;
}

std::vector<RowVectorPtr> JoinSource::flatten(
    const std::vector<RowVectorPtr>& vectors) const {
  std::vector<RowVectorPtr> flatVectors;
  for (const auto& vector : vectors) {
    auto flat = BaseVector::create<RowVector>(
        vector->type(), vector->size(), vector->pool());
    flat->copy(vector.get(), 0, 0, vector->size());
    flatVectors.push_back(flat);
  }

  return flatVectors;
}

std::vector<std::vector<RowVectorPtr>> JoinSource::splitInputByGroup(
    int32_t numGroups) const {
  if (numGroups == 1) {
    return {batches_};
  }

  // Partition 'input' based on the join keys for group execution with one
  // partition per each group.
  std::vector<column_index_t> partitionChannels(keys_.size());
  std::iota(partitionChannels.begin(), partitionChannels.end(), 0);
  std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
  hashers.reserve(keys_.size());
  for (auto channel : partitionChannels) {
    hashers.emplace_back(
        exec::VectorHasher::create(outputType_->childAt(channel), channel));
  }

  std::vector<std::vector<RowVectorPtr>> inputsByGroup{
      static_cast<size_t>(numGroups)};
  raw_vector<uint64_t> groupHashes;
  std::vector<BufferPtr> groupRows(numGroups);
  std::vector<vector_size_t*> rawGroupRows(numGroups);
  std::vector<vector_size_t> groupSizes(numGroups, 0);
  SelectivityVector inputRows;

  for (const auto& input : batches_) {
    const int numRows = input->size();
    inputRows.resize(numRows);
    inputRows.setAll();
    groupHashes.resize(numRows);
    std::fill(groupSizes.begin(), groupSizes.end(), 0);
    std::fill(groupHashes.begin(), groupHashes.end(), 0);

    for (auto i = 0; i < hashers.size(); ++i) {
      auto& hasher = hashers[i];
      auto* keyVector = input->childAt(hashers[i]->channel())->loadedVector();
      hashers[i]->decode(*keyVector, inputRows);
      if (hasher->channel() != kConstantChannel) {
        hashers[i]->hash(inputRows, i > 0, groupHashes);
      } else {
        hashers[i]->hashPrecomputed(inputRows, i > 0, groupHashes);
      }
    }

    for (int row = 0; row < numRows; ++row) {
      const int32_t groupId = groupHashes[row] % numGroups;
      if (groupRows[groupId] == nullptr ||
          (groupRows[groupId]->capacity() < numRows * sizeof(vector_size_t))) {
        groupRows[groupId] = allocateIndices(numRows, input->pool());
        rawGroupRows[groupId] = groupRows[groupId]->asMutable<vector_size_t>();
      }
      rawGroupRows[groupId][groupSizes[groupId]++] = row;
    }

    for (int32_t groupId = 0; groupId < numGroups; ++groupId) {
      const size_t groupSize = groupSizes[groupId];
      if (groupSize != 0) {
        VELOX_CHECK_NOT_NULL(groupRows[groupId]);
        groupRows[groupId]->setSize(
            groupSizes[groupId] * sizeof(vector_size_t));
        inputsByGroup[groupId].push_back(
            (groupSize == numRows)
                ? input
                : exec::wrap(groupSize, std::move(groupRows[groupId]), input));
      }
    }
  }
  return inputsByGroup;
}

std::vector<std::shared_ptr<JoinSource>> JoinMaker::getJoinSources(
    const JoinOrder joinOrder) const {
  auto joinSources = sources_;

  switch (joinOrder) {
    case JoinMaker::JoinOrder::NATURAL:
      break;
    case JoinMaker::JoinOrder::FLIPPED:
      std::reverse(joinSources.begin(), joinSources.end());
      break;
    default:
      VELOX_UNREACHABLE();
  }

  return joinSources;
}

core::JoinType JoinMaker::getJoinType(const JoinOrder joinOrder) const {
  switch (joinOrder) {
    case JoinMaker::JoinOrder::NATURAL:
      return joinType_;
    case JoinMaker::JoinOrder::FLIPPED:
      return flipJoinType(joinType_);
    default:
      VELOX_UNREACHABLE();
  }
}

std::string JoinMaker::makeNestedLoopJoinFilter(
    const std::shared_ptr<JoinSource>& left,
    const std::shared_ptr<JoinSource>& right) const {
  const std::string keysFilter = makeJoinFilter(left->keys(), right->keys());
  return filter_.empty() ? keysFilter
                         : fmt::format("{} AND {}", keysFilter, filter_);
}

core::PlanNodePtr JoinMaker::makeHashJoinPlan(
    test::PlanBuilder& probePlan,
    test::PlanBuilder& buildPlan,
    const std::shared_ptr<JoinSource>& probeSource,
    const std::shared_ptr<JoinSource>& buildSource,
    const core::JoinType joinType) const {
  return probePlan
      .hashJoin(
          probeSource->keys(),
          buildSource->keys(),
          buildPlan.planNode(),
          filter_,
          outputColumns_,
          joinType,
          nullAware_)
      .planNode();
}

core::PlanNodePtr JoinMaker::makeMergeJoinPlan(
    test::PlanBuilder& probePlan,
    test::PlanBuilder& buildPlan,
    const std::shared_ptr<JoinSource>& probeSource,
    const std::shared_ptr<JoinSource>& buildSource,
    const core::JoinType joinType) const {
  return probePlan.orderBy(probeSource->keys(), false)
      .mergeJoin(
          probeSource->keys(),
          buildSource->keys(),
          buildPlan.orderBy(buildSource->keys(), false).planNode(),
          filter_,
          outputColumns_,
          joinType)
      .planNode();
}

core::PlanNodePtr JoinMaker::makeNestedLoopJoinPlan(
    test::PlanBuilder& probePlan,
    test::PlanBuilder& buildPlan,
    const std::shared_ptr<JoinSource>& probeSource,
    const std::shared_ptr<JoinSource>& buildSource,
    const core::JoinType joinType) const {
  return probePlan
      .nestedLoopJoin(
          buildPlan.planNode(),
          makeNestedLoopJoinFilter(probeSource, buildSource),
          outputColumns_,
          joinType)
      .planNode();
}

JoinMaker::PlanWithSplits JoinMaker::makeHashJoin(
    const InputType inputType,
    const PartitionStrategy partitionStrategy,
    const JoinOrder joinOrder) const {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto joinSources = getJoinSources(joinOrder);

  const auto& probeSource = joinSources[0];
  const auto& buildSource = joinSources[1];

  test::PlanBuilder probeSourcePlan;
  test::PlanBuilder buildSourcePlan;

  if (partitionStrategy == PartitionStrategy::NONE) {
    probeSourcePlan =
        makeJoinSourcePlan(probeSource, inputType, planNodeIdGenerator);
    buildSourcePlan =
        makeJoinSourcePlan(buildSource, inputType, planNodeIdGenerator);
  } else {
    probeSourcePlan = makePartitionedJoinSourcePlan(
        partitionStrategy, probeSource, inputType, planNodeIdGenerator);
    buildSourcePlan = makePartitionedJoinSourcePlan(
        partitionStrategy, buildSource, inputType, planNodeIdGenerator);
  }

  return PlanWithSplits(makeHashJoinPlan(
      probeSourcePlan,
      buildSourcePlan,
      probeSource,
      buildSource,
      getJoinType(joinOrder)));
}

JoinMaker::PlanWithSplits JoinMaker::makeHashJoinWithTableScan(
    std::optional<int32_t> numGroups,
    bool mixedGroupedExecution,
    JoinOrder joinOrder) const {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto joinSources = getJoinSources(joinOrder);

  const auto& probeSource = joinSources[0];
  const auto& buildSource = joinSources[1];

  core::PlanNodeId probeTableScanId;
  core::PlanNodeId buildTableScanId;

  test::PlanBuilder probeSourcePlan = makeTableScanJoinSourcePlan(
      probeSource, planNodeIdGenerator, probeTableScanId);
  test::PlanBuilder buildSourcePlan = makeTableScanJoinSourcePlan(
      buildSource, planNodeIdGenerator, buildTableScanId);

  const auto plan = makeHashJoinPlan(
      probeSourcePlan,
      buildSourcePlan,
      probeSource,
      buildSource,
      getJoinType(joinOrder));

  if (mixedGroupedExecution) {
    return PlanWithSplits(
        plan,
        probeTableScanId,
        buildTableScanId,
        {{probeTableScanId, probeSource->groupedSplits(*numGroups)},
         {buildTableScanId, buildSource->splits()}},
        core::ExecutionStrategy::kGrouped,
        *numGroups,
        mixedGroupedExecution);
  }

  if (!numGroups.has_value()) {
    return PlanWithSplits(
        plan,
        probeTableScanId,
        buildTableScanId,
        {{probeTableScanId, probeSource->splits()},
         {buildTableScanId, buildSource->splits()}});
  } else {
    return PlanWithSplits(
        plan,
        probeTableScanId,
        buildTableScanId,
        {{probeTableScanId, probeSource->groupedSplits(*numGroups)},
         {buildTableScanId, buildSource->groupedSplits(*numGroups)}},
        core::ExecutionStrategy::kGrouped,
        *numGroups);
  }
}

JoinMaker::PlanWithSplits JoinMaker::makeMergeJoin(
    const InputType inputType,
    const JoinOrder joinOrder) const {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto joinSources = getJoinSources(joinOrder);

  const auto& probeSource = joinSources[0];
  const auto& buildSource = joinSources[1];

  auto probeSourcePlan =
      makeJoinSourcePlan(probeSource, inputType, planNodeIdGenerator);
  auto buildSourcePlan =
      makeJoinSourcePlan(buildSource, inputType, planNodeIdGenerator);

  return PlanWithSplits(makeMergeJoinPlan(
      probeSourcePlan,
      buildSourcePlan,
      probeSource,
      buildSource,
      getJoinType(joinOrder)));
}

JoinMaker::PlanWithSplits JoinMaker::makeMergeJoinWithTableScan(
    const JoinOrder joinOrder) const {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto joinSources = getJoinSources(joinOrder);

  const auto& probeSource = joinSources[0];
  const auto& buildSource = joinSources[1];

  core::PlanNodeId probeTableScanId;
  core::PlanNodeId buildTableScanId;

  test::PlanBuilder probeSourcePlan = makeTableScanJoinSourcePlan(
      probeSource, planNodeIdGenerator, probeTableScanId);
  test::PlanBuilder buildSourcePlan = makeTableScanJoinSourcePlan(
      buildSource, planNodeIdGenerator, buildTableScanId);

  const auto plan = makeMergeJoinPlan(
      probeSourcePlan,
      buildSourcePlan,
      probeSource,
      buildSource,
      getJoinType(joinOrder));

  return PlanWithSplits(
      plan,
      probeTableScanId,
      buildTableScanId,
      {{probeTableScanId, probeSource->splits()},
       {buildTableScanId, buildSource->splits()}});
}

JoinMaker::PlanWithSplits JoinMaker::makeNestedLoopJoin(
    const InputType inputType,
    const JoinOrder joinOrder) const {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto joinSources = getJoinSources(joinOrder);

  const auto& probeSource = joinSources[0];
  const auto& buildSource = joinSources[1];

  auto probeSourcePlan =
      makeJoinSourcePlan(probeSource, inputType, planNodeIdGenerator);
  auto buildSourcePlan =
      makeJoinSourcePlan(buildSource, inputType, planNodeIdGenerator);

  return PlanWithSplits(makeNestedLoopJoinPlan(
      probeSourcePlan,
      buildSourcePlan,
      probeSource,
      buildSource,
      getJoinType(joinOrder)));
}

JoinMaker::PlanWithSplits JoinMaker::makeNestedLoopJoinWithTableScan(
    const JoinOrder joinOrder) const {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto joinSources = getJoinSources(joinOrder);

  const auto& probeSource = joinSources[0];
  const auto& buildSource = joinSources[1];

  core::PlanNodeId probeTableScanId;
  core::PlanNodeId buildTableScanId;

  test::PlanBuilder probeSourcePlan = makeTableScanJoinSourcePlan(
      probeSource, planNodeIdGenerator, probeTableScanId);
  test::PlanBuilder buildSourcePlan = makeTableScanJoinSourcePlan(
      buildSource, planNodeIdGenerator, buildTableScanId);

  const auto plan = makeNestedLoopJoinPlan(
      probeSourcePlan,
      buildSourcePlan,
      probeSource,
      buildSource,
      getJoinType(joinOrder));

  return PlanWithSplits(
      plan,
      probeTableScanId,
      buildTableScanId,
      {{probeTableScanId, probeSource->splits()},
       {buildTableScanId, buildSource->splits()}});
}

bool JoinMaker::supportsFlippingHashJoin() const {
  //  Null-aware right semi project join doesn't support filter.
  if (!filter_.empty() && joinType_ == core::JoinType::kLeftSemiProject &&
      nullAware_) {
    return false;
  }

  return tryFlipJoinType(joinType_).has_value();
}

bool JoinMaker::supportsFlippingMergeJoin() const {
  const auto flippedJoinType = tryFlipJoinType(joinType_);

  if (!flippedJoinType.has_value()) {
    return false;
  }

  return core::MergeJoinNode::isSupported(*flippedJoinType);
}

bool JoinMaker::supportsFlippingNestedLoopJoin() const {
  const auto flippedJoinType = tryFlipJoinType(joinType_);

  if (!flippedJoinType.has_value()) {
    return false;
  }

  return core::NestedLoopJoinNode::isSupported(*flippedJoinType);
}

bool JoinMaker::supportsTableScan() const {
  for (const auto& source : sources_) {
    if (!test::isTableScanSupported(source->outputType())) {
      return false;
    }
  }

  return true;
}

core::JoinType flipJoinType(core::JoinType joinType) {
  auto flippedJoinType = tryFlipJoinType(joinType);

  if (!flippedJoinType.has_value()) {
    VELOX_UNSUPPORTED(fmt::format("Unable to flip join type: {}", joinType));
  }

  return *flippedJoinType;
}
} // namespace facebook::velox::exec
