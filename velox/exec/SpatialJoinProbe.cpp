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
#include "velox/exec/SpatialJoinProbe.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/SpatialJoinBuild.h"
#include "velox/exec/Task.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::exec {
namespace {

bool needsProbeMismatch(core::JoinType joinType) {
  return isLeftJoin(joinType);
}

std::vector<IdentityProjection> extractProjections(
    const RowTypePtr& srcType,
    const RowTypePtr& destType) {
  std::vector<IdentityProjection> projections;
  for (auto i = 0; i < srcType->size(); ++i) {
    auto name = srcType->nameOf(i);
    auto outIndex = destType->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      projections.emplace_back(i, outIndex.value());
    }
  }
  return projections;
}

} // namespace

SpatialJoinProbe::SpatialJoinProbe(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::SpatialJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "SpatialJoinProbe"),
      joinType_(joinNode->joinType()),
      outputBatchSize_{outputBatchRows()},
      joinNode_(joinNode) {
  auto probeType = joinNode_->sources()[0]->outputType();
  auto buildType = joinNode_->sources()[1]->outputType();
  identityProjections_ = extractProjections(probeType, outputType_);
  buildProjections_ = extractProjections(buildType, outputType_);
}

void SpatialJoinProbe::initialize() {
  Operator::initialize();

  VELOX_CHECK(joinNode_ != nullptr);
  if (joinNode_->joinCondition() != nullptr) {
    initializeFilter(
        joinNode_->joinCondition(),
        joinNode_->sources()[0]->outputType(),
        joinNode_->sources()[1]->outputType());
  }

  joinNode_.reset();
}

void SpatialJoinProbe::initializeFilter(
    const core::TypedExprPtr& filter,
    const RowTypePtr& probeType,
    const RowTypePtr& buildType) {
  VELOX_CHECK_NULL(joinCondition_);

  std::vector<core::TypedExprPtr> filters = {filter};
  joinCondition_ =
      std::make_unique<ExprSet>(std::move(filters), operatorCtx_->execCtx());

  column_index_t filterChannel = 0;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  const auto numFields = joinCondition_->expr(0)->distinctFields().size();
  names.reserve(numFields);
  types.reserve(numFields);

  for (const auto& field : joinCondition_->expr(0)->distinctFields()) {
    const auto& name = field->field();
    auto channel = probeType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterProbeProjections_.emplace_back(channelValue, filterChannel++);
      names.emplace_back(probeType->nameOf(channelValue));
      types.emplace_back(probeType->childAt(channelValue));
      continue;
    }
    channel = buildType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterBuildProjections_.emplace_back(channelValue, filterChannel++);
      names.emplace_back(buildType->nameOf(channelValue));
      types.emplace_back(buildType->childAt(channelValue));
      continue;
    }
    VELOX_FAIL(
        "Spatial join filter field {} not in probe or build input, filter: {}",
        field->toString(),
        filter->toString());
  }

  filterInputType_ = ROW(std::move(names), std::move(types));
}

BlockingReason SpatialJoinProbe::isBlocked(ContinueFuture* future) {
  switch (state_) {
    case ProbeOperatorState::kRunning:
      [[fallthrough]];
    case ProbeOperatorState::kFinish:
      return BlockingReason::kNotBlocked;
    case ProbeOperatorState::kWaitForPeers:
      if (future_.valid()) {
        *future = std::move(future_);
        return BlockingReason::kWaitForJoinProbe;
      }
      setState(ProbeOperatorState::kFinish);
      return BlockingReason::kNotBlocked;
    case ProbeOperatorState::kWaitForBuild: {
      VELOX_CHECK(!buildVectors_.has_value());
      if (!getBuildData(future)) {
        return BlockingReason::kWaitForJoinBuild;
      }
      VELOX_CHECK(buildVectors_.has_value());
      setState(ProbeOperatorState::kRunning);
      return BlockingReason::kNotBlocked;
    }
    default:
      VELOX_UNREACHABLE(probeOperatorStateName(state_));
  }
}

void SpatialJoinProbe::close() {
  if (joinCondition_ != nullptr) {
    joinCondition_->clear();
  }
  buildVectors_.reset();
  Operator::close();
}

void SpatialJoinProbe::addInput(RowVectorPtr input) {
  VELOX_CHECK_NULL(input_);

  // In getOutput(), we are going to wrap input in dictionaries a few rows at a
  // time. Since lazy vectors cannot be wrapped in different dictionaries, we
  // are going to load them here.
  for (auto& child : input->children()) {
    child->loadedVector();
  }
  input_ = std::move(input);
  if (input_->size() > 0) {
    probeSideEmpty_ = false;
  }
  VELOX_CHECK_EQ(buildIndex_, 0);
}

void SpatialJoinProbe::noMoreInput() {
  Operator::noMoreInput();
  if (state_ != ProbeOperatorState::kRunning || input_ != nullptr) {
    return;
  }
  setState(ProbeOperatorState::kFinish);
  return;
}

bool SpatialJoinProbe::getBuildData(ContinueFuture* future) {
  VELOX_CHECK(!buildVectors_.has_value());

  auto buildData =
      operatorCtx_->task()
          ->getSpatialJoinBridge(
              operatorCtx_->driverCtx()->splitGroupId, planNodeId())
          ->dataOrFuture(future);
  if (!buildData.has_value()) {
    return false;
  }

  buildVectors_ = std::move(buildData);
  return true;
}

RowVectorPtr SpatialJoinProbe::getOutput() {
  if (state_ == ProbeOperatorState::kFinish ||
      state_ == ProbeOperatorState::kWaitForPeers) {
    return nullptr;
  }
  RowVectorPtr output{nullptr};
  while (output == nullptr) {
    // Need more input.
    if (input_ == nullptr) {
      break;
    }

    // Generate actual join output by processing probe and build matches, and
    // probe mismaches (for left joins).
    output = generateOutput();
  }
  return output;
}

RowVectorPtr SpatialJoinProbe::generateOutput() {
  // If addToOutput() returns false, output_ is filled. Need to produce it.
  if (!addToOutput()) {
    VELOX_CHECK_GT(output_->size(), 0);
    return std::move(output_);
  }

  // Try to advance the probe cursor; call finish if no more probe input.
  if (advanceProbe()) {
    finishProbeInput();
    if (numOutputRows_ == 0) {
      // output_ can only be re-used across probe rows within the same input_.
      // Here we have to abandon the emtpy non-null output_ before we advance to
      // the next probe input.
      output_ = nullptr;
    }
  }

  if (!readyToProduceOutput()) {
    return nullptr;
  }

  output_->resize(numOutputRows_);
  return std::move(output_);
}

bool SpatialJoinProbe::readyToProduceOutput() {
  if (!output_ || numOutputRows_ == 0) {
    return false;
  }

  // If the input_ has no remaining rows or the output_ is fully filled,
  // it's right time for output.
  return !input_ || numOutputRows_ >= outputBatchSize_;
}

bool SpatialJoinProbe::advanceProbe() {
  if (hasProbedAllBuildData()) {
    probeRow_ += 1;
    probeRowHasMatch_ = false;
    buildIndex_ = 0;

    // If we finished processing the probe side.
    if (probeRow_ >= input_->size()) {
      return true;
    }
  }
  return false;
}

bool SpatialJoinProbe::addToOutput() {
  VELOX_CHECK_NOT_NULL(input_);
  prepareOutput();

  while (!hasProbedAllBuildData()) {
    const auto& currentBuild = buildVectors_.value()[buildIndex_];

    // Empty build vector; move to the next.
    if (currentBuild->size() == 0) {
      ++buildIndex_;
      buildRow_ = 0;
      continue;
    }

    // Only re-calculate the filter if we have a new build vector.
    if (buildRow_ == 0) {
      evaluateSpatialJoinFilter(currentBuild);
    }

    // Iterate over the filter results. For each match, add an output record.
    for (vector_size_t i = buildRow_; i < decodedFilterResult_.size(); ++i) {
      if (!isSpatialJoinConditionMatch(i)) {
        continue;
      }

      addOutputRow(i);
      ++numOutputRows_;
      probeRowHasMatch_ = true;

      // If the buffer is full, save state and produce it as output.
      if (numOutputRows_ == outputBatchSize_) {
        buildRow_ = i + 1;
        copyBuildValues(currentBuild);
        return false;
      }
    }

    // Before moving to the next build vector, copy the needed ranges.
    copyBuildValues(currentBuild);
    ++buildIndex_;
    buildRow_ = 0;
  }

  // Check if the current probed row needs to be added as a mismatch (for left
  // and full outer joins).
  checkProbeMismatchRow();

  // Signals that all input has been generated for the probeRow and build
  // vectors; safe to move to the next probe record.
  return true;
}

void SpatialJoinProbe::prepareOutput() {
  if (output_ != nullptr) {
    return;
  }

  std::vector<VectorPtr> localColumns(outputType_->size());

  probeOutputIndices_ = allocateIndices(outputBatchSize_, pool());
  rawProbeOutputIndices_ = probeOutputIndices_->asMutable<vector_size_t>();

  for (const auto& projection : identityProjections_) {
    localColumns[projection.outputChannel] = BaseVector::wrapInDictionary(
        {},
        probeOutputIndices_,
        outputBatchSize_,
        input_->childAt(projection.inputChannel));
  }

  // For other join types, add build side projections
  for (const auto& projection : buildProjections_) {
    localColumns[projection.outputChannel] = BaseVector::create(
        outputType_->childAt(projection.outputChannel),
        outputBatchSize_,
        operatorCtx_->pool());
  }

  numOutputRows_ = 0;
  output_ = std::make_shared<RowVector>(
      pool(), outputType_, nullptr, outputBatchSize_, std::move(localColumns));
}

void SpatialJoinProbe::evaluateSpatialJoinFilter(
    const RowVectorPtr& buildVector) {
  // First step to process is to get a batch so we can evaluate the join
  // filter.
  auto filterInput = getNextCrossProductBatch(
      buildVector,
      filterInputType_,
      filterProbeProjections_,
      filterBuildProjections_);

  if (filterInputRows_.size() != filterInput->size()) {
    filterInputRows_.resizeFill(filterInput->size(), true);
  }
  VELOX_CHECK(filterInputRows_.isAllSelected());

  std::vector<VectorPtr> filterResult;
  EvalCtx evalCtx(
      operatorCtx_->execCtx(), joinCondition_.get(), filterInput.get());
  joinCondition_->eval(0, 1, true, filterInputRows_, evalCtx, filterResult);
  VELOX_CHECK_GT(filterResult.size(), 0);
  filterOutput_ = filterResult[0];
  decodedFilterResult_.decode(*filterOutput_, filterInputRows_);
}

RowVectorPtr SpatialJoinProbe::getNextCrossProductBatch(
    const RowVectorPtr& buildVector,
    const RowTypePtr& outputType,
    const std::vector<IdentityProjection>& probeProjections,
    const std::vector<IdentityProjection>& buildProjections) {
  VELOX_CHECK_GT(buildVector->size(), 0);

  return genCrossProductMultipleBuildVectors(
      buildVector, outputType, probeProjections, buildProjections);
}

RowVectorPtr SpatialJoinProbe::genCrossProductMultipleBuildVectors(
    const RowVectorPtr& buildVector,
    const RowTypePtr& outputType,
    const std::vector<IdentityProjection>& probeProjections,
    const std::vector<IdentityProjection>& buildProjections) {
  std::vector<VectorPtr> projectedChildren(outputType->size());
  const vector_size_t numOutputRows = buildVector->size();

  // Project columns from the build side.
  projectChildren(
      projectedChildren, buildVector, buildProjections, numOutputRows, nullptr);

  // Wrap projections from the probe side as constants.
  for (const auto [inputChannel, outputChannel] : probeProjections) {
    projectedChildren[outputChannel] = BaseVector::wrapInConstant(
        numOutputRows, probeRow_, input_->childAt(inputChannel));
  }

  return std::make_shared<RowVector>(
      pool(), outputType, nullptr, numOutputRows, std::move(projectedChildren));
}

void SpatialJoinProbe::addOutputRow(vector_size_t buildRow) {
  // Probe side is always a dictionary; just populate the index.
  rawProbeOutputIndices_[numOutputRows_] = probeRow_;

  // For the build side, we accumulate the ranges to copy, then copy all of them
  // at once. If records are consecutive and can have a single copy range run.
  if (!buildCopyRanges_.empty() &&
      (buildCopyRanges_.back().sourceIndex + buildCopyRanges_.back().count) ==
          buildRow) {
    ++buildCopyRanges_.back().count;
  } else {
    buildCopyRanges_.push_back({buildRow, numOutputRows_, 1});
  }
}

void SpatialJoinProbe::copyBuildValues(const RowVectorPtr& buildVector) {
  if (buildCopyRanges_.empty() || isLeftSemiProjectJoin(joinType_)) {
    return;
  }

  for (const auto& projection : buildProjections_) {
    const auto& buildChild = buildVector->childAt(projection.inputChannel);
    const auto& outputChild = output_->childAt(projection.outputChannel);
    outputChild->copyRanges(buildChild.get(), buildCopyRanges_);
  }
  buildCopyRanges_.clear();
}

void SpatialJoinProbe::checkProbeMismatchRow() {
  // If we are processing the last batch of the build side, check if we need
  // to add a probe mismatch record.
  if (needsProbeMismatch(joinType_) && hasProbedAllBuildData() &&
      !probeRowHasMatch_) {
    prepareOutput();
    addProbeMismatchRow();
    ++numOutputRows_;
  }
}

void SpatialJoinProbe::addProbeMismatchRow() {
  // Probe side is always a dictionary; just populate the index.
  rawProbeOutputIndices_[numOutputRows_] = probeRow_;

  // Null out build projections.
  for (const auto& projection : buildProjections_) {
    const auto& outputChild = output_->childAt(projection.outputChannel);
    outputChild->setNull(numOutputRows_, true);
  }
}

void SpatialJoinProbe::finishProbeInput() {
  VELOX_CHECK_NOT_NULL(input_);
  input_.reset();
  buildIndex_ = 0;
  probeRow_ = 0;

  if (!noMoreInput_) {
    return;
  }

  setState(ProbeOperatorState::kFinish);
  return;
}

} // namespace facebook::velox::exec
