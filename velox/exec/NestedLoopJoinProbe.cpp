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
#include "velox/exec/NestedLoopJoinProbe.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::exec {
namespace {

bool needsProbeMismatch(core::JoinType joinType) {
  return isLeftJoin(joinType) || isFullJoin(joinType);
}

bool needsBuildMismatch(core::JoinType joinType) {
  return isRightJoin(joinType) || isFullJoin(joinType);
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

NestedLoopJoinProbe::NestedLoopJoinProbe(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::NestedLoopJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "NestedLoopJoinProbe"),
      outputBatchSize_{outputBatchRows()},
      joinNode_(joinNode),
      joinType_(joinNode_->joinType()) {
  auto probeType = joinNode_->sources()[0]->outputType();
  auto buildType = joinNode_->sources()[1]->outputType();
  identityProjections_ = extractProjections(probeType, outputType_);
  buildProjections_ = extractProjections(buildType, outputType_);
}

void NestedLoopJoinProbe::initialize() {
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

void NestedLoopJoinProbe::initializeFilter(
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
        "Join filter field {} not in probe or build input, filter: {}",
        field->toString(),
        filter->toString());
  }

  filterInputType_ = ROW(std::move(names), std::move(types));
}

BlockingReason NestedLoopJoinProbe::isBlocked(ContinueFuture* future) {
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

      // If we just got build data, check if this is a right or full join where
      // we need to hit track of hits on build records. If it is, initialize the
      // selectivity vectors that do so.
      if (needsBuildMismatch(joinType_)) {
        buildMatched_.resize(buildVectors_->size());
        for (auto i = 0; i < buildVectors_->size(); ++i) {
          buildMatched_[i].resizeFill(buildVectors_.value()[i]->size(), false);
        }
      }

      setState(ProbeOperatorState::kRunning);
      return BlockingReason::kNotBlocked;
    }
    default:
      VELOX_UNREACHABLE(probeOperatorStateName(state_));
  }
}

void NestedLoopJoinProbe::close() {
  if (joinCondition_ != nullptr) {
    joinCondition_->clear();
  }
  buildVectors_.reset();
  Operator::close();
}

void NestedLoopJoinProbe::addInput(RowVectorPtr input) {
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

void NestedLoopJoinProbe::noMoreInput() {
  Operator::noMoreInput();
  if (state_ != ProbeOperatorState::kRunning || input_ != nullptr) {
    return;
  }
  if (!needsBuildMismatch(joinType_)) {
    setState(ProbeOperatorState::kFinish);
    return;
  }
  beginBuildMismatch();
}

bool NestedLoopJoinProbe::getBuildData(ContinueFuture* future) {
  VELOX_CHECK(!buildVectors_.has_value());

  auto buildData =
      operatorCtx_->task()
          ->getNestedLoopJoinBridge(
              operatorCtx_->driverCtx()->splitGroupId, planNodeId())
          ->dataOrFuture(future);
  if (!buildData.has_value()) {
    return false;
  }

  buildVectors_ = std::move(buildData);
  return true;
}

RowVectorPtr NestedLoopJoinProbe::getOutput() {
  if (state_ == ProbeOperatorState::kFinish ||
      state_ == ProbeOperatorState::kWaitForPeers) {
    return nullptr;
  }
  RowVectorPtr output{nullptr};
  while (output == nullptr) {
    // If we are done processing all build and probe data, and this is the
    // operator producing build mismatches (only for right a full outer join).
    if (lastProbe_) {
      VELOX_CHECK(processingBuildMismatch());

      // Scans build input producing build mismatches by wrapping dictionaries
      // to build input, and null constant to probe projections.
      while (output == nullptr && !hasProbedAllBuildData()) {
        output = getBuildMismatchedOutput(
            buildVectors_.value()[buildIndex_],
            buildMatched_[buildIndex_],
            buildOutMapping_,
            buildProjections_,
            identityProjections_);
        ++buildIndex_;
      }
      if (hasProbedAllBuildData()) {
        setState(ProbeOperatorState::kFinish);
      }
      break;
    }

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

RowVectorPtr NestedLoopJoinProbe::generateOutput() {
  // If addToOutput() returns false, output_ is filled. Need to produce it.
  if (!addToOutput()) {
    VELOX_CHECK_GT(output_->size(), 0);
    return std::move(output_);
  }

  // Try to advance the probe cursor; call finish if no more probe input.
  if (advanceProbe()) {
    finishProbeInput();
  }

  if (output_ != nullptr && output_->size() == 0) {
    output_ = nullptr;
  }
  return std::move(output_);
}

bool NestedLoopJoinProbe::advanceProbe() {
  if (hasProbedAllBuildData()) {
    probeRow_ += probeRowCount_;
    probeRowHasMatch_ = false;
    buildIndex_ = 0;

    // If we finished processing the probe side.
    if (probeRow_ >= input_->size()) {
      return true;
    }
  }
  return false;
}

// Main join loop.
bool NestedLoopJoinProbe::addToOutput() {
  VELOX_CHECK_NOT_NULL(input_);

  // First, create a new output vector. By default, allocate space for
  // outputBatchSize_ rows. The output always generates dictionaries wrapped
  // around the probe vector being processed.
  //
  // Since cross join batches can be returned without filter evaluation, no need
  // to prepare output here.
  if (!isCrossJoin()) {
    prepareOutput();
  }

  while (!hasProbedAllBuildData()) {
    const auto& currentBuild = buildVectors_.value()[buildIndex_];

    // Empty build vector; move to the next.
    if (currentBuild->size() == 0) {
      ++buildIndex_;
      buildRow_ = 0;
      continue;
    }

    // If this is a cross join, there is no filter to evaluate. We can just
    // return the output vector directly. Also don't need to bother about adding
    // mismatched rows.
    if (isCrossJoin()) {
      output_ = getNextCrossProductBatch(
          currentBuild, outputType_, identityProjections_, buildProjections_);
      numOutputRows_ = output_->size();
      probeRowHasMatch_ = true;
      ++buildIndex_;
      buildRow_ = 0;
      return false;
    }

    // Only re-calculate the filter if we have a new build vector.
    if (buildRow_ == 0) {
      evaluateJoinFilter(currentBuild);
    }

    // Iterate over the filter results. For each match, add an output record.
    for (size_t i = buildRow_; i < decodedFilterResult_.size(); ++i) {
      if (isJoinConditionMatch(i)) {
        addOutputRow(i);
        ++numOutputRows_;
        probeRowHasMatch_ = true;

        // If this is a right or full join, we need to keep track of the build
        // records that got a hit (key match), so that at end we know which
        // build records to add and which to skip.
        if (needsBuildMismatch(joinType_)) {
          buildMatched_[buildIndex_].setValid(i, true);
        }

        // If the buffer is full, save state and produce it as output.
        if (numOutputRows_ == outputBatchSize_) {
          buildRow_ = i + 1;
          copyBuildValues(currentBuild);
          return false;
        }
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
  if (output_ != nullptr) {
    output_->resize(numOutputRows_);
  }

  // Signals that all input has been generated for the probeRow and build
  // vectors; safe to move to the next probe record.
  return true;
}

void NestedLoopJoinProbe::prepareOutput() {
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

void NestedLoopJoinProbe::evaluateJoinFilter(const RowVectorPtr& buildVector) {
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
  filterOutput_ = filterResult[0];
  decodedFilterResult_.decode(*filterOutput_, filterInputRows_);
}

RowVectorPtr NestedLoopJoinProbe::getNextCrossProductBatch(
    const RowVectorPtr& buildVector,
    const RowTypePtr& outputType,
    const std::vector<IdentityProjection>& probeProjections,
    const std::vector<IdentityProjection>& buildProjections) {
  VELOX_CHECK_GT(buildVector->size(), 0);

  // TODO: For now we only enable the build optimizations in cross-joins, but we
  // should allow it for other join types as well.
  if (isCrossJoin() && isSingleBuildRow()) {
    return genCrossProductSingleBuildRow(
        buildVector, outputType, probeProjections, buildProjections);
  } else if (isCrossJoin() && isSingleBuildVector()) {
    return genCrossProductSingleBuildVector(
        buildVector, outputType, probeProjections, buildProjections);
  } else {
    return genCrossProductMultipleBuildVectors(
        buildVector, outputType, probeProjections, buildProjections);
  }
}

RowVectorPtr NestedLoopJoinProbe::genCrossProductSingleBuildRow(
    const RowVectorPtr& buildVector,
    const RowTypePtr& outputType,
    const std::vector<IdentityProjection>& probeProjections,
    const std::vector<IdentityProjection>& buildProjections) {
  VELOX_CHECK(isSingleBuildRow());

  std::vector<VectorPtr> projectedChildren(outputType->size());
  size_t numOutputRows = input_->size();
  probeRowCount_ = input_->size();

  // Project columns from the probe side.
  projectChildren(
      projectedChildren, input_, probeProjections, numOutputRows, nullptr);

  // Wrap projections from the build side as constants.
  for (const auto [inputChannel, outputChannel] : buildProjections) {
    projectedChildren[outputChannel] = BaseVector::wrapInConstant(
        numOutputRows, 0, buildVector->childAt(inputChannel));
  }
  return std::make_shared<RowVector>(
      pool(), outputType, nullptr, numOutputRows, std::move(projectedChildren));
}

RowVectorPtr NestedLoopJoinProbe::genCrossProductSingleBuildVector(
    const RowVectorPtr& buildVector,
    const RowTypePtr& outputType,
    const std::vector<IdentityProjection>& probeProjections,
    const std::vector<IdentityProjection>& buildProjections) {
  VELOX_CHECK(isSingleBuildVector());
  std::vector<VectorPtr> projectedChildren(outputType->size());
  vector_size_t buildRowCount = buildVector->size();

  // Calculate how many probe rows we can cover without exceeding
  // outputBatchSize_.
  if (buildRowCount > outputBatchSize_) {
    probeRowCount_ = 1;
  } else {
    probeRowCount_ = std::min(
        (vector_size_t)outputBatchSize_ / buildRowCount,
        input_->size() - probeRow_);
  }
  size_t numOutputRows = probeRowCount_ * buildRowCount;

  // Generate probe dictionary indices.
  auto rawProbeIndices =
      initializeRowNumberMapping(probeIndices_, numOutputRows, pool());
  for (auto i = 0; i < probeRowCount_; ++i) {
    std::fill(
        rawProbeIndices.begin() + i * buildRowCount,
        rawProbeIndices.begin() + (i + 1) * buildRowCount,
        probeRow_ + i);
  }

  // Generate build dictionary indices.
  auto rawBuildIndices_ =
      initializeRowNumberMapping(buildIndices_, numOutputRows, pool());
  for (auto i = 0; i < probeRowCount_; ++i) {
    std::iota(
        rawBuildIndices_.begin() + i * buildRowCount,
        rawBuildIndices_.begin() + (i + 1) * buildRowCount,
        0);
  }

  projectChildren(
      projectedChildren,
      input_,
      probeProjections,
      numOutputRows,
      probeIndices_);
  projectChildren(
      projectedChildren,
      buildVector,
      buildProjections,
      numOutputRows,
      buildIndices_);

  return std::make_shared<RowVector>(
      pool(), outputType, nullptr, numOutputRows, std::move(projectedChildren));
}

RowVectorPtr NestedLoopJoinProbe::genCrossProductMultipleBuildVectors(
    const RowVectorPtr& buildVector,
    const RowTypePtr& outputType,
    const std::vector<IdentityProjection>& probeProjections,
    const std::vector<IdentityProjection>& buildProjections) {
  std::vector<VectorPtr> projectedChildren(outputType->size());
  size_t numOutputRows = buildVector->size();
  probeRowCount_ = 1;

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

void NestedLoopJoinProbe::addOutputRow(vector_size_t buildRow) {
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

void NestedLoopJoinProbe::copyBuildValues(const RowVectorPtr& buildVector) {
  if (!buildCopyRanges_.empty()) {
    for (const auto& projection : buildProjections_) {
      const auto& buildChild = buildVector->childAt(projection.inputChannel);
      const auto& outputChild = output_->childAt(projection.outputChannel);
      outputChild->copyRanges(buildChild.get(), buildCopyRanges_);
    }
    buildCopyRanges_.clear();
  }
}

void NestedLoopJoinProbe::addProbeMismatchRow() {
  // Probe side is always a dictionary; just populate the index.
  rawProbeOutputIndices_[numOutputRows_] = probeRow_;

  // Null out build projections.
  for (const auto& projection : buildProjections_) {
    const auto& outputChild = output_->childAt(projection.outputChannel);
    outputChild->setNull(numOutputRows_, true);
  }
}

void NestedLoopJoinProbe::checkProbeMismatchRow() {
  // If we are processing the last batch of the build side, check if we need
  // to add a probe mismatch record.
  if (needsProbeMismatch(joinType_) && hasProbedAllBuildData() &&
      !probeRowHasMatch_) {
    prepareOutput();
    addProbeMismatchRow();
    ++numOutputRows_;
  }
}

void NestedLoopJoinProbe::finishProbeInput() {
  VELOX_CHECK_NOT_NULL(input_);
  input_.reset();
  buildIndex_ = 0;
  probeRow_ = 0;

  if (!noMoreInput_) {
    return;
  }

  // From now one we finished processing the probe side. Check now if this is a
  // right or full outer join, and hence we may need to start emitting buid
  // mismatch records.
  if (!needsBuildMismatch(joinType_) || isBuildSideEmpty()) {
    setState(ProbeOperatorState::kFinish);
    return;
  }
  beginBuildMismatch();
}

void NestedLoopJoinProbe::beginBuildMismatch() {
  VELOX_CHECK(needsBuildMismatch(joinType_));

  // Check the state of peer operators. Only the last driver (operator) running
  // this code will survive and move on to process build mismatches.
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<Driver>> peers;
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    VELOX_CHECK(future_.valid());
    setState(ProbeOperatorState::kWaitForPeers);
    return;
  }

  lastProbe_ = true;

  // From now on, buildIndex_ is used to indexing into buildMismatched_
  VELOX_CHECK_EQ(buildIndex_, 0);

  // Colect and merge the build mismatch selectivity vectors from all peers.
  for (auto& peer : peers) {
    auto* op = peer->findOperator(planNodeId());
    auto* probe = dynamic_cast<NestedLoopJoinProbe*>(op);
    VELOX_CHECK_NOT_NULL(probe);
    for (auto i = 0; i < buildMatched_.size(); ++i) {
      buildMatched_[i].select(probe->buildMatched_[i]);
      probeSideEmpty_ &= probe->probeSideEmpty_;
    }
  }
  peers.clear();
  for (auto& matched : buildMatched_) {
    matched.updateBounds();
  }
  for (auto& promise : promises) {
    promise.setValue();
  }
}

RowVectorPtr NestedLoopJoinProbe::getBuildMismatchedOutput(
    const RowVectorPtr& data,
    const SelectivityVector& matched,
    BufferPtr& unmatchedMapping,
    const std::vector<IdentityProjection>& projections,
    const std::vector<IdentityProjection>& nullProjections) {
  // If data is all matched or the join is a cross product, there is no
  // mismatched rows. But there is an exception that if the join is a cross
  // product but the build or probe side is empty, there could still be
  // mismatched rows from the other side.
  if (matched.isAllSelected() ||
      (isCrossJoin() && !probeSideEmpty_ && !isBuildSideEmpty())) {
    return nullptr;
  }

  auto rawMapping =
      initializeRowNumberMapping(unmatchedMapping, data->size(), pool());
  int32_t numUnmatched{0};
  for (auto i = 0; i < data->size(); ++i) {
    if (!matched.isValid(i)) {
      rawMapping[numUnmatched++] = i;
    }
  }
  VELOX_CHECK_GT(numUnmatched, 0);

  std::vector<VectorPtr> projectedChildren(outputType_->size());
  projectChildren(
      projectedChildren, data, projections, numUnmatched, unmatchedMapping);
  for (auto [_, outputChannel] : nullProjections) {
    VELOX_CHECK_GT(projectedChildren.size(), outputChannel);
    projectedChildren[outputChannel] = BaseVector::createNullConstant(
        outputType_->childAt(outputChannel), numUnmatched, pool());
  }
  return std::make_shared<RowVector>(
      pool(), outputType_, nullptr, numUnmatched, std::move(projectedChildren));
}

} // namespace facebook::velox::exec
