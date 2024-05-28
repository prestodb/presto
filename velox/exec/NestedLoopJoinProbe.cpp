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
  if (needsProbeMismatch(joinType_)) {
    probeMatched_.resizeFill(input_->size(), false);
  }
}

RowVectorPtr NestedLoopJoinProbe::getOutput() {
  if (state_ == ProbeOperatorState::kFinish ||
      state_ == ProbeOperatorState::kWaitForPeers) {
    return nullptr;
  }
  RowVectorPtr output{nullptr};
  while (output == nullptr) {
    if (lastProbe_) {
      VELOX_CHECK(processingBuildMismatch());

      while (output == nullptr && !hasProbedAllBuildData()) {
        output = getMismatchedOutput(
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

    if (input_ == nullptr) {
      break;
    }

    // When input_ is not null but buildIndex_ is at the end, it means the
    // matching of input_ and buildData_ has finished. For left/full joins,
    // the next step is to emit output for mismatched probe side rows.
    if (hasProbedAllBuildData()) {
      output = needsProbeMismatch(joinType_) ? getMismatchedOutput(
                                                   input_,
                                                   probeMatched_,
                                                   probeOutMapping_,
                                                   identityProjections_,
                                                   buildProjections_)
                                             : nullptr;
      finishProbeInput();
      break;
    }

    const vector_size_t probeCnt = getNumProbeRows();
    output = doMatch(probeCnt);
    if (advanceProbeRows(probeCnt)) {
      if (!needsProbeMismatch(joinType_)) {
        finishProbeInput();
      }
    }
  }
  return output;
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
  auto numFields = joinCondition_->expr(0)->distinctFields().size();
  names.reserve(numFields);
  types.reserve(numFields);
  for (auto& field : joinCondition_->expr(0)->distinctFields()) {
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

RowVectorPtr NestedLoopJoinProbe::getMismatchedOutput(
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
      (joinCondition_ == nullptr && !probeSideEmpty_ && !buildSideEmpty_)) {
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

void NestedLoopJoinProbe::finishProbeInput() {
  VELOX_CHECK_NOT_NULL(input_);
  input_.reset();
  buildIndex_ = 0;
  if (!noMoreInput_) {
    return;
  }
  if (!needsBuildMismatch(joinType_) || buildSideEmpty_) {
    setState(ProbeOperatorState::kFinish);
    return;
  }
  beginBuildMismatch();
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

void NestedLoopJoinProbe::beginBuildMismatch() {
  VELOX_CHECK(needsBuildMismatch(joinType_));

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
  if (buildVectors_->empty()) {
    buildSideEmpty_ = true;
  }
  return true;
}

vector_size_t NestedLoopJoinProbe::getNumProbeRows() const {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!hasProbedAllBuildData());

  const auto inputSize = input_->size();
  auto numBuildRows = buildVectors_.value()[buildIndex_]->size();
  vector_size_t numProbeRows;
  if (numBuildRows > outputBatchSize_) {
    numProbeRows = 1;
  } else {
    numProbeRows = std::min(
        (vector_size_t)outputBatchSize_ / numBuildRows, inputSize - probeRow_);
  }
  return numProbeRows;
}

RowVectorPtr NestedLoopJoinProbe::getCrossProduct(
    vector_size_t probeCnt,
    const RowTypePtr& outputType,
    const std::vector<IdentityProjection>& probeProjections,
    const std::vector<IdentityProjection>& buildProjections) {
  VELOX_CHECK_GT(probeCnt, 0);
  VELOX_CHECK(!hasProbedAllBuildData());

  const auto buildSize = buildVectors_.value()[buildIndex_]->size();
  const auto numOutputRows = probeCnt * buildSize;
  const bool probeCntChanged = (probeCnt != numPrevProbedRows_);
  numPrevProbedRows_ = probeCnt;

  auto rawProbeIndices =
      initializeRowNumberMapping(probeIndices_, numOutputRows, pool());
  for (auto i = 0; i < probeCnt; ++i) {
    std::fill(
        rawProbeIndices.begin() + i * buildSize,
        rawProbeIndices.begin() + (i + 1) * buildSize,
        probeRow_ + i);
  }

  if (probeCntChanged) {
    auto rawBuildIndices_ =
        initializeRowNumberMapping(buildIndices_, numOutputRows, pool());
    for (auto i = 0; i < probeCnt; ++i) {
      std::iota(
          rawBuildIndices_.begin() + i * buildSize,
          rawBuildIndices_.begin() + (i + 1) * buildSize,
          0);
    }
  }

  std::vector<VectorPtr> projectedChildren(outputType->size());
  projectChildren(
      projectedChildren,
      input_,
      probeProjections,
      numOutputRows,
      probeIndices_);
  projectChildren(
      projectedChildren,
      buildVectors_.value()[buildIndex_],
      buildProjections,
      numOutputRows,
      buildIndices_);

  return std::make_shared<RowVector>(
      pool(), outputType, nullptr, numOutputRows, std::move(projectedChildren));
}

bool NestedLoopJoinProbe::advanceProbeRows(vector_size_t probeCnt) {
  probeRow_ += probeCnt;
  if (probeRow_ < input_->size()) {
    return false;
  }
  probeRow_ = 0;
  numPrevProbedRows_ = 0;
  do {
    ++buildIndex_;
  } while (!hasProbedAllBuildData() &&
           !buildVectors_.value()[buildIndex_]->size());
  return hasProbedAllBuildData();
}

RowVectorPtr NestedLoopJoinProbe::doMatch(vector_size_t probeCnt) {
  VELOX_CHECK_NOT_NULL(input_);
  VELOX_CHECK(!hasProbedAllBuildData());

  if (joinCondition_ == nullptr) {
    return getCrossProduct(
        probeCnt, outputType_, identityProjections_, buildProjections_);
  }

  auto filterInput = getCrossProduct(
      probeCnt,
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
  DecodedVector decodedFilterResult;
  decodedFilterResult.decode(*filterResult[0], filterInputRows_);

  const vector_size_t maxOutputRows = decodedFilterResult.size();
  auto rawProbeOutMapping =
      initializeRowNumberMapping(probeOutMapping_, maxOutputRows, pool());
  auto rawBuildOutMapping =
      initializeRowNumberMapping(buildOutMapping_, maxOutputRows, pool());
  auto* probeIndices = probeIndices_->asMutable<vector_size_t>();
  auto* buildIndices = buildIndices_->asMutable<vector_size_t>();
  int32_t numOutputRows{0};
  for (auto i = 0; i < maxOutputRows; ++i) {
    if (!decodedFilterResult.isNullAt(i) &&
        decodedFilterResult.valueAt<bool>(i)) {
      rawProbeOutMapping[numOutputRows] = probeIndices[i];
      rawBuildOutMapping[numOutputRows] = buildIndices[i];
      ++numOutputRows;
    }
  }
  if (needsProbeMismatch(joinType_)) {
    for (auto i = 0; i < numOutputRows; ++i) {
      probeMatched_.setValid(rawProbeOutMapping[i], true);
    }
    probeMatched_.updateBounds();
  }
  if (needsBuildMismatch(joinType_)) {
    for (auto i = 0; i < numOutputRows; ++i) {
      buildMatched_[buildIndex_].setValid(rawBuildOutMapping[i], true);
    }
  }

  if (numOutputRows == 0) {
    return nullptr;
  }

  std::vector<VectorPtr> projectedChildren(outputType_->size());
  projectChildren(
      projectedChildren,
      input_,
      identityProjections_,
      numOutputRows,
      probeOutMapping_);
  projectChildren(
      projectedChildren,
      buildVectors_.value()[buildIndex_],
      buildProjections_,
      numOutputRows,
      buildOutMapping_);

  return std::make_shared<RowVector>(
      pool(),
      outputType_,
      nullptr,
      numOutputRows,
      std::move(projectedChildren));
}

} // namespace facebook::velox::exec
