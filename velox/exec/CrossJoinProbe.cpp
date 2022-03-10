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
#include "velox/exec/CrossJoinProbe.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

CrossJoinProbe::CrossJoinProbe(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::CrossJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "CrossJoinProbe"),
      outputBatchSize_{driverCtx->queryConfig().preferredOutputBatchSize()} {
  bool isIdentityProjection = true;

  auto probeType = joinNode->sources()[0]->outputType();
  for (auto i = 0; i < probeType->size(); ++i) {
    auto name = probeType->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      identityProjections_.emplace_back(i, outIndex.value());
      if (outIndex != i) {
        isIdentityProjection = false;
      }
    }
  }

  auto buildType = joinNode->sources()[1]->outputType();
  for (auto i = 0; i < outputType_->size(); ++i) {
    auto tableChannel = buildType->getChildIdxIfExists(outputType_->nameOf(i));
    if (tableChannel.has_value()) {
      buildProjections_.emplace_back(tableChannel.value(), i);
    }
  }

  if (isIdentityProjection && buildProjections_.empty()) {
    isIdentityProjection_ = true;
  }
}

BlockingReason CrossJoinProbe::isBlocked(ContinueFuture* future) {
  if (buildData_.has_value()) {
    return BlockingReason::kNotBlocked;
  }

  auto buildData =
      operatorCtx_->task()
          ->getCrossJoinBridge(
              operatorCtx_->driverCtx()->splitGroupId, planNodeId())
          ->dataOrFuture(future);
  if (!buildData.has_value()) {
    return BlockingReason::kWaitForJoinBuild;
  }

  buildData_ = std::move(buildData);

  if (buildData_->empty()) {
    // Build side is empty. Return empty set of rows and  terminate the pipeline
    // early.
    buildSideEmpty_ = true;
  }

  return BlockingReason::kNotBlocked;
}

void CrossJoinProbe::addInput(RowVectorPtr input) {
  // In getOutput(), we are going to wrap input in dictionaries a few rows at a
  // time. Since lazy vectors cannot be wrapped in different dictionaries, we
  // are going to load them here.
  for (auto& child : input->children()) {
    child->loadedVector();
  }
  input_ = std::move(input);
}

RowVectorPtr CrossJoinProbe::getOutput() {
  if (!input_) {
    return nullptr;
  }

  const auto inputSize = input_->size();

  auto buildSize = buildData_.value()[buildIndex_]->size();
  vector_size_t probeCnt;
  if (buildSize > outputBatchSize_) {
    probeCnt = 1;
  } else {
    probeCnt = std::min(
        (vector_size_t)outputBatchSize_ / buildSize, inputSize - probeRow_);
  }

  auto size = probeCnt * buildSize;
  BufferPtr indices = allocateIndices(size, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < probeCnt; ++i) {
    std::fill(
        rawIndices + i * buildSize,
        rawIndices + (i + 1) * buildSize,
        probeRow_ + i);
  }
  auto output = fillOutput(size, indices);

  BufferPtr buildIndices = nullptr;
  if (probeCnt > 1) {
    buildIndices = allocateIndices(size, pool());
    auto* rawBuildIndices = buildIndices->asMutable<vector_size_t>();
    for (auto i = 0; i < probeCnt; ++i) {
      std::iota(
          rawBuildIndices + i * buildSize,
          rawBuildIndices + (i + 1) * buildSize,
          0);
    }
  }

  auto buildRowVector =
      buildData_.value()[buildIndex_]->asUnchecked<RowVector>();
  for (const auto& projection : buildProjections_) {
    VectorPtr buildVector = buildRowVector->childAt(projection.inputChannel);

    if (buildIndices) {
      buildVector = BaseVector::wrapInDictionary(
          BufferPtr(nullptr), buildIndices, size, buildVector);
    }
    output->childAt(projection.outputChannel) = buildVector;
  }

  probeRow_ += probeCnt;
  if (probeRow_ == inputSize) {
    probeRow_ = 0;
    ++buildIndex_;
    if (buildIndex_ == buildData_->size()) {
      buildIndex_ = 0;
      input_.reset();
    }
  }
  return output;
}

bool CrossJoinProbe::isFinished() {
  return buildSideEmpty_ || (noMoreInput_ && input_ == nullptr);
}

void CrossJoinProbe::close() {
  buildData_.reset();
  Operator::close();
}
} // namespace facebook::velox::exec
