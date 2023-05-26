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
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

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
      outputBatchSize_{outputBatchRows()} {
  auto probeType = joinNode->sources()[0]->outputType();
  for (auto i = 0; i < probeType->size(); ++i) {
    auto name = probeType->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      identityProjections_.emplace_back(i, outIndex.value());
    }
  }

  auto buildType = joinNode->sources()[1]->outputType();
  for (auto i = 0; i < outputType_->size(); ++i) {
    auto tableChannel = buildType->getChildIdxIfExists(outputType_->nameOf(i));
    if (tableChannel.has_value()) {
      buildProjections_.emplace_back(tableChannel.value(), i);
    }
  }
}

BlockingReason NestedLoopJoinProbe::isBlocked(ContinueFuture* future) {
  if (buildVectors_.has_value()) {
    return BlockingReason::kNotBlocked;
  }

  auto buildVectors =
      operatorCtx_->task()
          ->getNestedLoopJoinBridge(
              operatorCtx_->driverCtx()->splitGroupId, planNodeId())
          ->dataOrFuture(future);
  if (!buildVectors.has_value()) {
    return BlockingReason::kWaitForJoinBuild;
  }

  buildVectors_ = std::move(buildVectors);

  if (buildVectors_->empty()) {
    // Build side is empty. Return empty set of rows and  terminate the pipeline
    // early.
    buildSideEmpty_ = true;
  }

  return BlockingReason::kNotBlocked;
}

void NestedLoopJoinProbe::addInput(RowVectorPtr input) {
  // In getOutput(), we are going to wrap input in dictionaries a few rows at a
  // time. Since lazy vectors cannot be wrapped in different dictionaries, we
  // are going to load them here.
  for (auto& child : input->children()) {
    child->loadedVector();
  }
  input_ = std::move(input);
}

RowVectorPtr NestedLoopJoinProbe::getOutput() {
  if (input_ == nullptr) {
    return nullptr;
  }

  const auto inputSize = input_->size();

  auto numBuildRows = buildVectors_.value()[buildIndex_]->size();
  vector_size_t numProbeRows;
  if (numBuildRows > outputBatchSize_) {
    numProbeRows = 1;
  } else {
    numProbeRows = std::min(
        (vector_size_t)outputBatchSize_ / numBuildRows, inputSize - probeRow_);
  }

  auto numOutputRows = numProbeRows * numBuildRows;
  BufferPtr indices = allocateIndices(numOutputRows, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < numProbeRows; ++i) {
    std::fill(
        rawIndices + i * numBuildRows,
        rawIndices + (i + 1) * numBuildRows,
        probeRow_ + i);
  }
  auto output = fillOutput(numOutputRows, indices);

  BufferPtr buildIndices = nullptr;
  if (numProbeRows > 1) {
    buildIndices = allocateIndices(numOutputRows, pool());
    auto* rawBuildIndices = buildIndices->asMutable<vector_size_t>();
    for (auto i = 0; i < numProbeRows; ++i) {
      std::iota(
          rawBuildIndices + i * numBuildRows,
          rawBuildIndices + (i + 1) * numBuildRows,
          0);
    }
  }

  auto buildVector =
      buildVectors_.value()[buildIndex_]->asUnchecked<RowVector>();
  for (const auto& projection : buildProjections_) {
    VectorPtr childVector = buildVector->childAt(projection.inputChannel);

    if (buildIndices != nullptr) {
      childVector = BaseVector::wrapInDictionary(
          BufferPtr(nullptr), buildIndices, numOutputRows, childVector);
    }
    output->childAt(projection.outputChannel) = childVector;
  }

  probeRow_ += numProbeRows;
  if (probeRow_ == inputSize) {
    probeRow_ = 0;
    ++buildIndex_;
    if (buildIndex_ == buildVectors_->size()) {
      buildIndex_ = 0;
      input_.reset();
    }
  }
  return output;
}

bool NestedLoopJoinProbe::isFinished() {
  return buildSideEmpty_ || (noMoreInput_ && input_ == nullptr);
}

void NestedLoopJoinProbe::close() {
  buildVectors_.reset();
  Operator::close();
}
} // namespace facebook::velox::exec
