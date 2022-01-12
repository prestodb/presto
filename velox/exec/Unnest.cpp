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

#include "velox/exec/Unnest.h"
#include "velox/exec/OperatorUtils.h"

namespace facebook::velox::exec {
Unnest::Unnest(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::UnnestNode>& unnestNode)
    : Operator(
          driverCtx,
          unnestNode->outputType(),
          operatorId,
          unnestNode->id(),
          "Unnest") {
  if (unnestNode->unnestVariables().size() > 1) {
    VELOX_UNSUPPORTED(
        "Unnest operator doesn't support multiple unnest columns yet");
  }

  const auto& unnestVariable = unnestNode->unnestVariables()[0];
  if (!unnestVariable->type()->isArray()) {
    VELOX_UNSUPPORTED(
        "Unnest operator doesn't support non-ARRAY unnest variables yet: {}",
        unnestVariable->type()->toString())
  }

  if (unnestNode->withOrdinality()) {
    VELOX_UNSUPPORTED("Unnest operator doesn't support ordinality column yet");
  }

  const auto& inputType = unnestNode->sources()[0]->outputType();
  ChannelIndex outputChannel = 0;
  for (const auto& variable : unnestNode->replicateVariables()) {
    identityProjections_.emplace_back(
        inputType->getChildIdx(variable->name()), outputChannel++);
  }

  unnestChannel_ = inputType->getChildIdx(unnestVariable->name());
}

void Unnest::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

RowVectorPtr Unnest::getOutput() {
  if (!input_) {
    return nullptr;
  }

  auto size = input_->size();

  // Build repeated indices to apply to "replicated" columns.
  const auto& unnestVector = input_->childAt(unnestChannel_);

  inputRows_.resize(size);
  unnestDecoded_.decode(*unnestVector, inputRows_);

  auto unnestIndices = unnestDecoded_.indices();

  auto unnestBase = unnestDecoded_.base()->as<ArrayVector>();
  auto rawSizes = unnestBase->rawSizes();
  auto rawOffsets = unnestBase->rawOffsets();

  // Count number of elements.
  vector_size_t numElements = 0;
  for (auto row = 0; row < size; ++row) {
    if (!unnestDecoded_.isNullAt(row)) {
      numElements += rawSizes[unnestIndices[row]];
    }
  }

  if (numElements == 0) {
    // All arrays are null or empty.
    input_ = nullptr;
    return nullptr;
  }

  // Create "indices" buffer to repeat rows as many times as there are elements
  // in the array.
  BufferPtr repeatedIndices = allocateIndices(numElements, pool());
  auto* rawIndices = repeatedIndices->asMutable<vector_size_t>();
  vector_size_t index = 0;
  for (auto row = 0; row < size; ++row) {
    if (!unnestDecoded_.isNullAt(row)) {
      auto unnestSize = rawSizes[unnestIndices[row]];
      for (auto i = 0; i < unnestSize; i++) {
        rawIndices[index++] = row;
      }
    }
  }

  // Wrap "replicated" columns in a dictionary using 'repeatedIndices'.
  std::vector<VectorPtr> outputs(outputType_->size());
  for (const auto& projection : identityProjections_) {
    outputs[projection.outputChannel] = wrapChild(
        numElements, repeatedIndices, input_->childAt(projection.inputChannel));
  }

  // Make "elements" column. Elements may be out of order. Use a
  // dictionary to ensure the right order.
  BufferPtr elementIndices = allocateIndices(numElements, pool());
  auto* rawElementIndices = elementIndices->asMutable<vector_size_t>();
  index = 0;
  bool identityMapping = true;
  for (auto row = 0; row < size; ++row) {
    if (!unnestDecoded_.isNullAt(row)) {
      auto offset = rawOffsets[unnestIndices[row]];
      auto unnestSize = rawSizes[unnestIndices[row]];

      if (index != offset) {
        identityMapping = false;
      }

      for (auto i = 0; i < unnestSize; i++) {
        rawElementIndices[index++] = offset + i;
      }
    }
  }

  outputs[identityProjections_.size()] = identityMapping
      ? unnestBase->elements()
      : wrapChild(numElements, elementIndices, unnestBase->elements());

  input_ = nullptr;

  return std::make_shared<RowVector>(
      pool(), outputType_, BufferPtr(nullptr), numElements, std::move(outputs));
}

bool Unnest::isFinished() {
  return noMoreInput_ && input_ == nullptr;
}
} // namespace facebook::velox::exec
