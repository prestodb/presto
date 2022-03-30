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
#include "velox/vector/FlatVector.h"

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
          "Unnest"),
      withOrdinality_(unnestNode->withOrdinality()) {
  if (unnestNode->unnestVariables().size() > 1) {
    VELOX_UNSUPPORTED(
        "Unnest operator doesn't support multiple unnest columns yet");
  }

  const auto& unnestVariable = unnestNode->unnestVariables()[0];
  if (!unnestVariable->type()->isArray() && !unnestVariable->type()->isMap()) {
    VELOX_UNSUPPORTED("Unnest operator supports only ARRAY and MAP types")
  }

  if (withOrdinality_) {
    VELOX_CHECK_EQ(
        outputType_->children().back(),
        BIGINT(),
        "Ordinality column should be BIGINT type.")
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
  inputRows_.resize(size);

  const auto& unnestVector = input_->childAt(unnestChannel_);
  unnestDecoded_.decode(*unnestVector, inputRows_);
  auto unnestIndices = unnestDecoded_.indices();

  const ArrayVector* unnestBaseArray;
  const MapVector* unnestBaseMap;
  const vector_size_t* rawSizes;
  const vector_size_t* rawOffsets;
  if (unnestVector->typeKind() == TypeKind::ARRAY) {
    unnestBaseArray = unnestDecoded_.base()->as<ArrayVector>();
    rawSizes = unnestBaseArray->rawSizes();
    rawOffsets = unnestBaseArray->rawOffsets();
  } else {
    VELOX_CHECK(unnestVector->typeKind() == TypeKind::MAP);
    unnestBaseMap = unnestDecoded_.base()->as<MapVector>();
    rawSizes = unnestBaseMap->rawSizes();
    rawOffsets = unnestBaseMap->rawOffsets();
  }

  // Count number of elements.
  vector_size_t numElements = 0;
  for (auto row = 0; row < size; ++row) {
    if (!unnestDecoded_.isNullAt(row)) {
      numElements += rawSizes[unnestIndices[row]];
    }
  }

  if (numElements == 0) {
    // All arrays/maps are null or empty.
    input_ = nullptr;
    return nullptr;
  }

  // Create "indices" buffer to repeat rows as many times as there are elements
  // in the array(or map) in unnestDecoded.
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

  // Make dictionary index for elements column since they may be out of order.
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

  if (unnestVector->typeKind() == TypeKind::ARRAY) {
    // Construct unnest column using Array elements wrapped using above created
    // dictionary.
    outputs[identityProjections_.size()] = identityMapping
        ? unnestBaseArray->elements()
        : wrapChild(numElements, elementIndices, unnestBaseArray->elements());
  } else {
    // Construct two unnest columns for Map keys and values vectors wrapped
    // using above created dictionary.
    outputs[identityProjections_.size()] = identityMapping
        ? unnestBaseMap->mapKeys()
        : wrapChild(numElements, elementIndices, unnestBaseMap->mapKeys());
    outputs[identityProjections_.size() + 1] = identityMapping
        ? unnestBaseMap->mapValues()
        : wrapChild(numElements, elementIndices, unnestBaseMap->mapValues());
  }

  if (withOrdinality_) {
    auto ordinalityVector = std::dynamic_pointer_cast<FlatVector<int64_t>>(
        BaseVector::create(BIGINT(), numElements, pool()));

    // Set the ordinality at each result row to be the index of the element in
    // the original array (or map) plus one.
    index = 0;
    auto rawOrdinality = ordinalityVector->mutableRawValues();
    if (!unnestDecoded_.mayHaveNulls() && unnestDecoded_.isIdentityMapping()) {
      for (auto row = 0; row < size; ++row) {
        auto unnestSize = rawSizes[row];
        for (auto i = 0; i < unnestSize; i++) {
          rawOrdinality[index++] = i + 1;
        }
      }
    } else {
      for (auto row = 0; row < size; ++row) {
        if (!unnestDecoded_.isNullAt(row)) {
          auto unnestSize = rawSizes[unnestIndices[row]];
          for (auto i = 0; i < unnestSize; i++) {
            rawOrdinality[index++] = i + 1;
          }
        }
      }
    }

    // Ordinality column is always at the end.
    outputs.back() = std::move(ordinalityVector);
  }

  input_ = nullptr;
  return std::make_shared<RowVector>(
      pool(), outputType_, BufferPtr(nullptr), numElements, std::move(outputs));
}

bool Unnest::isFinished() {
  return noMoreInput_ && input_ == nullptr;
}
} // namespace facebook::velox::exec
