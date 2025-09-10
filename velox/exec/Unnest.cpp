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
#include "velox/common/base/Nulls.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {
namespace {
#ifndef NDEBUG
void debugCheckOutput(const RowVectorPtr& output) {
  for (auto i = 0; i < output->childrenSize(); ++i) {
    VELOX_CHECK_EQ(output->size(), output->childAt(i)->size());
  }
}
#else
void debugCheckOutput(const RowVectorPtr& output) {}
#endif
} // namespace

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
      withOrdinality_(unnestNode->hasOrdinality()),
      withMarker_(unnestNode->hasMarker()),
      maxOutputSize_(
          driverCtx->queryConfig().unnestSplitOutput()
              ? outputBatchRows()
              : std::numeric_limits<vector_size_t>::max()) {
  const auto& inputType = unnestNode->sources()[0]->outputType();
  const auto& unnestVariables = unnestNode->unnestVariables();
  for (const auto& variable : unnestVariables) {
    if (!variable->type()->isArray() && !variable->type()->isMap()) {
      VELOX_UNSUPPORTED(
          "Unnest operator supports only ARRAY and MAP types, the actual type is {}",
          variable->type()->toString());
    }
    unnestChannels_.push_back(inputType->getChildIdx(variable->name()));
  }
  unnestDecoded_.resize(unnestVariables.size());

  column_index_t checkOutputChannel = outputType_->size() - 1;
  if (withMarker_) {
    VELOX_CHECK_EQ(
        outputType_->childAt(checkOutputChannel),
        BOOLEAN(),
        "Marker column should be BOOLEAN type.");
    --checkOutputChannel;
  }
  if (withOrdinality_) {
    VELOX_CHECK_EQ(
        outputType_->childAt(checkOutputChannel),
        BIGINT(),
        "Ordinality column should be BIGINT type.");
  }

  column_index_t outputChannel = 0;
  for (const auto& variable : unnestNode->replicateVariables()) {
    identityProjections_.emplace_back(
        inputType->getChildIdx(variable->name()), outputChannel++);
  }
}

void Unnest::addInput(RowVectorPtr input) {
  input_ = std::move(input);

  for (auto& child : input_->children()) {
    child->loadedVector();
  }

  const auto size = input_->size();

  // The max number of elements at each row across all unnested columns.
  maxSizes_ = allocateSizes(size, pool());
  rawMaxSizes_ = maxSizes_->asMutable<vector_size_t>();

  rawSizes_.resize(unnestChannels_.size());
  rawOffsets_.resize(unnestChannels_.size());
  rawIndices_.resize(unnestChannels_.size());

  for (auto channel = 0; channel < unnestChannels_.size(); ++channel) {
    const auto& unnestVector = input_->childAt(unnestChannels_[channel]);

    auto& currentDecoded = unnestDecoded_[channel];
    currentDecoded.decode(*unnestVector);

    rawIndices_[channel] = currentDecoded.indices();

    if (unnestVector->typeKind() == TypeKind::ARRAY) {
      const auto* unnestBaseArray = currentDecoded.base()->as<ArrayVector>();
      VELOX_CHECK_NOT_NULL(unnestBaseArray);
      rawSizes_[channel] = unnestBaseArray->rawSizes();
      rawOffsets_[channel] = unnestBaseArray->rawOffsets();
    } else {
      VELOX_CHECK_EQ(unnestVector->typeKind(), TypeKind::MAP);
      const auto* unnestBaseMap = currentDecoded.base()->as<MapVector>();
      VELOX_CHECK_NOT_NULL(unnestBaseMap);
      rawSizes_[channel] = unnestBaseMap->rawSizes();
      rawOffsets_[channel] = unnestBaseMap->rawOffsets();
    }

    // Count max number of elements per row.
    auto* currentSizes = rawSizes_[channel];
    auto* currentIndices = rawIndices_[channel];
    for (auto row = 0; row < size; ++row) {
      if (!currentDecoded.isNullAt(row)) {
        const auto unnestSize = currentSizes[currentIndices[row]];
        rawMaxSizes_[row] = std::max(rawMaxSizes_[row], unnestSize);
      }
    }
  }
}

void Unnest::maybeFinishDrain() {
  if (FOLLY_UNLIKELY(isDraining())) {
    finishDrain();
  }
}

RowVectorPtr Unnest::getOutput() {
  if (!input_) {
    maybeFinishDrain();
    return nullptr;
  }

  const auto numInputRows = input_->size();
  VELOX_DCHECK_LT(nextInputRow_, numInputRows);

  // Limit the number of input rows to keep output batch size within
  // 'maxOutputSize_'. When the output size is 'maxOutputSize_', the
  // first and last row might not be processed completely, and their output
  // might be split into multiple batches.
  const auto rowRange = extractRowRange(numInputRows);
  if (rowRange.numInnerRows == 0) {
    finishInput();
    maybeFinishDrain();
    return nullptr;
  }

  const auto output = generateOutput(rowRange);
  VELOX_CHECK_NOT_NULL(output);
  if (rowRange.lastInnerRowEnd.has_value()) {
    // The last row is not processed completely.
    firstInnerRowStart_ = rowRange.lastInnerRowEnd.value();
    nextInputRow_ += rowRange.numInputRows - 1;
  } else {
    firstInnerRowStart_ = 0;
    nextInputRow_ += rowRange.numInputRows;
  }

  if (nextInputRow_ >= numInputRows) {
    finishInput();
  }
  debugCheckOutput(output);
  return output;
}

void Unnest::finishInput() {
  input_ = nullptr;
  nextInputRow_ = 0;
  firstInnerRowStart_ = 0;
}

Unnest::RowRange Unnest::extractRowRange(vector_size_t inputSize) const {
  vector_size_t numInputRows{0};
  vector_size_t numInnerRows{0};
  std::optional<vector_size_t> lastInnerRowEnd;
  bool hasEmptyUnnestValue{false};
  for (auto inputRow = nextInputRow_; inputRow < inputSize; ++inputRow) {
    const bool isFirstRow = (inputRow == nextInputRow_);
    vector_size_t remainingInnerRows = isFirstRow
        ? rawMaxSizes_[inputRow] - firstInnerRowStart_
        : rawMaxSizes_[inputRow];
    if (rawMaxSizes_[inputRow] == 0) {
      VELOX_CHECK_EQ(remainingInnerRows, 0);
      hasEmptyUnnestValue = true;
      if (withMarker_) {
        remainingInnerRows = 1;
      }
    }
    ++numInputRows;
    if (numInnerRows + remainingInnerRows > maxOutputSize_) {
      // A single row's output needs to be split into multiple batches.
      // Determines the range to process the first and last rows partially,
      // rather than processing from 0 to 'rawMaxSizes_[row]'.
      if (isFirstRow) {
        lastInnerRowEnd = firstInnerRowStart_ + maxOutputSize_ - numInnerRows;
      } else {
        lastInnerRowEnd = maxOutputSize_ - numInnerRows;
      }
      // Process maxOutputSize_ in this getOutput.
      numInnerRows = maxOutputSize_;
      break;
    }
    // Process this row completely.
    numInnerRows += remainingInnerRows;
    if (numInnerRows == maxOutputSize_) {
      break;
    }
  }
  VELOX_DCHECK_GE(numInnerRows, 0);
  VELOX_DCHECK_LE(numInnerRows, maxOutputSize_);
  return {
      nextInputRow_,
      numInputRows,
      lastInnerRowEnd,
      numInnerRows,
      hasEmptyUnnestValue};
};

void Unnest::generateRepeatedColumns(
    const RowRange& range,
    std::vector<VectorPtr>& outputs) {
  // Create "indices" buffer to repeat rows as many times as there are elements
  // in the array (or map) in unnestDecoded.
  auto repeatedIndices = allocateIndices(range.numInnerRows, pool());
  vector_size_t* rawRepeatedIndices =
      repeatedIndices->asMutable<vector_size_t>();

  const bool generateMarker = withMarker_ && range.hasEmptyUnnestValue;
  vector_size_t index{0};
  VELOX_CHECK_GT(range.numInputRows, 0);
  // Record the row number to process.
  if (generateMarker) {
    range.forEachRow(
        [&](vector_size_t row, vector_size_t /*start*/, vector_size_t size) {
          if (FOLLY_UNLIKELY(size == 0)) {
            rawRepeatedIndices[index++] = row;
          } else {
            std::fill(
                rawRepeatedIndices + index,
                rawRepeatedIndices + index + size,
                row);
            index += size;
          }
        },
        rawMaxSizes_,
        firstInnerRowStart_);
  } else {
    range.forEachRow(
        [&](vector_size_t row, vector_size_t /*start*/, vector_size_t size) {
          std::fill(
              rawRepeatedIndices + index,
              rawRepeatedIndices + index + size,
              row);
          index += size;
        },
        rawMaxSizes_,
        firstInnerRowStart_);
  }

  // Wrap "replicated" columns in a dictionary using 'repeatedIndices'.
  for (const auto& projection : identityProjections_) {
    outputs.at(projection.outputChannel) = BaseVector::wrapInDictionary(
        /*nulls=*/nullptr,
        repeatedIndices,
        range.numInnerRows,
        input_->childAt(projection.inputChannel));
  }
}

const Unnest::UnnestChannelEncoding Unnest::generateEncodingForChannel(
    column_index_t channel,
    const RowRange& range) {
  BufferPtr innerRowIndices = allocateIndices(range.numInnerRows, pool());
  auto* rawInnerRowIndices = innerRowIndices->asMutable<vector_size_t>();

  auto nulls = allocateNulls(range.numInnerRows, pool());
  auto* rawNulls = nulls->asMutable<uint64_t>();

  auto& currentDecoded = unnestDecoded_[channel];
  auto* currentSizes = rawSizes_[channel];
  auto* currentOffsets = rawOffsets_[channel];
  auto* currentIndices = rawIndices_[channel];

  // Make dictionary index for elements column since they may be out of order.
  vector_size_t index = 0;
  bool identityMapping = true;
  VELOX_DCHECK_GT(range.numInputRows, 0);

  range.forEachRow(
      [&](vector_size_t row, vector_size_t start, vector_size_t size) {
        const auto end = start + size;
        if (size == 0 && withMarker_) {
          identityMapping = false;
          bits::setNull(rawNulls, index++, true);
        } else if (!currentDecoded.isNullAt(row)) {
          const auto offset = currentOffsets[currentIndices[row]];
          const auto unnestSize = currentSizes[currentIndices[row]];
          // The 'identityMapping' is false when there exists a partially
          // processed row.
          if (index != offset || start != 0 || end != rawMaxSizes_[row] ||
              unnestSize < end) {
            identityMapping = false;
          }
          const auto currentUnnestSize = std::min(end, unnestSize);
          for (auto i = start; i < currentUnnestSize; ++i) {
            rawInnerRowIndices[index++] = offset + i;
          }
          for (auto i = std::max(start, currentUnnestSize); i < end; ++i) {
            bits::setNull(rawNulls, index++, true);
          }
        } else if (size > 0) {
          identityMapping = false;
          for (auto i = start; i < end; ++i) {
            bits::setNull(rawNulls, index++, true);
          }
        }
      },
      rawMaxSizes_,
      firstInnerRowStart_);

  return {innerRowIndices, nulls, identityMapping};
}

VectorPtr Unnest::generateOrdinalityVector(const RowRange& range) {
  VELOX_DCHECK_GT(range.numInputRows, 0);

  auto ordinalityVector = BaseVector::create<FlatVector<int64_t>>(
      BIGINT(), range.numInnerRows, pool());

  // Set the ordinality at each result row to be the index of the element in
  // the original array (or map) plus one.
  auto* rawOrdinality = ordinalityVector->mutableRawValues();
  const bool hasMarker = withMarker_ && range.hasEmptyUnnestValue;
  if (!hasMarker) {
    range.forEachRow(
        [&](vector_size_t /*row*/, vector_size_t start, vector_size_t size) {
          std::iota(rawOrdinality, rawOrdinality + size, start + 1);
          rawOrdinality += size;
        },
        rawMaxSizes_,
        firstInnerRowStart_);
  } else {
    range.forEachRow(
        [&](vector_size_t /*row*/, vector_size_t start, vector_size_t size) {
          if (FOLLY_LIKELY(size > 0)) {
            std::iota(rawOrdinality, rawOrdinality + size, start + 1);
            rawOrdinality += size;
          } else {
            // Set ordinality to 0 for output row with empty unnest value.
            //
            // NOTE: for non-empty unnest value row, the ordinality starts
            // from 1.
            VELOX_DCHECK_EQ(size, 0);
            *rawOrdinality++ = 0;
          }
        },
        rawMaxSizes_,
        firstInnerRowStart_);
  }
  return ordinalityVector;
}

VectorPtr Unnest::generateMarkerVector(const RowRange& range) {
  VELOX_CHECK(withMarker_);
  VELOX_DCHECK_GT(range.numInputRows, 0);

  if (!range.hasEmptyUnnestValue) {
    return BaseVector::createConstant(
        BOOLEAN(), true, range.numInnerRows, pool());
  }

  // Create a vector with all elements set to true initially assuming most
  // output rows have non-empty unnest values.
  auto markerBuffer =
      velox::AlignedBuffer::allocate<bool>(range.numInnerRows, pool(), true);
  auto markerVector = std::make_shared<velox::FlatVector<bool>>(
      pool(),
      /*type=*/BOOLEAN(),
      /*nulls=*/nullptr,
      range.numInnerRows,
      /*values=*/std::move(markerBuffer),
      /*stringBuffers=*/std::vector<velox::BufferPtr>{});
  // Set each output row with empty unnest values to false.
  auto* const rawMarker = markerVector->mutableRawValues<uint64_t>();
  size_t index{0};
  range.forEachRow(
      [&](vector_size_t /*row*/, vector_size_t start, vector_size_t size) {
        if (size > 0) {
          index += size;
        } else {
          VELOX_DCHECK_EQ(size, 0);
          bits::setBit(rawMarker, index++, false);
        }
      },
      rawMaxSizes_,
      firstInnerRowStart_);
  return markerVector;
}

RowVectorPtr Unnest::generateOutput(const RowRange& range) {
  std::vector<VectorPtr> outputs(outputType_->size());
  generateRepeatedColumns(range, outputs);

  // Create unnest columns.
  column_index_t outputColumnIndex = identityProjections_.size();
  for (auto channel = 0; channel < unnestChannels_.size(); ++channel) {
    const auto unnestChannelEncoding =
        generateEncodingForChannel(channel, range);

    const auto& currentDecoded = unnestDecoded_[channel];
    if (currentDecoded.base()->typeKind() == TypeKind::ARRAY) {
      // Construct unnest column using Array elements wrapped using above
      // created dictionary.
      const auto* unnestBaseArray = currentDecoded.base()->as<ArrayVector>();
      outputs[outputColumnIndex++] = unnestChannelEncoding.wrap(
          unnestBaseArray->elements(), range.numInnerRows);
    } else {
      // Construct two unnest columns for Map keys and values vectors wrapped
      // using above created dictionary.
      const auto* unnestBaseMap = currentDecoded.base()->as<MapVector>();
      outputs[outputColumnIndex++] = unnestChannelEncoding.wrap(
          unnestBaseMap->mapKeys(), range.numInnerRows);
      outputs[outputColumnIndex++] = unnestChannelEncoding.wrap(
          unnestBaseMap->mapValues(), range.numInnerRows);
    }
  }

  // 'Ordinality' and 'EmptyUnnestValue' columns are always at the end.
  if (withOrdinality_) {
    outputs[outputColumnIndex++] = generateOrdinalityVector(range);
  }
  if (withMarker_) {
    outputs[outputColumnIndex++] = generateMarkerVector(range);
  }

  return std::make_shared<RowVector>(
      pool(),
      outputType_,
      /*nulls=*/nullptr,
      range.numInnerRows,
      std::move(outputs));
}

VectorPtr Unnest::UnnestChannelEncoding::wrap(
    const VectorPtr& base,
    vector_size_t wrapSize) const {
  if (identityMapping) {
    if (wrapSize == base->size()) {
      return base;
    }
    auto* rawIndices = indices->asMutable<vector_size_t>();
    return base->slice(rawIndices[0], wrapSize);
  }

  const auto result =
      BaseVector::wrapInDictionary(nulls, indices, wrapSize, base);

  // Dictionary vectors whose size is much smaller than the size of the
  // 'alphabet' (base vector) create efficiency problems downstream. For
  // example, expression evaluation may peel the dictionary encoding and
  // evaluate the expression on the base vector. If a dictionary references
  // 1K rows of the base vector with row numbers 1'000'000...1'000'999, the
  // expression needs to allocate a result vector of size 1'001'000. This
  // causes large number of large allocations and wastes a lot of
  // resources.
  //
  // Make a flat copy of the necessary rows to avoid inefficient downstream
  // processing.
  //
  // TODO A better fix might be to change expression evaluation (and all other
  // operations) to handle dictionaries with large alphabets efficiently.
  return BaseVector::copy(*result);
}

bool Unnest::isFinished() {
  return noMoreInput_ && input_ == nullptr;
}

void Unnest::RowRange::forEachRow(
    const std::function<void(
        vector_size_t /*row*/,
        vector_size_t /*start*/,
        vector_size_t /*size*/)>& func,
    const vector_size_t* rawMaxSizes,
    vector_size_t firstInnerRowStart) const {
  // Process the first row.
  const auto firstInnerRowEnd = numInputRows == 1 && lastInnerRowEnd.has_value()
      ? lastInnerRowEnd.value()
      : rawMaxSizes[startInputRow];
  func(
      startInputRow, firstInnerRowStart, firstInnerRowEnd - firstInnerRowStart);

  const auto lastInputRow = startInputRow + numInputRows - 1;
  // Process the middle rows.
  for (auto inputRow = startInputRow + 1; inputRow < lastInputRow; ++inputRow) {
    func(inputRow, 0, rawMaxSizes[inputRow]);
  }

  // Process the last row if exists.
  if (numInputRows > 1) {
    if (lastInnerRowEnd.has_value()) {
      func(lastInputRow, 0, lastInnerRowEnd.value());
    } else {
      func(lastInputRow, 0, rawMaxSizes[lastInputRow]);
    }
  }
}
} // namespace facebook::velox::exec
