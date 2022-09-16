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
#include "velox/exec/MergeJoin.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::exec {

MergeJoin::MergeJoin(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::MergeJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "MergeJoin"),
      outputBatchSize_{driverCtx->queryConfig().preferredOutputBatchSize()},
      joinType_{joinNode->joinType()},
      numKeys_{joinNode->leftKeys().size()} {
  VELOX_USER_CHECK(
      joinNode->isInnerJoin() || joinNode->isLeftJoin(),
      "Merge join supports only inner and left joins. Other join types are not supported yet.");

  leftKeys_.reserve(numKeys_);
  rightKeys_.reserve(numKeys_);

  auto leftType = joinNode->sources()[0]->outputType();
  for (auto& key : joinNode->leftKeys()) {
    leftKeys_.push_back(leftType->getChildIdx(key->name()));
  }

  auto rightType = joinNode->sources()[1]->outputType();
  for (auto& key : joinNode->rightKeys()) {
    rightKeys_.push_back(rightType->getChildIdx(key->name()));
  }

  for (auto i = 0; i < leftType->size(); ++i) {
    auto name = leftType->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      leftProjections_.emplace_back(i, outIndex.value());
    }
  }

  for (auto i = 0; i < rightType->size(); ++i) {
    auto name = rightType->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      rightProjections_.emplace_back(i, outIndex.value());
    }
  }

  if (joinNode->filter()) {
    initializeFilter(joinNode->filter(), leftType, rightType);

    if (joinNode->isLeftJoin()) {
      leftJoinTracker_ = LeftJoinTracker(outputBatchSize_, pool());
    }
  }
}

void MergeJoin::initializeFilter(
    const core::TypedExprPtr& filter,
    const RowTypePtr& leftType,
    const RowTypePtr& rightType) {
  std::vector<core::TypedExprPtr> filters = {filter};
  filter_ =
      std::make_unique<ExprSet>(std::move(filters), operatorCtx_->execCtx());

  column_index_t filterChannel = 0;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  auto numFields = filter_->expr(0)->distinctFields().size();
  names.reserve(numFields);
  types.reserve(numFields);
  for (const auto& field : filter_->expr(0)->distinctFields()) {
    const auto& name = field->field();
    auto channel = leftType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterLeftInputs_.emplace_back(channelValue, filterChannel++);
      names.emplace_back(leftType->nameOf(channelValue));
      types.emplace_back(leftType->childAt(channelValue));
      continue;
    }
    channel = rightType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterRightInputs_.emplace_back(channelValue, filterChannel++);
      names.emplace_back(rightType->nameOf(channelValue));
      types.emplace_back(rightType->childAt(channelValue));
      continue;
    }
    VELOX_FAIL(
        "Merge join filter field not found in either left or right input: {}",
        field->toString());
  }

  filterInputType_ = ROW(std::move(names), std::move(types));
}

BlockingReason MergeJoin::isBlocked(ContinueFuture* future) {
  if (future_.valid()) {
    *future = std::move(future_);
    return BlockingReason::kWaitForExchange;
  }

  return BlockingReason::kNotBlocked;
}

bool MergeJoin::needsInput() const {
  return input_ == nullptr;
}

void MergeJoin::addInput(RowVectorPtr input) {
  input_ = std::move(input);
  index_ = 0;

  if (leftJoinTracker_) {
    leftJoinTracker_->resetLastVector();
  }
}

// static
int32_t MergeJoin::compare(
    const std::vector<column_index_t>& keys,
    const RowVectorPtr& batch,
    vector_size_t index,
    const std::vector<column_index_t>& otherKeys,
    const RowVectorPtr& otherBatch,
    vector_size_t otherIndex) {
  for (auto i = 0; i < keys.size(); ++i) {
    auto compare = batch->childAt(keys[i])->compare(
        otherBatch->childAt(otherKeys[i]).get(), index, otherIndex);
    if (compare != 0) {
      return compare;
    }
  }

  return 0;
}

bool MergeJoin::findEndOfMatch(
    Match& match,
    const RowVectorPtr& input,
    const std::vector<column_index_t>& keys) {
  if (match.complete) {
    return true;
  }

  auto prevInput = match.inputs.back();
  auto prevIndex = prevInput->size() - 1;

  auto numInput = input->size();

  vector_size_t endIndex = 0;
  while (endIndex < numInput &&
         compare(keys, input, endIndex, keys, prevInput, prevIndex) == 0) {
    ++endIndex;
  }

  if (endIndex == numInput) {
    // Inputs are kept past getting a new batch of inputs. LazyVectors
    // must be loaded before advancing to the next batch.
    loadColumns(input, *operatorCtx_->execCtx());
    match.inputs.push_back(input);
    match.endIndex = endIndex;
    return false;
  }

  if (endIndex > 0) {
    // Match ends here, no need to pre-load lazies.
    match.inputs.push_back(input);
    match.endIndex = endIndex;
  }
  match.complete = true;
  return true;
}

namespace {
void copyRow(
    const RowVectorPtr& source,
    vector_size_t sourceIndex,
    const RowVectorPtr& target,
    vector_size_t targetIndex,
    const std::vector<IdentityProjection>& projections) {
  for (const auto& projection : projections) {
    const auto& sourceChild = source->childAt(projection.inputChannel);
    const auto& targetChild = target->childAt(projection.outputChannel);
    targetChild->copy(sourceChild.get(), targetIndex, sourceIndex, 1);
  }
}
} // namespace

void MergeJoin::addOutputRowForLeftJoin() {
  copyRow(input_, index_, output_, outputSize_, leftProjections_);

  for (const auto& projection : rightProjections_) {
    const auto& target = output_->childAt(projection.outputChannel);
    target->setNull(outputSize_, true);
  }

  if (leftJoinTracker_) {
    // Record left-side row with no match on the right side.
    leftJoinTracker_->addMiss(outputSize_);
  }

  ++outputSize_;
}

void MergeJoin::addOutputRow(
    const RowVectorPtr& left,
    vector_size_t leftIndex,
    const RowVectorPtr& right,
    vector_size_t rightIndex) {
  copyRow(left, leftIndex, output_, outputSize_, leftProjections_);
  copyRow(right, rightIndex, output_, outputSize_, rightProjections_);

  if (filter_) {
    // TODO Re-use output_ columns when possible.

    copyRow(left, leftIndex, filterInput_, outputSize_, filterLeftInputs_);
    copyRow(right, rightIndex, filterInput_, outputSize_, filterRightInputs_);

    if (leftJoinTracker_) {
      // Record left-side row with a match on the right-side.
      leftJoinTracker_->addMatch(left, leftIndex, outputSize_);
    }
  }

  ++outputSize_;
}

void MergeJoin::prepareOutput() {
  if (output_ == nullptr) {
    std::vector<VectorPtr> localColumns(outputType_->size());
    for (auto i = 0; i < outputType_->size(); ++i) {
      localColumns[i] = BaseVector::create(
          outputType_->childAt(i), outputBatchSize_, operatorCtx_->pool());
    }

    output_ = std::make_shared<RowVector>(
        operatorCtx_->pool(),
        outputType_,
        nullptr,
        outputBatchSize_,
        std::move(localColumns));
    outputSize_ = 0;

    if (filterInput_ != nullptr) {
      // When filterInput_ contains array or map columns, their child vectors
      // (elements, keys and values) keep growing after each call to
      // 'copyRow'. Call BaseVector::resize(0) on these child vectors to avoid
      // that.
      // TODO Refactor this logic into a method on BaseVector.
      for (auto& child : filterInput_->children()) {
        if (child->typeKind() == TypeKind::ARRAY) {
          child->as<ArrayVector>()->elements()->resize(0);
        } else if (child->typeKind() == TypeKind::MAP) {
          auto* mapChild = child->as<MapVector>();
          mapChild->mapKeys()->resize(0);
          mapChild->mapValues()->resize(0);
        }
      }
    }
  }

  if (filter_ != nullptr && filterInput_ == nullptr) {
    std::vector<VectorPtr> inputs(filterInputType_->size());
    for (auto i = 0; i < filterInputType_->size(); ++i) {
      inputs[i] = BaseVector::create(
          filterInputType_->childAt(i), outputBatchSize_, operatorCtx_->pool());
    }

    filterInput_ = std::make_shared<RowVector>(
        operatorCtx_->pool(),
        filterInputType_,
        nullptr,
        outputBatchSize_,
        std::move(inputs));
  }
}

bool MergeJoin::addToOutput() {
  prepareOutput();

  size_t firstLeftBatch;
  vector_size_t leftStartIndex;
  if (leftMatch_->cursor) {
    firstLeftBatch = leftMatch_->cursor->batchIndex;
    leftStartIndex = leftMatch_->cursor->index;
  } else {
    firstLeftBatch = 0;
    leftStartIndex = leftMatch_->startIndex;
  }

  size_t numLefts = leftMatch_->inputs.size();
  for (size_t l = firstLeftBatch; l < numLefts; ++l) {
    auto left = leftMatch_->inputs[l];
    auto leftStart = l == firstLeftBatch ? leftStartIndex : 0;
    auto leftEnd = l == numLefts - 1 ? leftMatch_->endIndex : left->size();

    for (auto i = leftStart; i < leftEnd; ++i) {
      auto firstRightBatch =
          (l == firstLeftBatch && i == leftStart && rightMatch_->cursor)
          ? rightMatch_->cursor->batchIndex
          : 0;

      auto rightStartIndex =
          (l == firstLeftBatch && i == leftStart && rightMatch_->cursor)
          ? rightMatch_->cursor->index
          : rightMatch_->startIndex;

      auto numRights = rightMatch_->inputs.size();
      for (size_t r = firstRightBatch; r < numRights; ++r) {
        auto right = rightMatch_->inputs[r];
        auto rightStart = r == firstRightBatch ? rightStartIndex : 0;
        auto rightEnd =
            r == numRights - 1 ? rightMatch_->endIndex : right->size();

        for (auto j = rightStart; j < rightEnd; ++j) {
          if (outputSize_ == outputBatchSize_) {
            leftMatch_->setCursor(l, i);
            rightMatch_->setCursor(r, j);
            return true;
          }
          addOutputRow(left, i, right, j);
        }
      }
    }
  }

  leftMatch_.reset();
  rightMatch_.reset();

  return outputSize_ == outputBatchSize_;
}

RowVectorPtr MergeJoin::getOutput() {
  // Make sure to have is-blocked or needs-input as true if returning null
  // output. Otherwise, Driver assumes the operator is finished.

  // Use Operator::noMoreInput() as a no-more-input-on-the-left indicator and a
  // noMoreRightInput_ flag as no-more-input-on-the-right indicator.

  // TODO Finish early if ran out of data on either side of the join.

  for (;;) {
    auto output = doGetOutput();
    if (output != nullptr) {
      if (filter_) {
        output = applyFilter(output);
        if (output != nullptr) {
          return output;
        }

        // No rows survived the filter. Get more rows.
        continue;
      } else {
        return output;
      }
    }

    // Check if we need to get more data from the right side.
    if (!noMoreRightInput_ && !future_.valid() && !rightInput_) {
      if (!rightSource_) {
        rightSource_ = operatorCtx_->task()->getMergeJoinSource(
            operatorCtx_->driverCtx()->splitGroupId, planNodeId());
      }
      auto blockingReason = rightSource_->next(&future_, &rightInput_);
      if (blockingReason != BlockingReason::kNotBlocked) {
        return nullptr;
      }

      if (rightInput_) {
        rightIndex_ = 0;
      } else {
        noMoreRightInput_ = true;
      }
      continue;
    }

    return nullptr;
  }
}

RowVectorPtr MergeJoin::doGetOutput() {
  // Check if we ran out of space in the output vector in the middle of the
  // match.
  if (leftMatch_ && leftMatch_->cursor) {
    VELOX_CHECK(rightMatch_ && rightMatch_->cursor);

    // Not all rows from the last match fit in the output. Continue producing
    // results from the current match.
    if (addToOutput()) {
      return std::move(output_);
    }
  }

  // There is no output-in-progress match, but there could be incomplete
  // match.
  if (leftMatch_) {
    VELOX_CHECK(rightMatch_);

    if (input_) {
      // Look for continuation of a match on the left and/or right sides.
      if (!findEndOfMatch(leftMatch_.value(), input_, leftKeys_)) {
        // Continue looking for the end of the match.
        input_ = nullptr;
        return nullptr;
      }

      if (leftMatch_->inputs.back() == input_) {
        index_ = leftMatch_->endIndex;
      }
    } else if (noMoreInput_) {
      leftMatch_->complete = true;
    } else {
      // Need more input.
      return nullptr;
    }

    if (rightInput_) {
      if (!findEndOfMatch(rightMatch_.value(), rightInput_, rightKeys_)) {
        // Continue looking for the end of the match.
        rightInput_ = nullptr;
        return nullptr;
      }
      if (rightMatch_->inputs.back() == rightInput_) {
        rightIndex_ = rightMatch_->endIndex;
      }
    } else if (noMoreRightInput_) {
      rightMatch_->complete = true;
    } else {
      // Need more input.
      return nullptr;
    }
  }

  // There is no output-in-progress match, but there can be a complete match
  // ready for output.
  if (leftMatch_) {
    VELOX_CHECK(leftMatch_->complete);
    VELOX_CHECK(rightMatch_ && rightMatch_->complete);

    if (addToOutput()) {
      return std::move(output_);
    }
  }

  if (!input_ || !rightInput_) {
    if (isLeftJoin(joinType_)) {
      if (input_ && noMoreRightInput_) {
        prepareOutput();
        while (true) {
          if (outputSize_ == outputBatchSize_) {
            return std::move(output_);
          }

          addOutputRowForLeftJoin();

          ++index_;
          if (index_ == input_->size()) {
            // Ran out of rows on the left side.
            input_ = nullptr;
            return nullptr;
          }
        }
      }

      if (noMoreInput_ && output_) {
        output_->resize(outputSize_);
        return std::move(output_);
      }
    } else {
      if (noMoreInput_ || noMoreRightInput_) {
        if (output_) {
          output_->resize(outputSize_);
          return std::move(output_);
        }
        input_ = nullptr;
      }
    }

    return nullptr;
  }

  // Look for a new match starting with index_ row on the left and rightIndex_
  // row on the right.
  auto compareResult = compare();

  for (;;) {
    // Catch up input_ with rightInput_.
    while (compareResult < 0) {
      if (isLeftJoin(joinType_)) {
        prepareOutput();

        if (outputSize_ == outputBatchSize_) {
          return std::move(output_);
        }

        addOutputRowForLeftJoin();
      }

      ++index_;
      if (index_ == input_->size()) {
        // Ran out of rows on the left side.
        input_ = nullptr;
        return nullptr;
      }
      compareResult = compare();
    }

    // Catch up rightInput_ with input_.
    while (compareResult > 0) {
      ++rightIndex_;
      if (rightIndex_ == rightInput_->size()) {
        // Ran out of rows on the right side.
        rightInput_ = nullptr;
        return nullptr;
      }
      compareResult = compare();
    }

    if (compareResult == 0) {
      // Found a match. Identify all rows on the left and right that have the
      // matching keys.
      vector_size_t endIndex = index_ + 1;
      while (endIndex < input_->size() && compareLeft(endIndex) == 0) {
        ++endIndex;
      }

      if (endIndex == input_->size()) {
        // Matches continue in subsequent input. Load all lazies.
        loadColumns(input_, *operatorCtx_->execCtx());
      }
      leftMatch_ = Match{
          {input_}, index_, endIndex, endIndex < input_->size(), std::nullopt};

      vector_size_t endRightIndex = rightIndex_ + 1;
      while (endRightIndex < rightInput_->size() &&
             compareRight(endRightIndex) == 0) {
        ++endRightIndex;
      }

      rightMatch_ = Match{
          {rightInput_},
          rightIndex_,
          endRightIndex,
          endRightIndex < rightInput_->size(),
          std::nullopt};

      if (!leftMatch_->complete || !rightMatch_->complete) {
        if (!leftMatch_->complete) {
          // Need to continue looking for the end of match.
          input_ = nullptr;
        }

        if (!rightMatch_->complete) {
          // Need to continue looking for the end of match.
          rightInput_ = nullptr;
        }
        return nullptr;
      }

      index_ = endIndex;
      rightIndex_ = endRightIndex;

      if (addToOutput()) {
        return std::move(output_);
      }

      compareResult = compare();
    }
  }

  VELOX_UNREACHABLE();
}

RowVectorPtr MergeJoin::applyFilter(const RowVectorPtr& output) {
  const auto numRows = output->size();

  BufferPtr indices = allocateIndices(numRows, pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  vector_size_t numPassed = 0;

  if (leftJoinTracker_) {
    const auto& filterRows = leftJoinTracker_->matchingRows(numRows);

    if (!filterRows.hasSelections()) {
      // No matches in the output, no need to evaluate the filter.
      return output;
    }

    evaluateFilter(filterRows);

    // If all matches for a given left-side row fail the filter, add a row to
    // the output with nulls for the right-side columns.
    auto onMiss = [&](auto row) {
      rawIndices[numPassed++] = row;

      for (auto& projection : rightProjections_) {
        auto target = output->childAt(projection.outputChannel);
        target->setNull(row, true);
      }
    };

    for (auto i = 0; i < numRows; ++i) {
      if (filterRows.isValid(i)) {
        const bool passed = !decodedFilterResult_.isNullAt(i) &&
            decodedFilterResult_.valueAt<bool>(i);

        leftJoinTracker_->processFilterResult(i, passed, onMiss);

        if (passed) {
          rawIndices[numPassed++] = i;
        }
      } else {
        // This row doesn't have a match on the right side. Keep it
        // unconditionally.
        rawIndices[numPassed++] = i;
      }
    }

    if (!leftMatch_) {
      leftJoinTracker_->noMoreFilterResults(onMiss);
    }
  } else {
    filterRows_.resize(numRows);
    filterRows_.setAll();

    evaluateFilter(filterRows_);

    for (auto i = 0; i < numRows; ++i) {
      if (!decodedFilterResult_.isNullAt(i) &&
          decodedFilterResult_.valueAt<bool>(i)) {
        rawIndices[numPassed++] = i;
      }
    }
  }

  if (numPassed == 0) {
    // No rows passed.
    return nullptr;
  }

  if (numPassed == numRows) {
    // All rows passed.
    return output;
  }

  // Some, but not all rows passed.
  return wrap(numPassed, indices, output);
}

void MergeJoin::evaluateFilter(const SelectivityVector& rows) {
  EvalCtx evalCtx(operatorCtx_->execCtx(), filter_.get(), filterInput_.get());
  filter_->eval(0, 1, true, rows, evalCtx, filterResult_);

  decodedFilterResult_.decode(*filterResult_[0], rows);
}

bool MergeJoin::isFinished() {
  return noMoreInput_ && input_ == nullptr;
}

} // namespace facebook::velox::exec
