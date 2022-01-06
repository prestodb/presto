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
#include "velox/exec/Task.h"

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
      outputBatchSize_{
          driverCtx->execCtx->queryCtx()->config().preferredOutputBatchSize()},
      joinType_{joinNode->joinType()},
      numKeys_{joinNode->leftKeys().size()} {
  VELOX_USER_CHECK(
      joinNode->isInnerJoin() || joinNode->isLeftJoin(),
      "Merge join supports only inner and left joins. Other join types are not supported yet.");
  VELOX_USER_CHECK_NULL(
      joinNode->filter(), "Merge join doesn't support filter yet.");

  leftKeys_.resize(numKeys_);
  rightKeys_.resize(numKeys_);

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
}

BlockingReason MergeJoin::isBlocked(ContinueFuture* future) {
  if (hasFuture_) {
    *future = std::move(future_);
    hasFuture_ = false;
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
}

// static
int32_t MergeJoin::compare(
    const std::vector<ChannelIndex>& keys,
    const RowVectorPtr& batch,
    vector_size_t index,
    const std::vector<ChannelIndex>& otherKeys,
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

bool MergeJoin::findEndOfMatch(Match& match, const RowVectorPtr& input) {
  if (match.complete) {
    return true;
  }

  auto prevInput = match.inputs.back();
  auto prevIndex = prevInput->size() - 1;

  auto numInput = input->size();

  vector_size_t endIndex = 0;
  while (endIndex < numInput &&
         compareLeft(input, endIndex, prevInput, prevIndex) == 0) {
    ++endIndex;
  }

  if (endIndex == numInput) {
    match.inputs.push_back(input);
    match.endIndex = endIndex;
    return false;
  }

  if (endIndex > 0) {
    match.inputs.push_back(input);
    match.endIndex = endIndex;
  }

  match.complete = true;
  return true;
}

void MergeJoin::addOutputRowForLeftJoin() {
  for (auto& projection : leftProjections_) {
    auto source = input_->childAt(projection.inputChannel);
    auto target = output_->childAt(projection.outputChannel);
    target->copy(source.get(), outputSize_, index_, 1);
  }

  for (auto& projection : rightProjections_) {
    auto target = output_->childAt(projection.outputChannel);
    target->setNull(outputSize_, true);
  }

  ++outputSize_;
}

void MergeJoin::addOutputRow(
    const RowVectorPtr& left,
    vector_size_t leftIndex,
    const RowVectorPtr& right,
    vector_size_t rightIndex) {
  for (auto& projection : leftProjections_) {
    auto source = left->childAt(projection.inputChannel);
    auto target = output_->childAt(projection.outputChannel);
    target->copy(source.get(), outputSize_, leftIndex, 1);
  }

  for (auto& projection : rightProjections_) {
    auto source = right->childAt(projection.inputChannel);
    auto target = output_->childAt(projection.outputChannel);
    target->copy(source.get(), outputSize_, rightIndex, 1);
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

  // Use Operator::isFinishing() as a no-more-input-on-the-left indicator and a
  // noMoreRightInput_ flag as no-more-input-on-the-right indicator.

  // TODO Finish early if ran out of data on either side of the join.

  for (;;) {
    auto output = doGetOutput();
    if (output != nullptr) {
      return output;
    }

    // Check if we need to get more data from the right side.
    if (!noMoreRightInput_ && !hasFuture_ && !rightInput_) {
      if (!rightSource_) {
        rightSource_ = operatorCtx_->task()->getMergeJoinSource(planNodeId());
      }
      auto blockingReason = rightSource_->next(&future_, &rightInput_);
      if (blockingReason != BlockingReason::kNotBlocked) {
        hasFuture_ = true;
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
      if (!findEndOfMatch(leftMatch_.value(), input_)) {
        // Continue looking for the end of the match.
        input_ = nullptr;
        return nullptr;
      }

      if (leftMatch_->inputs.back() == input_) {
        index_ = leftMatch_->endIndex;
      }
    } else if (isFinishing()) {
      leftMatch_->complete = true;
    } else {
      // Need more input.
      return nullptr;
    }

    if (rightInput_) {
      if (!findEndOfMatch(rightMatch_.value(), rightInput_)) {
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

      if (isFinishing() && output_) {
        output_->resize(outputSize_);
        return std::move(output_);
      }
    } else {
      if (isFinishing() || noMoreRightInput_) {
        if (output_) {
          output_->resize(outputSize_);
          return std::move(output_);
        }
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
} // namespace facebook::velox::exec
