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

bool isSemiFilterJoin(core::JoinType joinType) {
  return isLeftSemiFilterJoin(joinType) || isRightSemiFilterJoin(joinType);
}
} // namespace

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
      outputBatchSize_{outputBatchRows()},
      joinType_{joinNode->joinType()},
      numKeys_{joinNode->leftKeys().size()},
      rightNodeId_{joinNode->sources()[1]->id()},
      joinNode_(joinNode) {
  VELOX_USER_CHECK(
      core::MergeJoinNode::isSupported(joinType_),
      "The join type is not supported by merge join: {}",
      core::JoinTypeName::toName(joinType_));
}

void MergeJoin::initialize() {
  Operator::initialize();
  VELOX_CHECK_NOT_NULL(joinNode_);

  leftKeyChannels_.reserve(numKeys_);
  rightKeyChannels_.reserve(numKeys_);

  const auto& leftType = joinNode_->sources()[0]->outputType();
  for (const auto& key : joinNode_->leftKeys()) {
    leftKeyChannels_.push_back(leftType->getChildIdx(key->name()));
  }

  const auto& rightType = joinNode_->sources()[1]->outputType();
  for (const auto& key : joinNode_->rightKeys()) {
    rightKeyChannels_.push_back(rightType->getChildIdx(key->name()));
  }

  for (auto i = 0; i < leftType->size(); ++i) {
    const auto name = leftType->nameOf(i);
    const auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      leftProjections_.emplace_back(i, outIndex.value());
    }
  }

  if (joinNode_->isRightSemiFilterJoin()) {
    VELOX_USER_CHECK(
        leftProjections_.empty(),
        "The left side projections should be empty for right semi join");
  }

  for (auto i = 0; i < rightType->size(); ++i) {
    const auto name = rightType->nameOf(i);
    const auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      rightProjections_.emplace_back(i, outIndex.value());
    }
  }

  if (joinNode_->isLeftSemiFilterJoin()) {
    VELOX_USER_CHECK(
        rightProjections_.empty(),
        "The right side projections should be empty for left semi join");
  }

  if (joinNode_->filter()) {
    initializeFilter(joinNode_->filter(), leftType, rightType);

    if (joinNode_->isLeftJoin() || joinNode_->isAntiJoin() ||
        joinNode_->isRightJoin() || joinNode_->isFullJoin() ||
        isSemiFilterJoin(joinType_)) {
      joinTracker_ = JoinTracker(outputBatchSize_, pool());
    }
  } else if (joinNode_->isAntiJoin()) {
    // Anti join needs to track the left side rows that have no match on the
    // right.
    joinTracker_ = JoinTracker(outputBatchSize_, pool());
  }

  joinNode_.reset();
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
  const auto numFields = filter_->expr(0)->distinctFields().size();
  names.reserve(numFields);
  types.reserve(numFields);

  // This function is called in a loop for all columns used in a filter
  // expression. For each column, 'channel' specifies column index in the left
  // or right side of the join input, 'outputs' specifies the mapping from input
  // columns of that side of the join to the output of the join; 'filters'
  // specifies the mapping from input columns to the 'filterInput_' for columns
  // that are not projected to the output.
  //
  // This function checks whether the column is projected to the output and if
  // so adds an entry to 'filterInputToOutputChannel_'. Otherwise, adds an entry
  // to 'filters'.
  //
  // At the end of the loop, each column used in a filter appears either in:
  //
  //  - filterInputToOutputChannel_: in case the column is projected to the join
  //  output;
  //  - filterLeftInputs_: in case the column is not projected to the join
  //  output and it comes from the left side of the join;
  //  - filterRightInputs_: in case the column is not projected to the join
  //  output and it comes from the right side of the join.
  const auto addChannel = [&](column_index_t channel,
                              const std::vector<IdentityProjection>& outputs,
                              const RowTypePtr& inputType,
                              std::vector<IdentityProjection>& filters) {
    names.emplace_back(inputType->nameOf(channel));
    types.emplace_back(inputType->childAt(channel));

    for (const auto [inputChannel, outputChannel] : outputs) {
      if (inputChannel == channel) {
        filterInputToOutputChannel_.emplace(filterChannel++, outputChannel);
        return;
      }
    }
    filters.emplace_back(channel, filterChannel++);
  };

  for (const auto& field : filter_->expr(0)->distinctFields()) {
    const auto& name = field->field();
    auto channel = leftType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      addChannel(
          channel.value(),
          leftProjections_,
          leftType,
          filterLeftInputProjections_);
      continue;
    }
    channel = rightType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      addChannel(
          channel.value(),
          rightProjections_,
          rightType,
          filterRightInputProjections_);
      continue;
    }
    VELOX_FAIL(
        "Merge join filter field not found in either left or right input: {}",
        field->toString());
  }

  filterInputType_ = ROW(std::move(names), std::move(types));
}

BlockingReason MergeJoin::isBlocked(ContinueFuture* future) {
  if (rightSideInputFuture_.valid()) {
    *future = std::move(rightSideInputFuture_);
    return BlockingReason::kWaitForMergeJoinRightSide;
  }

  return BlockingReason::kNotBlocked;
}

bool MergeJoin::needsInput() const {
  return input_ == nullptr;
}

void MergeJoin::addInput(RowVectorPtr input) {
  VELOX_CHECK_NULL(input_);
  input_ = std::move(input);
  leftRowIndex_ = 0;

  if (joinTracker_) {
    joinTracker_->resetLastVector();
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
    static const CompareFlags kCompareFlags = {
        .equalsOnly = true,
        .nullHandlingMode =
            CompareFlags::NullHandlingMode::kNullAsIndeterminate};
    const auto compare = batch->childAt(keys[i])->compare(
        otherBatch->childAt(otherKeys[i]).get(),
        index,
        otherIndex,
        kCompareFlags);

    // Comparing null with anything will return std::nullopt.
    if (!compare.has_value()) {
      // The SQL semantics of Presto and Spark will always return false if
      // comparing a NULL value with any other value.
      return -1;
    } else if (compare.value() != 0) {
      return compare.value();
    }
  }

  return 0;
}

bool MergeJoin::findEndOfMatch(
    const RowVectorPtr& input,
    const std::vector<column_index_t>& keys,
    Match& match) {
  if (match.complete) {
    return true;
  }

  const auto prevInput = match.inputs.back();
  const auto prevIndex = prevInput->size() - 1;
  const auto numInputRows = input->size();

  vector_size_t endRow = 0;
  while (endRow < numInputRows &&
         compare(keys, input, endRow, keys, prevInput, prevIndex) == 0) {
    ++endRow;
  }

  if (endRow == numInputRows) {
    // Inputs are kept past getting a new batch of inputs. LazyVectors
    // must be loaded before advancing to the next batch.
    loadColumns(input, *operatorCtx_->execCtx());
    match.inputs.push_back(input);
    match.endRowIndex = endRow;
    return false;
  }

  if (endRow > 0) {
    // Match ends here, no need to pre-load lazies.
    match.inputs.push_back(input);
    match.endRowIndex = endRow;
  }
  match.complete = true;
  return true;
}

inline void addNull(
    VectorPtr& target,
    vector_size_t index,
    const BufferPtr& indices,
    vector_size_t outputBatchSize,
    const VectorPtr& currentInput) {
  // When we call wrapInDictionary on the input projections, if they are
  // Constants, we will get back a resized ConstantVector rather than a
  // DictionaryVector.  This works great unless we want to add nulls due to join
  // misses. This branch handles converting the ConstantVector to a
  // DictionaryVector in this case. The assumption here is this check is cheap
  // and misses are rare so the cost of this check is outweighed by the benefits
  // of propagating ConstantVectors.
  if (target->isConstantEncoding()) {
    target = BaseVector::wrapInDictionary(
        allocateNulls(outputBatchSize, target->pool()),
        indices,
        outputBatchSize,
        BaseVector::wrapInConstant(currentInput->size(), 0, target));
  }

  target->setNull(index, true);
}

bool MergeJoin::tryAddOutputRowForLeftJoin() {
  VELOX_USER_CHECK(
      isLeftJoin(joinType_) || isAntiJoin(joinType_) || isFullJoin(joinType_));
  if (outputSize_ == outputBatchSize_) {
    return false;
  }

  rawLeftOutputIndices_[outputSize_] = leftRowIndex_++;

  for (const auto& projection : rightProjections_) {
    auto& target = output_->childAt(projection.outputChannel);
    addNull(
        target,
        outputSize_,
        rightOutputIndices_,
        outputBatchSize_,
        currentRight_);
  }

  if (joinTracker_) {
    // Record left-side row with no match on the right side.
    joinTracker_->addMiss(outputSize_);
  }

  ++outputSize_;
  return true;
}

bool MergeJoin::tryAddOutputRowForRightJoin() {
  VELOX_CHECK(isRightJoin(joinType_) || isFullJoin(joinType_));
  if (outputSize_ == outputBatchSize_) {
    return false;
  }

  rawRightOutputIndices_[outputSize_] = rightRowIndex_++;
  for (const auto& projection : leftProjections_) {
    auto& target = output_->childAt(projection.outputChannel);
    addNull(
        target,
        outputSize_,
        leftOutputIndices_,
        outputBatchSize_,
        currentLeft_);
  }

  if (joinTracker_) {
    // Record right-side row with no match on the left side.
    joinTracker_->addMiss(outputSize_);
  }

  ++outputSize_;
  return true;
}

void MergeJoin::flattenRightProjections() {
  auto& children = output_->children();

  for (const auto& projection : rightProjections_) {
    auto& currentVector = children[projection.outputChannel];
    auto newFlat = BaseVector::create(
        currentVector->type(), outputBatchSize_, operatorCtx_->pool());
    newFlat->copy(currentVector.get(), 0, 0, outputSize_);
    children[projection.outputChannel] = std::move(newFlat);
  }
  isRightFlattened_ = true;
}

bool MergeJoin::tryAddOutputRow(
    const RowVectorPtr& leftBatch,
    vector_size_t leftRow,
    const RowVectorPtr& rightBatch,
    vector_size_t rightRow) {
  if (outputSize_ == outputBatchSize_) {
    return false;
  }

  // All left side projections share the same dictionary indices (leftIndices_).
  rawLeftOutputIndices_[outputSize_] = leftRow;

  // The right side projections can be a dictionary, of flat in case they
  // crossed a buffer boundary. In the latter case, row values need to be
  // copied.
  if (!isRightFlattened_) {
    // All right side projections share the same dictionary indices
    // (rightIndices_).
    rawRightOutputIndices_[outputSize_] = rightRow;
  } else {
    copyRow(rightBatch, rightRow, output_, outputSize_, rightProjections_);
  }

  if (filter_) {
    copyRow(
        leftBatch,
        leftRow,
        filterInput_,
        outputSize_,
        filterLeftInputProjections_);
    copyRow(
        rightBatch,
        rightRow,
        filterInput_,
        outputSize_,
        filterRightInputProjections_);

    if (joinTracker_) {
      if (isRightJoin(joinType_) || isRightSemiFilterJoin(joinType_)) {
        // Record right-side row with a match on the left-side.
        joinTracker_->addMatch(rightBatch, rightRow, outputSize_);
      } else {
        // Record left-side row with a match on the right-side.
        joinTracker_->addMatch(leftBatch, leftRow, outputSize_);
      }
    }
  }

  // Anti join needs to track the left side rows that have no match on the
  // right.
  if (isAntiJoin(joinType_)) {
    VELOX_CHECK(joinTracker_.has_value());
    // Record left-side row with a match on the right-side.
    joinTracker_->addMatch(leftBatch, leftRow, outputSize_);
  }

  ++outputSize_;
  return true;
}

bool MergeJoin::prepareOutput(
    const RowVectorPtr& left,
    const RowVectorPtr& right) {
  // If there is already an allocated output_, check if we can use it.
  if (output_ != nullptr) {
    // If there is a new left, we can't continue using it as the old one is the
    // base for the current left dictionary.
    if (left != currentLeft_) {
      return true;
    }

    if (isRightJoin(joinType_) && right != currentRight_) {
      return true;
    }

    // If there is a new right, we need to flatten the dictionary.
    if (!isRightFlattened_ && right && currentRight_ != right) {
      flattenRightProjections();
    }
    return false;
  }

  // If output is nullptr, first allocate dictionary indices for the left and
  // right side projections.
  leftOutputIndices_ = allocateIndices(outputBatchSize_, pool());
  rawLeftOutputIndices_ = leftOutputIndices_->asMutable<vector_size_t>();

  rightOutputIndices_ = allocateIndices(outputBatchSize_, pool());
  rawRightOutputIndices_ = rightOutputIndices_->asMutable<vector_size_t>();

  // Create left side projection outputs.
  std::vector<VectorPtr> localColumns(outputType_->size());
  if (left == nullptr) {
    for (const auto& projection : leftProjections_) {
      localColumns[projection.outputChannel] = BaseVector::create(
          outputType_->childAt(projection.outputChannel),
          outputBatchSize_,
          operatorCtx_->pool());
    }
  } else {
    for (const auto& projection : leftProjections_) {
      localColumns[projection.outputChannel] = BaseVector::wrapInDictionary(
          {},
          leftOutputIndices_,
          outputBatchSize_,
          left->childAt(projection.inputChannel));
    }
  }
  currentLeft_ = left;

  // Create right side projection outputs.
  if (right == nullptr) {
    for (const auto& projection : rightProjections_) {
      localColumns[projection.outputChannel] = BaseVector::create(
          outputType_->childAt(projection.outputChannel),
          outputBatchSize_,
          operatorCtx_->pool());
    }
    isRightFlattened_ = true;
  } else {
    for (const auto& projection : rightProjections_) {
      localColumns[projection.outputChannel] = BaseVector::wrapInDictionary(
          {},
          rightOutputIndices_,
          outputBatchSize_,
          right->childAt(projection.inputChannel));
    }
    isRightFlattened_ = false;
  }
  currentRight_ = right;

  output_ = std::make_shared<RowVector>(
      operatorCtx_->pool(),
      outputType_,
      nullptr,
      outputBatchSize_,
      std::move(localColumns));
  outputSize_ = 0;

  if (filterInput_ != nullptr) {
    for (auto i = 0; i < filterInputType_->size(); ++i) {
      auto& child = filterInput_->childAt(i);
      // If 'child' is also projected to output, make 'child' to use the same
      // shared pointer to the output column.
      if (filterInputToOutputChannel_.find(i) !=
          filterInputToOutputChannel_.end()) {
        child = output_->childAt(filterInputToOutputChannel_[i]);
        continue;
      }

      // When filterInput_ contains array or map columns that are not
      // projected to output, their child vectors(elements, keys and values)
      // keep growing after each call to 'copyRow'. Call prepareForReuse() to
      // reset non-reusable buffers and updates child vectors for reusing.
      if (child->typeKind() == TypeKind::ARRAY) {
        child->as<ArrayVector>()->prepareForReuse();
      } else if (child->typeKind() == TypeKind::MAP) {
        child->as<MapVector>()->prepareForReuse();
      }
    }
  }

  if (filter_ != nullptr && filterInput_ == nullptr) {
    std::vector<VectorPtr> inputs(filterInputType_->size());
    for (auto i = 0; i < filterInputType_->size(); ++i) {
      if (filterInputToOutputChannel_.find(i) !=
          filterInputToOutputChannel_.end()) {
        inputs[i] = output_->childAt(filterInputToOutputChannel_[i]);
      } else {
        inputs[i] = BaseVector::create(
            filterInputType_->childAt(i),
            outputBatchSize_,
            operatorCtx_->pool());
      }
    }

    filterInput_ = std::make_shared<RowVector>(
        operatorCtx_->pool(),
        filterInputType_,
        nullptr,
        outputBatchSize_,
        std::move(inputs));
  }
  return false;
}

bool MergeJoin::addToOutput() {
  if (isRightJoin(joinType_) || isRightSemiFilterJoin(joinType_)) {
    return addToOutputForRightJoin();
  } else {
    return addToOutputForLeftJoin();
  }
}

bool MergeJoin::addToOutputForLeftJoin() {
  size_t firstLeftBatch;
  vector_size_t leftStartRowIndex;
  if (leftMatch_->cursor) {
    firstLeftBatch = leftMatch_->cursor->batchIndex;
    leftStartRowIndex = leftMatch_->cursor->rowIndex;
  } else {
    firstLeftBatch = 0;
    leftStartRowIndex = leftMatch_->startRowIndex;
  }

  const size_t numLeftBatches = leftMatch_->inputs.size();
  for (size_t leftBatchIndex = firstLeftBatch; leftBatchIndex < numLeftBatches;
       ++leftBatchIndex) {
    const auto& leftBatch = leftMatch_->inputs[leftBatchIndex];
    const auto leftStartRow =
        leftBatchIndex == firstLeftBatch ? leftStartRowIndex : 0;
    const auto leftEndRow = leftBatchIndex == numLeftBatches - 1
        ? leftMatch_->endRowIndex
        : leftBatch->size();

    for (auto leftRow = leftStartRow; leftRow < leftEndRow; ++leftRow) {
      const bool firstLeftRow =
          leftBatchIndex == firstLeftBatch && leftRow == leftStartRow;
      const auto firstRightBatch = (firstLeftRow && rightMatch_->cursor)
          ? rightMatch_->cursor->batchIndex
          : 0;

      const auto rightStartRowIndex = (firstLeftRow && rightMatch_->cursor)
          ? rightMatch_->cursor->rowIndex
          : rightMatch_->startRowIndex;

      const auto numRightBatches = rightMatch_->inputs.size();
      // TODO: Since semi joins only require determining if there is at least
      // one match on the other side, we could explore specialized algorithms
      // or data structures that short-circuit the join process once a match
      // is found.
      for (size_t rightBatchIndex =
               (isLeftSemiFilterJoin(joinType_) && !filter_)
               ? numRightBatches - 1
               : firstRightBatch;
           rightBatchIndex < numRightBatches;
           ++rightBatchIndex) {
        const auto& rightBatch = rightMatch_->inputs[rightBatchIndex];
        auto rightStartRow =
            rightBatchIndex == firstRightBatch ? rightStartRowIndex : 0;
        const auto rightEndRow = rightBatchIndex == numRightBatches - 1
            ? rightMatch_->endRowIndex
            : rightBatch->size();
        if (isLeftSemiFilterJoin(joinType_) && !filter_) {
          rightStartRow = rightEndRow - 1;
        }
        if (prepareOutput(leftBatch, rightBatch)) {
          output_->resize(outputSize_);
          leftMatch_->setCursor(leftBatchIndex, leftRow);
          rightMatch_->setCursor(rightBatchIndex, rightStartRow);
          return true;
        }

        for (auto rightRow = rightStartRow; rightRow < rightEndRow;
             ++rightRow) {
          if (!tryAddOutputRow(leftBatch, leftRow, rightBatch, rightRow)) {
            // If we run out of space in the current output_, we will need to
            // produce a buffer and continue processing left later. In this
            // case, we cannot leave left as a lazy vector, since we cannot have
            // two dictionaries wrapping the same lazy vector.
            loadColumns(currentLeft_, *operatorCtx_->execCtx());
            leftMatch_->setCursor(leftBatchIndex, leftRow);
            rightMatch_->setCursor(rightBatchIndex, rightRow);
            return true;
          }
        }
      }
    }
  }

  leftMatch_.reset();
  rightMatch_.reset();

  // If the current key match finished, but there are still records to be
  // processed in the left, we need to load lazy vectors (see comment above).
  if (input_ && leftRowIndex_ != input_->size()) {
    loadColumns(currentLeft_, *operatorCtx_->execCtx());
  }
  return outputSize_ == outputBatchSize_;
}

bool MergeJoin::addToOutputForRightJoin() {
  size_t firstRightBatch;
  vector_size_t rightStartRowIndex;
  if (rightMatch_->cursor) {
    firstRightBatch = rightMatch_->cursor->batchIndex;
    rightStartRowIndex = rightMatch_->cursor->rowIndex;
  } else {
    firstRightBatch = 0;
    rightStartRowIndex = rightMatch_->startRowIndex;
  }

  const size_t numRightBatches = rightMatch_->inputs.size();
  for (size_t rightBatchIndex = firstRightBatch;
       rightBatchIndex < numRightBatches;
       ++rightBatchIndex) {
    const auto& rightBatch = rightMatch_->inputs[rightBatchIndex];
    const auto rightStartRow =
        rightBatchIndex == firstRightBatch ? rightStartRowIndex : 0;
    const auto rightEndRow = rightBatchIndex == numRightBatches - 1
        ? rightMatch_->endRowIndex
        : rightBatch->size();

    for (auto rightRow = rightStartRow; rightRow < rightEndRow; ++rightRow) {
      const bool firstRightRow =
          rightBatchIndex == firstRightBatch && rightRow == rightStartRow;
      const auto firstLeftBatch = (firstRightRow && leftMatch_->cursor)
          ? leftMatch_->cursor->batchIndex
          : 0;
      const auto leftStartRowIndex = (firstRightRow && leftMatch_->cursor)
          ? leftMatch_->cursor->rowIndex
          : leftMatch_->startRowIndex;

      const auto numLeftBatches = leftMatch_->inputs.size();
      // TODO: Since semi joins only require determining if there is at least
      // one match on the other side, we could explore specialized algorithms
      // or data structures that short-circuit the join process once a match
      // is found.
      for (size_t leftBatchIndex =
               (isRightSemiFilterJoin(joinType_) && !filter_)
               ? numLeftBatches - 1
               : firstLeftBatch;
           leftBatchIndex < numLeftBatches;
           ++leftBatchIndex) {
        const auto& leftBatch = leftMatch_->inputs[leftBatchIndex];
        auto leftStartRow =
            leftBatchIndex == firstLeftBatch ? leftStartRowIndex : 0;
        const auto leftEndRow = leftBatchIndex == numLeftBatches - 1
            ? leftMatch_->endRowIndex
            : leftBatch->size();
        if (isRightSemiFilterJoin(joinType_) && !filter_) {
          // RightSemiFilter produce each row from the right at most once.
          leftStartRow = leftEndRow - 1;
        }

        if (prepareOutput(leftBatch, rightBatch)) {
          // Differently from left joins, for right joins we need to load lazies
          // (from the left) whenever we detect we have to move to the next
          // right batch, since this means that we will produce this buffer, but
          // we may have subsequent matches.
          loadColumns(leftBatch, *operatorCtx_->execCtx());
          output_->resize(outputSize_);
          leftMatch_->setCursor(leftBatchIndex, leftStartRow);
          rightMatch_->setCursor(rightBatchIndex, rightRow);
          return true;
        }

        for (auto leftRow = leftStartRow; leftRow < leftEndRow; ++leftRow) {
          if (!tryAddOutputRow(leftBatch, leftRow, rightBatch, rightRow)) {
            // If we run out of space in the current output_, we will need to
            // produce a buffer and continue processing left later. In this
            // case, we cannot leave left as a lazy vector, since we cannot have
            // two dictionaries wrapping the same lazy vector.
            loadColumns(currentLeft_, *operatorCtx_->execCtx());
            rightMatch_->setCursor(rightBatchIndex, rightRow);
            leftMatch_->setCursor(leftBatchIndex, leftRow);
            return true;
          }
        }
      }
    }
  }

  leftMatch_.reset();
  rightMatch_.reset();

  // If the current key match finished, but there are still records to be
  // processed in the left, we need to load lazy vectors (see comment above).
  if (rightInput_ && rightRowIndex_ != rightInput_->size()) {
    loadColumns(currentLeft_, *operatorCtx_->execCtx());
  }
  return outputSize_ == outputBatchSize_;
}

namespace {
vector_size_t firstNonNull(
    const RowVectorPtr& rowVector,
    const std::vector<column_index_t>& keys,
    vector_size_t start = 0) {
  for (auto row = start; row < rowVector->size(); ++row) {
    bool hasNull = false;
    for (auto key : keys) {
      if (rowVector->childAt(key)->isNullAt(row)) {
        hasNull = true;
        break;
      }
    }
    if (!hasNull) {
      return row;
    }
  }
  return rowVector->size();
}
} // namespace

RowVectorPtr MergeJoin::filterOutputForAntiJoin(const RowVectorPtr& output) {
  const auto numRows = output->size();
  const auto& filterRows = joinTracker_->matchingRows(numRows);
  const auto numPassed = numRows - filterRows.countSelected();
  if (numPassed == 0) {
    return nullptr;
  }
  if (numPassed == numRows) {
    // All rows passed.
    return output;
  }

  BufferPtr indices = allocateIndices(numPassed, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  size_t index{0};
  for (auto i = 0; i < numRows; ++i) {
    if (!filterRows.isValid(i)) {
      rawIndices[index++] = i;
    }
  }
  VELOX_CHECK_EQ(index, numPassed);

  // Some, but not all rows passed.
  return wrap(numPassed, indices, output);
}

RowVectorPtr MergeJoin::getOutput() {
  // Make sure to have is-blocked or needs-input as true if returning null
  // output. Otherwise, Driver assumes the operator is finished.

  // Use Operator::noMoreInput() as a no-more-input-on-the-left indicator and a
  // noMoreRightInput_ flag as no-more-input-on-the-right indicator.

  // TODO Finish early if ran out of data on either side of the join.
  for (;;) {
    auto output = doGetOutput();
    if (output != nullptr && output->size() > 0) {
      if (filter_) {
        output = applyFilter(output);
        if (output != nullptr) {
          for (const auto [channel, _] : filterInputToOutputChannel_) {
            filterInput_->childAt(channel).reset();
          }
          return output;
        }
        // No rows survived the filter. Get more rows.
        continue;
      }

      if (!isAntiJoin(joinType_)) {
        return output;
      }

      output = filterOutputForAntiJoin(output);
      if (output != nullptr && output->size() > 0) {
        return output;
      }
      // No rows survived the filter for anti join. Get more rows.
      continue;
    }

    if (processDrain()) {
      return nullptr;
    }

    // Check if we need to get more data from the right side.
    if (needsInputFromRightSide()) {
      if (!getNextFromRightSide()) {
        return nullptr;
      }
      continue;
    }

    return nullptr;
  }
  VELOX_UNREACHABLE();
}

bool MergeJoin::processDrain() {
  if (rightHasDrained_ && leftHasDrained_) {
    finishDrain();
    return true;
  }

  if (leftHasDrained_ && !input_) {
    if (isInnerJoin(joinType_) && (!rightMatch_ || rightMatch_->complete)) {
      operatorCtx_->task()->dropInput(rightNodeId_);
    }
    if (isLeftJoin(joinType_) || isAntiJoin(joinType_)) {
      operatorCtx_->task()->dropInput(rightNodeId_);
    }
  }

  if (rightHasDrained_ && !rightInput_) {
    if (isInnerJoin(joinType_) && (!leftMatch_ || leftMatch_->complete)) {
      operatorCtx_->task()->dropInput(this);
    }
    if (isRightJoin(joinType_)) {
      operatorCtx_->task()->dropInput(this);
    }
  }
  return false;
}

bool MergeJoin::needsInputFromRightSide() const {
  return !rightHasNoInput() && !rightSideInputFuture_.valid() && !rightInput_;
}

bool MergeJoin::getNextFromRightSide() {
  VELOX_CHECK(needsInputFromRightSide());
  if (rightSource_ == nullptr) {
    rightSource_ = operatorCtx_->task()->getMergeJoinSource(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  }

  while (!rightHasNoInput() && !rightInput_) {
    const auto blockingReason = rightSource_->next(
        &rightSideInputFuture_, &rightInput_, rightHasDrained_);
    if (blockingReason != BlockingReason::kNotBlocked) {
      VELOX_CHECK(!rightHasDrained_);
      return false;
    }

    if (rightInput_) {
      if (isFullJoin(joinType_) || isRightJoin(joinType_)) {
        rightRowIndex_ = 0;
      } else {
        rightRowIndex_ = firstNonNull(rightInput_, rightKeyChannels_);
        if (rightBatchFinished()) {
          // Ran out of rows on the right side.
          rightInput_ = nullptr;
        }
      }
    } else if (!rightHasDrained_) {
      noMoreRightInput_ = true;
    }
  }
  return true;
}

RowVectorPtr MergeJoin::handleRightSideNullRows() {
  if (!isRightJoin(joinType_) && !isFullJoin(joinType_)) {
    return nullptr;
  }
  const auto rightFirstNonNullIndex =
      firstNonNull(rightInput_, rightKeyChannels_, rightRowIndex_);
  if (rightFirstNonNullIndex <= rightRowIndex_) {
    return nullptr;
  }
  if (prepareOutput(nullptr, rightInput_)) {
    output_->resize(outputSize_);
    return std::move(output_);
  }
  for (int row = rightRowIndex_; row < rightFirstNonNullIndex; ++row) {
    if (!tryAddOutputRowForRightJoin()) {
      output_->resize(outputSize_);
      return std::move(output_);
    }
  }
  VELOX_CHECK_EQ(rightRowIndex_, rightFirstNonNullIndex);
  if (rightBatchFinished()) {
    // Ran out of rows on the right side.
    rightInput_ = nullptr;
    output_->resize(outputSize_);
    return std::move(output_);
  }
  return nullptr;
}

RowVectorPtr MergeJoin::doGetOutput() {
  // Check if we ran out of space in the output vector in the middle of the
  // match.
  if (leftMatch_ && leftMatch_->cursor) {
    VELOX_CHECK(rightMatch_ && rightMatch_->cursor);
    VELOX_CHECK(leftMatch_->complete);
    VELOX_CHECK(rightMatch_->complete);

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
      if (!findEndOfMatch(input_, leftKeyChannels_, leftMatch_.value())) {
        VELOX_CHECK(!leftMatch_->complete);
        // Continue looking for the end of the match.
        input_ = nullptr;
        return nullptr;
      }
      VELOX_CHECK(leftMatch_->complete);

      if (leftMatch_->inputs.back() == input_) {
        leftRowIndex_ = leftMatch_->endRowIndex;
      }
    } else if (leftHasNoInput()) {
      leftMatch_->complete = true;
    } else {
      // Need more input.
      return nullptr;
    }

    if (rightInput_) {
      if (!findEndOfMatch(
              rightInput_, rightKeyChannels_, rightMatch_.value())) {
        VELOX_CHECK(!rightMatch_->complete);
        // Continue looking for the end of the match.
        rightInput_ = nullptr;
        return nullptr;
      }
      VELOX_CHECK(rightMatch_->complete);

      if (rightMatch_->inputs.back() == rightInput_) {
        if (isFullJoin(joinType_) || isRightJoin(joinType_)) {
          rightRowIndex_ = rightMatch_->endRowIndex;
        } else {
          rightRowIndex_ = firstNonNull(
              rightInput_, rightKeyChannels_, rightMatch_->endRowIndex);
          if (rightRowIndex_ == rightInput_->size()) {
            rightInput_ = nullptr;
          }
        }
      }
    } else if (rightHasNoInput()) {
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
    if (isLeftJoin(joinType_) || isAntiJoin(joinType_)) {
      if (input_ && rightHasNoInput()) {
        // If output_ is currently wrapping a different buffer, return it
        // first.
        if (prepareOutput(input_, nullptr)) {
          output_->resize(outputSize_);
          return std::move(output_);
        }
        while (true) {
          if (!tryAddOutputRowForLeftJoin()) {
            return std::move(output_);
          }

          if (leftBatchFinished()) {
            input_ = nullptr;
            return produceOutput();
          }
        }
        VELOX_UNREACHABLE();
      }

      if (leftHasNoInput()) {
        if (output_ != nullptr) {
          output_->resize(outputSize_);
          return std::move(output_);
        }
        if (input_ == nullptr) {
          rightInput_ = nullptr;
        }
      }
    } else if (isRightJoin(joinType_)) {
      if (rightInput_ && leftHasNoInput()) {
        // If output_ is currently wrapping a different buffer, return it
        // first.
        if (prepareOutput(nullptr, rightInput_)) {
          output_->resize(outputSize_);
          return std::move(output_);
        }

        while (true) {
          if (!tryAddOutputRowForRightJoin()) {
            return std::move(output_);
          }

          if (rightBatchFinished()) {
            // Ran out of rows on the right side.
            rightInput_ = nullptr;
            return nullptr;
          }
        }
        VELOX_UNREACHABLE();
      }

      if (rightHasNoInput()) {
        if (output_ != nullptr) {
          output_->resize(outputSize_);
          return std::move(output_);
        }
        if (rightInput_ == nullptr) {
          input_ = nullptr;
        }
      }
    } else if (isFullJoin(joinType_)) {
      if (input_ && rightHasNoInput()) {
        // If output_ is currently wrapping a different buffer, return it
        // first.
        if (prepareOutput(input_, nullptr)) {
          output_->resize(outputSize_);
          return std::move(output_);
        }

        while (true) {
          if (!tryAddOutputRowForLeftJoin()) {
            return std::move(output_);
          }

          if (leftBatchFinished()) {
            input_ = nullptr;
            return produceOutput();
          }
        }
        VELOX_UNREACHABLE();
      }

      if (leftHasNoInput() && output_) {
        output_->resize(outputSize_);
        return std::move(output_);
      }

      if (rightInput_ && leftHasNoInput()) {
        // If output_ is currently wrapping a different buffer, return it
        // first.
        if (prepareOutput(nullptr, rightInput_)) {
          output_->resize(outputSize_);
          return std::move(output_);
        }

        while (true) {
          if (!tryAddOutputRowForRightJoin()) {
            return std::move(output_);
          }

          if (rightBatchFinished()) {
            // Ran out of rows on the right side.
            rightInput_ = nullptr;
            return nullptr;
          }
        }
        VELOX_UNREACHABLE();
      }

      if (rightHasNoInput() && output_) {
        output_->resize(outputSize_);
        return std::move(output_);
      }
    } else {
      if (leftHasNoInput() || rightHasNoInput()) {
        if (output_) {
          output_->resize(outputSize_);
          return std::move(output_);
        }
        input_ = nullptr;
        rightInput_ = nullptr;
      }
    }
    return nullptr;
  }

  const auto output = handleRightSideNullRows();
  if (output != nullptr || rightInput_ == nullptr) {
    return output;
  }
  VELOX_CHECK_NOT_NULL(rightInput_);

  // Look for a new match starting with index_ row on the left and rightIndex_
  // row on the right.
  auto compareResult = compare();
  for (;;) {
    // Catch up input_ with rightInput_.
    while (compareResult < 0) {
      if (isLeftJoin(joinType_) || isAntiJoin(joinType_) ||
          isFullJoin(joinType_)) {
        // If output_ is currently wrapping a different buffer, return it
        // first.
        if (prepareOutput(input_, nullptr)) {
          output_->resize(outputSize_);
          return std::move(output_);
        }

        if (!tryAddOutputRowForLeftJoin()) {
          return std::move(output_);
        }
      } else {
        leftRowIndex_ =
            firstNonNull(input_, leftKeyChannels_, leftRowIndex_ + 1);
      }

      if (leftBatchFinished()) {
        input_ = nullptr;
        return produceOutput();
      }
      compareResult = compare();
    }

    // Catch up rightInput_ with input_.
    while (compareResult > 0) {
      if (isRightJoin(joinType_) || isFullJoin(joinType_)) {
        // If output_ is currently wrapping a different buffer, return it
        // first.
        if (prepareOutput(nullptr, rightInput_)) {
          output_->resize(outputSize_);
          return std::move(output_);
        }

        if (!tryAddOutputRowForRightJoin()) {
          return std::move(output_);
        }
      } else {
        rightRowIndex_ =
            firstNonNull(rightInput_, rightKeyChannels_, rightRowIndex_ + 1);
      }

      if (rightBatchFinished()) {
        rightInput_ = nullptr;
        return produceOutput();
      }
      compareResult = compare();
    }

    if (compareResult == 0) {
      // Found a match. Identify all rows on the left and right that have the
      // matching keys.
      vector_size_t leftEndRow = leftRowIndex_ + 1;
      while (leftEndRow < input_->size() && compareLeft(leftEndRow) == 0) {
        ++leftEndRow;
      }

      if (leftEndRow == input_->size()) {
        // Matches continue in subsequent input. Load all lazies.
        loadColumns(input_, *operatorCtx_->execCtx());
      }
      leftMatch_ = Match{
          {input_},
          leftRowIndex_,
          leftEndRow,
          leftEndRow < input_->size(),
          std::nullopt};

      vector_size_t endRightRow = rightRowIndex_ + 1;
      while (endRightRow < rightInput_->size() &&
             compareRight(endRightRow) == 0) {
        ++endRightRow;
      }

      rightMatch_ = Match{
          {rightInput_},
          rightRowIndex_,
          endRightRow,
          endRightRow < rightInput_->size(),
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

      leftRowIndex_ = leftEndRow;
      if (isFullJoin(joinType_) || isRightJoin(joinType_)) {
        rightRowIndex_ = endRightRow;
      } else {
        rightRowIndex_ =
            firstNonNull(rightInput_, rightKeyChannels_, endRightRow);
      }

      if (rightBatchFinished()) {
        // Ran out of rows on the right side.
        rightInput_ = nullptr;
      }

      if (addToOutput()) {
        return std::move(output_);
      }

      if (!rightInput_) {
        return nullptr;
      }

      compareResult = compare();
    }
  }

  VELOX_UNREACHABLE();
}

RowVectorPtr MergeJoin::applyFilter(const RowVectorPtr& output) {
  const auto numRows = output->size();

  RowVectorPtr fullOuterOutput = nullptr;

  BufferPtr indices = allocateIndices(numRows, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  vector_size_t numPassed = 0;

  if (joinTracker_) {
    const auto& filterRows = joinTracker_->matchingRows(numRows);

    if (!filterRows.hasSelections()) {
      // No matches in the output, no need to evaluate the filter.
      return output;
    }

    evaluateFilter(filterRows);

    // If all matches for a given left-side row fail the filter, add a row to
    // the output with nulls for the right-side columns.
    const auto onMiss = [&](auto row) {
      if (isSemiFilterJoin(joinType_)) {
        return;
      }
      rawIndices[numPassed++] = row;

      if (isFullJoin(joinType_)) {
        // For filtered rows, it is necessary to insert additional data
        // to ensure the result set is complete. Specifically, we
        // need to generate two records: one record containing the
        // columns from the left table along with nulls for the
        // right table, and another record containing the columns
        // from the right table along with nulls for the left table.
        // For instance, the current output is filtered based on the condition
        // t > 1.

        // 1, 1
        // 2, 2
        // 3, 3

        // In this scenario, we need to additionally insert a record 1, 1.
        // Subsequently, we will set the values of the columns on the left to
        // null and the values of the columns on the right to null as well. By
        // doing so, we will obtain the final result set.

        // 1,   null
        // null,  1
        // 2, 2
        // 3, 3
        fullOuterOutput = BaseVector::create<RowVector>(
            output->type(), output->size() + 1, pool());

        for (auto i = 0; i < row + 1; ++i) {
          for (auto j = 0; j < output->type()->size(); ++j) {
            fullOuterOutput->childAt(j)->copy(
                output->childAt(j).get(), i, i, 1);
          }
        }

        for (auto j = 0; j < output->type()->size(); ++j) {
          fullOuterOutput->childAt(j)->copy(
              output->childAt(j).get(), row + 1, row, 1);
        }

        for (auto i = row + 1; i < output->size(); ++i) {
          for (auto j = 0; j < output->type()->size(); ++j) {
            fullOuterOutput->childAt(j)->copy(
                output->childAt(j).get(), i + 1, i, 1);
          }
        }

        for (auto& projection : leftProjections_) {
          auto& target = fullOuterOutput->childAt(projection.outputChannel);
          target->setNull(row, true);
        }

        for (auto& projection : rightProjections_) {
          auto& target = fullOuterOutput->childAt(projection.outputChannel);
          target->setNull(row + 1, true);
        }
      } else if (!isRightJoin(joinType_)) {
        for (auto& projection : rightProjections_) {
          auto& target = output->childAt(projection.outputChannel);
          target->setNull(row, true);
        }
      } else {
        for (auto& projection : leftProjections_) {
          auto& target = output->childAt(projection.outputChannel);
          target->setNull(row, true);
        }
      }
    };

    auto onMatch = [&](auto row, bool firstMatch) {
      const bool isNonSemiAntiJoin =
          !isSemiFilterJoin(joinType_) && !isAntiJoin(joinType_);

      if ((isSemiFilterJoin(joinType_) && firstMatch) || isNonSemiAntiJoin) {
        rawIndices[numPassed++] = row;
      }
    };

    for (auto i = 0; i < numRows; ++i) {
      if (filterRows.isValid(i)) {
        const bool passed = !decodedFilterResult_.isNullAt(i) &&
            decodedFilterResult_.valueAt<bool>(i);

        joinTracker_->processFilterResult(i, passed, onMiss, onMatch);
      } else {
        // This row doesn't have a match on the right side. Keep it
        // unconditionally.
        rawIndices[numPassed++] = i;
      }
    }

    // Every time we start a new left key match, `processFilterResult()` will
    // check if at least one row from the previous match passed the filter. If
    // none did, it calls onMiss to add a record with null right projections
    // to the output.
    //
    // Before we leave the current buffer, since we may not have seen the next
    // left key match yet, the last key match may still be pending to produce
    // a row (because `processFilterResult()` was not called yet).
    //
    // To handle this, we need to call `noMoreFilterResults()` unless the
    // same current left key match may continue in the next buffer. So there
    // are two cases to check:
    //
    // 1. If leftMatch_ is nullopt, there for sure the next buffer will
    // contain a different key match.
    //
    // 2. leftMatch_ may not be nullopt, but may be related to a different
    // (subsequent) left key. So we check if the last row in the batch has the
    // same left row number as the last key match.
    if (!leftMatch_ || !joinTracker_->isCurrentLeftMatch(numRows - 1)) {
      joinTracker_->noMoreFilterResults(onMiss);
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
    if (fullOuterOutput) {
      return fullOuterOutput;
    }
    return output;
  }

  // Some, but not all rows passed.
  if (fullOuterOutput) {
    return wrap(numPassed, indices, fullOuterOutput);
  }

  return wrap(numPassed, indices, output);
}

void MergeJoin::evaluateFilter(const SelectivityVector& rows) {
  EvalCtx evalCtx(operatorCtx_->execCtx(), filter_.get(), filterInput_.get());
  filter_->eval(0, 1, true, rows, evalCtx, filterResult_);

  decodedFilterResult_.decode(*filterResult_[0], rows);
}

bool MergeJoin::isFinished() {
  if (isRightJoin(joinType_)) {
    // If all rows on both the left and right sides match, we must also verify
    // the 'noMoreInput_' on the left side to ensure that all results are
    // complete.
    return noMoreInput_ && noMoreRightInput_ && rightInput_ == nullptr;
  }

  if (isFullJoin(joinType_)) {
    return noMoreInput_ && input_ == nullptr && noMoreRightInput_ &&
        rightInput_ == nullptr;
  }

  return noMoreInput_ && input_ == nullptr;
}

void MergeJoin::close() {
  if (rightSource_) {
    rightSource_->close();
  }
  Operator::close();
}

bool MergeJoin::startDrain() {
  VELOX_CHECK(!leftHasDrained_);
  leftHasDrained_ = true;
  return true;
}

void MergeJoin::finishDrain() {
  VELOX_CHECK(leftHasDrained_);
  VELOX_CHECK(rightHasDrained_);

  leftHasDrained_ = false;
  rightHasDrained_ = false;
  input_ = nullptr;
  rightInput_ = nullptr;
  leftMatch_.reset();
  rightMatch_.reset();
  if (joinTracker_.has_value()) {
    joinTracker_->reset();
  }
  Operator::finishDrain();
}

void MergeJoin::JoinTracker::reset() {
  resetLastVector();
  lastLeftRowNumber_ = 0;
  currentRow_ = -1;
  currentLeftRowNumber_ = -1;
  currentRowPassed_ = false;
}

} // namespace facebook::velox::exec
