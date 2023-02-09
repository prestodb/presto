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
#include "velox/exec/Window.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

namespace {
void initKeyInfo(
    const RowTypePtr& type,
    const std::vector<core::FieldAccessTypedExprPtr>& keys,
    const std::vector<core::SortOrder>& orders,
    std::vector<std::pair<column_index_t, core::SortOrder>>& keyInfo) {
  const core::SortOrder defaultPartitionSortOrder(true, true);

  keyInfo.reserve(keys.size());
  for (auto i = 0; i < keys.size(); ++i) {
    auto channel = exprToChannel(keys[i].get(), type);
    VELOX_CHECK(
        channel != kConstantChannel,
        "Window doesn't allow constant partition or sort keys");
    if (i < orders.size()) {
      keyInfo.push_back(std::make_pair(channel, orders[i]));
    } else {
      keyInfo.push_back(std::make_pair(channel, defaultPartitionSortOrder));
    }
  }
}

VectorPtr const toConstantVector(
    const std::shared_ptr<const core::ConstantTypedExpr>& constantExpr,
    velox::memory::MemoryPool* pool) {
  if (constantExpr->hasValueVector()) {
    return BaseVector::wrapInConstant(1, 0, constantExpr->valueVector());
  }
  if (constantExpr->value().isNull()) {
    return BaseVector::createNullConstant(constantExpr->type(), 1, pool);
  }
  return BaseVector::createConstant(constantExpr->value(), 1, pool);
}

}; // namespace

Window::Window(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::WindowNode>& windowNode)
    : Operator(
          driverCtx,
          windowNode->outputType(),
          operatorId,
          windowNode->id(),
          "Window"),
      outputBatchSizeInBytes_(
          driverCtx->queryConfig().preferredOutputBatchSize()),
      numInputColumns_(windowNode->sources()[0]->outputType()->size()),
      data_(std::make_unique<RowContainer>(
          windowNode->sources()[0]->outputType()->children(),
          pool())),
      decodedInputVectors_(numInputColumns_),
      stringAllocator_(pool()) {
  auto inputType = windowNode->sources()[0]->outputType();
  initKeyInfo(inputType, windowNode->partitionKeys(), {}, partitionKeyInfo_);
  initKeyInfo(
      inputType,
      windowNode->sortingKeys(),
      windowNode->sortingOrders(),
      sortKeyInfo_);
  allKeyInfo_.reserve(partitionKeyInfo_.size() + sortKeyInfo_.size());
  allKeyInfo_.insert(
      allKeyInfo_.cend(), partitionKeyInfo_.begin(), partitionKeyInfo_.end());
  allKeyInfo_.insert(
      allKeyInfo_.cend(), sortKeyInfo_.begin(), sortKeyInfo_.end());

  std::vector<exec::RowColumn> inputColumns;
  for (int i = 0; i < inputType->children().size(); i++) {
    inputColumns.push_back(data_->columnAt(i));
  }
  // The WindowPartition is structured over all the input columns data.
  // Individual functions access its input argument column values from it.
  // The RowColumns are copied by the WindowPartition, so its fine to use
  // a local variable here.
  windowPartition_ =
      std::make_unique<WindowPartition>(inputColumns, inputType->children());

  createWindowFunctions(windowNode, inputType);
}

Window::WindowFrame Window::createWindowFrame(
    core::WindowNode::Frame frame,
    const RowTypePtr& inputType) {
  auto createFrameChannelArg =
      [&](const core::TypedExprPtr& frame) -> std::optional<FrameChannelArg> {
    // frame is nullptr for non (kPreceding or kFollowing) frames.
    if (frame == nullptr) {
      return std::nullopt;
    }
    auto frameChannel = exprToChannel(frame.get(), inputType);
    if (frameChannel == kConstantChannel) {
      auto constant =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(frame)
              ->value();
      VELOX_CHECK(!constant.isNull(), "k in frame bounds must not be null");
      VELOX_USER_CHECK_GE(
          constant.value<int64_t>(), 1, "k in frame bounds must be at least 1");
      return std::make_optional(FrameChannelArg{
          kConstantChannel, nullptr, constant.value<int64_t>()});
    } else {
      return std::make_optional(FrameChannelArg{
          frameChannel, BaseVector::create(BIGINT(), 0, pool()), std::nullopt});
    }
  };

  return WindowFrame(
      {frame.type,
       frame.startType,
       frame.endType,
       createFrameChannelArg(frame.startValue),
       createFrameChannelArg(frame.endValue)});
}

void Window::createWindowFunctions(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    const RowTypePtr& inputType) {
  for (const auto& windowNodeFunction : windowNode->windowFunctions()) {
    std::vector<WindowFunctionArg> functionArgs;
    functionArgs.reserve(windowNodeFunction.functionCall->inputs().size());
    for (auto& arg : windowNodeFunction.functionCall->inputs()) {
      auto channel = exprToChannel(arg.get(), inputType);
      if (channel == kConstantChannel) {
        functionArgs.push_back(
            {arg->type(),
             toConstantVector(
                 std::dynamic_pointer_cast<const core::ConstantTypedExpr>(arg),
                 pool()),
             std::nullopt});
      } else {
        functionArgs.push_back({arg->type(), nullptr, channel});
      }
    }

    windowFunctions_.push_back(WindowFunction::create(
        windowNodeFunction.functionCall->name(),
        functionArgs,
        windowNodeFunction.functionCall->type(),
        operatorCtx_->pool(),
        &stringAllocator_));

    windowFrames_.push_back(
        createWindowFrame(windowNodeFunction.frame, inputType));
  }
}

void Window::addInput(RowVectorPtr input) {
  inputRows_.resize(input->size());

  for (auto col = 0; col < input->childrenSize(); ++col) {
    decodedInputVectors_[col].decode(*input->childAt(col), inputRows_);
  }

  // Add all the rows into the RowContainer.
  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }
  }
  numRows_ += inputRows_.size();
}

inline bool Window::compareRowsWithKeys(
    const char* lhs,
    const char* rhs,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& keys) {
  if (lhs == rhs) {
    return false;
  }
  for (auto& key : keys) {
    if (auto result = data_->compare(
            lhs,
            rhs,
            key.first,
            {key.second.isNullsFirst(), key.second.isAscending(), false})) {
      return result < 0;
    }
  }
  return false;
}

void Window::createPeerAndFrameBuffers() {
  // TODO: This computation needs to be revised. It only takes into account
  // the input columns size. We need to also account for the output columns.
  numRowsPerOutput_ = data_->estimatedNumRowsPerBatch(outputBatchSizeInBytes_);

  peerStartBuffer_ = AlignedBuffer::allocate<vector_size_t>(
      numRowsPerOutput_, operatorCtx_->pool());
  peerEndBuffer_ = AlignedBuffer::allocate<vector_size_t>(
      numRowsPerOutput_, operatorCtx_->pool());

  auto numFuncs = windowFunctions_.size();
  frameStartBuffers_.reserve(numFuncs);
  frameEndBuffers_.reserve(numFuncs);
  validFrames_.reserve(numFuncs);

  for (auto i = 0; i < numFuncs; i++) {
    BufferPtr frameStartBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    BufferPtr frameEndBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    frameStartBuffers_.push_back(frameStartBuffer);
    frameEndBuffers_.push_back(frameEndBuffer);
    validFrames_.push_back(SelectivityVector(numRowsPerOutput_));
  }
}

void Window::computePartitionStartRows() {
  // Randomly assuming that max 10000 partitions are in the data.
  partitionStartRows_.reserve(numRows_);
  auto partitionCompare = [&](const char* lhs, const char* rhs) -> bool {
    return compareRowsWithKeys(lhs, rhs, partitionKeyInfo_);
  };

  // Using a sequential traversal to find changing partitions.
  // This algorithm is inefficient and can be changed
  // i) Use a binary search kind of strategy.
  // ii) If we use a Hashtable instead of a full sort then the count
  // of rows in the partition can be directly used.
  partitionStartRows_.push_back(0);

  VELOX_CHECK_GT(sortedRows_.size(), 0);
  for (auto i = 1; i < sortedRows_.size(); i++) {
    if (partitionCompare(sortedRows_[i - 1], sortedRows_[i])) {
      partitionStartRows_.push_back(i);
    }
  }

  // Setting the startRow of the (last + 1) partition to be returningRows.size()
  // to help for last partition related calculations.
  partitionStartRows_.push_back(sortedRows_.size());
}

void Window::sortPartitions() {
  // This is a very inefficient but easy implementation to order the input rows
  // by partition keys + sort keys.
  // Sort the pointers to the rows in RowContainer (data_) instead of sorting
  // the rows.
  sortedRows_.resize(numRows_);
  RowContainerIterator iter;
  data_->listRows(&iter, numRows_, sortedRows_.data());

  std::sort(
      sortedRows_.begin(),
      sortedRows_.end(),
      [this](const char* leftRow, const char* rightRow) {
        return compareRowsWithKeys(leftRow, rightRow, allKeyInfo_);
      });

  computePartitionStartRows();

  currentPartition_ = 0;
}

void Window::noMoreInput() {
  Operator::noMoreInput();
  // No data.
  if (numRows_ == 0) {
    finished_ = true;
    return;
  }

  // At this point we have seen all the input rows. We can start
  // outputting rows now.
  // However, some preparation is needed. The rows should be
  // separated into partitions and sort by ORDER BY keys within
  // the partition. This will order the rows for getOutput().
  sortPartitions();
  createPeerAndFrameBuffers();
}

void Window::callResetPartition(vector_size_t partitionNumber) {
  partitionOffset_ = 0;
  auto partitionSize = partitionStartRows_[partitionNumber + 1] -
      partitionStartRows_[partitionNumber];
  auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[partitionNumber], partitionSize);
  windowPartition_->resetPartition(partition);
  for (int i = 0; i < windowFunctions_.size(); i++) {
    windowFunctions_[i]->resetPartition(windowPartition_.get());
  }
}

void Window::updateKRowsFrameBounds(
    bool isKPreceding,
    const FrameChannelArg& frameArg,
    vector_size_t startRow,
    vector_size_t numRows,
    vector_size_t* rawFrameBounds) {
  auto firstPartitionRow = partitionStartRows_[currentPartition_];

  if (frameArg.index == kConstantChannel) {
    auto constantOffset = frameArg.constant.value();
    std::iota(
        rawFrameBounds,
        rawFrameBounds + numRows,
        startRow + (isKPreceding ? -constantOffset : constantOffset) -
            firstPartitionRow);
  } else {
    windowPartition_->extractColumn(
        frameArg.index, partitionOffset_, numRows, 0, frameArg.value);
    auto offsets = frameArg.value->values()->as<int64_t>();
    for (auto i = 0; i < numRows; i++) {
      VELOX_USER_CHECK(
          !frameArg.value->isNullAt(i), "k in frame bounds cannot be null");
      VELOX_USER_CHECK_GE(
          offsets[i], 1, "k in frame bounds must be at least 1");
    }

    for (auto i = 0; i < numRows; i++) {
      rawFrameBounds[i] = (startRow + i) +
          (isKPreceding ? -vector_size_t(offsets[i])
                        : vector_size_t(offsets[i])) -
          firstPartitionRow;
    }
  }
}

void Window::updateFrameBounds(
    const WindowFrame& windowFrame,
    const bool isStartBound,
    const vector_size_t startRow,
    const vector_size_t numRows,
    const vector_size_t* rawPeerStarts,
    const vector_size_t* rawPeerEnds,
    vector_size_t* rawFrameBounds) {
  auto firstPartitionRow = partitionStartRows_[currentPartition_];
  auto lastPartitionRow = partitionStartRows_[currentPartition_ + 1] - 1;
  auto windowType = windowFrame.type;
  auto boundType = isStartBound ? windowFrame.startType : windowFrame.endType;
  auto frameArg = isStartBound ? windowFrame.start : windowFrame.end;

  switch (boundType) {
    case core::WindowNode::BoundType::kUnboundedPreceding:
      std::fill_n(rawFrameBounds, numRows, 0);
      break;
    case core::WindowNode::BoundType::kUnboundedFollowing:
      std::fill_n(
          rawFrameBounds, numRows, lastPartitionRow - firstPartitionRow);
      break;
    case core::WindowNode::BoundType::kCurrentRow: {
      if (windowType == core::WindowNode::WindowType::kRange) {
        const vector_size_t* rawPeerBuffer =
            isStartBound ? rawPeerStarts : rawPeerEnds;
        std::copy(rawPeerBuffer, rawPeerBuffer + numRows, rawFrameBounds);
      } else {
        // Fills the frameBound buffer with increasing value of row indices
        // (corresponding to CURRENT ROW) from the startRow of the current
        // output buffer. The startRow has to be adjusted relative to the
        // partition start row.
        std::iota(
            rawFrameBounds,
            rawFrameBounds + numRows,
            startRow - firstPartitionRow);
      }
      break;
    }
    case core::WindowNode::BoundType::kPreceding: {
      if (windowType == core::WindowNode::WindowType::kRows) {
        updateKRowsFrameBounds(
            true, frameArg.value(), startRow, numRows, rawFrameBounds);
      } else {
        VELOX_NYI("k preceding frame is only supported in ROWS mode");
      }
      break;
    }
    case core::WindowNode::BoundType::kFollowing: {
      if (windowType == core::WindowNode::WindowType::kRows) {
        updateKRowsFrameBounds(
            false, frameArg.value(), startRow, numRows, rawFrameBounds);
      } else {
        VELOX_NYI("k following frame is only supported in ROWS mode");
      }
      break;
    }
    default:
      VELOX_USER_FAIL("Invalid frame bound type");
  }
}

namespace {
// Frame end points are always expected to go from frameStart to frameEnd
// rows in increasing row numbers in the partition. k rows/range frames could
// potentially violate this.
// This function identifies the rows that violate the framing requirements
// and sets bits in the validFrames SelectivityVector for usage in the
// WindowFunction subsequently.
void computeValidFrames(
    vector_size_t lastRow,
    vector_size_t numRows,
    vector_size_t* rawFrameStarts,
    vector_size_t* rawFrameEnds,
    SelectivityVector& validFrames) {
  auto frameStart = 0;
  auto frameEnd = 0;

  for (auto i = 0; i < numRows; i++) {
    frameStart = rawFrameStarts[i];
    frameEnd = rawFrameEnds[i];
    // All valid frames require frameStart <= frameEnd to define the frame rows.
    // Also, frameEnd >= 0, so that the frameEnd doesn't fall before the
    // partition. And frameStart <= lastRow so that the frameStart doesn't fall
    // after the partition rows.
    if (frameStart <= frameEnd && frameEnd >= 0 && frameStart <= lastRow) {
      rawFrameStarts[i] = std::max(frameStart, 0);
      rawFrameEnds[i] = std::min(frameEnd, lastRow);
    } else {
      validFrames.setValid(i, false);
    }
  }
  validFrames.updateBounds();
}

}; // namespace

void Window::callApplyForPartitionRows(
    vector_size_t startRow,
    vector_size_t endRow,
    const std::vector<VectorPtr>& result,
    vector_size_t resultOffset) {
  if (partitionStartRows_[currentPartition_] == startRow) {
    callResetPartition(currentPartition_);
  }

  vector_size_t numRows = endRow - startRow;
  vector_size_t numFuncs = windowFunctions_.size();

  // Size buffers for the call to WindowFunction::apply.
  auto bufferSize = numRows * sizeof(vector_size_t);
  peerStartBuffer_->setSize(bufferSize);
  peerEndBuffer_->setSize(bufferSize);
  auto rawPeerStarts = peerStartBuffer_->asMutable<vector_size_t>();
  auto rawPeerEnds = peerEndBuffer_->asMutable<vector_size_t>();

  std::vector<vector_size_t*> rawFrameStarts;
  std::vector<vector_size_t*> rawFrameEnds;
  rawFrameStarts.reserve(numFuncs);
  rawFrameEnds.reserve(numFuncs);
  for (auto w = 0; w < numFuncs; w++) {
    frameStartBuffers_[w]->setSize(bufferSize);
    frameEndBuffers_[w]->setSize(bufferSize);

    auto rawFrameStart = frameStartBuffers_[w]->asMutable<vector_size_t>();
    auto rawFrameEnd = frameEndBuffers_[w]->asMutable<vector_size_t>();
    rawFrameStarts.push_back(rawFrameStart);
    rawFrameEnds.push_back(rawFrameEnd);
  }

  auto peerCompare = [&](const char* lhs, const char* rhs) -> bool {
    return compareRowsWithKeys(lhs, rhs, sortKeyInfo_);
  };
  auto firstPartitionRow = partitionStartRows_[currentPartition_];
  auto lastPartitionRow = partitionStartRows_[currentPartition_ + 1] - 1;
  for (auto i = startRow, j = 0; i < endRow; i++, j++) {
    // When traversing input partition rows, the peers are the rows
    // with the same values for the ORDER BY clause. These rows
    // are equal in some ways and affect the results of ranking functions.
    // This logic exploits the fact that all rows between the peerStartRow_
    // and peerEndRow_ have the same values for peerStartRow_ and peerEndRow_.
    // So we can compute them just once and reuse across the rows in that peer
    // interval. Note: peerStartRow_ and peerEndRow_ can be maintained across
    // getOutput calls.

    // Compute peerStart and peerEnd rows for the first row of the partition or
    // when past the previous peerGroup.
    if (i == firstPartitionRow || i >= peerEndRow_) {
      peerStartRow_ = i;
      peerEndRow_ = i;
      while (peerEndRow_ <= lastPartitionRow) {
        if (peerCompare(sortedRows_[peerStartRow_], sortedRows_[peerEndRow_])) {
          break;
        }
        peerEndRow_++;
      }
    }

    // Peer buffer values should be offsets from the start of the partition
    // as WindowFunction only sees one partition at a time.
    rawPeerStarts[j] = peerStartRow_ - firstPartitionRow;
    rawPeerEnds[j] = peerEndRow_ - 1 - firstPartitionRow;
  }

  for (auto i = 0; i < numFuncs; i++) {
    const auto& windowFrame = windowFrames_[i];
    // Default all rows to have validFrames. The invalidity of frames is only
    // computed for k rows/range frames at a later point.
    validFrames_[i].resizeFill(numRows, true);
    updateFrameBounds(
        windowFrame,
        true,
        startRow,
        numRows,
        rawPeerStarts,
        rawPeerEnds,
        rawFrameStarts[i]);
    updateFrameBounds(
        windowFrame,
        false,
        startRow,
        numRows,
        rawPeerStarts,
        rawPeerEnds,
        rawFrameEnds[i]);
    if (windowFrames_[i].start || windowFrames_[i].end) {
      // k preceding and k following bounds can be problematic. They can
      // go over the partition limits or result in empty frames. Fix the
      // frame boundaries and compute the validFrames SelectivityVector
      // for these cases. Not all functions care about validFrames viz.
      // Ranking functions do not care about frames. So the function decides
      // further what to do with empty frames.
      computeValidFrames(
          lastPartitionRow - firstPartitionRow,
          numRows,
          rawFrameStarts[i],
          rawFrameEnds[i],
          validFrames_[i]);
    }
  }

  // Invoke the apply method for the WindowFunctions.
  for (auto w = 0; w < numFuncs; w++) {
    windowFunctions_[w]->apply(
        peerStartBuffer_,
        peerEndBuffer_,
        frameStartBuffers_[w],
        frameEndBuffers_[w],
        validFrames_[w],
        resultOffset,
        result[w]);
  }

  numProcessedRows_ += numRows;
  partitionOffset_ += numRows;
  if (endRow == partitionStartRows_[currentPartition_ + 1]) {
    currentPartition_++;
  }
}

void Window::callApplyLoop(
    vector_size_t numOutputRows,
    const std::vector<VectorPtr>& windowOutputs) {
  // Compute outputs by traversing as many partitions as possible. This
  // logic takes care of partial partitions output also.

  vector_size_t resultIndex = 0;
  vector_size_t numOutputRowsLeft = numOutputRows;
  while (numOutputRowsLeft > 0) {
    auto rowsForCurrentPartition =
        partitionStartRows_[currentPartition_ + 1] - numProcessedRows_;
    if (rowsForCurrentPartition <= numOutputRowsLeft) {
      // Current partition can fit completely in the output buffer.
      // So output all its rows.
      callApplyForPartitionRows(
          numProcessedRows_,
          numProcessedRows_ + rowsForCurrentPartition,
          windowOutputs,
          resultIndex);
      resultIndex += rowsForCurrentPartition;
      numOutputRowsLeft -= rowsForCurrentPartition;
    } else {
      // Current partition can fit only partially in the output buffer.
      // Call apply for the rows that can fit in the buffer and break from
      // outputting.
      callApplyForPartitionRows(
          numProcessedRows_,
          numProcessedRows_ + numOutputRowsLeft,
          windowOutputs,
          resultIndex);
      numOutputRowsLeft = 0;
      break;
    }
  }
}

RowVectorPtr Window::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  auto numRowsLeft = numRows_ - numProcessedRows_;
  auto numOutputRows = std::min(numRowsPerOutput_, numRowsLeft);
  auto result = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(outputType_, numOutputRows, operatorCtx_->pool()));

  // Set all passthrough input columns.
  for (int i = 0; i < numInputColumns_; ++i) {
    data_->extractColumn(
        sortedRows_.data() + numProcessedRows_,
        numOutputRows,
        i,
        result->childAt(i));
  }

  // Construct vectors for the window function output columns.
  std::vector<VectorPtr> windowOutputs;
  windowOutputs.reserve(windowFunctions_.size());
  for (int i = numInputColumns_; i < outputType_->size(); i++) {
    auto output = BaseVector::create(
        outputType_->childAt(i), numOutputRows, operatorCtx_->pool());
    windowOutputs.emplace_back(std::move(output));
  }

  // Compute the output values of window functions.
  callApplyLoop(numOutputRows, windowOutputs);

  for (int j = numInputColumns_; j < outputType_->size(); j++) {
    result->childAt(j) = windowOutputs[j - numInputColumns_];
  }

  finished_ = (numProcessedRows_ == sortedRows_.size());
  return result;
}

} // namespace facebook::velox::exec
