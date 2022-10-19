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

void checkDefaultWindowFrame(const core::WindowNode::Function& windowFunction) {
  VELOX_CHECK_EQ(
      windowFunction.frame.type, core::WindowNode::WindowType::kRange);
  VELOX_CHECK_EQ(
      windowFunction.frame.startType,
      core::WindowNode::BoundType::kUnboundedPreceding);
  VELOX_CHECK_EQ(
      windowFunction.frame.endType, core::WindowNode::BoundType::kCurrentRow);
  VELOX_CHECK_EQ(windowFunction.frame.startValue, nullptr);
  VELOX_CHECK_EQ(windowFunction.frame.endValue, nullptr);
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
          operatorCtx_->mappedMemory())),
      decodedInputVectors_(numInputColumns_) {
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

void Window::createWindowFunctions(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    const RowTypePtr& inputType) {
  auto constantArg = [&](const core::TypedExprPtr arg) -> const VectorPtr {
    if (auto typedExpr =
            dynamic_cast<const core::ConstantTypedExpr*>(arg.get())) {
      if (typedExpr->hasValueVector()) {
        return BaseVector::wrapInConstant(1, 0, typedExpr->valueVector());
      }
      if (typedExpr->value().isNull()) {
        return BaseVector::createNullConstant(typedExpr->type(), 1, pool());
      }
      return BaseVector::createConstant(typedExpr->value(), 1, pool());
    }
    return nullptr;
  };

  auto fieldArgToChannel =
      [&](const core::TypedExprPtr arg) -> std::optional<column_index_t> {
    if (arg) {
      std::optional<column_index_t> argChannel =
          exprToChannel(arg.get(), inputType);
      VELOX_CHECK(
          argChannel.value() != kConstantChannel,
          "Window doesn't allow constant arguments or frame end-points");
      return argChannel;
    }
    return std::nullopt;
  };

  for (const auto& windowNodeFunction : windowNode->windowFunctions()) {
    std::vector<WindowFunctionArg> functionArgs;
    functionArgs.reserve(windowNodeFunction.functionCall->inputs().size());
    for (auto& arg : windowNodeFunction.functionCall->inputs()) {
      if (auto constant = constantArg(arg)) {
        functionArgs.push_back({arg->type(), constant, std::nullopt});
      } else {
        functionArgs.push_back(
            {arg->type(), nullptr, fieldArgToChannel(arg).value()});
      }
    }

    windowFunctions_.push_back(WindowFunction::create(
        windowNodeFunction.functionCall->name(),
        functionArgs,
        windowNodeFunction.functionCall->type(),
        operatorCtx_->pool()));

    checkDefaultWindowFrame(windowNodeFunction);

    windowFrames_.push_back(
        {windowNodeFunction.frame.type,
         windowNodeFunction.frame.startType,
         windowNodeFunction.frame.endType,
         fieldArgToChannel(windowNodeFunction.frame.startValue),
         fieldArgToChannel(windowNodeFunction.frame.endValue)});
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

  for (auto i = 0; i < numFuncs; i++) {
    BufferPtr frameStartBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    BufferPtr frameEndBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    frameStartBuffers_.push_back(frameStartBuffer);
    frameEndBuffers_.push_back(frameEndBuffer);
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
  auto partitionSize = partitionStartRows_[partitionNumber + 1] -
      partitionStartRows_[partitionNumber];
  auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[partitionNumber], partitionSize);
  windowPartition_->resetPartition(partition);
  for (int i = 0; i < windowFunctions_.size(); i++) {
    windowFunctions_[i]->resetPartition(windowPartition_.get());
  }
}

std::pair<vector_size_t, vector_size_t> Window::findFrameEndPoints(
    vector_size_t /*i*/,
    vector_size_t partitionStartRow,
    vector_size_t /*partitionEndRow*/,
    vector_size_t /*peerStartRow*/,
    vector_size_t peerEndRow,
    vector_size_t /*currentRow*/) {
  // TODO : We handle only the default window frame in this code. Add support
  // for all window frames subsequently.

  // Default window frame is Range UNBOUNDED PRECEDING CURRENT ROW.
  return std::make_pair(partitionStartRow, peerEndRow);
}

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

  std::vector<vector_size_t*> rawFrameStartBuffers;
  std::vector<vector_size_t*> rawFrameEndBuffers;
  rawFrameStartBuffers.reserve(numFuncs);
  rawFrameEndBuffers.reserve(numFuncs);
  for (auto w = 0; w < numFuncs; w++) {
    frameStartBuffers_[w]->setSize(bufferSize);
    frameEndBuffers_[w]->setSize(bufferSize);

    auto rawFrameStartBuffer =
        frameStartBuffers_[w]->asMutable<vector_size_t>();
    auto rawFrameEndBuffer = frameEndBuffers_[w]->asMutable<vector_size_t>();
    rawFrameStartBuffers.push_back(rawFrameStartBuffer);
    rawFrameEndBuffers.push_back(rawFrameEndBuffer);
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

    for (auto w = 0; w < numFuncs; w++) {
      auto frameEndPoints = findFrameEndPoints(
          w,
          firstPartitionRow,
          lastPartitionRow,
          peerStartRow_,
          peerEndRow_ - 1,
          i);
      rawFrameStartBuffers[w][j] = frameEndPoints.first - firstPartitionRow;
      rawFrameEndBuffers[w][j] = frameEndPoints.second - firstPartitionRow;
    }
  }
  // Invoke the apply method for the WindowFunctions.
  for (auto w = 0; w < numFuncs; w++) {
    windowFunctions_[w]->apply(
        peerStartBuffer_,
        peerEndBuffer_,
        frameStartBuffers_[w],
        frameEndBuffers_[w],
        resultOffset,
        result[w]);
  }

  numProcessedRows_ += numRows;
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
