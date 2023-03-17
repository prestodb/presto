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

#include "velox/experimental/exec/OffProcessExpressionEval.h"

namespace facebook::velox::exec {

// Maximum buffer size in bytes for stream groups before they are
// flushed.
constexpr int32_t kMaxStreamGroupSizeBytes{50000};

void OffProcessExpressionEvalNode::addDetails(std::stringstream& stream) const {
  stream << "expressions: ";
  for (auto i = 0; i < expressions_.size(); i++) {
    auto& expr = expressions_[i];
    if (i > 0) {
      stream << ", ";
    }
    stream << "(" << expr->type()->toString() << ", " << expr->toString()
           << ")";
  }
}

OffProcessExpressionEvalOperator::OffProcessExpressionEvalOperator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const OffProcessExpressionEvalNode>& planNode)
    : Operator(
          driverCtx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "OffProcessExpressionEval"),
      inputType_(planNode->sources().front()->outputType()),
      expressions_{planNode->expressions()} {}

void OffProcessExpressionEvalOperator::addInput(RowVectorPtr input) {
  if (!streamGroup_) {
    streamGroup_ = std::make_unique<VectorStreamGroup>(pool());
    streamGroup_->createStreamTree(inputType_, 1000);
  }

  IndexRange range{0, input->size()};
  streamGroup_->append(input, folly::Range<IndexRange*>(&range, 1));

  if (streamGroup_->size() > kMaxStreamGroupSizeBytes) {
    flushStreamGroup();
  }
}

void OffProcessExpressionEvalOperator::flushStreamGroup() {
  if (streamGroup_) {
    IOBufOutputStream stream(*pool());
    streamGroup_->flush(&stream);
    addRuntimeStat(
        "dataFlushes",
        RuntimeCounter(streamGroup_->size(), RuntimeCounter::Unit::kBytes));

    auto ioBuf = stream.getIOBuf();
    streamGroup_.reset();

    ioBuf_ = sendOffProcess(expressions_, std::move(ioBuf));
  }
}

std::unique_ptr<folly::IOBuf> OffProcessExpressionEvalOperator::sendOffProcess(
    const std::vector<core::TypedExprPtr>& /* expressions */,
    std::unique_ptr<folly::IOBuf>&& ioBuf) {
  // TODO: Implement a pluggable logic to send data off-process.
  return std::move(ioBuf);
}

void OffProcessExpressionEvalOperator::noMoreInput() {
  flushStreamGroup();
  Operator::noMoreInput();
}

RowVectorPtr OffProcessExpressionEvalOperator::getOutput() {
  if (!ioBuf_) {
    return nullptr;
  }

  auto outputVector = deserializeIOBuf(*ioBuf_);
  ioBuf_.reset();
  return outputVector;
}

RowVectorPtr OffProcessExpressionEvalOperator::deserializeIOBuf(
    const folly::IOBuf& ioBuf) {
  std::vector<ByteRange> ranges;
  ranges.reserve(4);

  for (const auto& range : ioBuf) {
    ranges.emplace_back(ByteRange{
        const_cast<uint8_t*>(range.data()), (int32_t)range.size(), 0});
  }
  byteStream_.resetInput(std::move(ranges));

  RowVectorPtr outputVector;
  VectorStreamGroup::read(&byteStream_, pool(), outputType_, &outputVector);
  return outputVector;
}

} // namespace facebook::velox::exec
