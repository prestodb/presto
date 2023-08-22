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

#include "velox/experimental/wave/exec/WaveDriver.h"
#include "velox/experimental/wave/exec/Instruction.h"
#include "velox/experimental/wave/exec/WaveOperator.h"

namespace facebook::velox::wave {

WaveDriver::WaveDriver(
    exec::DriverCtx* driverCtx,
    RowTypePtr outputType,
    core::PlanNodeId planNodeId,
    int32_t operatorId,
    std::unique_ptr<GpuArena> arena,
    std::vector<std::unique_ptr<WaveOperator>> waveOperators,
    SubfieldMap subfields,
    std::vector<std::unique_ptr<AbstractOperand>> operands)
    : exec::SourceOperator(
          driverCtx,
          outputType,
          operatorId,
          planNodeId,
          "Wave"),
      arena_(std::move(arena)),
      waveOperators_(std::move(waveOperators)),
      subfields_(std::move(subfields)),
      operands_(std::move(operands)) {
  for (auto& op : waveOperators_) {
    op->setDriver(this);
  }
}

RowVectorPtr WaveDriver::getOutput() {
  for (;;) {
    startMore();
    if (streams_.empty()) {
      finished_ = true;
      return nullptr;
    }
    auto& lastSet = waveOperators_.back()->outputIds();
    for (auto i = 0; i < streams_.size(); ++i) {
      auto stream = streams_[i].get();
      if (stream->isArrived(lastSet)) {
        auto result = makeResult(*stream, lastSet);
        if (streamAtEnd(*stream)) {
          streams_.erase(streams_.begin() + i);
        }
        return result;
      }
    }
  }
}

bool WaveDriver::streamAtEnd(WaveStream& stream) {
  return true;
}
RowVectorPtr WaveDriver::makeResult(
    WaveStream& stream,
    const OperandSet& lastSet) {
  auto& last = *waveOperators_.back();
  auto& rowType = last.outputType();
  std::vector<VectorPtr> children(rowType->size());
  auto result = std::make_shared<RowVector>(
      operatorCtx_->pool(),
      rowType,
      BufferPtr(nullptr),
      last.outputSize(),
      std::move(children));
  int32_t nthChild = 0;
  lastSet.forEach([&](int32_t id) {
    auto exe = stream.operandExecutable(id);
    VELOX_CHECK_NOT_NULL(exe);
    auto ordinal = exe->outputOperands.ordinal(id);
    auto waveVector = std::move(exe->output[ordinal]);
    result->childAt(nthChild++) = waveVector->toVelox(operatorCtx_->pool());
  });
  return result;
}

void WaveDriver::startMore() {
  auto rows = waveOperators_[0]->canAdvance();
  if (!rows) {
    return;
  }
  streams_.push_back(std::make_unique<WaveStream>(*arena_));
  auto& stream = *streams_.back();
  for (auto i = 0; i < waveOperators_.size(); ++i) {
    waveOperators_[i]->schedule(stream, rows);
  }
  prefetchReturn(stream);
}

void WaveDriver::prefetchReturn(WaveStream& stream) {
  // Schedule return buffers from last op to be on host side.
  auto& last = waveOperators_.back();
  auto& ids = last->outputIds();
}

std::string WaveDriver::toString() const {
  std::stringstream out;
  out << "{Wave" << std::endl;
  for (auto& op : waveOperators_) {
    out << op->toString() << std::endl;
  }
  return out.str();
}

} // namespace facebook::velox::wave
