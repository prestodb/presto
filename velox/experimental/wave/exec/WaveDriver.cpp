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
    std::vector<OperandId> resultOrder,
    SubfieldMap subfields,
    std::vector<std::unique_ptr<AbstractOperand>> operands)
    : exec::SourceOperator(
          driverCtx,
          outputType,
          operatorId,
          planNodeId,
          "Wave"),
      arena_(std::move(arena)),
      resultOrder_(std::move(resultOrder)),
      subfields_(std::move(subfields)),
      operands_(std::move(operands)) {
  VELOX_CHECK(!waveOperators.empty());
  pipelines_.emplace_back();
  for (auto& op : waveOperators) {
    op->setDriver(this);
    if (!op->isStreaming()) {
      pipelines_.emplace_back();
    }
    pipelines_.back().operators.push_back(std::move(op));
  }
}

RowVectorPtr WaveDriver::getOutput() {
  for (;;) {
    startMore();
    bool running = false;
    for (int i = pipelines_.size() - 1; i >= 0; --i) {
      if (pipelines_[i].streams.empty()) {
        continue;
      }
      auto& op = *pipelines_[i].operators.back();
      auto& lastSet = op.syncSet();
      auto& streams = pipelines_[i].streams;
      for (auto it = streams.begin(); it != streams.end();) {
        auto& stream = *it;
        if (!stream->isArrived(lastSet)) {
          ++it;
          continue;
        }
        RowVectorPtr result;
        if (i + 1 < pipelines_.size()) {
          pipelines_[i + 1].operators[0]->enqueue(
              makeWaveResult(op.outputType(), *stream, lastSet));
        } else {
          result = makeResult(*stream, lastSet);
        }
        if (streamAtEnd(*stream)) {
          it = streams.erase(it);
        } else {
          ++it;
        }
        if (result) {
          return result;
        }
      }
      if (i + 1 < pipelines_.size()) {
        pipelines_[i + 1].operators[0]->flush();
      }
      running = true;
    }
    if (!running) {
      finished_ = true;
      return nullptr;
    }
  }
}

bool WaveDriver::streamAtEnd(WaveStream& stream) {
  return true;
}

WaveVectorPtr WaveDriver::makeWaveResult(
    const TypePtr& rowType,
    WaveStream& stream,
    const OperandSet& lastSet) {
  auto result = WaveVector::create(rowType, *arena_);
  int32_t nthChild = 0;
  lastSet.forEach([&](int32_t id) {
    auto exe = stream.operandExecutable(id);
    VELOX_CHECK_NOT_NULL(exe);
    auto ordinal = exe->outputOperands.ordinal(id);
    result->setChildAt(nthChild++, std::move(exe->output[ordinal]));
  });
  return result;
}

RowVectorPtr WaveDriver::makeResult(
    WaveStream& stream,
    const OperandSet& lastSet) {
  auto& last = *pipelines_.back().operators.back();
  auto& rowType = last.outputType();
  std::vector<VectorPtr> children(rowType->size());
  auto result = std::make_shared<RowVector>(
      operatorCtx_->pool(),
      rowType,
      BufferPtr(nullptr),
      last.outputSize(stream),
      std::move(children));
  int32_t nthChild = 0;
  for (auto id : resultOrder_) {
    auto exe = stream.operandExecutable(id);
    VELOX_CHECK_NOT_NULL(exe);
    auto ordinal = exe->outputOperands.ordinal(id);
    auto waveVector = std::move(exe->output[ordinal]);
    result->childAt(nthChild++) = waveVector->toVelox(operatorCtx_->pool());
  };
  return result;
}

void WaveDriver::startMore() {
  for (int i = 0; i < pipelines_.size(); ++i) {
    auto& ops = pipelines_[i].operators;
    if (auto rows = ops[0]->canAdvance()) {
      auto stream = std::make_unique<WaveStream>(*arena_);
      for (auto& op : ops) {
        op->schedule(*stream, rows);
      }
      if (i == pipelines_.size() - 1) {
        prefetchReturn(*stream);
      }
      pipelines_[i].streams.push_back(std::move(stream));
      break;
    }
  }
}

void WaveDriver::prefetchReturn(WaveStream& stream) {
  // Schedule return buffers from last op to be on host side.
}

LaunchControl* WaveDriver::inputControl(
    WaveStream& stream,
    int32_t operatorId) {
  for (auto& pipeline : pipelines_) {
    if (operatorId > pipeline.operators.back()->operatorId()) {
      continue;
    }
    operatorId -= pipeline.operators[0]->operatorId();
    VELOX_CHECK_LT(0, operatorId, "Op 0 has no input control");
    for (auto i = operatorId - 1; i >= 0; --i) {
      if (i == 0 || pipeline.operators[i]->isFilter() ||
          pipeline.operators[i]->isExpanding()) {
        return stream.launchControls(i).back().get();
      }
    }
  }
  VELOX_FAIL();
}

std::string WaveDriver::toString() const {
  std::ostringstream out;
  out << "{Wave" << std::endl;
  for (auto& pipeline : pipelines_) {
    out << "{Pipeline" << std::endl;
    for (auto& op : pipeline.operators) {
      out << op->toString() << std::endl;
    }
  }
  return out.str();
}

} // namespace facebook::velox::wave
