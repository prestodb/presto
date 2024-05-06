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
  auto returnBatchSize = 10000 * outputType_->size() * 10;
  hostArena_ = std::make_unique<GpuArena>(
      returnBatchSize * 10, getHostAllocator(getDevice()));
  pipelines_.emplace_back();
  for (auto& op : waveOperators) {
    op->setDriver(this);
    if (!op->isStreaming()) {
      pipelines_.emplace_back();
    }
    pipelines_.back().operators.push_back(std::move(op));
  }
  pipelines_.back().needStatus = true;
}

RowVectorPtr WaveDriver::getOutput() {
  VLOG(1) << "Getting output";
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
        stream->setState(WaveStream::State::kNotRunning);
        RowVectorPtr result;
        if (i + 1 < pipelines_.size()) {
          auto waveResult = makeWaveResult(op.outputType(), *stream, lastSet);
          VLOG(1) << "Enqueue " << waveResult->size() << " rows to pipeline "
                  << i + 1;
          pipelines_[i + 1].operators[0]->enqueue(std::move(waveResult));
        } else {
          result = makeResult(*stream, lastSet);
          VLOG(1) << "Final output size: " << result->size();
        }
        if (streamAtEnd(*stream)) {
          waveStats_.add(stream->stats());
          it = streams.erase(it);
        } else {
          ++it;
        }
        if (result) {
          VLOG(1) << "Got output";
          return result;
        }
      }
      if (i + 1 < pipelines_.size()) {
        pipelines_[i + 1].operators[0]->flush(
            streams.empty() && pipelines_[i].operators[0]->isFinished());
      }
      running = true;
    }
    if (!running) {
      VLOG(1) << "No more output";
      updateStats();
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
  std::vector<WaveVectorPtr> children(rowType->size());
  int32_t nthChild = 0;
  lastSet.forEach([&](int32_t id) {
    auto exe = stream.operandExecutable(id);
    VELOX_CHECK_NOT_NULL(exe);
    auto ordinal = exe->outputOperands.ordinal(id);
    children[nthChild++] = std::move(exe->output[ordinal]);
  });
  return std::make_unique<WaveVector>(rowType, *arena_, std::move(children));
}

RowVectorPtr WaveDriver::makeResult(
    WaveStream& stream,
    const OperandSet& lastSet) {
  auto& last = *pipelines_.back().operators.back();
  auto& rowType = last.outputType();
  auto operatorId = last.operatorId();
  std::vector<VectorPtr> children(rowType->size());
  int32_t numRows = stream.getOutput(
      operatorId, *operatorCtx_->pool(), resultOrder_, children.data());
  auto result = std::make_shared<RowVector>(
      operatorCtx_->pool(),
      rowType,
      BufferPtr(nullptr),
      numRows,
      std::move(children));
  if (!numRows) {
    return nullptr;
  }
  return result;
}

void WaveDriver::startMore() {
  for (int i = 0; i < pipelines_.size(); ++i) {
    auto& ops = pipelines_[i].operators;
    blockingReason_ = ops[0]->isBlocked(&blockingFuture_);
    if (blockingReason_ != exec::BlockingReason::kNotBlocked) {
      return;
    }
    auto stream =
        std::make_unique<WaveStream>(*arena_, *hostArena_, &operands());
    stream->setState(WaveStream::State::kHost);

    if (auto rows = ops[0]->canAdvance(*stream)) {
      VLOG(1) << "Advance " << rows << " rows in pipeline " << i;
      stream->setNumRows(rows);
      if (i == pipelines_.size() - 1) {
        for (auto i : resultOrder_) {
          stream->markHostOutputOperand(*operands_[i]);
        }
      }
      stream->setReturnData(pipelines_[i].needStatus);
      for (auto& op : ops) {
        op->schedule(*stream, rows);
      }
      if (pipelines_[i].needStatus) {
        stream->resultToHost();
      }
      stream->setState(WaveStream::State::kNotRunning);
      pipelines_[i].streams.push_back(std::move(stream));
      break;
    }
  }
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

void WaveDriver::updateStats() {
  auto lockedStats = stats_.wlock();
  lockedStats->addRuntimeStat(
      "wave.numWaves", RuntimeCounter(waveStats_.numWaves));
  lockedStats->addRuntimeStat(
      "wave.numKernels", RuntimeCounter(waveStats_.numKernels));
  lockedStats->addRuntimeStat(
      "wave.numThreadBlocks", RuntimeCounter(waveStats_.numThreadBlocks));
  lockedStats->addRuntimeStat(
      "wave.numThreads", RuntimeCounter(waveStats_.numThreads));
  lockedStats->addRuntimeStat(
      "wave.numPrograms", RuntimeCounter(waveStats_.numPrograms));
  lockedStats->addRuntimeStat(
      "wave.numSync", RuntimeCounter(waveStats_.numSync));
  lockedStats->addRuntimeStat(
      "wave.bytesToDevice",
      RuntimeCounter(waveStats_.bytesToDevice, RuntimeCounter::Unit::kBytes));
  lockedStats->addRuntimeStat(
      "wave.bytesToHost",
      RuntimeCounter(waveStats_.bytesToHost, RuntimeCounter::Unit::kBytes));
  lockedStats->addRuntimeStat(
      "wave.hostOnlyTime",
      RuntimeCounter(
          waveStats_.hostOnlyTime.micros * 1000, RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "wave.hostParallelTime",
      RuntimeCounter(
          waveStats_.hostParallelTime.micros * 1000,
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "wave.waitTime",
      RuntimeCounter(
          waveStats_.waitTime.micros * 1000, RuntimeCounter::Unit::kNanos));
}

} // namespace facebook::velox::wave
