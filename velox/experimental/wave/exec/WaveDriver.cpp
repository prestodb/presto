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
#include <iostream>
#include "velox/common/process/TraceContext.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Task.h"
#include "velox/experimental/wave/exec/Instruction.h"
#include "velox/experimental/wave/exec/WaveOperator.h"

DEFINE_int32(
    max_streams_per_driver,
    4,
    "Number of parallel Cuda streams per CPU thread");

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
    std::vector<std::unique_ptr<AbstractOperand>> operands,
    std::vector<std::unique_ptr<AbstractState>> states,
    InstructionStatus instructionStatus)
    : exec::SourceOperator(
          driverCtx,
          outputType,
          operatorId,
          planNodeId,
          "Wave"),
      arena_(std::move(arena)),
      resultOrder_(std::move(resultOrder)),
      subfields_(std::move(subfields)),
      operands_(std::move(operands)),
      states_(std::move(states)),
      instructionStatus_(instructionStatus) {
  VELOX_CHECK(!waveOperators.empty());
  auto returnBatchSize = 10000 * outputType_->size() * 10;
  deviceArena_ = std::make_unique<GpuArena>(
      100000000, getDeviceAllocator(getDevice()), 400000000);
  pipelines_.emplace_back();
  for (auto& op : waveOperators) {
    op->setDriver(this);
    if (!op->isStreaming()) {
      pipelines_.emplace_back();
    }
    pipelines_.back().operators.push_back(std::move(op));
  }
  pipelines_.back().needStatus = true;
  // True unless ends with repartitioning.
  pipelines_.back().makesHostResult = true;
  pipelines_.front().canAdvance = true;
}

bool WaveDriver::shouldYield(
    exec::StopReason taskStopReason,
    size_t startTimeMs) const {
  // Checks task-level yield signal, driver-level yield signal and table scan
  // output processing time limit.
  return taskStopReason == exec::StopReason::kYield ||
      operatorCtx()->driverCtx()->driver->shouldYield() ||
      ((getOutputTimeLimitMs_ != 0) &&
       (getCurrentTimeMs() - startTimeMs) >= getOutputTimeLimitMs_);
}

RowVectorPtr WaveDriver::getOutput() {
  if (finished_) {
    return nullptr;
  }
  startTimeMs_ = getCurrentTimeMs();
  int32_t last = pipelines_.size() - 1;
  try {
    for (int32_t i = last; i >= 0; --i) {
      if (!pipelines_[i].canAdvance) {
        continue;
      }
      auto status = advance(i);
      switch (status) {
        case Advance::kBlocked:
          updateStats();
          return nullptr;
        case Advance::kResult:
          if (i == last) {
            if (pipelines_[i].makesHostResult) {
              return result_;
            } else {
              break;
            }
          }
          pipelines_[i + 1].canAdvance = true;
          i += 2;
          break;
        case Advance::kFinished:
          pipelines_[i].canAdvance = false;
          if (i == 0 || pipelines_[i].noMoreInput) {
            flush(i);
            if (i < last) {
              pipelines_[i + 1].noMoreInput = true;
              pipelines_[i + 1].canAdvance = true;
              i += 2;
              break;
            } else {
              // Last finished.
              finished_ = true;
              updateStats();
              return nullptr;
            }
          }
          break;
      }
    }
  } catch (const std::exception& e) {
    updateStats();
    setError();
    throw;
  }
  finished_ = true;
  updateStats();
  return nullptr;
}

void WaveDriver::flush(int32_t pipelineIdx) {
  //
  ;
}

namespace {
void moveTo(
    std::vector<std::unique_ptr<WaveStream>>& from,
    int32_t i,
    std::vector<std::unique_ptr<WaveStream>>& to,
    bool toRun = false) {
  if (!toRun) {
    VELOX_CHECK(from[i]->state() != WaveStream::State::kParallel);
  }
  to.push_back(std::move(from[i]));
  from.erase(from.begin() + i);
}
} // namespace

exec::BlockingReason WaveDriver::processArrived(Pipeline& pipeline) {
  for (auto streamIdx = 0; streamIdx < pipeline.arrived.size(); ++streamIdx) {
    bool continued = false;
    for (int32_t i = pipeline.operators.size() - 1; i >= 0; --i) {
      auto reason = pipeline.operators[i]->isBlocked(&blockingFuture_);
      if (reason != exec::BlockingReason::kNotBlocked) {
        return reason;
      }
      auto advance =
          pipeline.operators[i]->canAdvance(*pipeline.arrived[streamIdx]);
      if (!advance.empty()) {
        runOperators(
            pipeline, *pipeline.arrived[streamIdx], i, advance.numRows);
        moveTo(pipeline.arrived, i, pipeline.running, true);
        continued = true;
        break;
      }
    }

    if (continued) {
      --streamIdx;
    } else {
      /// Not blocked and not continuable, so must be at end.
      moveTo(pipeline.arrived, streamIdx, pipeline.finished);
      --streamIdx;
    }
  }
  return exec::BlockingReason::kNotBlocked;
}

void WaveDriver::runOperators(
    Pipeline& pipeline,
    WaveStream& stream,
    int32_t from,
    int32_t numRows) {
  // The stream is in 'host' state for any host to device data
  // transfer, then in parallel state after first kernel launch.
  ++stream.stats().numWaves;
  stream.setState(WaveStream::State::kHost);
  for (auto i = from; i < pipeline.operators.size(); ++i) {
    pipeline.operators[i]->schedule(stream, numRows);
  }
  stream.resultToHost();
}

// Global counter for busy wait iterations.
tsan_atomic<int64_t> totalWaitLoops;

void WaveDriver::waitForArrival(Pipeline& pipeline) {
  auto set = pipeline.operators.back()->syncSet();
  int64_t waitLoops = 0;
  WaveTimer timer(waveStats_.waitTime);
  while (!pipeline.running.empty()) {
    for (auto i = 0; i < pipeline.running.size(); ++i) {
      auto waitUs = pipeline.running.size() == 1 ? 0 : 10;
      if (pipeline.running[i]->isArrived(set, waitUs, 0)) {
        incStats((pipeline.running[i]->stats()));
        pipeline.running[i]->setState(WaveStream::State::kNotRunning);
        moveTo(pipeline.running, i, pipeline.arrived);
      }
      ++waitLoops;
    }
  }
  totalWaitLoops += waitLoops;
}

namespace {
bool shouldStop(exec::StopReason taskStopReason) {
  return taskStopReason != exec::StopReason::kNone &&
      taskStopReason != exec::StopReason::kYield;
}
} // namespace

Advance WaveDriver::advance(int pipelineIdx) {
  auto& pipeline = pipelines_[pipelineIdx];
  int64_t waitLoops = 0;
  // Set to true when any stream is seen not ready, false when any stream is
  // seen ready.
  bool isWaiting = false;
  // Time when a stream was first seen not ready.
  int64_t waitingSince = 0;
  // Total wait time. Incremented when isWaiting is set to false from true.
  int64_t waitUs = 0;
  for (;;) {
    const exec::StopReason taskStopReason =
        operatorCtx()->driverCtx()->task->shouldStop();
    if (shouldStop(taskStopReason) ||
        shouldYield(taskStopReason, startTimeMs_)) {
      blockingReason_ = exec::BlockingReason::kYield;
      blockingFuture_ = ContinueFuture{folly::Unit{}};
      // A point for test code injection.
      common::testutil::TestValue::adjust(
          "facebook::velox::wave::WaveDriver::getOutput::yield", this);
      totalWaitLoops += waitLoops;
      waveStats_.waitTime.micros += waitUs;

      return Advance::kBlocked;
    }

    if (pipeline.sinkFull) {
      pipeline.sinkFull = false;
      for (auto i = 0; i < pipeline.arrived.size(); ++i) {
        pipeline.arrived[i]->resetSink();
      }
    }
    blockingReason_ = processArrived(pipeline);
    if (blockingReason_ != exec::BlockingReason::kNotBlocked) {
      totalWaitLoops += waitLoops;
      return Advance::kBlocked;
    }
    if (pipeline.running.empty() && pipeline.arrived.empty() &&
        !pipeline.finished.empty()) {
      totalWaitLoops += waitLoops;
      return Advance::kFinished;
    }
    auto& op = *pipeline.operators.back();
    auto& lastSet = op.syncSet();
    for (auto i = 0; i < pipeline.running.size(); ++i) {
      bool isArrived;
      int64_t start = WaveTime::getMicro();
      isArrived = pipeline.running[i]->isArrived(lastSet);
      waveStats_.waitTime.micros += WaveTime::getMicro() - start;
      if (isArrived) {
        auto arrived = pipeline.running[i].get();
        arrived->setState(WaveStream::State::kNotRunning);
        incStats(arrived->stats());
        if (isWaiting) {
          waitUs += WaveTime::getMicro() - waitingSince;
          isWaiting = false;
        }
        moveTo(pipeline.running, i, pipeline.arrived);
        if (pipeline.makesHostResult) {
          result_ = makeResult(*arrived, lastSet);
          if (result_ && result_->size() != 0) {
            totalWaitLoops += waitLoops;
            waveStats_.waitTime.micros += waitUs;
            return Advance::kResult;
          }
          --i;
        } else if (arrived->isSinkFull()) {
          pipeline.sinkFull = true;
          waitForArrival(pipeline);
          totalWaitLoops += waitLoops;
          waveStats_.waitTime.micros += waitUs;
          return Advance::kResult;
        }
      } else if (!isWaiting) {
        waitingSince = WaveTime::getMicro();
        isWaiting = true;
      } else {
        ++waitLoops;
      }
    }
    if (pipeline.finished.empty() &&
        pipeline.running.size() + pipeline.arrived.size() <
            FLAGS_max_streams_per_driver) {
      auto stream = std::make_unique<WaveStream>(
          *arena_, *deviceArena_, &operands(), &stateMap_, instructionStatus_);
      stream->setState(WaveStream::State::kHost);
      pipeline.arrived.push_back(std::move(stream));
    }
  }
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

void WaveDriver::setError() {
  for (auto& pipeline : pipelines_) {
    for (auto& stream : pipeline.running) {
      stream->setError();
    }
  }
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
      "wave.hostOnlyNanos",
      RuntimeCounter(
          waveStats_.hostOnlyTime.micros * 1000, RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "wave.hostParallelNanos",
      RuntimeCounter(
          waveStats_.hostParallelTime.micros * 1000,
          RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "wave.waitNanos",
      RuntimeCounter(
          waveStats_.waitTime.micros * 1000, RuntimeCounter::Unit::kNanos));
  lockedStats->addRuntimeStat(
      "wave.stagingNanos",
      RuntimeCounter(
          waveStats_.stagingTime.micros * 1000, RuntimeCounter::Unit::kNanos));
  if (FLAGS_wave_transfer_timing) {
    lockedStats->addRuntimeStat(
        "wave.transferWaitNanos",
        RuntimeCounter(
            waveStats_.transferWaitTime.micros * 1000,
            RuntimeCounter::Unit::kNanos));
  }
}

} // namespace facebook::velox::wave
