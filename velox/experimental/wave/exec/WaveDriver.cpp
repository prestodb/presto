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
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/WaveOperator.h"

DEFINE_int32(
    max_streams_per_driver,
    4,
    "Number of parallel Cuda streams per CPU thread");

namespace facebook::velox::wave {

std::mutex WaveBarrier::barriersMutex_;
std::unordered_map<std::string, std::weak_ptr<WaveBarrier>>
    WaveBarrier::barriers_;

WaveBarrier::WaveBarrier(std::string idString)
    : idString_(std::move(idString)) {}

std::shared_ptr<WaveBarrier> WaveBarrier::get(
    const std::string& taskId,
    int32_t driverId,
    int32_t operatorId) {
  auto id = fmt::format("{}:{}:{}", taskId, driverId, operatorId);
  std::lock_guard<std::mutex> l(barriersMutex_);
  auto it = barriers_.find(id);
  if (it != barriers_.end()) {
    auto ptr = it->second.lock();
    if (ptr) {
      return ptr;
    }
  }
  auto barrier = std::make_shared<WaveBarrier>(id);
  barriers_[id] = barrier;
  return barrier;
}

WaveBarrier::~WaveBarrier() {
  std::lock_guard<std::mutex> l(barriersMutex_);
  barriers_.erase(idString_);
}

void waitFor(ContinueFuture future) {
  std::move(future).via(&folly::QueuedImmediateExecutor::instance()).wait();
}

bool waitForBool(folly::SemiFuture<bool> future) {
  return std::move(future)
      .via(&folly::QueuedImmediateExecutor::instance())
      .get();
}

#if !defined(__APPLE__)
#define VELOX_CHECK_TID(n) VELOX_CHECK(n)
#else
#define VELOX_CHECK_TID()
#endif
namespace {
int32_t getTid() {
#if !defined(__APPLE__)
  // This is a debugging feature disabled on the Mac since syscall
  // is deprecated on that platform.
  return syscall(FOLLY_SYS_gettid);
#else
  return 0;
#endif
}

bool isTidIn(std::vector<int32_t>& tids) {
  return std::find(tids.begin(), tids.end(), getTid()) != tids.end();
}

bool isTidNotIn(std::vector<int32_t>& tids) {
  return std::find(tids.begin(), tids.end(), getTid()) == tids.end();
}

bool isTidNotIn(std::vector<int32_t>& tids, std::mutex& mtx) {
  std::lock_guard<std::mutex> l(mtx);
  return std::find(tids.begin(), tids.end(), getTid()) == tids.end();
}

} // namespace

std::string WaveBarrier::toStringLocked() {
  std::stringstream out;
  out << "{barrier: " << (exclusiveToken_ ? "excl" : "")
      << " joined=" << numJoined_ << " yielded=" << numInArrive_ << " ";
  if (!waitingForExcl_.empty()) {
    out << " wait for excl: ";
    for (auto t : waitingForExcl_) {
      out << t << " ";
    }
  }
  if (!waitingForExclDone_.empty()) {
    out << " wait for excl done: ";
    for (auto t : waitingForExclDone_) {
      out << t << " ";
    }
  }
  out << "}";
  return out.str();
}

std::string WaveBarrier::toString() {
  std::lock_guard<std::mutex> l(mutex_);
  return toStringLocked();
}

void WaveBarrier::enter() {
  ContinueFuture waitFuture;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_TID(isTidNotIn(waitingForExcl_));
    VELOX_CHECK_TID(isTidNotIn(waitingForExclDone_));
    if (!exclusiveToken_ && exclusiveTokens_.empty()) {
      ++numJoined_;
      return;
    }
    auto [promise, tempFuture] = makeVeloxContinuePromiseContract("WaveDriver");

    waitFuture = std::move(tempFuture);
    promises_.push_back(std::move(promise));
    ++numJoined_;
    ++numInArrive_;
    waitingForExclDone_.push_back(getTid());
  }
  waitFor(std::move(waitFuture));
  VELOX_CHECK_TID(isTidNotIn(waitingForExclDone_, mutex_));
}

void WaveBarrier::maybeReleaseAcquireLocked() {
  if (exclusiveToken_) {
    return;
  }
  if (numJoined_ - numInArrive_ == exclusivePromises_.size() &&
      !exclusivePromises_.empty()) {
    exclusiveToken_ = exclusiveTokens_.back();
    waitingForExcl_.pop_back();
    auto promise = std::move(exclusivePromises_.back());
    exclusivePromises_.pop_back();
    exclusiveTokens_.pop_back();
    exclPipelines_.pop_back();
    promise.setValue(true);
  }
}

void WaveBarrier::leave() {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK_TID(isTidNotIn(waitingForExcl_));
  VELOX_CHECK_TID(isTidNotIn(waitingForExclDone_));

  --numJoined_;
  maybeReleaseAcquireLocked();
}

void WaveBarrier::acquire(
    Pipeline* pipeline,
    void* reason,
    std::function<void()> preWait) {
  folly::SemiFuture<bool> future(false);
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (numJoined_ == 1) {
      exclusiveToken_ = reason;
      exclusiveTid_ = getTid();
      return;
    }
  }
  if (preWait) {
    preWait();
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto promise = folly::Promise<bool>();
    future = promise.getSemiFuture();
    exclusivePromises_.push_back(std::move(promise));
    exclusiveTokens_.push_back(reason);
    exclPipelines_.push_back(pipeline);
    waitingForExcl_.push_back(getTid());
    maybeReleaseAcquireLocked();
  }
  waitForBool(std::move(future));
  exclusiveTid_ = getTid();
}

void WaveBarrier::release() {
  VELOX_CHECK_NOT_NULL(exclusiveToken_);
  ContinueFuture waitFuture;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (numJoined_ == 1) {
      exclusiveToken_ = nullptr;
      exclusiveTid_ = 0;
      return;
    }
    if (exclusivePromises_.empty()) {
      exclusiveToken_ = nullptr;
      exclusiveTid_ = 0;
      VELOX_CHECK_EQ(numInArrive_, promises_.size());
      numInArrive_ = 0;
      for (auto& promise : promises_) {
        promise.setValue();
      }
      waitingForExclDone_.clear();
      waitingPipelines_.clear();
      promises_.clear();
      return;
    }
    auto [promise, future] = makeVeloxContinuePromiseContract("WaveDriver");
    ++numInArrive_;
    promises_.push_back(std::move(promise));
    waitFuture = std::move(future);
    exclusiveTid_ = 0;
    exclusiveToken_ = nullptr;
    maybeReleaseAcquireLocked();
  }
  waitFor(std::move(waitFuture));
  VELOX_CHECK_TID(isTidNotIn(waitingForExclDone_, mutex_));
}

void WaveBarrier::mayYield(Pipeline* pipeline, std::function<void()> preWait) {
  ContinueFuture waitFuture;
  folly::Promise<bool> exclPromise;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (exclusiveTokens_.empty()) {
      return;
    }
  }
  // Somebody has acquire() pending. The acquire() will not complete until this
  // adds itself to waiting. But before acknowledging the acquire, do preWait.
  if (preWait) {
    preWait();
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto [promise, future] = makeVeloxContinuePromiseContract("WaveDriver");
    promises_.push_back(std::move(promise));
    waitFuture = std::move(future);
    ++numInArrive_;
    waitingPipelines_.push_back(pipeline);
    waitingForExclDone_.push_back(getTid());
    if (numJoined_ - numInArrive_ == exclusivePromises_.size() &&
        !exclusivePromises_.empty()) {
      exclusiveToken_ = exclusiveTokens_.back();
      exclPromise = std::move(exclusivePromises_.back());
      exclusivePromises_.pop_back();
      exclusiveTokens_.pop_back();
      exclPipelines_.pop_back();
      waitingForExcl_.pop_back();
    }
  }
  if (exclPromise.valid()) {
    exclPromise.setValue(true);
  }
  waitFor(std::move(waitFuture));
  VELOX_CHECK_TID(isTidNotIn(waitingForExclDone_, mutex_));
}

std::vector<WaveStream*> WaveBarrier::waitingStreams() const {
  std::vector<WaveStream*> result;
  for (auto& pipeline : exclPipelines_) {
    for (auto& s : pipeline->arrived) {
      result.push_back(s.get());
    }
  }
  for (auto& pipeline : waitingPipelines_) {
    for (auto& s : pipeline->arrived) {
      result.push_back(s.get());
    }
  }
  return result;
}

WaveDriver::WaveDriver(
    exec::DriverCtx* driverCtx,
    RowTypePtr outputType,
    core::PlanNodeId planNodeId,
    int32_t operatorId,
    std::shared_ptr<GpuArena> arena,
    std::vector<std::unique_ptr<WaveOperator>> waveOperators,
    std::vector<OperandId> resultOrder,
    std::shared_ptr<WaveRuntimeObjects> runtime)
    : exec::SourceOperator(
          driverCtx,
          outputType,
          operatorId,
          planNodeId,
          "Wave"),
      barrier_(WaveBarrier::get(driverCtx->task->taskId(), 0, operatorId)),
      arena_(std::move(arena)),
      resultOrder_(std::move(resultOrder)),
      runtime_(std::move(runtime)),
      subfields_(runtime_->subfields),
      operands_(runtime_->operands),
      states_(runtime_->states) {
  VELOX_CHECK(!waveOperators.empty());
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
  barrier_->enter();
  startTimeMs_ = getCurrentTimeMs();
  [[maybe_unused]] auto guard = folly::makeGuard([&]() { barrier_->leave(); });
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
              if (maybeWaitForPeers()) {
                return nullptr;
              }
              pipelineFinished(i - 2);
              break;
            } else {
              // Last finished.
              finished_ = true;
              updateStats();
              if (maybeWaitForPeers()) {
                return nullptr;
              }
              pipelineFinished(i);

              return nullptr;
            }
          }
          break;
      }
    }
  } catch (const std::exception&) {
    updateStats();
    setError();
    throw;
  }
  finished_ = true;
  updateStats();
  return nullptr;
}

bool WaveDriver::maybeWaitForPeers() {
  if (operatorCtx_->task()->numDrivers(operatorCtx_->driver()) == 1) {
    return false;
  }
  if (barrier_->stateMap().states.empty()) {
    return false;
  }
  TR("wait_for_peers\n");
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<exec::Driver>> peers;

  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(),
          operatorCtx_->driver(),
          &blockingFuture_,
          promises,
          peers)) {
    blockingReason_ = exec::BlockingReason::kYield;
    return true;
  }

  // Realize the promises so that the other Drivers (which were not
  // the last to finish) can continue from the barrier.
  peers.clear();
  for (auto& promise : promises) {
    promise.setValue();
  }

  return false;
}

void WaveDriver::flush(int32_t pipelineIdx) {
  //
  ;
}

void WaveDriver::pipelineFinished(int32_t pipelineIdx) {
  auto& pipeline = pipelines_[pipelineIdx];
  VELOX_CHECK(pipeline.arrived.empty());
  VELOX_CHECK(pipeline.running.empty());
  VELOX_CHECK(!pipeline.finished.empty());
  for (auto i = 0; i < pipeline.operators.size(); ++i) {
    pipeline.operators[i]->pipelineFinished(*pipeline.finished[0]);
  }
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

void interpretError(const KernelError* error) {
  auto string = waveRegistry().message(error->messageEnum);
  VELOX_USER_FAIL(string);
}
} // namespace

exec::BlockingReason WaveDriver::processArrived(Pipeline& pipeline) {
  for (auto streamIdx = 0; streamIdx < pipeline.arrived.size(); ++streamIdx) {
    bool continued = false;
    for (int32_t i = pipeline.operators.size() - 1; i >= 0; --i) {
      auto reason = pipeline.operators[i]->isBlocked(
          *pipeline.arrived[streamIdx], &blockingFuture_);
      if (reason != exec::BlockingReason::kNotBlocked) {
        return reason;
      }
      auto advance =
          pipeline.operators[i]->canAdvance(*pipeline.arrived[streamIdx]);
      if (!advance.empty()) {
        prepareAdvance(pipeline, *pipeline.arrived[streamIdx], i, advance);

        runOperators(
            pipeline, *pipeline.arrived[streamIdx], i, advance[0].numRows);
        TR(pipeline.arrived[streamIdx], "running");
        moveTo(pipeline.arrived, streamIdx, pipeline.running, true);
        continued = true;
        break;
      }
    }

    if (continued) {
      --streamIdx;
    } else {
      /// Not blocked and not continuable, so must be at end.
      pipeline.arrived[streamIdx]->releaseStreamsAndEvents();
      TR(pipeline.arrived[streamIdx], "finished");
      moveTo(pipeline.arrived, streamIdx, pipeline.finished);
      --streamIdx;
    }
  }
  return exec::BlockingReason::kNotBlocked;
}

void WaveDriver::prepareAdvance(
    Pipeline& pipeline,
    WaveStream& stream,
    int32_t from,
    std::vector<AdvanceResult>& advanceVector) {
  VELOX_CHECK(
      stream.state() == WaveStream::State::kNotRunning ||
      stream.state() == WaveStream::State::kHost);
  void* driversToken = nullptr;
  int32_t exclusiveIndex = 0;
  for (auto i = 0; i < advanceVector.size(); ++i) {
    auto& advance = advanceVector[i];
    if (!advance.updateStatus) {
      continue;
    }
    if (advance.syncDrivers) {
      VELOX_CHECK_NULL(driversToken);
      driversToken = advance.reason;
      VELOX_CHECK_NOT_NULL(driversToken);
      exclusiveIndex = i;
    } else if (advance.syncStreams) {
      waitForArrival(pipeline);
    } else {
      // No sync, like adding memory to string pool for func.
      std::vector<WaveStream*> empty;
      pipeline.operators[from]->callUpdateStatus(stream, empty, advance);
    }
  }
  if (driversToken) {
    TR((&stream), "acquire");
    barrier_->acquire(
        &pipeline, driversToken, [&]() { waitForArrival(pipeline); });
    auto guard = folly::makeGuard([&]() {
      TR((&stream), "release");
      barrier_->release();
    });

    waitForArrival(pipeline);
    auto otherStreams = barrier_->waitingStreams();
    pipeline.operators[from]->callUpdateStatus(
        stream, otherStreams, advanceVector[exclusiveIndex]);
  }
}

void WaveDriver::runOperators(
    Pipeline& pipeline,
    WaveStream& stream,
    int32_t from,
    int32_t numRows) {
  // Pause here if other WaveDrivers need exclusive access.
  barrier_->mayYield(&pipeline, [&]() { waitForArrival(pipeline); });
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
        pipeline.running[i]->checkBlockStatuses();
        pipeline.running[i]->throwIfError(interpretError);
        TR(pipeline.running[i], "arrived inside wait");
        moveTo(pipeline.running, i, pipeline.arrived);
      }
      ++waitLoops;
      --i;
    }
    if (waitLoops > 1000000) {
      totalWaitLoops += waitLoops;
      waitLoops = 0;
      for (auto i = 0; i < pipeline.running.size(); ++i) {
        TR(pipeline.running[i], "pending");
      }
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
        arrived->throwIfError(interpretError);
        TR(pipeline.running[i], "arrived");
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
      // Ordinal of WaveStream across this pipeline across all parallel
      // WaveDrivers.
      int16_t streamId = pipeline.arrived.size() + pipeline.running.size() +
          (FLAGS_max_streams_per_driver * operatorCtx_->driverCtx()->driverId);
      auto stream = std::make_unique<WaveStream>(
          arena_,
          *deviceArena_,
          &operands(),
          &barrier_->stateMap(),
          pipeline.operators[0]->instructionStatus(),
          streamId);
      TR(stream, "created");
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
  if (last.isSink()) {
    return nullptr;
  }
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
  hasError_ = true;
  for (auto& pipeline : pipelines_) {
    for (auto& stream : pipeline.running) {
      stream->setError();
    }
    for (auto& stream : pipeline.arrived) {
      stream->setError();
    }
    for (auto& stream : pipeline.finished) {
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
