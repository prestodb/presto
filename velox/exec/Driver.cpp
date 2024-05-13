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

#include "Driver.h"
#include <folly/ScopeGuard.h>
#include <folly/executors/QueuedImmediateExecutor.h>
#include <folly/executors/thread_factory/InitThreadFactory.h>
#include <gflags/gflags.h>
#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/process/TraceContext.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {
namespace {

// Ensures that the thread is removed from its Task's thread count on exit.
class CancelGuard {
 public:
  CancelGuard(
      Task* task,
      ThreadState* state,
      std::function<void(StopReason)> onTerminate)
      : task_(task), state_(state), onTerminate_(std::move(onTerminate)) {}

  void notThrown() {
    isThrow_ = false;
  }

  ~CancelGuard() {
    bool onTerminateCalled{false};
    if (isThrow_) {
      // Runtime error. Driver is on thread, hence safe.
      state_->isTerminated = true;
      onTerminate_(StopReason::kNone);
      onTerminateCalled = true;
    }
    task_->leave(*state_, onTerminateCalled ? nullptr : onTerminate_);
  }

 private:
  Task* const task_;
  ThreadState* const state_;
  const std::function<void(StopReason reason)> onTerminate_;

  bool isThrow_{true};
};

// Checks if output channel is produced using identity projection and returns
// input channel if so.
std::optional<column_index_t> getIdentityProjection(
    const std::vector<IdentityProjection>& projections,
    column_index_t outputChannel) {
  for (const auto& projection : projections) {
    if (projection.outputChannel == outputChannel) {
      return projection.inputChannel;
    }
  }
  return std::nullopt;
}

void validateOperatorResult(RowVectorPtr& result, Operator& op) {
  try {
    result->validate({});
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Output validation failed for [operator: {}, plan node ID: {}]: {}",
        op.operatorType(),
        op.planNodeId(),
        e.what());
  }
}

thread_local DriverThreadContext* driverThreadCtx{nullptr};

void recordSilentThrows(Operator& op) {
  auto numThrow = threadNumVeloxThrow();
  if (numThrow > 0) {
    op.stats().wlock()->addRuntimeStat(
        "numSilentThrow", RuntimeCounter(numThrow));
  }
}

// Check whether the future is set by isBlocked method.
inline void checkIsBlockFutureValid(
    const Operator* op,
    const ContinueFuture& future) {
  VELOX_CHECK(
      future.valid(),
      "The operator {} is blocked but blocking future is not "
      "set by isBlocked method.",
      op->operatorType());
}

// Used to generate context for exceptions that are thrown while executing an
// operator. Eg output: 'Operator: FilterProject(1) PlanNodeId: 1 TaskId:
// test_cursor 1 PipelineId: 0 DriverId: 0 OperatorAddress: 0x61a000003c80'
std::string addContextOnException(
    VeloxException::Type exceptionType,
    void* arg) {
  if (exceptionType != VeloxException::Type::kSystem) {
    return "";
  }
  auto* op = static_cast<Operator*>(arg);
  return fmt::format("Operator: {}", op->toString());
}

} // namespace

DriverCtx::DriverCtx(
    std::shared_ptr<Task> _task,
    int _driverId,
    int _pipelineId,
    uint32_t _splitGroupId,
    uint32_t _partitionId)
    : driverId(_driverId),
      pipelineId(_pipelineId),
      splitGroupId(_splitGroupId),
      partitionId(_partitionId),
      task(_task),
      threadDebugInfo({task->queryCtx()->queryId(), task->taskId(), nullptr}) {}

const core::QueryConfig& DriverCtx::queryConfig() const {
  return task->queryCtx()->queryConfig();
}

velox::memory::MemoryPool* DriverCtx::addOperatorPool(
    const core::PlanNodeId& planNodeId,
    const std::string& operatorType) {
  return task->addOperatorPool(
      planNodeId, splitGroupId, pipelineId, driverId, operatorType);
}

std::optional<common::SpillConfig> DriverCtx::makeSpillConfig(
    int32_t operatorId) const {
  const auto& queryConfig = task->queryCtx()->queryConfig();
  if (!queryConfig.spillEnabled()) {
    return std::nullopt;
  }
  if (task->spillDirectory().empty()) {
    return std::nullopt;
  }
  common::GetSpillDirectoryPathCB getSpillDirPathCb =
      [this]() -> std::string_view {
    return task->getOrCreateSpillDirectory();
  };
  const auto& spillFilePrefix =
      fmt::format("{}_{}_{}", pipelineId, driverId, operatorId);
  common::UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb =
      [this](uint64_t bytes) {
        task->queryCtx()->updateSpilledBytesAndCheckLimit(bytes);
      };
  return common::SpillConfig(
      std::move(getSpillDirPathCb),
      std::move(updateAndCheckSpillLimitCb),
      spillFilePrefix,
      queryConfig.maxSpillFileSize(),
      queryConfig.spillWriteBufferSize(),
      task->queryCtx()->spillExecutor(),
      queryConfig.minSpillableReservationPct(),
      queryConfig.spillableReservationGrowthPct(),
      queryConfig.spillStartPartitionBit(),
      queryConfig.spillNumPartitionBits(),
      queryConfig.maxSpillLevel(),
      queryConfig.maxSpillRunRows(),
      queryConfig.writerFlushThresholdBytes(),
      queryConfig.spillCompressionKind(),
      queryConfig.spillFileCreateConfig());
}

std::atomic_uint64_t BlockingState::numBlockedDrivers_{0};

BlockingState::BlockingState(
    std::shared_ptr<Driver> driver,
    ContinueFuture&& future,
    Operator* op,
    BlockingReason reason)
    : driver_(std::move(driver)),
      future_(std::move(future)),
      operator_(op),
      reason_(reason),
      sinceMicros_(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count()) {
  // Set before leaving the thread.
  driver_->state().hasBlockingFuture = true;
  numBlockedDrivers_++;
}

// static
void BlockingState::setResume(std::shared_ptr<BlockingState> state) {
  VELOX_CHECK(!state->driver_->isOnThread());
  auto& exec = folly::QueuedImmediateExecutor::instance();
  std::move(state->future_)
      .via(&exec)
      .thenValue([state](auto&& /* unused */) {
        auto& driver = state->driver_;
        auto& task = driver->task();

        std::lock_guard<std::timed_mutex> l(task->mutex());
        if (!driver->state().isTerminated) {
          state->operator_->recordBlockingTime(
              state->sinceMicros_, state->reason_);
        }
        VELOX_CHECK(!driver->state().suspended());
        VELOX_CHECK(driver->state().hasBlockingFuture);
        driver->state().hasBlockingFuture = false;
        if (task->pauseRequested()) {
          // The thread will be enqueued at resume.
          return;
        }
        Driver::enqueue(state->driver_);
      })
      .thenError(
          folly::tag_t<std::exception>{}, [state](std::exception const& e) {
            try {
              VELOX_FAIL(
                  "A ContinueFuture for task {} was realized with error: {}",
                  state->driver_->task()->taskId(),
                  e.what())
            } catch (const VeloxException&) {
              state->driver_->task()->setError(std::current_exception());
            }
          });
}

std::string stopReasonString(StopReason reason) {
  switch (reason) {
    case StopReason::kNone:
      return "NONE";
    case StopReason::kBlock:
      return "BLOCK";
    case StopReason::kTerminate:
      return "TERMINATE";
    case StopReason::kAlreadyTerminated:
      return "ALREADY_TERMINATED";
    case StopReason::kYield:
      return "YIELD";
    case StopReason::kPause:
      return "PAUSE";
    case StopReason::kAlreadyOnThread:
      return "ALREADY_ON_THREAD";
    case StopReason::kAtEnd:
      return "AT_END";
    default:
      return fmt::format("UNKNOWN_REASON {}", static_cast<int>(reason));
  }
}

std::ostream& operator<<(std::ostream& out, const StopReason& reason) {
  return out << stopReasonString(reason);
}

// static
void Driver::enqueue(std::shared_ptr<Driver> driver) {
  process::ScopedThreadDebugInfo scopedInfo(
      driver->driverCtx()->threadDebugInfo);
  // This is expected to be called inside the Driver's Tasks's mutex.
  driver->enqueueInternal();
  if (driver->closed_) {
    return;
  }
  driver->task()->queryCtx()->executor()->add(
      [driver]() { Driver::run(driver); });
}

void Driver::init(
    std::unique_ptr<DriverCtx> ctx,
    std::vector<std::unique_ptr<Operator>> operators) {
  VELOX_CHECK_NULL(ctx_);
  ctx_ = std::move(ctx);
  cpuSliceMs_ = task()->driverCpuTimeSliceLimitMs();
  VELOX_CHECK(operators_.empty());
  operators_ = std::move(operators);
  curOperatorId_ = operators_.size() - 1;
  trackOperatorCpuUsage_ = ctx_->queryConfig().operatorTrackCpuUsage();
}

void Driver::initializeOperators() {
  if (operatorsInitialized_) {
    return;
  }
  operatorsInitialized_ = true;
  for (auto& op : operators_) {
    op->initialize();
  }
}

void Driver::pushdownFilters(int operatorIndex) {
  auto* op = operators_[operatorIndex].get();
  const auto& filters = op->getDynamicFilters();
  if (filters.empty()) {
    return;
  }
  const auto& planNodeId = op->planNodeId();

  op->addRuntimeStat("dynamicFiltersProduced", RuntimeCounter(filters.size()));

  // Walk operator list upstream and find a place to install the filters.
  for (const auto& entry : filters) {
    auto channel = entry.first;
    for (auto i = operatorIndex - 1; i >= 0; --i) {
      auto prevOp = operators_[i].get();

      if (i == 0) {
        // Source operator.
        VELOX_CHECK(
            prevOp->canAddDynamicFilter(),
            "Cannot push down dynamic filters produced by {}",
            op->toString());
        prevOp->addDynamicFilter(planNodeId, channel, entry.second);
        prevOp->addRuntimeStat("dynamicFiltersAccepted", RuntimeCounter(1));
        break;
      }

      const auto& identityProjections = prevOp->identityProjections();
      const auto inputChannel =
          getIdentityProjection(identityProjections, channel);
      if (!inputChannel.has_value()) {
        // Filter channel is not an identity projection.
        VELOX_CHECK(
            prevOp->canAddDynamicFilter(),
            "Cannot push down dynamic filters produced by {}",
            op->toString());
        prevOp->addDynamicFilter(planNodeId, channel, entry.second);
        prevOp->addRuntimeStat("dynamicFiltersAccepted", RuntimeCounter(1));
        break;
      }

      // Continue walking upstream.
      channel = inputChannel.value();
    }
  }

  op->clearDynamicFilters();
}

RowVectorPtr Driver::next(std::shared_ptr<BlockingState>& blockingState) {
  enqueueInternal();
  auto self = shared_from_this();
  facebook::velox::process::ScopedThreadDebugInfo scopedInfo(
      self->driverCtx()->threadDebugInfo);
  ScopedDriverThreadContext scopedDriverThreadContext(*self->driverCtx());
  RowVectorPtr result;
  auto stop = runInternal(self, blockingState, result);

  // We get kBlock if 'result' was produced; kAtEnd if pipeline has finished
  // processing and no more results will be produced; kAlreadyTerminated on
  // error.
  VELOX_CHECK(
      stop == StopReason::kBlock || stop == StopReason::kAtEnd ||
      stop == StopReason::kAlreadyTerminated || stop == StopReason::kTerminate);

  return result;
}

void Driver::enqueueInternal() {
  VELOX_CHECK(!state_.isEnqueued);
  state_.isEnqueued = true;
  // When enqueuing, starting timing the queue time.
  queueTimeStartMicros_ = getCurrentTimeMicro();
}

// Call an Oprator method. record silenced throws, but not a query
// terminating throw. Annotate exceptions with Operator info.
#define CALL_OPERATOR(call, operatorPtr, operatorId, operatorMethod)       \
  try {                                                                    \
    Operator::NonReclaimableSectionGuard nonReclaimableGuard(operatorPtr); \
    RuntimeStatWriterScopeGuard statsWriterGuard(operatorPtr);             \
    threadNumVeloxThrow() = 0;                                             \
    opCallStatus_.start(operatorId, operatorMethod);                       \
    ExceptionContextSetter exceptionContext(                               \
        {addContextOnException, operatorPtr, true});                       \
    auto stopGuard = folly::makeGuard([&]() { opCallStatus_.stop(); });    \
    call;                                                                  \
    recordSilentThrows(*operatorPtr);                                      \
  } catch (const VeloxException&) {                                        \
    throw;                                                                 \
  } catch (const std::exception& e) {                                      \
    VELOX_FAIL(                                                            \
        "Operator::{} failed for [operator: {}, plan node ID: {}]: {}",    \
        operatorMethod,                                                    \
        operatorPtr->operatorType(),                                       \
        operatorPtr->planNodeId(),                                         \
        e.what());                                                         \
  }

void OpCallStatus::start(int32_t operatorId, const char* operatorMethod) {
  timeStartMs = getCurrentTimeMs();
  opId = operatorId;
  method = operatorMethod;
}

void OpCallStatus::stop() {
  timeStartMs = 0;
}

size_t OpCallStatusRaw::callDuration() const {
  return empty() ? 0 : (getCurrentTimeMs() - timeStartMs);
}

/*static*/ std::string OpCallStatusRaw::formatCall(
    Operator* op,
    const char* operatorMethod) {
  return op
      ? fmt::format(
            "{}.{}::{}", op->operatorType(), op->planNodeId(), operatorMethod)
      : fmt::format("null::{}", operatorMethod);
}

CpuWallTiming Driver::processLazyTiming(
    Operator& op,
    const CpuWallTiming& timing) {
  if (&op == operators_[0].get()) {
    return timing;
  }
  auto lockStats = op.stats().wlock();
  uint64_t cpuDelta = 0;
  uint64_t wallDelta = 0;
  auto it = lockStats->runtimeStats.find(LazyVector::kCpuNanos);
  if (it != lockStats->runtimeStats.end()) {
    auto cpu = it->second.sum;
    cpuDelta = cpu >= lockStats->lastLazyCpuNanos
        ? cpu - lockStats->lastLazyCpuNanos
        : 0;
    if (cpuDelta == 0) {
      // return early if no change. Checking one counter is enough. If
      // this did not change and the other did, the change would be
      // insignificant and tracking would catch up when this counter next
      // changed.
      return timing;
    }
    lockStats->lastLazyCpuNanos = cpu;
  } else {
    // Return early if no lazy activity. Lazy CPU and wall times are recorded
    // together, checking one is enough.
    return timing;
  }
  it = lockStats->runtimeStats.find(LazyVector::kWallNanos);
  if (it != lockStats->runtimeStats.end()) {
    auto wall = it->second.sum;
    wallDelta = wall >= lockStats->lastLazyWallNanos
        ? wall - lockStats->lastLazyWallNanos
        : 0;
    if (wallDelta > 0) {
      lockStats->lastLazyWallNanos = wall;
    }
  }
  operators_[0]->stats().wlock()->getOutputTiming.add(
      CpuWallTiming{1, wallDelta, cpuDelta});
  return CpuWallTiming{
      1,
      timing.wallNanos >= wallDelta ? timing.wallNanos - wallDelta : 0,
      timing.cpuNanos >= cpuDelta ? timing.cpuNanos - cpuDelta : 0};
}

bool Driver::shouldYield() const {
  if (cpuSliceMs_ == 0) {
    return false;
  }
  return execTimeMs() >= cpuSliceMs_;
}

StopReason Driver::runInternal(
    std::shared_ptr<Driver>& self,
    std::shared_ptr<BlockingState>& blockingState,
    RowVectorPtr& result) {
  const auto now = getCurrentTimeMicro();
  const auto queuedTime = (now - queueTimeStartMicros_) * 1'000;
  // Update the next operator's queueTime.
  StopReason stop =
      closed_ ? StopReason::kTerminate : task()->enter(state_, now);
  if (stop != StopReason::kNone) {
    if (stop == StopReason::kTerminate) {
      // ctx_ still has a reference to the Task. 'this' is not on
      // thread from the Task's viewpoint, hence no need to call
      // close().
      ctx_->task->setError(std::make_exception_ptr(VeloxRuntimeError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          "",
          "Cancelled",
          error_source::kErrorSourceRuntime,
          error_code::kInvalidState,
          false)));
    }
    return stop;
  }

  // Update the queued time after entering the Task to ensure the stats have not
  // been deleted.
  if (curOperatorId_ < operators_.size()) {
    operators_[curOperatorId_]->addRuntimeStat(
        "queuedWallNanos",
        RuntimeCounter(queuedTime, RuntimeCounter::Unit::kNanos));
  }

  CancelGuard guard(task().get(), &state_, [&](StopReason reason) {
    // This is run on error or cancel exit.
    if (reason == StopReason::kTerminate) {
      task()->setError(std::make_exception_ptr(VeloxRuntimeError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          "",
          "Cancelled",
          error_source::kErrorSourceRuntime,
          error_code::kInvalidState,
          false)));
    }
    close();
  });

  try {
    // Invoked to initialize the operators once before driver starts execution.
    initializeOperators();

    TestValue::adjust("facebook::velox::exec::Driver::runInternal", this);

    const int32_t numOperators = operators_.size();
    ContinueFuture future = ContinueFuture::makeEmpty();

    for (;;) {
      for (int32_t i = numOperators - 1; i >= 0; --i) {
        stop = task()->shouldStop();
        if (stop != StopReason::kNone) {
          guard.notThrown();
          return stop;
        }

        auto* op = operators_[i].get();

        if (FOLLY_UNLIKELY(shouldYield())) {
          recordYieldCount();
          guard.notThrown();
          return StopReason::kYield;
        }

        // In case we are blocked, this index will point to the operator, whose
        // queuedTime we should update.
        curOperatorId_ = i;

        CALL_OPERATOR(
            blockingReason_ = op->isBlocked(&future),
            op,
            curOperatorId_,
            kOpMethodIsBlocked);
        if (blockingReason_ != BlockingReason::kNotBlocked) {
          blockedOperatorId_ = curOperatorId_;
          checkIsBlockFutureValid(op, future);
          blockingState = std::make_shared<BlockingState>(
              self, std::move(future), op, blockingReason_);
          guard.notThrown();
          return StopReason::kBlock;
        }

        if (i < numOperators - 1) {
          Operator* nextOp = operators_[i + 1].get();

          CALL_OPERATOR(
              blockingReason_ = nextOp->isBlocked(&future),
              nextOp,
              curOperatorId_ + 1,
              kOpMethodIsBlocked);
          if (blockingReason_ != BlockingReason::kNotBlocked) {
            blockedOperatorId_ = curOperatorId_ + 1;
            checkIsBlockFutureValid(nextOp, future);
            blockingState = std::make_shared<BlockingState>(
                self, std::move(future), nextOp, blockingReason_);
            guard.notThrown();
            return StopReason::kBlock;
          }

          bool needsInput;
          CALL_OPERATOR(
              needsInput = nextOp->needsInput(),
              nextOp,
              curOperatorId_ + 1,
              kOpMethodNeedsInput);
          if (needsInput) {
            uint64_t resultBytes = 0;
            RowVectorPtr intermediateResult;
            {
              auto timer = createDeltaCpuWallTimer(
                  [op, this](const CpuWallTiming& deltaTiming) {
                    processLazyTiming(*op, deltaTiming);
                    op->stats().wlock()->getOutputTiming.add(deltaTiming);
                  });
              TestValue::adjust(
                  "facebook::velox::exec::Driver::runInternal::getOutput", op);
              CALL_OPERATOR(
                  intermediateResult = op->getOutput(),
                  op,
                  curOperatorId_,
                  kOpMethodGetOutput);
              if (intermediateResult) {
                VELOX_CHECK(
                    intermediateResult->size() > 0,
                    "Operator::getOutput() must return nullptr or "
                    "a non-empty vector: {}",
                    op->operatorType());
                if (ctx_->queryConfig().validateOutputFromOperators()) {
                  validateOperatorResult(intermediateResult, *op);
                }
                resultBytes = intermediateResult->estimateFlatSize();
                {
                  auto lockedStats = op->stats().wlock();
                  lockedStats->addOutputVector(
                      resultBytes, intermediateResult->size());
                }
              }
            }
            pushdownFilters(i);
            if (intermediateResult) {
              auto timer = createDeltaCpuWallTimer(
                  [nextOp, this](const CpuWallTiming& timing) {
                    auto selfDelta = processLazyTiming(*nextOp, timing);
                    nextOp->stats().wlock()->addInputTiming.add(selfDelta);
                  });
              {
                auto lockedStats = nextOp->stats().wlock();
                lockedStats->addInputVector(
                    resultBytes, intermediateResult->size());
              }
              TestValue::adjust(
                  "facebook::velox::exec::Driver::runInternal::addInput",
                  nextOp);

              CALL_OPERATOR(
                  nextOp->addInput(intermediateResult),
                  nextOp,
                  curOperatorId_ + 1,
                  kOpMethodAddInput);

              // The next iteration will see if operators_[i + 1] has
              // output now that it got input.
              i += 2;
              continue;
            } else {
              stop = task()->shouldStop();
              if (stop != StopReason::kNone) {
                guard.notThrown();
                return stop;
              }
              // The op is at end. If this is finishing, propagate the
              // finish to the next op. The op could have run out
              // because it is blocked. If the op is the source and it
              // is not blocked and empty, this is finished. If this is
              // not the source, just try to get output from the one
              // before.
              CALL_OPERATOR(
                  blockingReason_ = op->isBlocked(&future),
                  op,
                  curOperatorId_,
                  kOpMethodIsBlocked);
              if (blockingReason_ != BlockingReason::kNotBlocked) {
                blockedOperatorId_ = curOperatorId_;
                checkIsBlockFutureValid(op, future);
                blockingState = std::make_shared<BlockingState>(
                    self, std::move(future), op, blockingReason_);
                guard.notThrown();
                return StopReason::kBlock;
              }
              bool finished{false};
              CALL_OPERATOR(
                  finished = op->isFinished(),
                  op,
                  curOperatorId_,
                  kOpMethodIsFinished);
              if (finished) {
                auto timer = createDeltaCpuWallTimer(
                    [op, this](const CpuWallTiming& timing) {
                      processLazyTiming(*op, timing);
                      op->stats().wlock()->finishTiming.add(timing);
                    });
                TestValue::adjust(
                    "facebook::velox::exec::Driver::runInternal::noMoreInput",
                    nextOp);
                CALL_OPERATOR(
                    nextOp->noMoreInput(),
                    nextOp,
                    curOperatorId_ + 1,
                    kOpMethodNoMoreInput);
                break;
              }
            }
          }
        } else {
          // A sink (last) operator, after getting unblocked, gets
          // control here, so it can advance. If it is again blocked,
          // this will be detected when trying to add input, and we
          // will come back here after this is again on thread.
          {
            auto timer = createDeltaCpuWallTimer(
                [op, this](const CpuWallTiming& timing) {
                  auto selfDelta = processLazyTiming(*op, timing);
                  op->stats().wlock()->getOutputTiming.add(selfDelta);
                });
            CALL_OPERATOR(
                result = op->getOutput(),
                op,
                curOperatorId_,
                kOpMethodGetOutput);
            if (result) {
              VELOX_CHECK(
                  result->size() > 0,
                  "Operator::getOutput() must return nullptr or "
                  "a non-empty vector: {}",
                  op->operatorType());
              if (ctx_->queryConfig().validateOutputFromOperators()) {
                validateOperatorResult(result, *op);
              }
              {
                auto lockedStats = op->stats().wlock();
                lockedStats->addOutputVector(
                    result->estimateFlatSize(), result->size());
              }

              // This code path is used only in single-threaded execution.
              blockingReason_ = BlockingReason::kWaitForConsumer;
              guard.notThrown();
              return StopReason::kBlock;
            }
          }
          bool finished{false};
          CALL_OPERATOR(
              finished = op->isFinished(),
              op,
              curOperatorId_,
              kOpMethodIsFinished);
          if (finished) {
            guard.notThrown();
            close();
            return StopReason::kAtEnd;
          }
          pushdownFilters(i);
          continue;
        }
      }
    }
  } catch (velox::VeloxException&) {
    task()->setError(std::current_exception());
    // The CancelPoolGuard will close 'self' and remove from Task.
    return StopReason::kAlreadyTerminated;
  } catch (std::exception&) {
    task()->setError(std::current_exception());
    // The CancelGuard will close 'self' and remove from Task.
    return StopReason::kAlreadyTerminated;
  }
}

#undef CALL_OPERATOR

// static
std::atomic_uint64_t& Driver::yieldCount() {
  static std::atomic_uint64_t count{0};
  return count;
}

// static
void Driver::recordYieldCount() {
  ++yieldCount();
  RECORD_METRIC_VALUE(kMetricDriverYieldCount);
}

// static
void Driver::run(std::shared_ptr<Driver> self) {
  process::TraceContext trace("Driver::run");
  facebook::velox::process::ScopedThreadDebugInfo scopedInfo(
      self->driverCtx()->threadDebugInfo);
  ScopedDriverThreadContext scopedDriverThreadContext(*self->driverCtx());
  std::shared_ptr<BlockingState> blockingState;
  RowVectorPtr nullResult;
  auto reason = self->runInternal(self, blockingState, nullResult);

  // When Driver runs on an executor, the last operator (sink) must not produce
  // any results.
  VELOX_CHECK_NULL(
      nullResult,
      "The last operator (sink) must not produce any results. "
      "Results need to be consumed by either a callback or another operator. ")

  // There can be a race between Task terminating and the Driver being on the
  // thread and exiting the runInternal() in a blocked state. If this happens
  // the Driver won't be closed, so we need to check the Task here and exit w/o
  // going into the resume mode waiting on a promise.
  if (reason == StopReason::kBlock &&
      self->task()->shouldStop() == StopReason::kTerminate) {
    return;
  }

  switch (reason) {
    case StopReason::kBlock:
      // Set the resume action outside the Task so that, if the
      // future is already realized we do not have a second thread
      // entering the same Driver.
      BlockingState::setResume(blockingState);
      return;

    case StopReason::kYield:
      // Go to the end of the queue.
      enqueue(self);
      return;

    case StopReason::kPause:
    case StopReason::kTerminate:
    case StopReason::kAlreadyTerminated:
    case StopReason::kAtEnd:
      return;
    default:
      VELOX_FAIL("Unhandled stop reason");
  }
}

void Driver::initializeOperatorStats(std::vector<OperatorStats>& stats) {
  stats.resize(operators_.size(), OperatorStats(0, 0, "", ""));
  // Initialize the place in stats given by the operatorId. Use the
  // operatorId instead of i as the index to document the usage. The
  // operators are sequentially numbered but they could be reordered
  // in the pipeline later, so the ordinal position of the Operator is
  // not always the index into the stats.
  for (auto& op : operators_) {
    auto id = op->operatorId();
    VELOX_DCHECK_LT(id, stats.size());
    stats[id] = op->stats(false);
  }
}

void Driver::closeOperators() {
  // Close operators.
  for (auto& op : operators_) {
    op->close();
  }

  // Add operator stats to the task.
  for (auto& op : operators_) {
    auto stats = op->stats(true);
    stats.numDrivers = 1;
    task()->addOperatorStats(stats);
  }
}

void Driver::updateStats() {
  DriverStats stats;
  if (state_.totalPauseTimeMs > 0) {
    stats.runtimeStats[DriverStats::kTotalPauseTime] = RuntimeMetric(
        1'000'000 * state_.totalPauseTimeMs, RuntimeCounter::Unit::kNanos);
  }
  if (state_.totalOffThreadTimeMs > 0) {
    stats.runtimeStats[DriverStats::kTotalOffThreadTime] = RuntimeMetric(
        1'000'000 * state_.totalOffThreadTimeMs, RuntimeCounter::Unit::kNanos);
  }
  task()->addDriverStats(ctx_->pipelineId, std::move(stats));
}

void Driver::close() {
  if (closed_) {
    // Already closed.
    return;
  }
  if (!isOnThread() && !isTerminated()) {
    LOG(FATAL) << "Driver::close is only allowed from the Driver's thread";
  }
  closeOperators();
  updateStats();
  closed_ = true;
  Task::removeDriver(ctx_->task, this);
}

void Driver::closeByTask() {
  VELOX_CHECK(isOnThread());
  VELOX_CHECK(isTerminated());
  closeOperators();
  updateStats();
  closed_ = true;
}

bool Driver::mayPushdownAggregation(Operator* aggregation) const {
  for (auto i = 1; i < operators_.size(); ++i) {
    auto op = operators_[i].get();
    if (aggregation == op) {
      return true;
    }
    if (!op->isFilter() || !op->preservesOrder()) {
      return false;
    }
  }
  VELOX_FAIL(
      "Aggregation operator not found in its Driver: {}",
      aggregation->toString());
}

std::unordered_set<column_index_t> Driver::canPushdownFilters(
    const Operator* filterSource,
    const std::vector<column_index_t>& channels) const {
  int filterSourceIndex = -1;
  for (auto i = 0; i < operators_.size(); ++i) {
    auto op = operators_[i].get();
    if (filterSource == op) {
      filterSourceIndex = i;
      break;
    }
  }
  VELOX_CHECK_GE(
      filterSourceIndex,
      0,
      "Operator not found in its Driver: {}",
      filterSource->toString());

  std::unordered_set<column_index_t> supportedChannels;
  for (auto i = 0; i < channels.size(); ++i) {
    auto channel = channels[i];
    for (auto j = filterSourceIndex - 1; j >= 0; --j) {
      auto* prevOp = operators_[j].get();

      if (j == 0) {
        // Source operator.
        if (prevOp->canAddDynamicFilter()) {
          supportedChannels.emplace(channels[i]);
        }
        break;
      }

      const auto& identityProjections = prevOp->identityProjections();
      const auto inputChannel =
          getIdentityProjection(identityProjections, channel);
      if (!inputChannel.has_value()) {
        // Filter channel is not an identity projection.
        if (prevOp->canAddDynamicFilter()) {
          supportedChannels.emplace(channels[i]);
        }
        break;
      }

      // Continue walking upstream.
      channel = inputChannel.value();
    }
  }

  return supportedChannels;
}

Operator* Driver::findOperator(std::string_view planNodeId) const {
  for (auto& op : operators_) {
    if (op->planNodeId() == planNodeId) {
      return op.get();
    }
  }
  return nullptr;
}

Operator* Driver::findOperator(int32_t operatorId) const {
  VELOX_CHECK_LT(operatorId, operators_.size());
  return operators_[operatorId].get();
}

Operator* Driver::findOperatorNoThrow(int32_t operatorId) const {
  return (operatorId < operators_.size()) ? operators_[operatorId].get()
                                          : nullptr;
}

std::vector<Operator*> Driver::operators() const {
  std::vector<Operator*> operators;
  operators.reserve(operators_.size());
  for (auto& op : operators_) {
    operators.push_back(op.get());
  }
  return operators;
}

std::string Driver::toString() const {
  std::stringstream out;
  out << "{Driver." << driverCtx()->pipelineId << "." << driverCtx()->driverId
      << ": ";
  if (state_.isTerminated) {
    out << "terminated, ";
  }
  if (state_.hasBlockingFuture) {
    std::string blockedOp = (blockedOperatorId_ < operators_.size())
        ? operators_[blockedOperatorId_]->toString()
        : "<unknown op>";
    out << "blocked (" << blockingReasonToString(blockingReason_) << " "
        << blockedOp << "), ";
  } else if (state_.isEnqueued) {
    out << "enqueued ";
  } else if (state_.isOnThread()) {
    out << "running ";
  } else {
    out << "unknown state";
  }

  out << "{Operators: ";
  for (auto& op : operators_) {
    out << op->toString() << ", ";
  }
  out << "}";
  const auto ocs = opCallStatus();
  if (!ocs.empty()) {
    out << "{OpCallStatus: executing "
        << ocs.formatCall(findOperatorNoThrow(ocs.opId), ocs.method) << " for "
        << ocs.callDuration() << "ms}";
  }
  out << "}";
  return out.str();
}

folly::dynamic Driver::toJson() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["blockingReason"] = blockingReasonToString(blockingReason_);
  obj["state"] = state_.toJson();
  obj["closed"] = closed_.load();
  obj["queueTimeStartMicros"] = queueTimeStartMicros_;
  const auto ocs = opCallStatus();
  if (!ocs.empty()) {
    obj["curOpCall"] =
        ocs.formatCall(findOperatorNoThrow(ocs.opId), ocs.method);
    obj["curOpCallDuration"] = ocs.callDuration();
  }

  folly::dynamic operatorsObj = folly::dynamic::object;
  int index = 0;
  for (auto& op : operators_) {
    operatorsObj[std::to_string(index++)] = op->toJson();
  }
  obj["operators"] = operatorsObj;

  return obj;
}

SuspendedSection::SuspendedSection(Driver* driver) : driver_(driver) {
  if (driver->task()->enterSuspended(driver->state()) != StopReason::kNone) {
    VELOX_FAIL("Terminate detected when entering suspended section");
  }
}

SuspendedSection::~SuspendedSection() {
  if (driver_->task()->leaveSuspended(driver_->state()) != StopReason::kNone) {
    LOG(WARNING)
        << "Terminate detected when leaving suspended section for driver "
        << driver_->driverCtx()->driverId << " from task "
        << driver_->task()->taskId();
  }
}

std::string Driver::label() const {
  return fmt::format("<Driver {}:{}>", task()->taskId(), ctx_->driverId);
}

std::string blockingReasonToString(BlockingReason reason) {
  switch (reason) {
    case BlockingReason::kNotBlocked:
      return "kNotBlocked";
    case BlockingReason::kWaitForConsumer:
      return "kWaitForConsumer";
    case BlockingReason::kWaitForSplit:
      return "kWaitForSplit";
    case BlockingReason::kWaitForProducer:
      return "kWaitForProducer";
    case BlockingReason::kWaitForJoinBuild:
      return "kWaitForJoinBuild";
    case BlockingReason::kWaitForJoinProbe:
      return "kWaitForJoinProbe";
    case BlockingReason::kWaitForMergeJoinRightSide:
      return "kWaitForMergeJoinRightSide";
    case BlockingReason::kWaitForMemory:
      return "kWaitForMemory";
    case BlockingReason::kWaitForConnector:
      return "kWaitForConnector";
    case BlockingReason::kWaitForSpill:
      return "kWaitForSpill";
    case BlockingReason::kYield:
      return "kYield";
  }
  VELOX_UNREACHABLE();
  return "";
}

DriverThreadContext* driverThreadContext() {
  return driverThreadCtx;
}

ScopedDriverThreadContext::ScopedDriverThreadContext(const DriverCtx& driverCtx)
    : savedDriverThreadCtx_(driverThreadCtx),
      currentDriverThreadCtx_{.driverCtx = driverCtx} {
  driverThreadCtx = &currentDriverThreadCtx_;
}

ScopedDriverThreadContext::~ScopedDriverThreadContext() {
  driverThreadCtx = savedDriverThreadCtx_;
}

} // namespace facebook::velox::exec
