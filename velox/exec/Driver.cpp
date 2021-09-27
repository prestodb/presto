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

#include <folly/executors/QueuedImmediateExecutor.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include <folly/executors/thread_factory/InitThreadFactory.h>
#include <gflags/gflags.h>
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"
#include "velox/expression/Expr.h"

DEFINE_int32(
    velox_num_query_threads,
    std::thread::hardware_concurrency(),
    "Process-wide number of query execution threads");

namespace facebook::velox::exec {
namespace {
// Basic implementation of the connector::ExpressionEvaluator interface.
class SimpleExpressionEvaluator : public connector::ExpressionEvaluator {
 public:
  explicit SimpleExpressionEvaluator(core::ExecCtx* execCtx)
      : execCtx_(execCtx) {}

  std::unique_ptr<exec::ExprSet> compile(
      const std::shared_ptr<const core::ITypedExpr>& expression)
      const override {
    auto expressions = {expression};
    return std::make_unique<exec::ExprSet>(std::move(expressions), execCtx_);
  }

  void evaluate(
      exec::ExprSet* exprSet,
      const SelectivityVector& rows,
      RowVectorPtr& input,
      VectorPtr* result) const override {
    exec::EvalCtx context(execCtx_, exprSet, input.get());

    std::vector<VectorPtr> results = {*result};
    exprSet->eval(0, 1, true, rows, &context, &results);

    *result = results[0];
  }

 private:
  core::ExecCtx* execCtx_;
};
} // namespace

DriverCtx::DriverCtx(
    std::shared_ptr<Task> _task,
    int _driverId,
    int _pipelineId,
    int32_t _numDrivers)
    : task(_task),
      execCtx(std::make_unique<core::ExecCtx>(
          task->addDriverPool(),
          task->queryCtx().get())),
      expressionEvaluator(
          std::make_unique<SimpleExpressionEvaluator>(execCtx.get())),
      driverId(_driverId),
      pipelineId(_pipelineId),
      numDrivers(_numDrivers) {}

velox::memory::MemoryPool* FOLLY_NONNULL DriverCtx::addOperatorPool() {
  return task->addOperatorPool(execCtx->pool());
}

std::unique_ptr<connector::ConnectorQueryCtx>
DriverCtx::createConnectorQueryCtx(const std::string& connectorId) const {
  return std::make_unique<connector::ConnectorQueryCtx>(
      execCtx->pool(),
      task->queryCtx()->getConnectorConfig(connectorId),
      expressionEvaluator.get());
}

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
}

// static
void BlockingState::setResume(
    std::shared_ptr<BlockingState> state,
    folly::Executor* executor) {
  VELOX_CHECK(!state->driver_->isOnThread());
  auto& exec = folly::QueuedImmediateExecutor::instance();
  std::move(state->future_)
      .via(&exec)
      .thenValue([state, executor](bool /* unused */) {
        state->operator_->recordBlockingTime(state->sinceMicros_);
        auto driver = state->driver_;
        {
          std::lock_guard<std::mutex> l(*driver->cancelPool()->mutex());
          VELOX_CHECK(!driver->state().isSuspended);
          VELOX_CHECK(driver->state().hasBlockingFuture);
          driver->state().hasBlockingFuture = false;
          if (driver->cancelPool()->pauseRequested()) {
            // The thread will be enqueued at resume.
            return;
          }
        }
        Driver::enqueue(state->driver_);
      })
      .thenError(
          folly::tag_t<std::exception>{}, [state](std::exception const& e) {
            LOG(ERROR)
                << "A ContinueFuture should not be realized with an error"
                << e.what();
            state->driver_->setError(std::make_exception_ptr(e));
          });
}

namespace {

// Ensures that the thread is removed from a CancelPool on exit.
class CancelPoolGuard {
 public:
  CancelPoolGuard(
      core::CancelPool* cancelPool,
      core::ThreadState* state,
      std::function<void(core::StopReason)> onTerminate)
      : cancelPool_(cancelPool), state_(state), onTerminate_(onTerminate) {}

  void notThrown() {
    isThrow_ = false;
  }

  ~CancelPoolGuard() {
    bool onTerminateCalled = false;
    if (isThrow_) {
      // Runtime error. Driver is on thread, hence safe.
      state_->isTerminated = true;
      onTerminate_(core::StopReason::kNone);
      onTerminateCalled = true;
    }
    auto reason = cancelPool_->leave(*state_);
    if (reason == core::StopReason::kTerminate) {
      // Terminate requested via cancelPool. The Driver is not on
      // thread but 'terminated_' is set, hence no other threads will
      // enter. onTerminateCalled will be true if both runtime error and
      // terminate requested via CancelPool.
      if (!onTerminateCalled) {
        onTerminate_(reason);
      }
    }
  }

 private:
  core::CancelPool* cancelPool_;
  core::ThreadState* state_;
  std::function<void(core::StopReason reason)> onTerminate_;
  bool isThrow_ = true;
};
} // namespace

static std::unique_ptr<folly::CPUThreadPoolExecutor>& getExecutor() {
  static std::unique_ptr<folly::CPUThreadPoolExecutor> executor;
  return executor;
}

// static
folly::CPUThreadPoolExecutor* Driver::executor(int32_t threads) {
  static std::mutex mutex;
  if (getExecutor().get()) {
    return getExecutor().get();
  }
  std::lock_guard<std::mutex> l(mutex);
  if (!getExecutor().get()) {
    auto numThreads = threads > 0 ? threads : FLAGS_velox_num_query_threads;
    auto queue = std::make_unique<
        folly::UnboundedBlockingQueue<folly::CPUThreadPoolExecutor::CPUTask>>();
    getExecutor().reset(new folly::CPUThreadPoolExecutor(
        numThreads,
        std::move(queue),
        std::make_shared<folly::NamedThreadFactory>("velox_query")));
  }
  return getExecutor().get();
}

// static
void Driver::testingJoinAndReinitializeExecutor(int32_t threads) {
  executor()->join();
  getExecutor().reset();
  executor(threads);
}

// static
void Driver::enqueue(
    std::shared_ptr<Driver> driver,
    folly::Executor* executor) {
  // This is expected to be called inside the Driver's CancelPool mutex.
  VELOX_CHECK(!driver->state().isEnqueued);
  driver->state().isEnqueued = true;
  auto currentExecutor = (executor ? executor : Driver::executor());
  currentExecutor->add(
      [driver, currentExecutor]() { Driver::run(driver, currentExecutor); });
}

Driver::Driver(
    std::unique_ptr<DriverCtx> ctx,
    std::vector<std::unique_ptr<Operator>>&& operators)
    : ctx_(std::move(ctx)),
      task_(ctx_->task),
      cancelPool_(ctx_->task->cancelPool()),
      operators_(std::move(operators)) {
  // Operators need access to their Driver for adaptation.
  ctx_->driver = this;
}

namespace {
/// Checks if output channel is produced using identity projection and returns
/// input channel if so.
std::optional<ChannelIndex> getIdentityProjection(
    const std::vector<IdentityProjection>& projections,
    ChannelIndex outputChannel) {
  for (const auto& projection : projections) {
    if (projection.outputChannel == outputChannel) {
      return projection.inputChannel;
    }
  }
  return std::nullopt;
}
} // namespace

void Driver::pushdownFilters(int operatorIndex) {
  auto op = operators_[operatorIndex].get();
  const auto& filters = op->getDynamicFilters();
  if (filters.empty()) {
    return;
  }

  op->stats().addRuntimeStat("dynamicFiltersProduced", filters.size());

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
        prevOp->addDynamicFilter(channel, entry.second);
        prevOp->stats().addRuntimeStat("dynamicFiltersAccepted", 1);
        break;
      }

      const auto& identityProjections = prevOp->identityProjections();
      auto inputChannel = getIdentityProjection(identityProjections, channel);
      if (!inputChannel.has_value()) {
        // Filter channel is not an identity projection.
        VELOX_CHECK(
            prevOp->canAddDynamicFilter(),
            "Cannot push down dynamic filters produced by {}",
            op->toString());
        prevOp->addDynamicFilter(channel, entry.second);
        prevOp->stats().addRuntimeStat("dynamicFiltersAccepted", 1);
        break;
      }

      // Continue walking upstream.
      channel = inputChannel.value();
    }
  }

  op->clearDynamicFilters();
}

core::StopReason Driver::runInternal(
    std::shared_ptr<Driver>& self,
    std::shared_ptr<BlockingState>* blockingState) {
  auto stop = cancelPool_->enter(state_);
  if (stop != core::StopReason::kNone) {
    if (stop == core::StopReason::kTerminate) {
      task_->setError(std::make_exception_ptr(VeloxRuntimeError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          "",
          "Cancelled",
          error_source::kErrorSourceRuntime,
          error_code::kInvalidState,
          false)));
      close();
    }
    return stop;
  }
  CancelPoolGuard guard(
      cancelPool_.get(), &state_, [this](core::StopReason reason) {
        auto task = task_.get();
        if (task && reason == core::StopReason::kTerminate) {
          task_->setError(std::make_exception_ptr(VeloxRuntimeError(
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
    int32_t numOperators = operators_.size();
    ContinueFuture future(false);

    for (;;) {
      for (int32_t i = numOperators - 1; i >= 0; --i) {
        stop = cancelPool_->shouldStop();
        if (stop != core::StopReason::kNone) {
          guard.notThrown();
          return stop;
        }

        auto op = operators_[i].get();
        blockingReason_ = op->isBlocked(&future);
        if (blockingReason_ != BlockingReason::kNotBlocked) {
          *blockingState = std::make_shared<BlockingState>(
              self, std::move(future), op, blockingReason_);
          guard.notThrown();
          return core::StopReason::kBlock;
        }
        Operator* nextOp = nullptr;
        if (i < operators_.size() - 1) {
          nextOp = operators_[i + 1].get();
          blockingReason_ = nextOp->isBlocked(&future);
          if (blockingReason_ != BlockingReason::kNotBlocked) {
            *blockingState = std::make_shared<BlockingState>(
                self, std::move(future), nextOp, blockingReason_);
            guard.notThrown();
            return core::StopReason::kBlock;
          }
          if (nextOp->needsInput()) {
            uint64_t resultBytes = 0;
            RowVectorPtr result;
            {
              OperationTimer timer(op->stats().getOutputTiming);
              result = op->getOutput();
              if (result) {
                op->stats().outputPositions += result->size();
                resultBytes = result->retainedSize();
                op->stats().outputBytes += resultBytes;
              }
            }
            pushdownFilters(i);
            if (result) {
              OperationTimer timer(nextOp->stats().addInputTiming);
              nextOp->stats().inputPositions += result->size();
              nextOp->stats().inputBytes += resultBytes;
              nextOp->addInput(result);
              // The next iteration will see if operators_[i + 1] has
              // output now that it got input.
              i += 2;
              continue;
            } else {
              stop = cancelPool_->shouldStop();
              if (stop != core::StopReason::kNone) {
                guard.notThrown();
                return stop;
              }
              // The op is at end. If this is finishing, propagate the
              // finish to the next op. The op could have run out
              // because it is blocked. If the op is the source and it
              // is not blocked and empty, this is finished. If this is
              // not the source, just try to get output from the one
              // before.
              blockingReason_ = op->isBlocked(&future);
              if (blockingReason_ != BlockingReason::kNotBlocked) {
                *blockingState = std::make_shared<BlockingState>(
                    self, std::move(future), op, blockingReason_);
                guard.notThrown();
                return core::StopReason::kBlock;
              }
              if (op->isFinishing()) {
                if (!nextOp->isFinishing()) {
                  OperationTimer timer(nextOp->stats().finishTiming);
                  nextOp->finish();
                  break;
                }
              }
            }
          }
        } else {
          // A sink (last) operator, after getting unblocked, gets
          // control here so it can advance. If it is again blocked,
          // this will be detected when trying to add input and we
          // will come back here after this is again on thread.
          {
            OperationTimer timer(op->stats().getOutputTiming);
            op->getOutput();
          }
          pushdownFilters(i);
          continue;
        }
        if (i == 0) {
          // The source is not blocked and not interrupted.
          if (op->isFinishing()) {
            guard.notThrown();
            close();
            return core::StopReason::kAtEnd;
          }
          OperationTimer timer(op->stats().finishTiming);
          op->finish();
          break;
        }
      }
    }
  } catch (velox::VeloxException& e) {
    task_->setError(std::current_exception());
    // The CancelPoolGuard will close 'self' and remove from task_.
    return core::StopReason::kAlreadyTerminated;
  } catch (std::exception& e) {
    task_->setError(std::current_exception());
    // The CancelPoolGuard will close 'self' and remove from task_.
    return core::StopReason::kAlreadyTerminated;
  }
}

// static
void Driver::run(std::shared_ptr<Driver> self, folly::Executor* executor) {
  std::shared_ptr<BlockingState> blockingState;
  auto reason = self->runInternal(self, &blockingState);
  switch (reason) {
    case core::StopReason::kBlock:
      // Set the resume action outside of the CancelPool so that, if the
      // future is already realized we do not have a second thread
      // entering the same Driver.
      BlockingState::setResume(blockingState, executor);
      return;

    case core::StopReason::kYield:
      // Go to the end of the queue.
      enqueue(self, executor);
      return;

    case core::StopReason::kPause:
    case core::StopReason::kTerminate:
    case core::StopReason::kAlreadyTerminated:
    case core::StopReason::kAtEnd:
      return;
    default:
      VELOX_CHECK(false, "Unhandled stop reason");
  }
}

void Driver::initializeOperatorStats(std::vector<OperatorStats>& stats) {
  stats.resize(operators_.size(), OperatorStats(0, 0, "", ""));
  // initialize the place in stats given by the operatorId. Use the
  // operatorId instead of i as the index to document the usage. The
  // operators are sequentially numbered but they could be reordered
  // in the pipeline later, so the ordinal position of the Operator is
  // not always the index into the stats.
  for (auto& op : operators_) {
    auto id = op->stats().operatorId;
    assert(id < stats.size());
    stats[id] = op->stats();
  }
}

void Driver::addStatsToTask() {
  for (auto& op : operators_) {
    auto& stats = op->stats();
    stats.memoryStats.update(op->pool()->getMemoryUsageTracker());
    task_->addOperatorStats(stats);
  }
}

void Driver::close() {
  if (!task_) {
    // Already closed.
    return;
  }
  addStatsToTask();
  for (auto& op : operators_) {
    op->close();
  }
  Task::removeDriver(task_, this);
  task_ = nullptr;
}

bool Driver::terminate() {
  auto stop = cancelPool_->enterForTerminate(state_);
  if (stop == core::StopReason::kTerminate) {
    close();
    return true;
  }
  return false;
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

std::unordered_set<ChannelIndex> Driver::canPushdownFilters(
    Operator* FOLLY_NONNULL filterSource,
    const std::vector<ChannelIndex>& channels) const {
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

  std::unordered_set<ChannelIndex> supportedChannels;
  for (auto i = 0; i < channels.size(); ++i) {
    auto channel = channels[i];
    for (auto j = filterSourceIndex - 1; j >= 0; --j) {
      auto prevOp = operators_[j].get();

      if (j == 0) {
        // Source operator.
        if (prevOp->canAddDynamicFilter()) {
          supportedChannels.emplace(channels[i]);
        }
        break;
      }

      const auto& identityProjections = prevOp->identityProjections();
      auto inputChannel = getIdentityProjection(identityProjections, channel);
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

Operator* FOLLY_NULLABLE
Driver::findOperator(std::string_view planNodeId) const {
  for (auto& op : operators_) {
    if (op->planNodeId() == planNodeId) {
      return op.get();
    }
  }
  return nullptr;
}

void Driver::setError(std::exception_ptr exception) {
  ctx_->task->setError(exception);
}

std::string Driver::toString() {
  std::stringstream out;
  out << "{Driver: ";
  if (state_.isOnThread()) {
    out << "running ";
  } else {
    out << "blocked " << static_cast<int>(blockingReason_) << " ";
  }
  for (auto& op : operators_) {
    out << op->toString() << " ";
  }
  out << "}";
  return out.str();
}
SuspendedSection::SuspendedSection(Driver* FOLLY_NONNULL driver)
    : driver_(driver) {
  if (driver->cancelPool()->enterSuspended(driver->state()) !=
      core::StopReason::kNone) {
    VELOX_FAIL("Terminate detected when entering suspended section");
  }
}

SuspendedSection::~SuspendedSection() {
  if (driver_->cancelPool()->leaveSuspended(driver_->state()) !=
      core::StopReason::kNone) {
    VELOX_FAIL("Terminate detected when leaving suspended section");
  }
}

std::string Driver::label() const {
  return fmt::format("<Driver {}:{}>", ctx_->task->taskId(), ctx_->driverId);
}

} // namespace facebook::velox::exec
