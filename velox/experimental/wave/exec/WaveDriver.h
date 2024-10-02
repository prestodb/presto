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

#pragma once

#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/wave/exec/WaveOperator.h"

DECLARE_int32(max_streams_per_driver);

namespace facebook::velox::wave {
enum class Advance { kBlocked, kResult, kFinished };

/// Synchronizes between WaveDrivers on different Drivers of a Task
/// pipeline. All threads inside WaveDriver::getOutput are the
/// coordinated set. One or more of these cn acquire the barrier in
/// exclusive mode. When all threads have arrived, the exclusive
/// requesting thread returns from acquire with true. After it calls
/// release(), the next exclusive thread, if any returns from its
/// acquire(). If no more exclusive requesting threads, all arrive()
/// calls return. arrive() returns immediately if no exclusive is
/// requested by any thread.
class WaveBarrier {
 public:
  WaveBarrier(std::string idString);

  ~WaveBarrier();

  /// Calling thread joins the set to being coordinated. If a thread holds or is
  /// waiting for exclusive, the caller blocks until the exclusive is over.
  void enter();

  /// Calling thread leaves the set being coordinated. Never blocks.
  void leave();

  /// Gets exclusive access. All other threads in the coordinated set are
  /// stopped wen this returns true. If this returns false, another thread
  /// already acquired and released the barrier for 'reason'.
  bool acquire(void* reason);

  /// Releases exclusive. The calling thread must have called acquire() first
  /// and received a true return value.
  void release();

  /// Calling thread arrives. If there is no acquire() pending,
  /// returns immediately. If there is an acquire() pending, blocks
  /// until all threads with acquire() have called
  /// release(). Acquires are continued one by one after all threads
  /// are either blocked in arrive() or acquire().
  void arrive();

  static std::shared_ptr<WaveBarrier>
  get(const std::string& taskId, int32_t driverId, int32_t operatorId);

 private:
  // Releases an exclusive waiting caller if non-exclusives are in
  // arrive or have left.
  void maybeReleaseAcquireLocked();

  // Serializes all non-static state.
  std::mutex mutex_;

  // Concatenation of task id and pipeline and driver id.
  std::string idString_;

  // Number of threads to coordinate.
  int32_t numJoined_{0};

  // Number of threads blocked in arrive().
  int32_t numInArrive_{0};
  std::vector<ContinuePromise> promises_;
  std::vector<folly::Promise<bool>> exclusivePromises_;
  std::vector<void*> exclusiveTokens_;
  void* exclusiveToken_{nullptr};

  static std::mutex barriersMutex_;
  static std::unordered_map<std::string, std::weak_ptr<WaveBarrier>> barriers_;
};

class WaveDriver : public exec::SourceOperator {
 public:
  WaveDriver(
      exec::DriverCtx* driverCtx,
      RowTypePtr outputType,
      core::PlanNodeId planNodeId,
      int32_t operatorId,
      std::unique_ptr<GpuArena> arena,
      std::vector<std::unique_ptr<WaveOperator>> waveOperators,
      std::vector<OperandId> resultOrder_,
      SubfieldMap subfields,
      std::vector<std::unique_ptr<AbstractOperand>> operands,
      std::vector<std::unique_ptr<AbstractState>> states,
      InstructionStatus instructionStatus);

  RowVectorPtr getOutput() override;

  exec::BlockingReason isBlocked(ContinueFuture* future) override {
    if (blockingFuture_.valid()) {
      *future = std::move(blockingFuture_);
      return blockingReason_;
    }
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

  void setReplaced(std::vector<std::unique_ptr<exec::Operator>> original) {
    cpuOperators_ = std::move(original);
  }

  GpuArena& arena() const {
    return *arena_;
  }

  const std::vector<std::unique_ptr<AbstractOperand>>& operands() {
    return operands_;
  }

  const SubfieldMap* subfields() {
    return &subfields_;
  }

  /// Returns the control block with thread block level sizes and statuses for
  /// input of  operator with id 'operator'. This is the control for the source
  /// or previous cardinality change.
  LaunchControl* inputControl(WaveStream& stream, int32_t operatorId);

  std::string toString() const override;

  void addDynamicFilter(
      const core::PlanNodeId& producer,
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override {
    pipelines_[0].operators[0]->addDynamicFilter(
        producer, outputChannel, filter);
  }

  exec::OperatorCtx* operatorCtx() const {
    return operatorCtx_.get();
  }

 private:
  struct Pipeline {
    // Wave operators replacing 'cpuOperators_' on GPU path.
    std::vector<std::unique_ptr<WaveOperator>> operators;

    // The set of currently pending kernel DAGs for this Pipeline.  If the
    // source operator can produce multiple consecutive batches before the batch
    // is executed to completion, multiple such batches can be on device
    // independently of each other. Limited by max_streams_per_driver.
    std::vector<std::unique_ptr<WaveStream>> running;

    std::vector<std::unique_ptr<WaveStream>> arrived;

    std::vector<std::unique_ptr<WaveStream>> continuable;

    std::vector<std::unique_ptr<WaveStream>> blocked;

    /// Streams ready to recycle. A stream's device side resources are usually
    /// reusable for a new batch from the source operator.
    std::vector<std::unique_ptr<WaveStream>> finished;

    /// True if status copy to host is needed after the last kernel. True if
    /// returns vectors to host or if can produce multiple batches of output for
    /// one input.
    bool needStatus{false};
    bool sinkFull{false};

    /// True if produces Batches in RowVectors.
    bool makesHostResult{false};
    bool canAdvance{false};
    bool noMoreInput{false};
  };

  // True if all output from 'stream' is fetched.
  bool streamAtEnd(WaveStream& stream);

  // Makes a RowVector from the result buffers of the last stage of executables
  // in 'stream'.
  RowVectorPtr makeResult(WaveStream& stream, const OperandSet& outputIds);
  Advance advance(int pipelineIdx);
  exec::BlockingReason processArrived(Pipeline& pipeline);

  // Waits for all streams of 'pipeline' to arrive. used for sink
  // full, where we need a barrier between updating and processing the
  // sink, like repartitioning or aggregation.
  void waitForArrival(Pipeline& pipeline);

  // Runs or continues operators starting at 'from'.
  void runOperators(
      Pipeline& pipeline,
      WaveStream& stream,
      int32_t from,
      int32_t numRows);

  // Carries out advance actions like rehashing tables or getting more memory.
  // Synchronizes with 'barrier_' if needed.
  void prepareAdvance(
      Pipeline& pipeline,
      WaveStream& stream,
      int32_t from,
      std::vector<AdvanceResult>& advance);

  // Finishes any pending activity, so that the final result at the
  // end is ready to consume by another pipeline. This is called once,
  // after there is guaranteed no more input.
  void flush(int32_t pipelineIdx);

  // Copies from 'waveStats_' to runtimeStates consumed by
  // exec::Driver.
  void updateStats();

  bool shouldYield(exec::StopReason taskStopReason, size_t startTimeMs) const;

  // Sets the WaveStreams to error state.
  void setError();

  // Supports Task-wide sync between WaveDrivers on different exec::Drivers.
  std::shared_ptr<WaveBarrier> barrier_;

  std::unique_ptr<GpuArena> arena_;
  std::unique_ptr<GpuArena> deviceArena_;

  ContinueFuture blockingFuture_{ContinueFuture::makeEmpty()};
  exec::BlockingReason blockingReason_;

  size_t startTimeMs_;
  size_t getOutputTimeLimitMs_{0};
  bool finished_{false};

  void incStats(WaveStats& stats) {
    waveStats_.add(stats);
    stats.clear();
  }

  std::vector<Pipeline> pipelines_;

  // The replaced Operators from the Driver. Can be used for a CPU fallback.
  std::vector<std::unique_ptr<exec::Operator>> cpuOperators_;

  // Top level column order in getOutput result.
  std::vector<OperandId> resultOrder_;

  // Dedupped Subfields. Handed over by CompileState.
  SubfieldMap subfields_;
  // Operands handed over by compilation.
  std::vector<std::unique_ptr<AbstractOperand>> operands_;

  std::vector<std::unique_ptr<AbstractState>> states_;

  WaveStats waveStats_;

  // States shared between WaveStreams and WaveDrivers, for example join/group
  // by tables.
  OperatorStateMap stateMap_;

  // Space reserved in BlockStatus array for instruction level return state.
  InstructionStatus instructionStatus_;

  RowVectorPtr result_;
};

} // namespace facebook::velox::wave
