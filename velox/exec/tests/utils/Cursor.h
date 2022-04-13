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
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec::test {

// Parameters for initializing a TaskCursor or RowCursor.
struct CursorParameters {
  // Root node of the plan tree
  std::shared_ptr<const core::PlanNode> planNode;

  int32_t destination = 0;

  // Maximum number of drivers per pipeline.
  int32_t maxDrivers = 1;

  // Maximum number of split groups processed concurrently.
  int32_t numConcurrentSplitGroups = 1;

  // Optional, created if not present.
  std::shared_ptr<core::QueryCtx> queryCtx;

  uint64_t bufferedBytes = 512 * 1024;

  // Ungrouped (by default) or grouped (bucketed) execution.
  core::ExecutionStrategy executionStrategy{
      core::ExecutionStrategy::kUngrouped};

  // Number of splits groups the task will be processing. Must be 1 for
  // ungrouped execution.
  int numSplitGroups{1};
};

class TaskQueue {
 public:
  struct TaskQueueEntry {
    RowVectorPtr vector;
    uint64_t bytes;
  };

  explicit TaskQueue(uint64_t maxBytes)
      : pool_(memory::getDefaultScopedMemoryPool()),
        maxBytes_(maxBytes),
        consumerFuture_(false) {}

  void setNumProducers(int32_t n) {
    numProducers_ = n;
  }

  // Adds a batch of rows to the queue and returns kNotBlocked if the
  // producer may continue. Returns kWaitForConsumer if the queue is
  // full after the addition and sets '*future' to a future that is
  // realized when the producer may continue.
  exec::BlockingReason enqueue(
      RowVectorPtr vector,
      exec::ContinueFuture* future);

  // Returns nullptr when all producers are at end. Otherwise blocks.
  RowVectorPtr dequeue();

  void close();

  bool hasNext();

  velox::memory::MemoryPool* pool() const {
    return pool_.get();
  }

 private:
  // Owns the vectors in 'queue_', hence must be declared first.
  std::unique_ptr<velox::memory::MemoryPool> pool_;
  std::deque<TaskQueueEntry> queue_;
  std::optional<int32_t> numProducers_;
  int32_t producersFinished_ = 0;
  uint64_t totalBytes_ = 0;
  // Blocks the producer if 'totalBytes' exceeds 'maxBytes' after
  // adding the result.
  uint64_t maxBytes_;
  std::mutex mutex_;
  std::vector<VeloxPromise<bool>> producerUnblockPromises_;
  bool consumerBlocked_ = false;
  VeloxPromise<bool> consumerPromise_;
  folly::Future<bool> consumerFuture_;
  bool closed_ = false;
};

class TaskCursor {
 public:
  explicit TaskCursor(const CursorParameters& params);

  ~TaskCursor() {
    queue_->close();
    if (task_ && !atEnd_) {
      task_->requestCancel();
    }
  }

  /// Starts the task if not started yet.
  void start();

  /// Fetches another batch from the task queue.
  /// Starts the task if not started yet.
  bool moveNext();

  bool hasNext();

  RowVectorPtr& current() {
    return current_;
  }

  const std::shared_ptr<Task>& task() {
    return task_;
  }

 private:
  const int32_t maxDrivers_;
  const int32_t numConcurrentSplitGroups_;
  const int32_t numSplitGroups_;
  bool started_ = false;
  std::shared_ptr<TaskQueue> queue_;
  std::shared_ptr<exec::Task> task_;
  RowVectorPtr current_;
  static std::atomic<int32_t> serial_;
  bool atEnd_{false};
};

class RowCursor {
 public:
  explicit RowCursor(CursorParameters& params) {
    cursor_ = std::make_unique<TaskCursor>(params);
  }

  bool isNullAt(int32_t columnIndex) const {
    checkOnRow();
    return decoded_[columnIndex]->isNullAt(currentRow_);
  }

  template <typename T>
  T valueAt(int32_t columnIndex) const {
    checkOnRow();
    return decoded_[columnIndex]->valueAt<T>(currentRow_);
  }

  bool next();

  bool hasNext();

  std::shared_ptr<Task> task() const {
    return cursor_->task();
  }

 private:
  void checkOnRow() const {
    VELOX_CHECK(
        currentRow_ >= 0 && currentRow_ < numRows_, "Cursor not on row.");
  }

  std::unique_ptr<TaskCursor> cursor_;
  std::vector<std::unique_ptr<DecodedVector>> decoded_;
  SelectivityVector allRows_;
  vector_size_t currentRow_ = 0;
  vector_size_t numRows_ = 0;
};

} // namespace facebook::velox::exec::test
