/*
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

#include <memory>
#include "presto_cpp/main/types/PrestoTaskId.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/exec/Task.h"

namespace facebook::velox {
struct RuntimeMetric;
}

namespace facebook::presto {

template <typename T>
struct PromiseHolder {
  explicit PromiseHolder(folly::Promise<T> p) : promise(std::move(p)) {}
  folly::Promise<T> promise;

  void atDestruction(
      std::function<void(folly::Promise<T> promise)> atDestruction) {
    atDestruction_ = atDestruction;
  }

  ~PromiseHolder() {
    if (atDestruction_ && !promise.isFulfilled()) {
      atDestruction_(std::move(promise));
    }
  }

 private:
  std::function<void(folly::Promise<T> promise)> atDestruction_;
};

template <typename T>
using PromiseHolderPtr = std::shared_ptr<PromiseHolder<T>>;

template <typename T>
using PromiseHolderWeakPtr = std::weak_ptr<PromiseHolder<T>>;

struct Result {
  int64_t sequence;
  int64_t nextSequence;
  std::unique_ptr<folly::IOBuf> data;
  bool complete;
};

struct ResultRequest {
  PromiseHolderWeakPtr<std::unique_ptr<Result>> promise;
  protocol::TaskId taskId;
  int64_t bufferId;
  int64_t token;
  protocol::DataSize maxSize;
};

struct PrestoTask {
  const PrestoTaskId id;
  std::shared_ptr<velox::exec::Task> task;
  bool taskStarted = false;
  uint64_t lastHeartbeatMs{0};
  mutable std::mutex mutex;

  // Error before task is created or when task is being created.
  std::exception_ptr error = nullptr;

  // Contains state info but is never returned.
  protocol::TaskInfo info;

  // Pending result requests keyed on buffer ID. May arrive before 'task' is
  // created. May be accessed on different threads outside of 'mutex', hence
  // shared_ptr to define lifetime.
  std::unordered_map<int64_t, std::shared_ptr<ResultRequest>> resultRequests;

  // Pending status request. May arrive before there is a Task.
  PromiseHolderWeakPtr<std::unique_ptr<protocol::TaskStatus>> statusRequest;

  // Info request. May arrive before there is a Task.
  PromiseHolderWeakPtr<std::unique_ptr<protocol::TaskInfo>> infoRequest;

  explicit PrestoTask(const std::string& taskId);

  /// Updates when this task was touched last time.
  void updateHeartbeatLocked();

  /// Returns time (ms) since the task was touched last time (last heartbeat).
  /// Returns zero, if never (shouldn't happen).
  uint64_t timeSinceLastHeartbeatMs() const;

  protocol::TaskStatus updateStatus() {
    std::lock_guard<std::mutex> l(mutex);
    return updateStatusLocked();
  }

  protocol::TaskInfo updateInfo() {
    std::lock_guard<std::mutex> l(mutex);
    return updateInfoLocked();
  }

  // Turn the task numbers (per state) into a string.
  static std::string taskNumbersToString(
      const std::array<size_t, 5>& taskNumbers);

  protocol::TaskStatus updateStatusLocked();
  protocol::TaskInfo updateInfoLocked();
};

using TaskMap =
    std::unordered_map<protocol::TaskId, std::shared_ptr<PrestoTask>>;

protocol::RuntimeMetric toRuntimeMetric(
    const std::string& name,
    const facebook::velox::RuntimeMetric& metric);

} // namespace facebook::presto
