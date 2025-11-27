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

#include <folly/Synchronized.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/core/QueryCtx.h"

namespace facebook::presto {
class QueryContextCache {
 public:
  using QueryCtxWeakPtr = std::weak_ptr<velox::core::QueryCtx>;
  using QueryIdList = std::list<protocol::QueryId>;
  struct QueryCtxCacheValue {
    QueryCtxWeakPtr queryCtx;
    QueryIdList::iterator idListIterator;
    bool hasStartedTasks{false};
  };
  using QueryCtxMap = std::unordered_map<protocol::QueryId, QueryCtxCacheValue>;

  explicit QueryContextCache(size_t initial_capacity = kInitialCapacity)
      : capacity_(initial_capacity) {}

  size_t capacity() const {
    return capacity_;
  }
  size_t size() const {
    return queryCtxs_.size();
  }

  const QueryCtxMap& ctxMap() const {
    return queryCtxs_;
  }

  std::shared_ptr<velox::core::QueryCtx> get(const protocol::QueryId& queryId);

  std::shared_ptr<velox::core::QueryCtx> insert(
      const protocol::QueryId& queryId,
      std::shared_ptr<velox::core::QueryCtx> queryCtx);

  bool hasStartedTasks(const protocol::QueryId& queryId) const;

  void setTasksStarted(const protocol::QueryId& queryId);

  void evict();

  void clear();

 private:
  static constexpr size_t kInitialCapacity = 256UL;

  size_t capacity_;

  QueryCtxMap queryCtxs_;
  QueryIdList queryIds_;
};

class QueryContextManager {
 public:
  QueryContextManager(
      folly::Executor* driverExecutor,
      folly::Executor* spillerExecutor);

  virtual ~QueryContextManager() = default;

  std::shared_ptr<velox::core::QueryCtx> findOrCreateQueryCtx(
      const protocol::TaskId& taskId,
      const protocol::TaskUpdateRequest& taskUpdateRequest);

  std::shared_ptr<velox::core::QueryCtx> findOrCreateBatchQueryCtx(
      const protocol::TaskId& taskId,
      const protocol::TaskUpdateRequest& taskUpdateRequest);

  /// Returns true if the given task's query has at least one task started.
  bool queryHasStartedTasks(const protocol::TaskId& taskId) const;

  /// Sets flag indicating that task's query has at least one task started.
  void setQueryHasStartedTasks(const protocol::TaskId& taskId);

  /// Calls the given functor for every present query context.
  void visitAllContexts(
      const std::function<
          void(const protocol::QueryId&, const velox::core::QueryCtx*)>&
          visitor) const;

  /// Test method to clear the query context cache.
  void clearCache();

 protected:
  folly::Executor* const driverExecutor_{nullptr};
  folly::Executor* const spillerExecutor_{nullptr};
  QueryContextCache queryContextCache_;

 private:
  virtual std::shared_ptr<velox::core::QueryCtx> createAndCacheQueryCtxLocked(
      const protocol::QueryId& queryId,
      velox::core::QueryConfig&& queryConfig,
      std::unordered_map<
          std::string,
          std::shared_ptr<velox::config::ConfigBase>>&& connectorConfigs,
      std::shared_ptr<velox::memory::MemoryPool>&& pool);

  std::shared_ptr<velox::core::QueryCtx> findOrCreateQueryCtxLocked(
      const protocol::TaskId& taskId,
      velox::core::QueryConfig&& queryConfig,
      std::unordered_map<
          std::string,
          std::shared_ptr<velox::config::ConfigBase>>&& connectorConfigStrings);

  mutable std::mutex queryContextCacheMutex_;
};

} // namespace facebook::presto
