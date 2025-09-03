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

  QueryContextCache(size_t initial_capacity = kInitialCapacity)
      : capacity_(initial_capacity) {}

  size_t capacity() const {
    return capacity_;
  }
  size_t size() const {
    return queryCtxs_.size();
  }

  std::shared_ptr<velox::core::QueryCtx> get(const protocol::QueryId& queryId) {
    auto iter = queryCtxs_.find(queryId);
    if (iter != queryCtxs_.end()) {
      queryIds_.erase(iter->second.idListIterator);

      if (auto queryCtx = iter->second.queryCtx.lock()) {
        // Move the queryId to front, if queryCtx is still alive.
        queryIds_.push_front(queryId);
        iter->second.idListIterator = queryIds_.begin();
        return queryCtx;
      } else {
        queryCtxs_.erase(iter);
      }
    }
    return nullptr;
  }

  std::shared_ptr<velox::core::QueryCtx> insert(
      const protocol::QueryId& queryId,
      std::shared_ptr<velox::core::QueryCtx> queryCtx) {
    if (queryCtxs_.size() >= capacity_) {
      evict();
    }
    queryIds_.push_front(queryId);
    queryCtxs_[queryId] = {
        folly::to_weak_ptr(queryCtx), queryIds_.begin(), false};
    return queryCtx;
  }

  bool hasStartedTasks(const protocol::QueryId& queryId) const {
    auto iter = queryCtxs_.find(queryId);
    if (iter != queryCtxs_.end()) {
      return iter->second.hasStartedTasks;
    }
    return false;
  }

  void setHasStartedTasks(const protocol::QueryId& queryId) {
    auto iter = queryCtxs_.find(queryId);
    if (iter != queryCtxs_.end()) {
      iter->second.hasStartedTasks = true;
    }
  }

  void evict() {
    // Evict least recently used queryCtx if it is not referenced elsewhere.
    for (auto victim = queryIds_.end(); victim != queryIds_.begin();) {
      --victim;
      if (!queryCtxs_[*victim].queryCtx.lock()) {
        queryCtxs_.erase(*victim);
        queryIds_.erase(victim);
        return;
      }
    }

    // All queries are still inflight. Increase capacity.
    capacity_ = std::max(kInitialCapacity, capacity_ * 2);
  }
  const QueryCtxMap& ctxs() const {
    return queryCtxs_;
  }

  void testingClear();

 private:
  size_t capacity_;

  QueryCtxMap queryCtxs_;
  QueryIdList queryIds_;

  static constexpr size_t kInitialCapacity = 256UL;
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

  /// Returns true if the given task's query has at least one task started.
  bool queryHasStartedTasks(const protocol::TaskId& taskId) const;

  /// Sets flag indicating that task's query has at least one task started.
  void setQueryHasStartedTasks(const protocol::TaskId& taskId);

  /// Calls the given functor for every present query context.
  void visitAllContexts(const std::function<void(
                            const protocol::QueryId&,
                            const velox::core::QueryCtx*)>& visitor) const;

  /// Test method to clear the query context cache.
  void testingClearCache();

 protected:
  folly::Executor* const driverExecutor_{nullptr};
  folly::Executor* const spillerExecutor_{nullptr};

 private:
  virtual std::shared_ptr<velox::core::QueryCtx> createAndCacheQueryCtx(
    QueryContextCache& cache,
    const protocol::QueryId& queryId,
    velox::core::QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>&& connectorConfigs,
    std::shared_ptr<velox::memory::MemoryPool>&& pool);

  std::shared_ptr<velox::core::QueryCtx> findOrCreateQueryCtx(
      const protocol::TaskId& taskId,
      velox::core::QueryConfig&& queryConfig,
      std::unordered_map<
          std::string,
          std::shared_ptr<velox::config::ConfigBase>>&& connectorConfigStrings);

  folly::Synchronized<QueryContextCache> queryContextCache_;
};

} // namespace facebook::presto
