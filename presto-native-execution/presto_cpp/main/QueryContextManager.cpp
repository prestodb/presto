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

#include "presto_cpp/main/QueryContextManager.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "presto_cpp/main/PrestoToVeloxQueryConfig.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/properties/session/SessionProperties.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/core/QueryConfig.h"

using namespace facebook::velox;

using facebook::presto::protocol::QueryId;
using facebook::presto::protocol::TaskId;

namespace facebook::presto {
namespace {

inline QueryId queryIdFromTaskId(const TaskId& taskId) {
  return taskId.substr(0, taskId.find('.'));
}

} // namespace

std::shared_ptr<velox::core::QueryCtx> QueryContextCache::get(
    const protocol::QueryId& queryId) {
  auto iter = queryCtxs_.find(queryId);
  if (iter == queryCtxs_.end()) {
    return nullptr;
  }

  queryIds_.erase(iter->second.idListIterator);

  if (auto queryCtx = iter->second.queryCtx.lock()) {
    // Move the queryId to front, if queryCtx is still alive.
    queryIds_.push_front(queryId);
    iter->second.idListIterator = queryIds_.begin();
    return queryCtx;
  }
  queryCtxs_.erase(iter);
  return nullptr;
}

std::shared_ptr<velox::core::QueryCtx> QueryContextCache::insert(
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

bool QueryContextCache::hasStartedTasks(
    const protocol::QueryId& queryId) const {
  auto iter = queryCtxs_.find(queryId);
  if (iter != queryCtxs_.end()) {
    return iter->second.hasStartedTasks;
  }
  return false;
}

void QueryContextCache::setTasksStarted(const protocol::QueryId& queryId) {
  auto iter = queryCtxs_.find(queryId);
  if (iter != queryCtxs_.end()) {
    iter->second.hasStartedTasks = true;
  }
}

void QueryContextCache::evict() {
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

QueryContextManager::QueryContextManager(
    folly::Executor* driverExecutor,
    folly::Executor* spillerExecutor)
    : driverExecutor_(driverExecutor), spillerExecutor_(spillerExecutor) {}

std::shared_ptr<velox::core::QueryCtx>
QueryContextManager::findOrCreateQueryCtx(
    const protocol::TaskId& taskId,
    const protocol::TaskUpdateRequest& taskUpdateRequest) {
  std::lock_guard<std::mutex> lock(queryContextCacheMutex_);
  return findOrCreateQueryCtxLocked(
      taskId,
      toVeloxConfigs(
          taskUpdateRequest.session, taskUpdateRequest.extraCredentials),
      toConnectorConfigs(taskUpdateRequest));
}

std::shared_ptr<velox::core::QueryCtx>
QueryContextManager::findOrCreateBatchQueryCtx(
    const protocol::TaskId& taskId,
    const protocol::TaskUpdateRequest& taskUpdateRequest) {
  std::lock_guard<std::mutex> lock(queryContextCacheMutex_);
  auto queryCtx = findOrCreateQueryCtxLocked(
      taskId,
      toVeloxConfigs(
          taskUpdateRequest.session, taskUpdateRequest.extraCredentials),
      toConnectorConfigs(taskUpdateRequest));
  if (queryCtx->pool()->aborted()) {
    // In Batch mode, only one query is running at a time. When tasks fail
    // during memory arbitration, the query memory pool will be set
    // aborted, failing any successive tasks immediately. Yet one task
    // should not fail other newly admitted tasks because of task retries
    // and server reuse. Failure control among tasks should be
    // independent. So if query memory pool is aborted already, a cache clear is
    // performed to allow successive tasks to create a new query context to
    // continue execution.
    VELOX_CHECK_EQ(queryContextCache_.size(), 1);
    queryContextCache_.clear();
    queryCtx = findOrCreateQueryCtxLocked(
        taskId,
        toVeloxConfigs(
            taskUpdateRequest.session, taskUpdateRequest.extraCredentials),
        toConnectorConfigs(taskUpdateRequest));
  }
  return queryCtx;
}

bool QueryContextManager::queryHasStartedTasks(
    const protocol::TaskId& taskId) const {
  std::lock_guard<std::mutex> lock(queryContextCacheMutex_);
  return queryContextCache_.hasStartedTasks(queryIdFromTaskId(taskId));
}

void QueryContextManager::setQueryHasStartedTasks(
    const protocol::TaskId& taskId) {
  std::lock_guard<std::mutex> lock(queryContextCacheMutex_);
  queryContextCache_.setTasksStarted(queryIdFromTaskId(taskId));
}

std::shared_ptr<core::QueryCtx>
QueryContextManager::createAndCacheQueryCtxLocked(
    const QueryId& queryId,
    velox::core::QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>&&
        connectorConfigs,
    std::shared_ptr<memory::MemoryPool>&& pool) {
  auto queryCtx = core::QueryCtx::create(
      driverExecutor_,
      std::move(queryConfig),
      std::move(connectorConfigs),
      cache::AsyncDataCache::getInstance(),
      std::move(pool),
      spillerExecutor_,
      queryId);
  return queryContextCache_.insert(queryId, std::move(queryCtx));
}

std::shared_ptr<core::QueryCtx> QueryContextManager::findOrCreateQueryCtxLocked(
    const TaskId& taskId,
    velox::core::QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>&&
        connectorConfigs) {
  const QueryId queryId{queryIdFromTaskId(taskId)};

  if (auto queryCtx = queryContextCache_.get(queryId)) {
    return queryCtx;
  }

  // NOTE: the monotonically increasing 'poolId' is appended to 'queryId' to
  // ensure that the name of root memory pool instance is always unique. In some
  // edge case, we found some background activities such as the long-running
  // memory arbitration process will still hold the query root memory pool even
  // though the query ctx has been evicted out of the cache. The query ctx cache
  // is still indexed by the query id.
  static std::atomic_uint64_t poolId{0};
  std::optional<memory::MemoryPool::DebugOptions> poolDbgOpts;
  auto debugMemoryPoolNameRegex = queryConfig.debugMemoryPoolNameRegex();
  if (!debugMemoryPoolNameRegex.empty()) {
    poolDbgOpts = memory::MemoryPool::DebugOptions{
        .debugPoolNameRegex = std::move(debugMemoryPoolNameRegex),
        .debugPoolWarnThresholdBytes =
            queryConfig.debugMemoryPoolWarnThresholdBytes()};
  }
  auto pool = memory::MemoryManager::getInstance()->addRootPool(
      fmt::format("{}_{}", queryId, poolId++),
      queryConfig.queryMaxMemoryPerNode(),
      nullptr,
      poolDbgOpts);

  return createAndCacheQueryCtxLocked(
      queryId,
      std::move(queryConfig),
      std::move(connectorConfigs),
      std::move(pool));
}

void QueryContextManager::visitAllContexts(
    const std::function<
        void(const protocol::QueryId&, const velox::core::QueryCtx*)>& visitor)
    const {
  std::lock_guard<std::mutex> lock(queryContextCacheMutex_);
  for (const auto& it : queryContextCache_.ctxMap()) {
    if (const auto queryCtxSP = it.second.queryCtx.lock()) {
      visitor(it.first, queryCtxSP.get());
    }
  }
}

void QueryContextManager::clearCache() {
  std::lock_guard<std::mutex> lock(queryContextCacheMutex_);
  queryContextCache_.clear();
}

void QueryContextCache::clear() {
  queryCtxs_.clear();
  queryIds_.clear();
}

} // namespace facebook::presto
