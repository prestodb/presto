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
#include "presto_cpp/main/SessionProperties.h"
#include "presto_cpp/main/common/Configs.h"
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

QueryContextManager::QueryContextManager(
    folly::Executor* driverExecutor,
    folly::Executor* spillerExecutor)
    : driverExecutor_(driverExecutor), spillerExecutor_(spillerExecutor) {}

std::shared_ptr<velox::core::QueryCtx>
QueryContextManager::findOrCreateQueryCtx(
    const protocol::TaskId& taskId,
    const protocol::TaskUpdateRequest& taskUpdateRequest) {
  return findOrCreateQueryCtx(
      taskId,
      toVeloxConfigs(taskUpdateRequest.session),
      toConnectorConfigs(taskUpdateRequest));
}

bool QueryContextManager::queryHasStartedTasks(
    const protocol::TaskId& taskId) const {
  return queryContextCache_.rlock()->hasStartedTasks(queryIdFromTaskId(taskId));
}

void QueryContextManager::setQueryHasStartedTasks(
    const protocol::TaskId& taskId) {
  queryContextCache_.wlock()->setHasStartedTasks(queryIdFromTaskId(taskId));
}

std::shared_ptr<core::QueryCtx> QueryContextManager::createAndCacheQueryCtx(
    QueryContextCache& cache,
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
  return cache.insert(queryId, std::move(queryCtx));
}

std::shared_ptr<core::QueryCtx> QueryContextManager::findOrCreateQueryCtx(
    const TaskId& taskId,
    velox::core::QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>&&
        connectorConfigs) {
  const QueryId queryId{queryIdFromTaskId(taskId)};

  auto lockedCache = queryContextCache_.wlock();
  if (auto queryCtx = lockedCache->get(queryId)) {
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

  return createAndCacheQueryCtx(
      *lockedCache,
      queryId,
      std::move(queryConfig),
      std::move(connectorConfigs),
      std::move(pool));
}

void QueryContextManager::visitAllContexts(
    const std::function<
        void(const protocol::QueryId&, const velox::core::QueryCtx*)>& visitor)
    const {
  auto lockedCache = queryContextCache_.rlock();
  for (const auto& it : lockedCache->ctxs()) {
    if (const auto queryCtxSP = it.second.queryCtx.lock()) {
      visitor(it.first, queryCtxSP.get());
    }
  }
}

void QueryContextManager::testingClearCache() {
  queryContextCache_.wlock()->testingClear();
}

void QueryContextCache::testingClear() {
  queryCtxs_.clear();
  queryIds_.clear();
}

} // namespace facebook::presto
