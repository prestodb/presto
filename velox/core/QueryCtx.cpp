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

#include "velox/core/QueryCtx.h"
#include "velox/common/base/SpillConfig.h"

namespace facebook::velox::core {

/*static*/ std::shared_ptr<QueryCtx> QueryCtx::create(
    folly::Executor* executor,
    QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<Config>> connectorConfigs,
    cache::AsyncDataCache* cache,
    std::shared_ptr<memory::MemoryPool> pool,
    folly::Executor* spillExecutor,
    const std::string& queryId) {
  std::shared_ptr<QueryCtx> queryCtx(new QueryCtx(
      executor,
      std::move(queryConfig),
      std::move(connectorConfigs),
      cache,
      std::move(pool),
      spillExecutor,
      queryId));
  queryCtx->maybeSetReclaimer();
  return queryCtx;
}

QueryCtx::QueryCtx(
    folly::Executor* executor,
    QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<Config>>
        connectorSessionProperties,
    cache::AsyncDataCache* cache,
    std::shared_ptr<memory::MemoryPool> pool,
    folly::Executor* spillExecutor,
    const std::string& queryId)
    : queryId_(queryId),
      executor_(executor),
      spillExecutor_(spillExecutor),
      cache_(cache),
      connectorSessionProperties_(connectorSessionProperties),
      pool_(std::move(pool)),
      queryConfig_{std::move(queryConfig)} {
  initPool(queryId);
}

/*static*/ std::string QueryCtx::generatePoolName(const std::string& queryId) {
  // We attach a monotonically increasing sequence number to ensure the pool
  // name is unique.
  static std::atomic<int64_t> seqNum{0};
  return fmt::format("query.{}.{}", queryId.c_str(), seqNum++);
}

void QueryCtx::maybeSetReclaimer() {
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK(!underArbitration_);
  if (pool_->reclaimer() != nullptr) {
    return;
  }
  pool_->setReclaimer(QueryCtx::MemoryReclaimer::create(this, pool_.get()));
}

void QueryCtx::updateSpilledBytesAndCheckLimit(uint64_t bytes) {
  const auto numSpilledBytes = numSpilledBytes_.fetch_add(bytes) + bytes;
  if (queryConfig_.maxSpillBytes() > 0 &&
      numSpilledBytes > queryConfig_.maxSpillBytes()) {
    VELOX_SPILL_LIMIT_EXCEEDED(fmt::format(
        "Query exceeded per-query local spill limit of {}",
        succinctBytes(queryConfig_.maxSpillBytes())));
  }
}

std::unique_ptr<memory::MemoryReclaimer> QueryCtx::MemoryReclaimer::create(
    QueryCtx* queryCtx,
    memory::MemoryPool* pool) {
  return std::unique_ptr<memory::MemoryReclaimer>(
      new QueryCtx::MemoryReclaimer(queryCtx->shared_from_this(), pool));
}

uint64_t QueryCtx::MemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  auto queryCtx = ensureQueryCtx();
  if (queryCtx == nullptr) {
    return 0;
  }
  VELOX_CHECK_EQ(pool->name(), pool_->name());

  const auto leaveGuard =
      folly::makeGuard([&]() { queryCtx->finishArbitration(); });
  queryCtx->startArbitration();
  return memory::MemoryReclaimer::reclaim(pool, targetBytes, maxWaitMs, stats);
}

bool QueryCtx::checkUnderArbitration(ContinueFuture* future) {
  VELOX_CHECK_NOT_NULL(future);
  std::lock_guard<std::mutex> l(mutex_);
  if (!underArbitration_) {
    VELOX_CHECK(arbitrationPromises_.empty());
    return false;
  }
  arbitrationPromises_.emplace_back("QueryCtx::waitArbitration");
  *future = arbitrationPromises_.back().getSemiFuture();
  return true;
}

void QueryCtx::startArbitration() {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(!underArbitration_);
  VELOX_CHECK(arbitrationPromises_.empty());
  underArbitration_ = true;
}

void QueryCtx::finishArbitration() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(underArbitration_);
    underArbitration_ = false;
    promises.swap(arbitrationPromises_);
  }
  for (auto& promise : promises) {
    promise.setValue();
  }
}
} // namespace facebook::velox::core
