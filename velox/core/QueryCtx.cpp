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

QueryCtx::QueryCtx(
    folly::Executor* executor,
    QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<Config>>
        connectorSessionProperties,
    cache::AsyncDataCache* cache,
    std::shared_ptr<memory::MemoryPool> pool,
    std::shared_ptr<folly::Executor> spillExecutor,
    const std::string& queryId)
    : queryId_(queryId),
      executor_(executor),
      spillExecutor_(spillExecutor.get()),
      cache_(cache),
      connectorSessionProperties_(connectorSessionProperties),
      pool_(std::move(pool)),
      queryConfig_{std::move(queryConfig)} {
  initPool(queryId);
}

QueryCtx::QueryCtx(
    folly::Executor::KeepAlive<> executorKeepalive,
    std::unordered_map<std::string, std::string> queryConfigValues,
    std::unordered_map<std::string, std::shared_ptr<Config>>
        connectorSessionProperties,
    cache::AsyncDataCache* cache,
    std::shared_ptr<memory::MemoryPool> pool,
    const std::string& queryId)
    : queryId_(queryId),
      cache_(cache),
      connectorSessionProperties_(connectorSessionProperties),
      pool_(std::move(pool)),
      executorKeepalive_(std::move(executorKeepalive)),
      queryConfig_{std::move(queryConfigValues)} {
  initPool(queryId);
}

/*static*/ std::string QueryCtx::generatePoolName(const std::string& queryId) {
  // We attach a monotonically increasing sequence number to ensure the pool
  // name is unique.
  static std::atomic<int64_t> seqNum{0};
  return fmt::format("query.{}.{}", queryId.c_str(), seqNum++);
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

} // namespace facebook::velox::core
