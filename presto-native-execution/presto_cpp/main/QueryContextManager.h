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
#include <list>
#include <memory>
#include <unordered_map>

#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/core/QueryCtx.h"

namespace facebook::presto {

folly::CPUThreadPoolExecutor* driverCPUExecutor();

class QueryContextCache {
 public:
  using QueryCtxWeakPtr = std::weak_ptr<velox::core::QueryCtx>;
  using QueryIdList = std::list<protocol::QueryId>;
  using QueryCtxCacheValue = std::pair<QueryCtxWeakPtr, QueryIdList::iterator>;
  using QueryCtxMap = std::unordered_map<protocol::QueryId, QueryCtxCacheValue>;

  QueryContextCache(size_t initial_capacity = kInitialCapacity)
      : capacity_(initial_capacity) {}

  size_t capacity() const {
    return capacity_;
  }
  size_t size() const {
    return queryCtxs_.size();
  }

  std::shared_ptr<velox::core::QueryCtx> get(protocol::QueryId queryId) {
    auto iter = queryCtxs_.find(queryId);
    if (iter != queryCtxs_.end()) {
      queryIds_.erase(iter->second.second);

      if (auto queryCtx = iter->second.first.lock()) {
        // Move the queryId to front, if queryCtx is still alive.
        queryIds_.push_front(queryId);
        iter->second.second = queryIds_.begin();
        return queryCtx;
      } else {
        queryCtxs_.erase(iter);
      }
    }
    return nullptr;
  }

  std::shared_ptr<velox::core::QueryCtx> insert(
      protocol::QueryId queryId,
      std::shared_ptr<velox::core::QueryCtx> queryCtx) {
    if (queryCtxs_.size() >= capacity_) {
      evict();
    }
    queryIds_.push_front(queryId);
    queryCtxs_[queryId] =
        std::make_pair(folly::to_weak_ptr(queryCtx), queryIds_.begin());
    return queryCtx;
  }

  void evict() {
    // Evict least recently used queryCtx if it is not referenced elsewhere.
    for (auto victim = queryIds_.end(); victim != queryIds_.begin();) {
      --victim;
      if (!queryCtxs_[*victim].first.lock()) {
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

 private:
  size_t capacity_;

  QueryCtxMap queryCtxs_;
  QueryIdList queryIds_;

  static constexpr size_t kInitialCapacity = 256UL;
};

class QueryContextManager {
 public:
  QueryContextManager(
      std::unordered_map<std::string, std::string>& properties,
      std::unordered_map<std::string, std::string>& nodeProperties)
      : properties_(properties), nodeProperties_(nodeProperties) {}

  std::shared_ptr<velox::core::QueryCtx> findOrCreateQueryCtx(
      const protocol::TaskId& taskId,
      std::unordered_map<std::string, std::string>&& configStrings,
      std::unordered_map<
          std::string,
          std::unordered_map<std::string, std::string>>&&
          connectorConfigStrings);

  void overrideProperties(
      const std::string& property,
      const std::string& value) {
    properties_[property] = value;
  }

  // Calls the given functor for every present query context.
  void visitAllContexts(std::function<void(
                            const protocol::QueryId&,
                            const velox::core::QueryCtx*)> visitor) const;

  static constexpr const char* kQueryMaxMemoryPerNode =
      "query.max-memory-per-node";
  static constexpr const char* kQueryMaxTotalMemoryPerNode =
      "query.max-total-memory-per-node";
  static constexpr int64_t kDefaultMaxMemoryPerNode =
      std::numeric_limits<int64_t>::max();

 private:
  int64_t getMaxMemoryPerNode(
      const std::string& property,
      int64_t defaultMaxMemoryPerNode) {
    int64_t maxMemoryInBytes = defaultMaxMemoryPerNode;
    auto it = properties_.find(property);
    if (it != properties_.end()) {
      // This can overflow if the properties exceeds 8EB.
      maxMemoryInBytes =
          protocol::DataSize(it->second).getValue(protocol::DataUnit::BYTE);
    }
    // Return sane value if it is indeed overflow.
    return (maxMemoryInBytes <= 0) ? defaultMaxMemoryPerNode : maxMemoryInBytes;
  }

  folly::Synchronized<QueryContextCache> queryContextCache_;
  std::unordered_map<std::string, std::string> properties_;
  std::unordered_map<std::string, std::string> nodeProperties_;
};
} // namespace facebook::presto
