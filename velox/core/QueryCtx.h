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

#include <folly/Executor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/QueryConfig.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/VectorPool.h"

namespace facebook::velox::core {

class QueryCtx {
 public:
  /// QueryCtx is used in different places. When used with `Task::start()`, it's
  /// required that the caller supplies the executor and ensure its lifetime
  /// outlives the tasks that use it. In contrast, when used in expression
  /// evaluation through `ExecCtx` or 'Task::next()' for single thread execution
  /// mode, executor is not needed. Hence, we don't require executor to always
  /// be passed in here, but instead, ensure that executor exists when actually
  /// being used.
  QueryCtx(
      folly::Executor* executor = nullptr,
      QueryConfig&& queryConfig = QueryConfig{{}},
      std::unordered_map<std::string, std::shared_ptr<Config>>
          connectorConfigs = {},
      cache::AsyncDataCache* cache = cache::AsyncDataCache::getInstance(),
      std::shared_ptr<memory::MemoryPool> pool = nullptr,
      folly::Executor* spillExecutor = nullptr,
      const std::string& queryId = "");

  QueryCtx(
      folly::Executor* executor,
      QueryConfig&& queryConfig,
      std::unordered_map<std::string, std::shared_ptr<Config>> connectorConfigs,
      cache::AsyncDataCache* cache,
      std::shared_ptr<memory::MemoryPool> pool,
      std::shared_ptr<folly::Executor> spillExecutor,
      const std::string& queryId = "");

  /// Constructor to block the destruction of executor while this
  /// object is alive.
  ///
  /// This constructor does not keep the ownership of executor.
  explicit QueryCtx(
      folly::Executor::KeepAlive<> executorKeepalive,
      std::unordered_map<std::string, std::string> queryConfigValues = {},
      std::unordered_map<std::string, std::shared_ptr<Config>>
          connectorConfigs = {},
      cache::AsyncDataCache* cache = cache::AsyncDataCache::getInstance(),
      std::shared_ptr<memory::MemoryPool> pool = nullptr,
      const std::string& queryId = "");

  static std::string generatePoolName(const std::string& queryId);

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  cache::AsyncDataCache* cache() const {
    return cache_;
  }

  folly::Executor* executor() const {
    VELOX_CHECK(isExecutorSupplied(), "Executor was not supplied.");
    if (executor_ != nullptr) {
      return executor_;
    }
    return executorKeepalive_.get();
  }

  bool isExecutorSupplied() const {
    return executor_ != nullptr || executorKeepalive_.get() != nullptr;
  }

  const QueryConfig& queryConfig() const {
    return queryConfig_;
  }

  Config* connectorSessionProperties(const std::string& connectorId) const {
    auto it = connectorSessionProperties_.find(connectorId);
    if (it == connectorSessionProperties_.end()) {
      return getEmptyConfig();
    }
    return it->second.get();
  }

  /// Overrides the previous configuration. Note that this function is NOT
  /// thread-safe and should probably only be used in tests.
  void testingOverrideConfigUnsafe(
      std::unordered_map<std::string, std::string>&& values) {
    this->queryConfig_.testingOverrideConfigUnsafe(std::move(values));
  }

  // Overrides the previous connector-specific configuration. Note that this
  // function is NOT thread-safe and should probably only be used in tests.
  void setConnectorSessionOverridesUnsafe(
      const std::string& connectorId,
      std::unordered_map<std::string, std::string>&& configOverrides) {
    connectorSessionProperties_[connectorId] =
        std::make_shared<MemConfig>(std::move(configOverrides));
  }

  folly::Executor* spillExecutor() const {
    return spillExecutor_;
  }

  const std::string& queryId() const {
    return queryId_;
  }

  void testingOverrideMemoryPool(std::shared_ptr<memory::MemoryPool> pool) {
    pool_ = std::move(pool);
  }

  /// Updates the aggregated spill bytes of this query, and and throws if
  /// exceeds the max spill bytes limit.
  void updateSpilledBytesAndCheckLimit(uint64_t bytes);

 private:
  static Config* getEmptyConfig() {
    static const std::unique_ptr<Config> kEmptyConfig =
        std::make_unique<MemConfig>();
    return kEmptyConfig.get();
  }

  void initPool(const std::string& queryId) {
    if (pool_ == nullptr) {
      pool_ = memory::deprecatedDefaultMemoryManager().addRootPool(
          QueryCtx::generatePoolName(queryId));
    }
  }

  const std::string queryId_;
  folly::Executor* const executor_{nullptr};
  folly::Executor* const spillExecutor_{nullptr};
  cache::AsyncDataCache* const cache_;

  std::unordered_map<std::string, std::shared_ptr<Config>>
      connectorSessionProperties_;
  std::shared_ptr<memory::MemoryPool> pool_;
  folly::Executor::KeepAlive<> executorKeepalive_;
  QueryConfig queryConfig_;
  std::atomic<uint64_t> numSpilledBytes_{0};
};

// Represents the state of one thread of query execution.
class ExecCtx {
 public:
  ExecCtx(memory::MemoryPool* pool, QueryCtx* queryCtx)
      : pool_(pool),
        queryCtx_(queryCtx),
        exprEvalCacheEnabled_(
            !queryCtx ||
            queryCtx->queryConfig().isExpressionEvaluationCacheEnabled()),
        vectorPool_(
            exprEvalCacheEnabled_ ? std::make_unique<VectorPool>(pool)
                                  : nullptr) {}

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  QueryCtx* queryCtx() const {
    return queryCtx_;
  }

  /// Returns an uninitialized  SelectivityVector from a pool. Allocates new one
  /// if none is available. Make sure to call 'releaseSelectivityVector' when
  /// done using the vector to allow for reuse.
  ///
  /// Prefer using LocalSelectivityVector which takes care of returning the
  /// vector to the pool on destruction.
  std::unique_ptr<SelectivityVector> getSelectivityVector(int32_t size) {
    VELOX_CHECK(exprEvalCacheEnabled_ || selectivityVectorPool_.empty());
    if (selectivityVectorPool_.empty()) {
      return std::make_unique<SelectivityVector>(size);
    }
    auto vector = std::move(selectivityVectorPool_.back());
    selectivityVectorPool_.pop_back();
    vector->resize(size);
    return vector;
  }

  // Returns an arbitrary SelectivityVector with undefined
  // content. The caller is responsible for setting the size and
  // assigning the contents.
  std::unique_ptr<SelectivityVector> getSelectivityVector() {
    VELOX_CHECK(exprEvalCacheEnabled_ || selectivityVectorPool_.empty());
    if (selectivityVectorPool_.empty()) {
      return std::make_unique<SelectivityVector>();
    }
    auto vector = std::move(selectivityVectorPool_.back());
    selectivityVectorPool_.pop_back();
    return vector;
  }

  // Returns true if the vector was moved into the pool.
  bool releaseSelectivityVector(std::unique_ptr<SelectivityVector>&& vector) {
    if (exprEvalCacheEnabled_) {
      selectivityVectorPool_.push_back(std::move(vector));
      return true;
    }
    return false;
  }

  std::unique_ptr<DecodedVector> getDecodedVector() {
    VELOX_CHECK(exprEvalCacheEnabled_ || decodedVectorPool_.empty());
    if (decodedVectorPool_.empty()) {
      return std::make_unique<DecodedVector>();
    }
    auto vector = std::move(decodedVectorPool_.back());
    decodedVectorPool_.pop_back();
    return vector;
  }

  // Returns true if the vector was moved into the pool.
  bool releaseDecodedVector(std::unique_ptr<DecodedVector>&& vector) {
    if (exprEvalCacheEnabled_) {
      decodedVectorPool_.push_back(std::move(vector));
      return true;
    }
    return false;
  }

  VectorPool* vectorPool() {
    return vectorPool_.get();
  }

  /// Gets a possibly recycled vector of 'type and 'size'. Allocates from
  /// 'pool_' if no pre-allocated vector.
  VectorPtr getVector(const TypePtr& type, vector_size_t size) {
    if (vectorPool_) {
      return vectorPool_->get(type, size);
    } else {
      return BaseVector::create(type, size, pool_);
    }
  }

  /// Moves 'vector' to the pool if it is reusable, else leaves it in
  /// place. Returns true if the vector was moved into the pool.
  bool releaseVector(VectorPtr& vector) {
    if (vectorPool_) {
      return vectorPool_->release(vector);
    }
    return false;
  }

  /// Moves elements of 'vectors' to the pool if reusable, else leaves them
  /// in place. Returns number of vectors that were moved into the pool.
  size_t releaseVectors(std::vector<VectorPtr>& vectors) {
    if (vectorPool_) {
      return vectorPool_->release(vectors);
    }
    return 0;
  }

  bool exprEvalCacheEnabled() const {
    return exprEvalCacheEnabled_;
  }

 private:
  // Pool for all Buffers for this thread.
  memory::MemoryPool* const pool_;
  QueryCtx* const queryCtx_;

  const bool exprEvalCacheEnabled_;
  // A pool of preallocated DecodedVectors for use by expressions and operators.
  std::vector<std::unique_ptr<DecodedVector>> decodedVectorPool_;
  // A pool of preallocated SelectivityVectors for use by expressions
  // and operators.
  std::vector<std::unique_ptr<SelectivityVector>> selectivityVectorPool_;
  std::unique_ptr<VectorPool> vectorPool_;
};

} // namespace facebook::velox::core
