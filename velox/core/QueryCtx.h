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

namespace facebook::velox {
class Config;
};

namespace facebook::velox::core {

class QueryCtx : public std::enable_shared_from_this<QueryCtx> {
 public:
  ~QueryCtx() {
    VELOX_CHECK(!underArbitration_);
  }

  /// QueryCtx is used in different places. When used with `Task::start()`, it's
  /// required that the caller supplies the executor and ensure its lifetime
  /// outlives the tasks that use it. In contrast, when used in expression
  /// evaluation through `ExecCtx` or 'Task::next()' for single thread execution
  /// mode, executor is not needed. Hence, we don't require executor to always
  /// be passed in here, but instead, ensure that executor exists when actually
  /// being used.
  static std::shared_ptr<QueryCtx> create(
      folly::Executor* executor = nullptr,
      QueryConfig&& queryConfig = QueryConfig{{}},
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
          connectorConfigs = {},
      cache::AsyncDataCache* cache = cache::AsyncDataCache::getInstance(),
      std::shared_ptr<memory::MemoryPool> pool = nullptr,
      folly::Executor* spillExecutor = nullptr,
      const std::string& queryId = "");

  static std::string generatePoolName(const std::string& queryId);

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  cache::AsyncDataCache* cache() const {
    return cache_;
  }

  folly::Executor* executor() const {
    return executor_;
  }

  bool isExecutorSupplied() const {
    return executor_ != nullptr;
  }

  const QueryConfig& queryConfig() const {
    return queryConfig_;
  }

  config::ConfigBase* connectorSessionProperties(
      const std::string& connectorId) const {
    auto it = connectorSessionProperties_.find(connectorId);
    if (it == connectorSessionProperties_.end()) {
      return getEmptyConfig();
    }
    return it->second.get();
  }

  const std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>&
  connectorSessionProperties() const {
    return connectorSessionProperties_;
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
        std::make_shared<config::ConfigBase>(std::move(configOverrides));
  }

  folly::Executor* spillExecutor() const {
    return spillExecutor_;
  }

  const std::string& queryId() const {
    return queryId_;
  }

  /// Checks if the associated query is under memory arbitration or not. The
  /// function returns true if it is and set future which is fulfilled when the
  /// the memory arbiration finishes.
  bool checkUnderArbitration(ContinueFuture* future);

  /// Updates the aggregated spill bytes of this query, and throws if exceeds
  /// the max spill bytes limit.
  void updateSpilledBytesAndCheckLimit(uint64_t bytes);

  /// Updates the aggregated trace bytes of this query, and return true if
  /// exceeds the max query trace bytes limit.
  bool updateTracedBytesAndCheckLimit(uint64_t bytes);

  void testingOverrideMemoryPool(std::shared_ptr<memory::MemoryPool> pool) {
    pool_ = std::move(pool);
  }

  /// Indicates if the query is under memory arbitration or not.
  bool testingUnderArbitration() const {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(underArbitration_ || arbitrationPromises_.empty());
    return underArbitration_;
  }

 private:
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
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
          connectorConfigs = {},
      cache::AsyncDataCache* cache = cache::AsyncDataCache::getInstance(),
      std::shared_ptr<memory::MemoryPool> pool = nullptr,
      folly::Executor* spillExecutor = nullptr,
      const std::string& queryId = "");

  class MemoryReclaimer : public memory::MemoryReclaimer {
   public:
    static std::unique_ptr<memory::MemoryReclaimer> create(
        QueryCtx* queryCtx,
        memory::MemoryPool* pool);

    uint64_t reclaim(
        memory::MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        memory::MemoryReclaimer::Stats& stats) override;

   protected:
    MemoryReclaimer(
        const std::shared_ptr<QueryCtx>& queryCtx,
        memory::MemoryPool* pool)
        : queryCtx_(queryCtx), pool_(pool) {
      VELOX_CHECK_NOT_NULL(pool_);
    }

    // Gets the shared pointer to the associated query ctx to ensure its
    // liveness during the query memory reclaim operation.
    //
    // NOTE: an operator's memory pool can outlive its operator.
    std::shared_ptr<QueryCtx> ensureQueryCtx() const {
      return queryCtx_.lock();
    }

    const std::weak_ptr<QueryCtx> queryCtx_;
    memory::MemoryPool* const pool_;
  };

  static config::ConfigBase* getEmptyConfig() {
    static const std::unique_ptr<config::ConfigBase> kEmptyConfig =
        std::make_unique<config::ConfigBase>(
            std::unordered_map<std::string, std::string>());
    return kEmptyConfig.get();
  }

  void initPool(const std::string& queryId) {
    if (pool_ == nullptr) {
      pool_ = memory::memoryManager()->addRootPool(
          QueryCtx::generatePoolName(queryId), memory::kMaxMemory);
    }
  }

  // Setup the memory reclaimer for arbitration if user provided memory pool
  // hasn't set it.
  void maybeSetReclaimer();

  // Invoked to start memory arbitration on this query.
  void startArbitration();
  // Invoked to stop memory arbitration on this query.
  void finishArbitration();

  const std::string queryId_;
  folly::Executor* const executor_{nullptr};
  folly::Executor* const spillExecutor_{nullptr};
  cache::AsyncDataCache* const cache_;

  std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
      connectorSessionProperties_;
  std::shared_ptr<memory::MemoryPool> pool_;
  QueryConfig queryConfig_;
  std::atomic<uint64_t> numSpilledBytes_{0};
  std::atomic<uint64_t> numTracedBytes_{0};

  mutable std::mutex mutex_;
  // Indicates if this query is under memory arbitration or not.
  bool underArbitration_{false};
  std::vector<ContinuePromise> arbitrationPromises_;
};

// Represents the state of one thread of query execution.
class ExecCtx {
 public:
  ExecCtx(memory::MemoryPool* pool, QueryCtx* queryCtx)
      : pool_(pool),
        queryCtx_(queryCtx),
        optimizationParams_(queryCtx),
        vectorPool_(
            optimizationParams_.exprEvalCacheEnabled
                ? std::make_unique<VectorPool>(pool)
                : nullptr) {}

  struct OptimizationParams {
    explicit OptimizationParams(QueryCtx* queryCtx) {
      const core::QueryConfig defaultQueryConfig = core::QueryConfig({});

      const core::QueryConfig& queryConfig =
          queryCtx ? queryCtx->queryConfig() : defaultQueryConfig;

      exprEvalCacheEnabled = queryConfig.isExpressionEvaluationCacheEnabled();
      dictionaryMemoizationEnabled =
          !queryConfig.debugDisableExpressionsWithMemoization() &&
          exprEvalCacheEnabled;
      peelingEnabled = !queryConfig.debugDisableExpressionsWithPeeling();
      sharedSubExpressionReuseEnabled =
          !queryConfig.debugDisableCommonSubExpressions();
      deferredLazyLoadingEnabled =
          !queryConfig.debugDisableExpressionsWithLazyInputs();
      maxSharedSubexprResultsCached =
          queryConfig.maxSharedSubexprResultsCached();
    }

    /// True if caches in expression evaluation used for performance are
    /// enabled, including VectorPool, DecodedVectorPool, SelectivityVectorPool
    /// and dictionary memoization.
    bool exprEvalCacheEnabled;
    /// True if dictionary memoization optimization is enabled during experssion
    /// evaluation, whichallows the reuse of results between consecutive input
    /// batches if they are dictionary encoded and have the same
    /// alphabet(undelying flat vector).
    bool dictionaryMemoizationEnabled;
    /// True if peeling is enabled during experssion evaluation.
    bool peelingEnabled;
    /// True if shared subexpression reuse is enabled during experssion
    /// evaluation.
    bool sharedSubExpressionReuseEnabled;
    /// True if loading lazy inputs are deferred till they need to be
    /// accessed during experssion evaluation.
    bool deferredLazyLoadingEnabled;
    /// The maximum number of distinct inputs to cache results in a
    /// given shared subexpression during experssion evaluation.
    uint32_t maxSharedSubexprResultsCached;
  };

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
    VELOX_CHECK(
        optimizationParams_.exprEvalCacheEnabled ||
        selectivityVectorPool_.empty());
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
    VELOX_CHECK(
        optimizationParams_.exprEvalCacheEnabled ||
        selectivityVectorPool_.empty());
    if (selectivityVectorPool_.empty()) {
      return std::make_unique<SelectivityVector>();
    }
    auto vector = std::move(selectivityVectorPool_.back());
    selectivityVectorPool_.pop_back();
    return vector;
  }

  // Returns true if the vector was moved into the pool.
  bool releaseSelectivityVector(std::unique_ptr<SelectivityVector>&& vector) {
    if (optimizationParams_.exprEvalCacheEnabled) {
      selectivityVectorPool_.push_back(std::move(vector));
      return true;
    }
    return false;
  }

  std::unique_ptr<DecodedVector> getDecodedVector() {
    VELOX_CHECK(
        optimizationParams_.exprEvalCacheEnabled || decodedVectorPool_.empty());
    if (decodedVectorPool_.empty()) {
      return std::make_unique<DecodedVector>();
    }
    auto vector = std::move(decodedVectorPool_.back());
    decodedVectorPool_.pop_back();
    return vector;
  }

  // Returns true if the vector was moved into the pool.
  bool releaseDecodedVector(std::unique_ptr<DecodedVector>&& vector) {
    if (optimizationParams_.exprEvalCacheEnabled) {
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

  const OptimizationParams& optimizationParams() const {
    return optimizationParams_;
  }

 private:
  // Pool for all Buffers for this thread.
  memory::MemoryPool* const pool_;
  QueryCtx* const queryCtx_;

  const OptimizationParams optimizationParams_;
  // A pool of preallocated DecodedVectors for use by expressions and
  // operators.
  std::vector<std::unique_ptr<DecodedVector>> decodedVectorPool_;
  // A pool of preallocated SelectivityVectors for use by expressions
  // and operators.
  std::vector<std::unique_ptr<SelectivityVector>> selectivityVectorPool_;
  std::unique_ptr<VectorPool> vectorPool_;
};

} // namespace facebook::velox::core
