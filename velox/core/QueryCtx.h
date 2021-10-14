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
#include "velox/common/memory/MappedMemory.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/CancelPool.h"
#include "velox/core/Context.h"
#include "velox/core/QueryConfig.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::core {

class QueryCtx : public Context {
 public:
  static std::shared_ptr<QueryCtx> create() {
    return create(
        std::make_shared<MemConfig>(), {}, memory::MappedMemory::getInstance());
  }

  static std::shared_ptr<QueryCtx> create(
      std::shared_ptr<Config> config,
      std::unordered_map<std::string, std::shared_ptr<Config>> connectorConfigs,
      memory::MappedMemory* mappedMemory,
      std::unique_ptr<memory::MemoryPool> pool =
          memory::getProcessDefaultMemoryManager().getRoot().addScopedChild(
              kQueryRootMemoryPool),
      std::shared_ptr<folly::Executor> executor = nullptr) {
    return std::make_shared<QueryCtx>(
        config,
        connectorConfigs,
        mappedMemory,
        std::move(pool),
        std::move(executor));
  }

  // TODO: Make constructors private once presto_cpp
  // is updated to use factory methods.
  QueryCtx()
      : QueryCtx(
            std::make_shared<MemConfig>(),
            {},
            memory::MappedMemory::getInstance(),
            memory::getProcessDefaultMemoryManager().getRoot().addScopedChild(
                kQueryRootMemoryPool)) {}

  explicit QueryCtx(const std::shared_ptr<Config>& config)
      : QueryCtx(
            config,
            {},
            memory::MappedMemory::getInstance(),
            memory::getProcessDefaultMemoryManager().getRoot().addScopedChild(
                kQueryRootMemoryPool)) {}

  QueryCtx(
      std::shared_ptr<Config> config,
      std::unordered_map<std::string, std::shared_ptr<Config>> connectorConfigs,
      memory::MappedMemory* mappedMemory,
      std::unique_ptr<memory::MemoryPool> pool =
          memory::getProcessDefaultMemoryManager().getRoot().addScopedChild(
              kQueryRootMemoryPool),
      std::shared_ptr<folly::Executor> executor = nullptr)
      : Context{ContextScope::QUERY},
        pool_(std::move(pool)),
        mappedMemory_(mappedMemory),
        connectorConfigs_(connectorConfigs),
        executor_{std::move(executor)},
        config_{this} {
    setConfigOverrides(config);
  }

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  memory::MappedMemory* mappedMemory() const {
    return mappedMemory_;
  }

  folly::Executor* executor() const {
    return executor_.get();
  }

  const QueryConfig& config() const {
    return config_;
  }

  Config* getConnectorConfig(const std::string& connectorId) const {
    auto it = connectorConfigs_.find(connectorId);
    if (it == connectorConfigs_.end()) {
      return getEmptyConfig();
    }
    return it->second.get();
  }

  // Multiple logical servers (hosts) can be colocated in one
  // process. This returns the logical host on behalf of which the
  // Task referencing this is running. This is used as a key to select
  // the appropriate host-level singleton resource for different
  // purposes, e.g. memory or outgoing exchange buffers.
  std::string host() const {
    static std::string local = "local";
    return get<std::string>("host", local);
  }

  // Overrides the previous configuration. Note that this function is NOT
  // thread-safe and should probably only be used in tests.
  void setConfigOverridesUnsafe(
      std::unordered_map<std::string, std::string>&& configOverrides) {
    setConfigOverrides(
        std::make_shared<const MemConfig>(std::move(configOverrides)));
  }

 private:
  static Config* getEmptyConfig() {
    static const std::unique_ptr<Config> kEmptyConfig =
        std::make_unique<MemConfig>();
    return kEmptyConfig.get();
  }

  static constexpr const char* kQueryRootMemoryPool = "query_root";

  CancelPoolPtr cancelPool_;
  std::unique_ptr<memory::MemoryPool> pool_;
  memory::MappedMemory* mappedMemory_;
  std::unordered_map<std::string, std::shared_ptr<Config>> connectorConfigs_;
  std::shared_ptr<folly::Executor> executor_;
  QueryConfig config_;
};

// Represents the state of one thread of query execution.
class ExecCtx : public Context {
 public:
  ExecCtx(memory::MemoryPool* pool, QueryCtx* queryCtx)
      : Context{ContextScope::QUERY}, pool_(pool), queryCtx_(queryCtx) {}

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  QueryCtx* queryCtx() const {
    return queryCtx_;
  }

  /// Returns a SelectivityVector from a pool. Allocates new one if none is
  /// available. Make sure to call 'releaseSelectivityVector' when done using
  /// the vector to allow for reuse.
  ///
  /// Prefer using LocalSelectivityVector which takes care of returning the
  /// vector to the pool on destruction.
  std::unique_ptr<SelectivityVector> getSelectivityVector(int32_t size) {
    if (selectivityVectorPool_.empty()) {
      return std::make_unique<SelectivityVector>(size);
    }
    auto vector = std::move(selectivityVectorPool_.back());
    selectivityVectorPool_.pop_back();
    vector->resize(size);
    return vector;
  }

  void releaseSelectivityVector(std::unique_ptr<SelectivityVector>&& vector) {
    selectivityVectorPool_.push_back(std::move(vector));
  }

  std::unique_ptr<DecodedVector> getDecodedVector() {
    if (decodedVectorPool_.empty()) {
      return std::make_unique<DecodedVector>();
    }
    auto vector = std::move(decodedVectorPool_.back());
    decodedVectorPool_.pop_back();
    return vector;
  }

  void releaseDecodedVector(std::unique_ptr<DecodedVector>&& vector) {
    decodedVectorPool_.push_back(std::move(vector));
  }

 private:
  // Pool for all Buffers for this thread
  memory::MemoryPool* pool_;
  QueryCtx* queryCtx_;
  // A pool of preallocated DecodedVectors for use by expressions and operators.
  std::vector<std::unique_ptr<DecodedVector>> decodedVectorPool_;
  // A pool of preallocated SelectivityVectors for use by expressions
  // and operators.
  std::vector<std::unique_ptr<SelectivityVector>> selectivityVectorPool_;
};

} // namespace facebook::velox::core
