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

#include "velox/common/memory/MemoryArbitrator.h"

namespace facebook::velox::exec {
/// Provides the default leaf memory reclaimer implementation for velox task
/// execution.
class MemoryReclaimer : public memory::MemoryReclaimer {
 public:
  virtual ~MemoryReclaimer() = default;

  static std::unique_ptr<memory::MemoryReclaimer> create(int32_t priority = 0);

  void enterArbitration() override;

  void leaveArbitration() noexcept override;

  void abort(memory::MemoryPool* pool, const std::exception_ptr& error)
      override;

 protected:
  explicit MemoryReclaimer(int32_t priortity);
};

/// Provides the parallel memory reclaimer implementation for velox task
/// execution. It parallelize the memory reclamation from all its child memory
/// pools.
class ParallelMemoryReclaimer : public memory::MemoryReclaimer {
 public:
  virtual ~ParallelMemoryReclaimer() = default;

  static std::unique_ptr<memory::MemoryReclaimer> create(
      folly::Executor* executor,
      int32_t priority = 0);

  uint64_t reclaim(
      memory::MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      Stats& stats) override;

 protected:
  ParallelMemoryReclaimer(folly::Executor* executor, int32_t priority);

  folly::Executor* const executor_{nullptr};
};

/// Callback used by memory arbitration to check if a driver thread under memory
/// arbitration has been put in suspension state. This is to prevent arbitration
/// deadlock as the arbitrator might reclaim memory from the task of the driver
/// thread which is under arbitration. The task reclaim needs to wait for the
/// drivers to go off thread. A suspended driver thread is not counted as
/// running.
void memoryArbitrationStateCheck(memory::MemoryPool& pool);
} // namespace facebook::velox::exec
