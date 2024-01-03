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

#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/QueryCtx.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::memory;

namespace facebook::velox::exec::test {

std::shared_ptr<core::QueryCtx> newQueryCtx(
    const std::unique_ptr<MemoryManager>& memoryManager,
    const std::shared_ptr<folly::Executor>& executor,
    int64_t memoryCapacity,
    std::unique_ptr<MemoryReclaimer>&& reclaimer) {
  std::unordered_map<std::string, std::shared_ptr<Config>> configs;
  std::shared_ptr<MemoryPool> pool = memoryManager->addRootPool(
      "",
      memoryCapacity,
      reclaimer != nullptr ? std::move(reclaimer) : MemoryReclaimer::create());
  auto queryCtx = std::make_shared<core::QueryCtx>(
      executor.get(),
      core::QueryConfig({}),
      configs,
      cache::AsyncDataCache::getInstance(),
      std::move(pool));
  return queryCtx;
}

std::unique_ptr<memory::MemoryManager> createMemoryManager() {
  memory::MemoryManagerOptions options;
  options.allocatorCapacity = kMemoryCapacity;
  options.arbitratorKind = "SHARED";
  options.memoryPoolInitCapacity = kMemoryPoolInitCapacity;
  options.memoryPoolTransferCapacity = kMemoryPoolTransferCapacity;
  options.memoryReclaimWaitMs = 0;
  options.checkUsageLeak = true;
  options.arbitrationStateCheckCb = memoryArbitrationStateCheck;
  return std::make_unique<memory::MemoryManager>(options);
}

} // namespace facebook::velox::exec::test
