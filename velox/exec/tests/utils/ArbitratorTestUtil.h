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

#include <memory>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/Driver.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec::test {

constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;

constexpr uint64_t kMemoryCapacity = 512 * MB;
constexpr uint64_t kMemoryPoolInitCapacity = 16 * MB;
constexpr uint64_t kMemoryPoolTransferCapacity = 8 * MB;

class FakeMemoryReclaimer : public exec::MemoryReclaimer {
 public:
  FakeMemoryReclaimer() = default;

  static std::unique_ptr<MemoryReclaimer> create() {
    return std::make_unique<FakeMemoryReclaimer>();
  }

  void enterArbitration() override {
    auto* driverThreadCtx = driverThreadContext();
    if (driverThreadCtx == nullptr) {
      return;
    }
    auto* driver = driverThreadCtx->driverCtx.driver;
    ASSERT_TRUE(driver != nullptr);
    if (driver->task()->enterSuspended(driver->state()) != StopReason::kNone) {
      VELOX_FAIL("Terminate detected when entering suspension");
    }
  }

  void leaveArbitration() noexcept override {
    auto* driverThreadCtx = driverThreadContext();
    if (driverThreadCtx == nullptr) {
      return;
    }
    auto* driver = driverThreadCtx->driverCtx.driver;
    ASSERT_TRUE(driver != nullptr);
    driver->task()->leaveSuspended(driver->state());
  }
};

std::shared_ptr<core::QueryCtx> newQueryCtx(
    const std::unique_ptr<facebook::velox::memory::MemoryManager>&
        memoryManager,
    const std::shared_ptr<folly::Executor>& executor,
    int64_t memoryCapacity = facebook::velox::memory::kMaxMemory,
    std::unique_ptr<MemoryReclaimer>&& reclaimer = nullptr);

std::unique_ptr<memory::MemoryManager> createMemoryManager();

} // namespace facebook::velox::exec::test
