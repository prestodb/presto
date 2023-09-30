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

#include "velox/exec/MemoryReclaimer.h"

#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {
std::unique_ptr<memory::MemoryReclaimer> MemoryReclaimer::create() {
  return std::unique_ptr<memory::MemoryReclaimer>(new MemoryReclaimer());
}

void MemoryReclaimer::enterArbitration() {
  DriverThreadContext* driverThreadCtx = driverThreadContext();
  if (FOLLY_UNLIKELY(driverThreadCtx == nullptr)) {
    // Skips the driver suspension handling if this memory arbitration
    // request is not issued from a driver thread.
    return;
  }

  Driver* const driver = driverThreadCtx->driverCtx.driver;
  if (driver->task()->enterSuspended(driver->state()) != StopReason::kNone) {
    // There is no need for arbitration if the associated task has already
    // terminated.
    VELOX_FAIL("Terminate detected when entering suspension");
  }
}

void MemoryReclaimer::leaveArbitration() noexcept {
  DriverThreadContext* driverThreadCtx = driverThreadContext();
  if (FOLLY_UNLIKELY(driverThreadCtx == nullptr)) {
    // Skips the driver suspension handling if this memory arbitration
    // request is not issued from a driver thread.
    return;
  }
  Driver* const driver = driverThreadCtx->driverCtx.driver;
  driver->task()->leaveSuspended(driver->state());
}

void memoryArbitrationStateCheck(memory::MemoryPool& pool) {
  const auto* driverThreadCtx = driverThreadContext();
  if (driverThreadCtx != nullptr) {
    Driver* driver = driverThreadCtx->driverCtx.driver;
    if (!driver->state().isSuspended) {
      VELOX_FAIL(
          "Driver thread is not suspended under memory arbitration processing: {}, request memory pool: {}",
          driver->toString(),
          pool.name());
    }
  }
}
} // namespace facebook::velox::exec
