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

#include "velox/common/memory/Memory.h"

namespace facebook::velox::exec {

namespace test {
class ScaledScanControllerTestHelper;
}

/// Controller used to scales table scan processing based on the query memory
/// usage.
class ScaledScanController {
 public:
  /// 'nodePool' is the table scan node pool. 'numDrivers' is number of the
  /// table scan drivers. 'scaleUpMemoryUsageRatio' specifies the memory usage
  /// ratio used to make scan scale up decision.
  ScaledScanController(
      memory::MemoryPool* nodePool,
      uint32_t numDrivers,
      double scaleUpMemoryUsageRatio);

  ~ScaledScanController();

  ScaledScanController(const ScaledScanController&) = delete;
  ScaledScanController(ScaledScanController&&) = delete;
  ScaledScanController& operator=(const ScaledScanController&) = delete;
  ScaledScanController& operator=(ScaledScanController&&) = delete;

  /// Invoked by a scan operator to check if it needs to stop waiting for scan
  /// up to start processing. If so, 'future' is set to a future that will be
  /// ready when the controller decides to start this scan operator processing,
  /// or all the splits from the scan node have been dispatched to finish all
  /// the scan operators. 'driverIcx' is the driver id of the scan operator.
  /// Initially, only the first scan operator at driver index 0 is allowed to
  /// run.
  bool shouldStop(uint32_t driverIdx, ContinueFuture* future);

  /// Invoked by a scan operator to update per-driver memory usage estimation
  /// after finish processing a non-empty split. 'driverIdx' is the driver id of
  /// the scan operator. 'driverMemoryUsage' is the peak memory usage of the
  /// scan operator.
  void updateAndTryScale(uint32_t driverIdx, uint64_t driverMemoryUsage);

  struct Stats {
    uint32_t numRunningDrivers{0};

    std::string toString() const;
  };

  Stats stats() const {
    std::lock_guard<std::mutex> l(lock_);
    return {.numRunningDrivers = numRunningDrivers_};
  }

  /// Invoked by the closed scan operator to close the controller. It returns
  /// true on the first invocation, and otherwise false.
  bool close();

  void testingSetMemoryRatio(double scaleUpMemoryUsageRatio) {
    std::lock_guard<std::mutex> l(lock_);
    *const_cast<double*>(&scaleUpMemoryUsageRatio_) = scaleUpMemoryUsageRatio;
  }

 private:
  // Invoked to check if we can scale up scan processing. If so, call
  // 'scaleUpLocked' for scale up processing.
  void tryScaleLocked(std::optional<ContinuePromise>& driverePromise);

  // Invoked to scale up scan processing by bumping up the number of running
  // scan drivers by one. 'drverPromise' returns the promise to fulfill if the
  // scaled scan driver has been stopped.
  void scaleUpLocked(std::optional<ContinuePromise>& driverePromise);

  // Invoked to check if we need to stop waiting for scale up processing of the
  // specified scan driver. If 'driverIdx' is beyond 'numRunningDrivers_', then
  // a promise is set in 'driverPromises_' and the associated future is returned
  // in 'future'.
  bool shouldStopLocked(
      uint32_t driverIdx,
      facebook::velox::ContinueFuture* future);

  /// Invoked to update the per-driver memory usage estimation with a new driver
  /// report.
  void updateDriverScanUsageLocked(uint32_t driverIdx, uint64_t memoryUsage);

  memory::MemoryPool* const queryPool_;
  memory::MemoryPool* const nodePool_;
  const uint32_t numDrivers_;
  const double scaleUpMemoryUsageRatio_;

  mutable std::mutex lock_;
  uint32_t numRunningDrivers_{1};

  // The estimated per-driver memory usage of the table scan node.
  uint64_t estimatedDriverUsage_{0};

  // The number of drivers that have reported memory usage.
  uint32_t numDriverReportedUsage_{0};

  // The driver resume promises with one per each driver index.
  std::vector<std::optional<ContinuePromise>> driverPromises_;

  bool closed_{false};

  friend class test::ScaledScanControllerTestHelper;
};
} // namespace facebook::velox::exec
