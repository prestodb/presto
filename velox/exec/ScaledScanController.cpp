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
#include "velox/exec/ScaledScanController.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

ScaledScanController::ScaledScanController(
    memory::MemoryPool* nodePool,
    uint32_t numDrivers,
    double scaleUpMemoryUsageRatio)
    : queryPool_(nodePool->root()),
      nodePool_(nodePool),
      numDrivers_(numDrivers),
      scaleUpMemoryUsageRatio_(scaleUpMemoryUsageRatio),
      driverPromises_(numDrivers_) {
  VELOX_CHECK_NOT_NULL(queryPool_);
  VELOX_CHECK_NOT_NULL(nodePool_);
  VELOX_CHECK_GT(numDrivers_, 0);
  VELOX_CHECK_GE(scaleUpMemoryUsageRatio_, 0.0);
  VELOX_CHECK_LE(scaleUpMemoryUsageRatio_, 1.0);
}

bool ScaledScanController::shouldStop(
    uint32_t driverIdx,
    facebook::velox::ContinueFuture* future) {
  VELOX_CHECK_LT(driverIdx, numDrivers_);

  std::lock_guard<std::mutex> l(lock_);
  if (closed_) {
    return false;
  }
  return shouldStopLocked(driverIdx, future);
}

bool ScaledScanController::shouldStopLocked(
    uint32_t driverIdx,
    facebook::velox::ContinueFuture* future) {
  VELOX_CHECK(!closed_);
  if (driverIdx < numRunningDrivers_) {
    return false;
  }

  VELOX_CHECK(!driverPromises_[driverIdx].has_value());
  auto [driverPromise, driverFuture] = makeVeloxContinuePromiseContract(
      fmt::format("Table scan driver {} scale promise", driverIdx));
  driverPromises_[driverIdx] = std::move(driverPromise);
  *future = std::move(driverFuture);
  return true;
}

void ScaledScanController::updateAndTryScale(
    uint32_t driverIdx,
    uint64_t memoryUsage) {
  VELOX_CHECK_LT(driverIdx, numDrivers_);

  std::optional<ContinuePromise> driverPromise;
  SCOPE_EXIT {
    if (driverPromise.has_value()) {
      driverPromise->setValue();
    }
  };
  {
    std::lock_guard<std::mutex> l(lock_);
    if (closed_) {
      return;
    }

    VELOX_CHECK_LT(driverIdx, numRunningDrivers_);

    updateDriverScanUsageLocked(driverIdx, memoryUsage);

    tryScaleLocked(driverPromise);
  }
}

void ScaledScanController::updateDriverScanUsageLocked(
    uint32_t driverIdx,
    uint64_t memoryUsage) {
  if (estimatedDriverUsage_ == 0) {
    estimatedDriverUsage_ = memoryUsage;
  } else {
    estimatedDriverUsage_ = (estimatedDriverUsage_ * 3 + memoryUsage) / 4;
  }

  if (numDriverReportedUsage_ == numRunningDrivers_) {
    return;
  }
  VELOX_CHECK_EQ(numDriverReportedUsage_ + 1, numRunningDrivers_);

  if (driverIdx + 1 < numRunningDrivers_) {
    return;
  }
  VELOX_CHECK_EQ(driverIdx, numRunningDrivers_ - 1);
  ++numDriverReportedUsage_;
}

void ScaledScanController::tryScaleLocked(
    std::optional<ContinuePromise>& driverPromise) {
  VELOX_CHECK_LE(numDriverReportedUsage_, numRunningDrivers_);

  if (numRunningDrivers_ == numDrivers_) {
    return;
  }
  if (numDriverReportedUsage_ < numRunningDrivers_) {
    // We shall only make the next scale up decision until we have received
    // the memory usage updates from all the running scan drivers.
    return;
  }

  const uint64_t peakNodeUsage = nodePool_->peakBytes();
  const uint64_t estimatedPeakNodeUsageAfterScale = std::max(
      estimatedDriverUsage_ * (numRunningDrivers_ + 1),
      peakNodeUsage + estimatedDriverUsage_);

  const uint64_t currNodeUsage = nodePool_->reservedBytes();
  const uint64_t currQueryUsage = queryPool_->reservedBytes();
  const uint64_t currOtherUsage =
      currQueryUsage > currNodeUsage ? currQueryUsage - currNodeUsage : 0;

  const uint64_t estimatedQueryUsageAfterScale = std::max(
      currQueryUsage + estimatedDriverUsage_,
      currOtherUsage + estimatedPeakNodeUsageAfterScale);

  const uint64_t maxQueryCapacity = queryPool_->maxCapacity();
  if (estimatedQueryUsageAfterScale >
      maxQueryCapacity * scaleUpMemoryUsageRatio_) {
    return;
  }

  scaleUpLocked(driverPromise);
}

void ScaledScanController::scaleUpLocked(
    std::optional<ContinuePromise>& driverPromise) {
  VELOX_CHECK_LT(numRunningDrivers_, numDrivers_);

  ++numRunningDrivers_;
  if (driverPromises_[numRunningDrivers_ - 1].has_value()) {
    driverPromise = std::move(driverPromises_[numRunningDrivers_ - 1]);
    driverPromises_[numRunningDrivers_ - 1].reset();
  }
}

ScaledScanController::~ScaledScanController() {
  close();
}

bool ScaledScanController::close() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(lock_);
    if (closed_) {
      return false;
    }

    promises.reserve(driverPromises_.size());
    for (auto& promise : driverPromises_) {
      if (promise.has_value()) {
        promises.emplace_back(std::move(promise.value()));
        promise.reset();
      }
    }
    closed_ = true;
  }

  for (auto& promise : promises) {
    promise.setValue();
  }
  return true;
}

std::string ScaledScanController::Stats::toString() const {
  return fmt::format("numRunningDrivers: {}", numRunningDrivers);
}
} // namespace facebook::velox::exec
