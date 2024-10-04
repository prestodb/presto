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

#include "velox/common/base/AdmissionController.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/time/Timer.h"

namespace facebook::velox::common {

void AdmissionController::accept(uint64_t resourceUnits) {
  ContinueFuture future;
  uint64_t updatedValue = 0;
  VELOX_CHECK_LE(
      resourceUnits,
      config_.maxLimit,
      "A single request cannot exceed the max limit");
  {
    std::lock_guard<std::mutex> l(mu_);
    if (unitsUsed_ + resourceUnits > config_.maxLimit) {
      auto [unblockPromise, unblockFuture] = makeVeloxContinuePromiseContract();
      Request req;
      req.unitsRequested = resourceUnits;
      req.promise = std::move(unblockPromise);
      queue_.push_back(std::move(req));
      future = std::move(unblockFuture);
    } else {
      updatedValue = unitsUsed_ += resourceUnits;
    }
  }
  if (!future.valid()) {
    // Only upadate if there was no wait, as the releasing thread is responsible
    // for updating the metric.
    if (!config_.resourceUsageAvgMetric.empty()) {
      RECORD_METRIC_VALUE(config_.resourceUsageAvgMetric, updatedValue);
    }
    return;
  }
  if (!config_.resourceQueuedCountMetric.empty()) {
    RECORD_METRIC_VALUE(config_.resourceQueuedCountMetric);
  }
  uint64_t waitTimeUs{0};
  {
    MicrosecondTimer timer(&waitTimeUs);
    future.wait();
  }
  if (!config_.resourceQueuedTimeMsHistogramMetric.empty()) {
    RECORD_HISTOGRAM_METRIC_VALUE(
        config_.resourceQueuedTimeMsHistogramMetric, waitTimeUs / 1'000);
  }
}

void AdmissionController::release(uint64_t resourceUnits) {
  uint64_t updatedValue = 0;
  {
    std::lock_guard<std::mutex> l(mu_);
    VELOX_CHECK_LE(
        resourceUnits,
        unitsUsed_,
        "Cannot release more units than have been acquired");
    unitsUsed_ -= resourceUnits;
    while (!queue_.empty()) {
      auto& request = queue_.front();
      if (unitsUsed_ + request.unitsRequested > config_.maxLimit) {
        break;
      }
      unitsUsed_ += request.unitsRequested;
      request.promise.setValue();
      queue_.pop_front();
    }
    updatedValue = unitsUsed_;
  }
  if (!config_.resourceUsageAvgMetric.empty()) {
    RECORD_METRIC_VALUE(config_.resourceUsageAvgMetric, updatedValue);
  }
}
} // namespace facebook::velox::common
