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

#include "velox/common/base/PeriodicStatsReporter.h"
#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox {

namespace {
#define REPORT_IF_NOT_ZERO(name, counter)   \
  if ((counter) != 0) {                     \
    RECORD_METRIC_VALUE((name), (counter)); \
  }
} // namespace

PeriodicStatsReporter::PeriodicStatsReporter(
    const velox::memory::MemoryArbitrator* arbitrator,
    const Options& options)
    : arbitrator_(arbitrator), options_(options) {}

void PeriodicStatsReporter::start() {
  LOG(INFO) << "Starting PeriodicStatsReporter with options "
            << options_.toString();
  addTask(
      "report_arbitrator_stats",
      [this]() { reportArbitratorStats(); },
      options_.arbitratorStatsIntervalMs);
}

void PeriodicStatsReporter::stop() {
  LOG(INFO) << "Stopping PeriodicStatsReporter";
  scheduler_.stop();
}

void PeriodicStatsReporter::reportArbitratorStats() {
  if (arbitrator_ == nullptr) {
    return;
  }

  const auto stats = arbitrator_->stats();
  RECORD_METRIC_VALUE(
      kMetricArbitratorFreeCapacityBytes,
      stats.freeCapacityBytes + stats.freeReservedCapacityBytes);
  RECORD_METRIC_VALUE(
      kMetricArbitratorFreeReservedCapacityBytes,
      stats.freeReservedCapacityBytes);
}

} // namespace facebook::velox
