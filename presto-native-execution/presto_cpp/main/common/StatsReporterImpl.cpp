/*
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

#include "StatsReporterImpl.h"

namespace facebook::presto {

void StatsReporterImpl::registerMetricExportType(
    folly::StringPiece key,
    facebook::velox::StatType statType) const {
  registerMetricExportType(key.start(), statType);
}

void StatsReporterImpl::registerMetricExportType(
    const char* key,
    facebook::velox::StatType statType) const {
  std::lock_guard<std::mutex> lock(mutex_);
  registeredStats_.emplace(key, statType);
  metricsMap_.emplace(key, 0);
}

void StatsReporterImpl::addMetricValue(const char* key, size_t value) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = registeredStats_.find(key);
  if (it == registeredStats_.end()) {
    VLOG(1) << "addMetricValue() for unregistered stat " << key;
    return;
  }
  if (it->second == facebook::velox::StatType::COUNT) {
    // increment the counter.
    metricsMap_[key] += value;
    return;
  }
  // Gauge type metric value must be reset.
  metricsMap_[key] = value;
}

void StatsReporterImpl::addMetricValue(const std::string& key, size_t value)
    const {
  addMetricValue(key.c_str(), value);
}

void StatsReporterImpl::addMetricValue(folly::StringPiece key, size_t value)
    const {
  addMetricValue(key.start(), value);
}

const std::string StatsReporterImpl::getMetrics(
    const MetricsSerializer& serializer) {
  std::lock_guard<std::mutex> lock(mutex_);
  return serializer.serialize(registeredStats_, metricsMap_);
}

// Initialize singleton for the reporter
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
  return new StatsReporterImpl();
});
} // namespace facebook::presto
