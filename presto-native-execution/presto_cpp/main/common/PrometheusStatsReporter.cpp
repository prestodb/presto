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

#include "PrometheusStatsReporter.h"

namespace facebook::presto {

void PrometheusStatsReporter::registerMetricExportType(
    folly::StringPiece key,
    facebook::velox::StatType statType) const {
  registerMetricExportType(key.start(), statType);
}

void PrometheusStatsReporter::registerMetricExportType(
    const char* key,
    facebook::velox::StatType statType) const {
  std::lock_guard<std::mutex> lock(mutex_);
  registeredStats_.emplace(key, statType);
  metricsMap_.emplace(key, 0);
}

const uint64_t getCurrentEpochTimestamp() {
  auto p1 = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::seconds>(p1.time_since_epoch())
      .count();
}

void PrometheusStatsReporter::addMetricValue(const char* key, size_t value)
    const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = registeredStats_.find(key);
  if (it == registeredStats_.end()) {
    VLOG(1) << "addMetricValue() for unregistered stat " << key;
    return;
  }
  if (it->second == facebook::velox::StatType::COUNT) {
    // increment the counter.
    metricsMap_[key]++;
    return;
  }
  // Gauge type metric value must be reset.
  metricsMap_[key] = value;
}

void PrometheusStatsReporter::addMetricValue(
    const std::string& key,
    size_t value) const {
  addMetricValue(key.c_str(), value);
}

void PrometheusStatsReporter::addMetricValue(
    folly::StringPiece key,
    size_t value) const {
  addMetricValue(key.start(), value);
}

const std::string PrometheusStatsReporter::getMetricsForPrometheus() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::stringstream ss;
  for (const auto metric : metricsMap_) {
    auto metricName = metric.first;
    std::replace(metricName.begin(), metricName.end(), '.', '_');
    auto statType = registeredStats_[metric.first];
    ss << "# HELP " << metricName << std::endl;
    std::string statTypeStr = "gauge";
    if (statType == facebook::velox::StatType::COUNT) {
      statTypeStr = "counter";
    }
    ss << "# TYPE " << metricName << " " << statTypeStr << std::endl;
    ss << metricName << "{cluster=\"" << cluster_ << "\""
       << ",worker=\"" << workerPod_ << "\"} " << metric.second << std::endl;
  }
  return ss.str();
}

// Initialize singleton for the reporter
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
  return new PrometheusStatsReporter();
});
} // namespace facebook::presto
