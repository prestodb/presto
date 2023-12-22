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
  if (statType == velox::StatType::COUNT) {
    counterMetrics.emplace(key, 0);
  }
  if (statType == velox::StatType::SUM) {
    gaugeMetrics.emplace(key, std::vector<std::shared_ptr<Gauge>>());
  }
}

const uint64_t getCurrentEpochTimestamp() {
  auto p1 = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::seconds>(p1.time_since_epoch())
      .count();
}

void PrometheusStatsReporter::addMetricValue(const char* key, size_t value)
    const {
  auto it = registeredStats_.find(key);
  if (it == registeredStats_.end()) {
    VLOG(2) << "addMetricValue() for unregistered stat";
    return;
  }
  if (it->second == facebook::velox::StatType::COUNT) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto countItr = counterMetrics.find(key);
    if (countItr == counterMetrics.end()) {
      counterMetrics.emplace(key, 1);
      return;
    }
    countItr->second++;
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  std::shared_ptr<Gauge> metric = std::make_shared<Gauge>();
  metric->timestamp = getCurrentEpochTimestamp();
  metric->value = value;
  gaugeMetrics[key].push_back(std::move(metric));
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
  for (const auto gauge : gaugeMetrics) {
    auto metricName = gauge.first.substr(gauge.first.find(".") + 1);
    ss << "# HELP " << metricName << std::endl;
    ss << "# TYPE " << metricName << " gauge" << std::endl;
    for (auto metric : gauge.second) {
      ss << metricName << "{cluster=\"" << cluster_ << "\""
         << ",worker=\"" << workerPod_ << "\"} " << metric->value << " "
         << metric->timestamp * 1000 << std::endl;
    }
    std::vector<std::shared_ptr<Gauge>> metricsVector = gauge.second;
    metricsVector.clear();
  }

  for (const auto counter : counterMetrics) {
    auto metricName = counter.first.substr(counter.first.find(".") + 1);
    ss << "# HELP " << metricName << std::endl;
    ss << "# TYPE " << metricName << " "
       << "counter" << std::endl;
    ss << metricName << "{cluster=\"" << cluster_ << "\",worker=\""
       << workerPod_ << "\"} " << counter.second << std::endl;
  }
  return ss.str();
}

// Initialize singleton for the reporter
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
  return new PrometheusStatsReporter();
});
} // namespace facebook::presto
