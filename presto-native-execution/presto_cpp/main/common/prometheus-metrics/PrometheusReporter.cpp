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

#include "PrometheusReporter.h"
namespace facebook::presto::prometheus {
void PrometheusReporter::registerMetricExportType(
    const char* key,
    facebook::velox::StatType statType) const {
  if (registeredMetrics_.count(key)) {
    // Already registered;
    VLOG(1) << "Trying to register already registered metric " << key;
    return;
  }
  // Prometheus format requires '.' to be replaced.
  std::string keyStr = std::string(key);
  std::replace(keyStr.begin(), keyStr.end(), '.', '_');
  switch (statType) {
    case facebook::velox::StatType::COUNT: {
      auto& counterFamily =
          ::prometheus::BuildCounter().Name(keyStr).Register(*registry_);
      auto& counter = counterFamily.Add(labels_);
      countersMap_.emplace(std::string(key), counter);
      break;
    }
    case facebook::velox::StatType::SUM:
    case facebook::velox::StatType::AVG:
    case facebook::velox::StatType::RATE: {
      auto& gaugeFamily =
          ::prometheus::BuildGauge().Name(keyStr).Register(*registry_);
      auto& gauge = gaugeFamily.Add(labels_);
      gaugeMap_.emplace(std::string(key), gauge);
      break;
    }
    default:
      VELOX_UNSUPPORTED(
          "Unsupported metric type {}", std::to_string((int)statType));
  }
  registeredMetrics_.emplace(key, statType);
}

void PrometheusReporter::registerMetricExportType(
    folly::StringPiece key,
    facebook::velox::StatType statType) const {
  registerMetricExportType(key.start(), statType);
}

void PrometheusReporter::registerHistogramMetricExportType(
    const char* key,
    int64_t bucketWidth,
    int64_t min,
    int64_t max,
    const std::vector<int32_t>& pcts) const {
  std::lock_guard<std::mutex> l(mutex_);
  if (registeredHistogramMetrics_.count(key)) {
    // Already registered;
    VLOG(1) << "Trying to register already registered metric " << key;
    return;
  }
  int numBuckets = (max - min) / bucketWidth;
  auto bound = min + bucketWidth;
  std::string keyStr = std::string(key);
  std::replace(keyStr.begin(), keyStr.end(), '.', '_');
  auto& histogramFamily =
      ::prometheus::BuildHistogram().Name(keyStr).Register(*registry_);
  ::prometheus::Histogram::BucketBoundaries bucketBoundaries;
  while (numBuckets) {
    bucketBoundaries.push_back(bound);
    bound += bucketWidth;
    numBuckets--;
  }
  VELOX_CHECK_GE(bucketBoundaries.size(), 1);
  auto& histogramMetric = histogramFamily.Add(labels_, bucketBoundaries);
  histogramMap_.emplace(key, histogramMetric);
  // If percentiles are provided, create a Summary type metric and register.
  if (pcts.size() > 0) {
    auto& summaryFamily = ::prometheus::BuildSummary()
                              .Name(keyStr.append(kSummarySuffix))
                              .Register(*registry_);
    ::prometheus::Summary::Quantiles quantiles;
    for (auto pct : pcts) {
      quantiles.push_back(
          ::prometheus::detail::CKMSQuantiles::Quantile(pct / (double)100, 0));
    }
    auto& summaryMetric = summaryFamily.Add({labels_}, quantiles);
    summaryMap_.emplace(std::string(key), summaryMetric);
  }
  registeredHistogramMetrics_.insert(key);
}

void PrometheusReporter::registerHistogramMetricExportType(
    folly::StringPiece key,
    int64_t bucketWidth,
    int64_t min,
    int64_t max,
    const std::vector<int32_t>& pcts) const {
  registerHistogramMetricExportType(key.begin(), bucketWidth, min, max, pcts);
}

void PrometheusReporter::addMetricValue(const std::string& key, size_t value)
    const {
  addMetricValue(key.c_str(), value);
}

void PrometheusReporter::addMetricValue(const char* key, size_t value) const {
  std::lock_guard<std::mutex> l(mutex_);
  if (!registeredMetrics_.count(key)) {
    VLOG(1) << "addMetricValue for unregistred metric " << key;
    return;
  }
  auto statType = registeredMetrics_.find(key)->second;
  switch (statType) {
    case velox::StatType::COUNT:
      countersMap_.find(key)->second.Increment(value);
      break;
    case velox::StatType::SUM:
    case velox::StatType::AVG:
    case velox::StatType::RATE:
      gaugeMap_.find(key)->second.Set(value);
      break;
  };
}

void PrometheusReporter::addMetricValue(folly::StringPiece key, size_t value)
    const {
  addMetricValue(key.begin(), value);
}

void PrometheusReporter::addHistogramMetricValue(
    const std::string& key,
    size_t value) const {
  addHistogramMetricValue(key.c_str(), value);
}

void PrometheusReporter::addHistogramMetricValue(const char* key, size_t value)
    const {
  std::lock_guard<std::mutex> l(mutex_);
  if (!registeredHistogramMetrics_.count(key)) {
    VLOG(1) << "addMetricValue for unregistered metric " << key;
    return;
  }
  histogramMap_.find(key)->second.Observe(value);
  if (summaryMap_.count(key)) {
    summaryMap_.find(key)->second.Observe(value);
  }
}

void PrometheusReporter::addHistogramMetricValue(
    folly::StringPiece key,
    size_t value) const {
  addHistogramMetricValue(key.begin(), value);
}

std::string PrometheusReporter::getSerializedMetrics() const {
  if (registeredMetrics_.empty() && registeredHistogramMetrics_.empty()) {
    return "";
  }
  ::prometheus::TextSerializer serializer;
  return serializer.Serialize(registry_->Collect());
}
}; // namespace facebook::presto::prometheus