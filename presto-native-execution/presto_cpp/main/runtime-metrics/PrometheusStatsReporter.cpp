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

#include "presto_cpp/main/runtime-metrics/PrometheusStatsReporter.h"

#include <prometheus/collectable.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/summary.h>
#include <prometheus/text_serializer.h>

namespace facebook::presto::prometheus {

// Initialize singleton for the reporter
folly::Singleton<facebook::velox::BaseStatsReporter> reporter(
    []() -> facebook::velox::BaseStatsReporter* {
      return facebook::presto::prometheus::PrometheusStatsReporter::
          createPrometheusReporter()
              .release();
    });

static constexpr std::string_view kSummarySuffix("_summary");

struct PrometheusStatsReporter::PrometheusImpl {
  explicit PrometheusImpl(const ::prometheus::Labels& labels) {
    registry = std::make_shared<::prometheus::Registry>();
    for (const auto& itr : labels) {
      this->labels[itr.first] = itr.second;
    }
  }

  ::prometheus::Labels labels;
  std::shared_ptr<::prometheus::Registry> registry;
};

PrometheusStatsReporter::PrometheusStatsReporter(
    const std::map<std::string, std::string>& labels, int numThreads)
    : executor_(std::make_shared<folly::CPUThreadPoolExecutor>(numThreads)),
      impl_(std::make_shared<PrometheusImpl>(labels)) {
}

void PrometheusStatsReporter::registerMetricExportType(
    const char* key,
    facebook::velox::StatType statType) const {
  if (registeredMetricsMap_.find(key) != registeredMetricsMap_.end()) {
    VLOG(1) << "Trying to register already registered metric " << key;
    return;
  }
  // '.' is replaced with '_'.
  std::string sanitizedMetricKey = std::string(key);
  std::replace(sanitizedMetricKey.begin(), sanitizedMetricKey.end(), '.', '_');
  switch (statType) {
    case facebook::velox::StatType::COUNT: {
      // A new MetricFamily object is built for every new metric key.
      auto& counterFamily = ::prometheus::BuildCounter()
                                .Name(sanitizedMetricKey)
                                .Register(*impl_->registry);
      auto& counter = counterFamily.Add(impl_->labels);
      registeredMetricsMap_.insert({key, StatsInfo{statType, &counter}});
    } break;
    case facebook::velox::StatType::SUM:
    case facebook::velox::StatType::AVG:
    case facebook::velox::StatType::RATE: {
      auto& gaugeFamily = ::prometheus::BuildGauge()
                              .Name(sanitizedMetricKey)
                              .Register(*impl_->registry);
      auto& gauge = gaugeFamily.Add(impl_->labels);
      registeredMetricsMap_.insert(
          std::string(key), StatsInfo{statType, &gauge});
    } break;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported metric type {}", velox::statTypeString(statType));
  }
}

void PrometheusStatsReporter::registerMetricExportType(
    folly::StringPiece key,
    facebook::velox::StatType statType) const {
  registerMetricExportType(key.toString().c_str(), statType);
}

void PrometheusStatsReporter::registerHistogramMetricExportType(
    const char* key,
    int64_t bucketWidth,
    int64_t min,
    int64_t max,
    const std::vector<int32_t>& pcts) const {
  if (registeredMetricsMap_.find(key) != registeredMetricsMap_.end()) {
    // Already registered;
    VLOG(1) << "Trying to register already registered metric " << key;
    return;
  }
  auto numBuckets = (max - min) / bucketWidth;
  auto bound = min + bucketWidth;
  std::string sanitizedMetricKey = std::string(key);
  // '.' is replaced with '_'.
  std::replace(sanitizedMetricKey.begin(), sanitizedMetricKey.end(), '.', '_');

  auto& histogramFamily = ::prometheus::BuildHistogram()
                              .Name(sanitizedMetricKey)
                              .Register(*impl_->registry);

  ::prometheus::Histogram::BucketBoundaries bucketBoundaries;
  while (numBuckets > 0) {
    bucketBoundaries.push_back(bound);
    bound += bucketWidth;
    numBuckets--;
  }
  VELOX_CHECK_GE(bucketBoundaries.size(), 1);
  auto& histogramMetric = histogramFamily.Add(impl_->labels, bucketBoundaries);

  registeredMetricsMap_.insert(
      key, StatsInfo{velox::StatType::HISTOGRAM, &histogramMetric});
  // If percentiles are provided, create a Summary type metric and register.
  if (pcts.size() > 0) {
    auto summaryMetricKey = sanitizedMetricKey + std::string(kSummarySuffix);
    auto& summaryFamily = ::prometheus::BuildSummary()
                              .Name(summaryMetricKey)
                              .Register(*impl_->registry);
    ::prometheus::Summary::Quantiles quantiles;
    for (auto pct : pcts) {
      quantiles.push_back(::prometheus::detail::CKMSQuantiles::Quantile(
          pct / static_cast<double>(100), 0));
    }
    auto& summaryMetric = summaryFamily.Add({impl_->labels}, quantiles);
    registeredMetricsMap_.insert(
        std::string(key).append(kSummarySuffix),
        StatsInfo{velox::StatType::HISTOGRAM, &summaryMetric});
  }
}

void PrometheusStatsReporter::registerHistogramMetricExportType(
    folly::StringPiece key,
    int64_t bucketWidth,
    int64_t min,
    int64_t max,
    const std::vector<int32_t>& pcts) const {
  registerHistogramMetricExportType(
      key.toString().c_str(), bucketWidth, min, max, pcts);
}

void PrometheusStatsReporter::addMetricValue(
    const std::string& key,
    size_t value) const {
  addMetricValue(key.c_str(), value);
}

void PrometheusStatsReporter::addMetricValue(const char* key, size_t value)
    const {
  executor_->add([this, key = std::string(key), value]() {
    auto metricIterator = registeredMetricsMap_.find(key);
    if (metricIterator == registeredMetricsMap_.end()) {
      VLOG(1) << "addMetricValue called for unregistered metric " << key;
      return;
    }
    auto statsInfo = metricIterator->second;
    switch (statsInfo.statType) {
      case velox::StatType::COUNT: {
        auto* counter =
            reinterpret_cast<::prometheus::Counter*>(statsInfo.metricPtr);
        counter->Increment(static_cast<double>(value));
        break;
      }
      case velox::StatType::SUM: {
        auto* gauge = reinterpret_cast<::prometheus::Gauge*>(statsInfo.metricPtr);
        gauge->Increment(static_cast<double>(value));
        break;
      }
      case velox::StatType::AVG:
      case velox::StatType::RATE: {
        // Overrides the existing state.
        auto* gauge = reinterpret_cast<::prometheus::Gauge*>(statsInfo.metricPtr);
        gauge->Set(static_cast<double>(value));
        break;
      }
      default:
        VELOX_UNSUPPORTED(
            "Unsupported metric type {}",
            velox::statTypeString(statsInfo.statType));
    };
  });
}

void PrometheusStatsReporter::addMetricValue(
    folly::StringPiece key,
    size_t value) const {
  addMetricValue(key.toString().c_str(), value);
}

void PrometheusStatsReporter::addHistogramMetricValue(
    const std::string& key,
    size_t value) const {
  addHistogramMetricValue(key.c_str(), value);
}

void PrometheusStatsReporter::addHistogramMetricValue(
    const char* key,
    size_t value) const {
  executor_->add([this, key = std::string(key), value]() {
    auto metricIterator = registeredMetricsMap_.find(key);
    if (metricIterator == registeredMetricsMap_.end()) {
      VLOG(1) << "addMetricValue for unregistered metric " << key;
      return;
    }
    auto histogram = reinterpret_cast<::prometheus::Histogram*>(
        metricIterator->second.metricPtr);
    histogram->Observe(value);

    std::string summaryKey = std::string(key).append(kSummarySuffix);
    metricIterator = registeredMetricsMap_.find(summaryKey);
    if (metricIterator != registeredMetricsMap_.end()) {
      auto summary = reinterpret_cast<::prometheus::Summary*>(
          metricIterator->second.metricPtr);
      summary->Observe(value);
    }
  });
}

void PrometheusStatsReporter::addHistogramMetricValue(
    folly::StringPiece key,
    size_t value) const {
  addHistogramMetricValue(key.toString().c_str(), value);
}

std::string PrometheusStatsReporter::fetchMetrics() {
  if (registeredMetricsMap_.empty()) {
    return "";
  }
  ::prometheus::TextSerializer serializer;
  // Registry::Collect() acquires lock on a mutex.
  return serializer.Serialize(impl_->registry->Collect());
}

void PrometheusStatsReporter::waitForCompletion() const {
  executor_->join();
}

} // namespace facebook::presto::prometheus
