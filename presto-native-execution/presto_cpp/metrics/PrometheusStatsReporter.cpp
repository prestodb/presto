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
#include "velox/common/base/StatsReporter.h"
#include "prometheus/client_metric.h"
#include "prometheus/counter.h"
#include "prometheus/summary.h"
#include "prometheus/gauge.h"


namespace facebook::presto {

void PrometheusStatsReporter::registerMetricExportType(folly::StringPiece key, facebook::velox::StatType statType) const {
  registerMetricExportType(key.start(), statType);
}


void PrometheusStatsReporter::registerMetricExportType(const char* key, velox::StatType statType) const {
  std::string keyStr = sanitizeMetricName(key);
  statTypeMap.insert({keyStr, statType});
  LOG(INFO) << "Registered metric: " << keyStr << " with type: " << statTypeToString(statType);

  switch (statType) {
    case velox::StatType::AVG:{
      registerSummaryMetricForAverageStatType(keyStr);
      break;
    }
    case velox::StatType::SUM: {
      registerGaugeMetric(keyStr);
      break;
    }
    case velox::StatType::COUNT: {
      registerCounterMetric(keyStr);
      break;
    }
    case velox::StatType::RATE:
      // ToDo: RATE Type is not yet used anywhere
    default:
      LOG(WARNING) << "Unsupported stat type for key: " << keyStr;
      break;
  }
}



void PrometheusStatsReporter::addMetricValue(const char* key, size_t value)
    const {
  addMetricValue(std::string(key), value);
}

void PrometheusStatsReporter::addMetricValue(folly::StringPiece key, size_t value)
    const {
  addMetricValue(key.str(), value);
}

void PrometheusStatsReporter::addMetricValue(const std::string& key, size_t value) const {
  std::string keyStr = sanitizeMetricName(key);
  auto statTypeIt = statTypeMap.find(keyStr);
  if (statTypeIt == statTypeMap.end()) {
    LOG(WARNING) << "Metric not registered: " << keyStr;
    return;
  }

  switch (statTypeIt->second) {
    case velox::StatType::AVG: {
      updateSummaryMetric(keyStr, value);
      break;
    }
    case velox::StatType::SUM: {
      updateGaugeMetric(keyStr, value);
      break;
    }
    case velox::StatType::COUNT: {
      updateCounterMetric(keyStr, value);
      break;
    }
    case velox::StatType::RATE:
      // ToDo: RATE Type is not yet used anywhere
    default:
      LOG(WARNING) << "Unsupported stat type for key: " << keyStr;
      break;
  }
}

void PrometheusStatsReporter::registerSummaryMetricForAverageStatType(const std::string& keyStr) const {
  auto& summaryFamily = prometheus::BuildSummary()
                            .Name(keyStr)
                            .Help("Summary for " + keyStr)
                            .Register(*registry);
  auto& summary = summaryFamily.Add({{"cluster", cluster_}, {"node", node_}, {"host", host_}},
                                    prometheus::Summary::Quantiles{{0.5, 0.05}});
  summaryMap.insert({keyStr, &summary});
}

void PrometheusStatsReporter::registerGaugeMetric(const std::string& keyStr) const {
  auto& gaugeFamily = prometheus::BuildGauge()
                          .Name(keyStr)
                          .Help("Gauge for " + keyStr)
                          .Register(*registry);
  auto& gauge = gaugeFamily.Add({{"cluster", cluster_}, {"node", node_}, {"host", host_}});
  gaugeMap.insert({keyStr, &gauge});
}

void PrometheusStatsReporter::registerCounterMetric(const std::string& keyStr) const {
  auto& counterFamily = prometheus::BuildCounter()
                            .Name(keyStr)
                            .Help("Counter for " + keyStr)
                            .Register(*registry);
  auto& counter = counterFamily.Add({{"cluster", cluster_}, {"node", node_}, {"host", host_}});
  counterMap.insert({keyStr, &counter});
}

void PrometheusStatsReporter::updateSummaryMetric(const std::string& keyStr, size_t value) const {
  auto summaryIt = summaryMap.find(keyStr);
  if (summaryIt != summaryMap.end()) {
    summaryIt->second->Observe(static_cast<double>(value));
  } else {
    LOG(WARNING) << "Summary metric not found: " << keyStr;
  }
}

void PrometheusStatsReporter::updateGaugeMetric(const std::string& keyStr, size_t value) const {
  auto gaugeIt = gaugeMap.find(keyStr);
  if (gaugeIt != gaugeMap.end()) {
    gaugeIt->second->Increment(static_cast<double>(value));
  } else {
    LOG(WARNING) << "Gauge metric not found: " << keyStr;
  }
}

void PrometheusStatsReporter::updateCounterMetric(const std::string& keyStr, size_t value) const {
  auto counterIt = counterMap.find(keyStr);
  if (counterIt != counterMap.end()) {
    counterIt->second->Increment(static_cast<double>(value));
  } else {
    LOG(WARNING) << "Counter metric not found: " << keyStr;
  }
}

std::string PrometheusStatsReporter::sanitizeMetricName(
    const std::string& name) {
  std::string sanitized = name;
  std::replace(sanitized.begin(), sanitized.end(), '.', '_');
  return sanitized;
}
std::string PrometheusStatsReporter::statTypeToString(
    velox::StatType statType) const {
  switch (statType) {
    case velox::StatType::AVG: return "AVG";
    case velox::StatType::SUM: return "SUM";
    case velox::StatType::RATE: return "RATE";
    case velox::StatType::COUNT: return "COUNT";
    default: return "Unknown StatType";
  }
}


} // namespace facebook::presto
