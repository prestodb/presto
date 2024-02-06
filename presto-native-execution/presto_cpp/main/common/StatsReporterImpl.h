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

#include <folly/dynamic.h>
#include <fstream>
#include <iostream>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/velox/common/base/Exceptions.h"

namespace facebook::presto {

class MetricsSerializer {
 public:
  virtual std::string serialize(
      const std::unordered_map<std::string, facebook::velox::StatType>&
          metricStatTypes,
      const std::unordered_map<std::string, size_t>& metricValues) const = 0;
};

namespace prometheus {
using Labels = std::unordered_map<std::string, std::string>;
class PrometheusSerializer : public MetricsSerializer {
 public:
  explicit PrometheusSerializer(const Labels& labels) : labels_(labels) {}

  std::string serialize(
      const std::unordered_map<std::string, facebook::velox::StatType>&
          metricStatTypes,
      const std::unordered_map<std::string, size_t>& metricValues) const {
    std::stringstream ss;
    for (const auto metric : metricValues) {
      auto metricName = metric.first;
      std::replace(metricName.begin(), metricName.end(), '.', '_');
      auto statType = metricStatTypes.find(metric.first)->second;
      ss << "# HELP " << metricName << std::endl;
      std::string statTypeStr = "gauge";
      if (statType == facebook::velox::StatType::COUNT) {
        statTypeStr = "counter";
      }
      ss << "# TYPE " << metricName << " " << statTypeStr << std::endl;
      int i = 0;
      ss << metricName << "{";
      for (auto& label : labels_) {
        ss << label.first << "=\"" << label.second << "\"";
        if (i < labels_.size() - 1) {
          // Comma separated labels.
          ss << ",";
        }
        ++i;
      }
      ss << "} " << metric.second << std::endl;
    }
    return ss.str();
  }

 private:
  // A map of labels assigned to each metric which helps in filtering at client
  // end.
  const Labels labels_;
};
} // namespace prometheus.

/// An implementation of BaseStatsReporter which gathers runtime metrics and
/// and maintains them in-memory. Users can call
/// StatsReporterImpl::getMetrics(MetricSerializer) to get metrics in custom
/// formatted string.
class StatsReporterImpl : public facebook::velox::BaseStatsReporter {
 public:
  StatsReporterImpl(
      const std::string cluster = "",
      const std::string worker = "") {
    if (cluster.empty()) {
      auto nodeConfig = facebook::presto::NodeConfig::instance();
      cluster_ = nodeConfig->nodeEnvironment();
    } else {
      cluster_ = cluster;
    }
    char* hostName = std::getenv("HOSTNAME");
    workerPod_ = !hostName ? worker : hostName;
  }

  /// Register a stat of the given stat type.
  /// @param key The key to identify the stat.
  /// @param statType How the stat is aggregated.
  void registerMetricExportType(
      const char* key,
      facebook::velox::StatType statType) const override;

  void registerMetricExportType(
      folly::StringPiece key,
      facebook::velox::StatType statType) const override;

  void registerHistogramMetricExportType(
      const char* /*key*/,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& /* pcts */) const override {}

  void registerHistogramMetricExportType(
      folly::StringPiece /* key */,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& /* pcts */) const override {}

  void addMetricValue(const std::string& key, size_t value = 1) const override;

  void addMetricValue(const char* key, size_t value = 1) const override;

  void addMetricValue(folly::StringPiece key, size_t value = 1) const override;

  virtual void addHistogramMetricValue(const std::string& key, size_t value)
      const override {}

  virtual void addHistogramMetricValue(const char* key, size_t value)
      const override {}

  virtual void addHistogramMetricValue(folly::StringPiece key, size_t value)
      const override {}

  const facebook::velox::StatType getRegisteredStatType(
      const std::string& metricName) {
    std::lock_guard<std::mutex> lock(mutex_);
    return registeredStats_[metricName];
  }

  /*
   * Serializes the metrics collected so far in the format suitable for
   * back filling Prometheus server.
   *
   * Given a metric name and a set of labels, time series are frequently
   * identified using this notation:
   *
   *       <metric name>{<label name>=<label value>, ...}
   *
   * For example, a time series with the metric name num_tasks_aborted
   * and the labels cluster="<cluster_id>" and worker="worker-id"
   * could be written like this:
   *  # HELP num_tasks_aborted
   *  # TYPE num_tasks_aborted gauge*
   *  num_tasks_aborted{cluster="<cluster_id>", worker="worker-id"} value
   * timestamp
   *
   * Above info is from:
   * https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
   */
  const std::string getMetrics(const MetricsSerializer& serializer);

 private:
  /// Mapping of registered stats key to StatType.
  mutable std::unordered_map<std::string, facebook::velox::StatType>
      registeredStats_;
  /// A mapping from stats key of type COUNT to value.
  mutable std::unordered_map<std::string, size_t> metricsMap_;
  // Mutex to control access to registeredStats_ and metricMap_ members.
  mutable std::mutex mutex_;
  std::string cluster_;
  std::string workerPod_;
}; // class StatsReporterImpl
} // namespace facebook::presto
