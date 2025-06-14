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

#include "presto_cpp/main/common/Configs.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/StatsReporter.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/concurrency/ConcurrentHashMap.h>


namespace facebook::presto::prometheus {

struct StatsInfo {
  velox::StatType statType;
  void* metricPtr;
};

/// Prometheus CPP library exposes following classes:
/// 1. Registry.
/// 2. Family.
/// 3. Specific metric type classes like Counter, Gauge, Histogram etc.
///
/// A family of metrics will have the same metric name. For example
/// http_latency_ms. Different members of a family will have unique labels.
/// For metric http_latency_ms labels could be {method="GET"}, {method="PUT"},
/// {method="POST"} etc. Prometheus treats {<metric_name>, [labels]} as unique
/// metric object.
class PrometheusStatsReporter : public facebook::velox::BaseStatsReporter {
  class PrometheusImpl;

 public:
  /**
   * @brief Constructor with optional thread count
   * @param labels Labels for metrics.
   * @param numThreads Number of threads in the executor
   */
  explicit PrometheusStatsReporter(
      const std::map<std::string, std::string>& labels, int numThreads);

  void registerMetricExportType(const char* key, velox::StatType)
      const override;

  void registerMetricExportType(folly::StringPiece key, velox::StatType)
      const override;

  void registerHistogramMetricExportType(
      const char* key,
      int64_t bucketWidth,
      int64_t min,
      int64_t max,
      const std::vector<int32_t>& pcts) const override;

  void registerHistogramMetricExportType(
      folly::StringPiece key,
      int64_t bucketWidth,
      int64_t min,
      int64_t max,
      const std::vector<int32_t>& pcts) const override;

  void addMetricValue(const std::string& key, size_t value = 1) const override;

  void addMetricValue(const char* key, size_t value = 1) const override;

  void addMetricValue(folly::StringPiece key, size_t value = 1) const override;

  void addHistogramMetricValue(const std::string& key, size_t value)
      const override;

  void addHistogramMetricValue(const char* key, size_t value) const override;

  void addHistogramMetricValue(folly::StringPiece key, size_t value)
      const override;

  std::string fetchMetrics() override;

  /**
   * Waits for all pending metric updates to complete.
   * This is only used in tests to ensure correct timing.
   */
  void waitForCompletion() const;

  static std::unique_ptr<velox::BaseStatsReporter> createPrometheusReporter() {
    auto nodeConfig = NodeConfig::instance();
    const std::string cluster = nodeConfig->nodeEnvironment();
    const char* hostName = std::getenv("HOSTNAME");
    const std::string worker = !hostName ? "" : hostName;
    std::map<std::string, std::string> labels{
        {"cluster", cluster}, {"worker", worker}};
    return std::make_unique<PrometheusStatsReporter>(labels, nodeConfig->prometheusExecutorThreads());
  }

  // Visible for testing
  mutable folly::ConcurrentHashMap<std::string, StatsInfo> registeredMetricsMap_;

 private:
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<PrometheusImpl> impl_;
  // A map of labels assigned to each metric which helps in filtering at client
  // end.
  VELOX_FRIEND_TEST(PrometheusReporterTest, testCountAndGauge);
  VELOX_FRIEND_TEST(PrometheusReporterTest, testHistogramSummary);
  VELOX_FRIEND_TEST(PrometheusReporterTest, testConcurrentReporting);
};
} // namespace facebook::presto::prometheus
