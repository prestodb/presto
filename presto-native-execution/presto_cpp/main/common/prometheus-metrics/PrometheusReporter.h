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
#include <prometheus/registry.h>
#include "presto_cpp/main/common/StatsReporterImpl.h"
#include "prometheus/collectable.h"
#include "prometheus/counter.h"
#include "prometheus/gauge.h"
#include "prometheus/histogram.h"
#include "prometheus/summary.h"
#include "prometheus/text_serializer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/velox/common/base/GTestMacros.h"

namespace facebook::presto::prometheus {
using CounterMap = std::unordered_map<std::string, ::prometheus::Counter&>;
using GaugeMap = std::unordered_map<std::string, ::prometheus::Gauge&>;
using HistogramMap = std::unordered_map<std::string, ::prometheus::Histogram&>;
using SummaryMap = std::unordered_map<std::string, ::prometheus::Summary&>;

static constexpr std::string_view kSummarySuffix("_summary");

class PrometheusReporter : public facebook::velox::BaseStatsReporter {
 public:
  explicit PrometheusReporter(const ::prometheus::Labels& labels)
      : labels_(labels) {
    registry_ = std::make_shared<::prometheus::Registry>();
  }
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

  std::string getSerializedMetrics() const;

 private:
  mutable std::mutex mutex_;
  // A map of labels assigned to each metric which helps in filtering at client
  // end.
  const ::prometheus::Labels labels_;
  std::shared_ptr<::prometheus::Registry> registry_;
  // Maintaining maps for each metric type is for convenience,
  // the ::prometheus::Registry allows us to Add new metric object each time
  // which overwrites the existing metric object in its internal mapping.
  mutable CounterMap countersMap_;
  mutable GaugeMap gaugeMap_;
  mutable HistogramMap histogramMap_;
  mutable SummaryMap summaryMap_;
  mutable std::unordered_map<std::string, ::facebook::velox::StatType>
      registeredMetrics_;
  // @TODO: may be add StatType::HISTOGRAM to avoid this member.
  mutable std::unordered_set<std::string> registeredHistogramMetrics_;
  VELOX_FRIEND_TEST(PrometheusReporterTest, testAllMetrics);
}; // class PrometheusReporter
}; // namespace facebook::presto::prometheus