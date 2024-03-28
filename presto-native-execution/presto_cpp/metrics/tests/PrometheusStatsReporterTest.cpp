#include <gtest/gtest.h>
#include <folly/init/Init.h>
#include "prometheus/counter.h"
#include "prometheus/summary.h"
#include "prometheus/gauge.h"
#include "presto_cpp/metrics/PrometheusStatsReporter.h"
#include "velox/common/base/StatsReporter.h"

class PrometheusStatsReporterTest : public ::testing::Test {
};

// Derived from StatsReporterTest
TEST_F(PrometheusStatsReporterTest, trivialPReporter) {
  auto reporter = std::dynamic_pointer_cast<facebook::presto::PrometheusStatsReporter>(
      folly::Singleton<facebook::velox::BaseStatsReporter>::try_get());
  DEFINE_METRIC("key1", facebook::velox::StatType::COUNT);
  DEFINE_METRIC("key2",  facebook::velox::StatType::SUM);
  DEFINE_METRIC("key3",  facebook::velox::StatType::AVG);
  //TODO: Turn on when adding histogram
  //  REPORT_ADD_HISTOGRAM_EXPORT_PERCENTILE("key4", 10, 0, 100, 50, 99, 100);

  EXPECT_EQ( facebook::velox::StatType::COUNT, reporter->statTypeMap["key1"]);
  EXPECT_EQ( facebook::velox::StatType::SUM, reporter->statTypeMap["key2"]);
  EXPECT_EQ( facebook::velox::StatType::AVG, reporter->statTypeMap["key3"]);

  //TODO: Turn on when adding histogram
  std::vector<int32_t> expected = {50, 99, 100};
  //  EXPECT_EQ(expected, reporter->histogramPercentilesMap["key4"]);
  EXPECT_TRUE(
      reporter->statTypeMap.find("key5") == reporter->statTypeMap.end());

  RECORD_METRIC_VALUE("key1", 10);
  RECORD_METRIC_VALUE("key1", 11);
  RECORD_METRIC_VALUE("key1", 15);
  RECORD_METRIC_VALUE("key2", 1001);
  RECORD_METRIC_VALUE("key2", 1200);
  RECORD_METRIC_VALUE("key3");
  RECORD_METRIC_VALUE("key3", 1100);
  RECORD_METRIC_VALUE("key3", 5502);
  //TODO: Turn on when adding histogram
  //  REPORT_ADD_HISTOGRAM_VALUE("key4", 50);
  //  REPORT_ADD_HISTOGRAM_VALUE("key4", 100);

  EXPECT_EQ(36, reporter->counterMap["key1"]->Value());
  EXPECT_EQ(2201, reporter->gaugeMap["key2"]->Value());
  // Calculate the average from observed values
  auto summary =   reporter->summaryMap["key3"]->Collect().summary;
  auto observedAvg = summary.sample_sum / summary.sample_count;
  EXPECT_EQ(2201, observedAvg);
  //TODO: Turn on when adding histogram
  //  EXPECT_EQ(100, reporter->gaugeMap["key4"]->Value());
};

// Registering to folly Singleton with intended reporter type
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
  return new facebook::presto::PrometheusStatsReporter();
});


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  facebook::velox::BaseStatsReporter::registered = true;
  return RUN_ALL_TESTS();
}
