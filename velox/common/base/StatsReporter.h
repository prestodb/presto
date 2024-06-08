/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#pragma once

#include <folly/Singleton.h>
#include <memory>

/// StatsReporter designed to assist in reporting various metrics of the
/// application that uses velox library. The library itself does not implement
/// the StatsReporter and it should be implemented by the application.
///
/// To initialize the reporter singleton in your application use this pattern
/// (note that MyReporter should implement the abstract class
/// BaseStatsReporter):
///
///   folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
///     return new MyReporter();
///   });
///
/// Then, for every metric that needs to be reported, it is required to register
/// one (usually) or more types (StatType) before reporting the metric:
///
///   DEFINE_METRIC("my_stat1", facebook::velox::StatType::COUNT);
///
/// To register one histogram, it requires the min and max value of
///  the range, the bucket width as well as the percentiles to be reported.
///   DEFINE_HISTOGRAM_METRIC("my_stat2", 10, 0, 100, 50, 99, 100);
///
/// The StatType controls how metric is aggregated.
/// After that, every call to RECORD_METRIC_VALUE increases the metric by the
/// given value:
///
///   By default the following will add 1 to the metric if not provided value
///   RECORD_METRIC_VALUE("my_stat1");
///   RECORD_METRIC_VALUE("my_stat2", 10);
///   RECORD_METRIC_VALUE("my_stat1", numOfFailures);

namespace facebook::velox {

enum class StatType {
  /// Tracks the average of the inserted values.
  AVG,
  /// Tracks the sum of the inserted values.
  SUM,
  /// Tracks the sum of the inserted values per second.
  RATE,
  /// Tracks the count of inserted values.
  COUNT,
  /// Tracks the histogram of inserted values.
  HISTOGRAM,
};

inline std::string statTypeString(StatType stat) {
  switch (stat) {
    case StatType::AVG:
      return "Avg";
    case StatType::SUM:
      return "Sum";
    case StatType::RATE:
      return "Rate";
    case StatType::COUNT:
      return "Count";
    case StatType::HISTOGRAM:
      return "Histogram";
    default:
      return fmt::format("UNKNOWN: {}", static_cast<int>(stat));
  }
}

/// This is the base stats reporter interface that should be extended by
/// different implementations.
class BaseStatsReporter {
 public:
  virtual ~BaseStatsReporter() {}

  /// Register a stat of the given stat type.
  /// @param key The key to identify the stat.
  /// @param statType How the stat is aggregated.
  virtual void registerMetricExportType(const char* key, StatType statType)
      const = 0;

  virtual void registerMetricExportType(
      folly::StringPiece key,
      StatType statType) const = 0;

  /// Register a histogram with a list of percentiles defined.
  /// @param key The key to identify the histogram.
  /// @param bucketWidth The width of the buckets.
  /// @param min The starting value of the buckets.
  /// @param max The ending value of the buckets.
  /// @param pcts The aggregated percentiles to be reported.
  virtual void registerHistogramMetricExportType(
      const char* key,
      int64_t bucketWidth,
      int64_t min,
      int64_t max,
      const std::vector<int32_t>& pcts) const = 0;

  virtual void registerHistogramMetricExportType(
      folly::StringPiece key,
      int64_t bucketWidth,
      int64_t min,
      int64_t max,
      const std::vector<int32_t>& pcts) const = 0;

  /// Add the given value to the stat.
  virtual void addMetricValue(const std::string& key, size_t value = 1)
      const = 0;

  virtual void addMetricValue(const char* key, size_t value = 1) const = 0;

  virtual void addMetricValue(folly::StringPiece key, size_t value = 1)
      const = 0;

  /// Add the given value to the histogram.
  virtual void addHistogramMetricValue(const std::string& key, size_t value)
      const = 0;

  virtual void addHistogramMetricValue(const char* key, size_t value) const = 0;

  virtual void addHistogramMetricValue(folly::StringPiece key, size_t value)
      const = 0;

  static bool registered;
};

// This is a dummy reporter that does nothing
class DummyStatsReporter : public BaseStatsReporter {
 public:
  void registerMetricExportType(const char* /*key*/, StatType /*statType*/)
      const override {}

  void registerMetricExportType(
      folly::StringPiece /*key*/,
      StatType /*statType*/) const override {}

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

  void addMetricValue(const std::string& /* key */, size_t /* value */)
      const override {}

  void addMetricValue(const char* /* key */, size_t /* value */)
      const override {}

  void addMetricValue(folly::StringPiece /* key */, size_t /* value */)
      const override {}

  void addHistogramMetricValue(const std::string& /* key */, size_t /* value */)
      const override {}

  void addHistogramMetricValue(const char* /* key */, size_t /* value */)
      const override {}

  void addHistogramMetricValue(folly::StringPiece /* key */, size_t /* value */)
      const override {}
};

#define DEFINE_METRIC(key, type)                               \
  {                                                            \
    if (::facebook::velox::BaseStatsReporter::registered) {    \
      auto reporter = folly::Singleton<                        \
          facebook::velox::BaseStatsReporter>::try_get_fast(); \
      if (FOLLY_LIKELY(reporter != nullptr)) {                 \
        reporter->registerMetricExportType((key), (type));     \
      }                                                        \
    }                                                          \
  }

#define RECORD_METRIC_VALUE(key, ...)                          \
  {                                                            \
    if (::facebook::velox::BaseStatsReporter::registered) {    \
      auto reporter = folly::Singleton<                        \
          facebook::velox::BaseStatsReporter>::try_get_fast(); \
      if (FOLLY_LIKELY(reporter != nullptr)) {                 \
        reporter->addMetricValue((key), ##__VA_ARGS__);        \
      }                                                        \
    }                                                          \
  }

#define DEFINE_HISTOGRAM_METRIC(key, bucket, min, max, ...)    \
  {                                                            \
    if (::facebook::velox::BaseStatsReporter::registered) {    \
      auto reporter = folly::Singleton<                        \
          facebook::velox::BaseStatsReporter>::try_get_fast(); \
      if (FOLLY_LIKELY(reporter != nullptr)) {                 \
        reporter->registerHistogramMetricExportType(           \
            (key),                                             \
            (bucket),                                          \
            (min),                                             \
            (max),                                             \
            (std::vector<int32_t>({__VA_ARGS__})));            \
      }                                                        \
    }                                                          \
  }

#define RECORD_HISTOGRAM_METRIC_VALUE(key, ...)                  \
  {                                                              \
    if (::facebook::velox::BaseStatsReporter::registered) {      \
      auto reporter = folly::Singleton<                          \
          facebook::velox::BaseStatsReporter>::try_get_fast();   \
      if (FOLLY_LIKELY(reporter != nullptr)) {                   \
        reporter->addHistogramMetricValue((key), ##__VA_ARGS__); \
      }                                                          \
    }                                                            \
  }
} // namespace facebook::velox
