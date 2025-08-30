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

  /// Register a quantile metric for quantile stats with export types,
  /// quantiles, and sliding window periods.
  /// @param key The key to identify the stat.
  /// @param statTypes The list of stat types to export (e.g., AVG, SUM, COUNT).
  /// @param pcts The quantile percentiles to track (as values between 0.0
  /// and 1.0, e.g., 0.5 for 50th percentile, 0.95 for 95th percentile).
  /// @param slidingWindowsSeconds The sliding window periods in seconds.
  virtual void registerQuantileMetricExportType(
      const char* key,
      const std::vector<StatType>& statTypes,
      const std::vector<double>& pcts,
      const std::vector<size_t>& slidingWindowsSeconds = {60}) const = 0;

  virtual void registerQuantileMetricExportType(
      folly::StringPiece key,
      const std::vector<StatType>& statTypes,
      const std::vector<double>& pcts,
      const std::vector<size_t>& slidingWindowsSeconds = {60}) const = 0;

  /// Register a dynamic quantile metric with a template key pattern that
  /// supports runtime substitution.
  /// @param keyPattern The key pattern with {} placeholders for substitution.
  /// @param statTypes The list of stat types to export.
  /// @param pcts The quantile percentiles to track.
  /// @param slidingWindowsSeconds The sliding window periods in seconds.
  virtual void registerDynamicQuantileMetricExportType(
      const char* keyPattern,
      const std::vector<StatType>& statTypes,
      const std::vector<double>& pcts,
      const std::vector<size_t>& slidingWindowsSeconds = {60}) const = 0;

  virtual void registerDynamicQuantileMetricExportType(
      folly::StringPiece keyPattern,
      const std::vector<StatType>& statTypes,
      const std::vector<double>& pcts,
      const std::vector<size_t>& slidingWindowsSeconds = {60}) const = 0;

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

  /// Add the given value to a quantile metric.
  virtual void addQuantileMetricValue(const std::string& key, size_t value = 1)
      const = 0;

  virtual void addQuantileMetricValue(const char* key, size_t value = 1)
      const = 0;

  virtual void addQuantileMetricValue(folly::StringPiece key, size_t value = 1)
      const = 0;

  /// Add the given value to a quantile metric.
  virtual void addDynamicQuantileMetricValue(
      const std::string& key,
      folly::Range<const folly::StringPiece*> subkeys,
      size_t value = 1) const = 0;

  virtual void addDynamicQuantileMetricValue(
      const char* key,
      folly::Range<const folly::StringPiece*> subkeys,
      size_t value = 1) const = 0;

  virtual void addDynamicQuantileMetricValue(
      folly::StringPiece key,
      folly::Range<const folly::StringPiece*> subkeys,
      size_t value = 1) const = 0;

  /// Return the aggregated metrics in a serialized string format.
  virtual std::string fetchMetrics() = 0;

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

  void registerQuantileMetricExportType(
      const char* /* key */,
      const std::vector<StatType>& /* statTypes */,
      const std::vector<double>& /* pcts */,
      const std::vector<size_t>& /* slidingWindowsSeconds */) const override {}

  void registerQuantileMetricExportType(
      folly::StringPiece /* key */,
      const std::vector<StatType>& /* statTypes */,
      const std::vector<double>& /* pcts */,
      const std::vector<size_t>& /* slidingWindowsSeconds */) const override {}

  void registerDynamicQuantileMetricExportType(
      const char* /* keyPattern */,
      const std::vector<StatType>& /* statTypes */,
      const std::vector<double>& /* pcts */,
      const std::vector<size_t>& /* slidingWindowsSeconds */) const override {}

  void registerDynamicQuantileMetricExportType(
      folly::StringPiece /* keyPattern */,
      const std::vector<StatType>& /* statTypes */,
      const std::vector<double>& /* pcts */,
      const std::vector<size_t>& /* slidingWindowsSeconds */) const override {}

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

  void addQuantileMetricValue(const std::string& /* key */, size_t /* value */)
      const override {}

  void addQuantileMetricValue(const char* /* key */, size_t /* value */)
      const override {}

  void addQuantileMetricValue(folly::StringPiece /* key */, size_t /* value */)
      const override {}

  void addDynamicQuantileMetricValue(
      const std::string& /* key */,
      folly::Range<const folly::StringPiece*> /* subkeys */,
      size_t /* value */) const override {}

  void addDynamicQuantileMetricValue(
      const char* /* key */,
      folly::Range<const folly::StringPiece*> /* subkeys */,
      size_t /* value */) const override {}

  void addDynamicQuantileMetricValue(
      folly::StringPiece /* key */,
      folly::Range<const folly::StringPiece*> /* subkeys */,
      size_t /* value */) const override {}

  std::string fetchMetrics() override {
    return "";
  }
};

/// Helper functions to create vectors from variadic arguments, reducing
/// boilerplate in quantile stat definitions.

/// Create a vector of StatTypes from variadic arguments.
/// Usage: statTypes(StatType::AVG, StatType::COUNT, StatType::SUM)
template <typename... Args>
std::vector<StatType> statTypes(Args... args) {
  return std::vector<StatType>{args...};
}

/// Create a vector of percentiles from variadic arguments.
/// Usage: percentiles(0.5, 0.95, 0.99)
template <typename... Args>
std::vector<double> percentiles(Args... args) {
  return std::vector<double>{static_cast<double>(args)...};
}

/// Create a vector of sliding window periods in seconds from variadic
/// arguments. Usage: slidingWindowsSeconds(60, 600, 3600)
template <typename... Args>
std::vector<size_t> slidingWindowsSeconds(Args... args) {
  return std::vector<size_t>{static_cast<size_t>(args)...};
}

/// Helper class that stores subkeys in a member array and converts to
/// folly::Range. This is a temporary object that lives just for the duration of
/// the macro call.
template <size_t N>
class subkeys {
  std::array<folly::StringPiece, N> pieces_;

 public:
  template <typename... Args>
  subkeys(Args&&... args)
      : pieces_{folly::StringPiece(std::forward<Args>(args))...} {}

  /// Conversion operator to folly::Range<const folly::StringPiece*>
  operator folly::Range<const folly::StringPiece*>() const {
    return folly::Range<const folly::StringPiece*>(
        pieces_.data(), pieces_.size());
  }
};

/// Template deduction guide for subkeys class
template <typename... Args>
subkeys(Args&&...) -> subkeys<sizeof...(Args)>;

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

#define DEFINE_QUANTILE_STAT(key, statTypes, percentiles, slidingWindows) \
  {                                                                       \
    if (::facebook::velox::BaseStatsReporter::registered) {               \
      auto reporter = folly::Singleton<                                   \
          facebook::velox::BaseStatsReporter>::try_get_fast();            \
      if (FOLLY_LIKELY(reporter != nullptr)) {                            \
        reporter->registerQuantileMetricExportType(                       \
            (key), (statTypes), (percentiles), (slidingWindows));         \
      }                                                                   \
    }                                                                     \
  }

#define RECORD_QUANTILE_STAT_VALUE(key, ...)                    \
  {                                                             \
    if (::facebook::velox::BaseStatsReporter::registered) {     \
      auto reporter = folly::Singleton<                         \
          facebook::velox::BaseStatsReporter>::try_get_fast();  \
      if (FOLLY_LIKELY(reporter != nullptr)) {                  \
        reporter->addQuantileMetricValue((key), ##__VA_ARGS__); \
      }                                                         \
    }                                                           \
  }

#define DEFINE_DYNAMIC_QUANTILE_STAT(                                    \
    keyPattern, statTypes, percentiles, slidingWindows)                  \
  {                                                                      \
    if (::facebook::velox::BaseStatsReporter::registered) {              \
      auto reporter = folly::Singleton<                                  \
          facebook::velox::BaseStatsReporter>::try_get_fast();           \
      if (FOLLY_LIKELY(reporter != nullptr)) {                           \
        reporter->registerDynamicQuantileMetricExportType(               \
            (keyPattern), (statTypes), (percentiles), (slidingWindows)); \
      }                                                                  \
    }                                                                    \
  }

#define RECORD_DYNAMIC_QUANTILE_STAT_VALUE(keyPattern, subkeys, ...) \
  {                                                                  \
    if (::facebook::velox::BaseStatsReporter::registered) {          \
      auto reporter = folly::Singleton<                              \
          facebook::velox::BaseStatsReporter>::try_get_fast();       \
      if (FOLLY_LIKELY(reporter != nullptr)) {                       \
        reporter->addDynamicQuantileMetricValue(                     \
            (keyPattern), (subkeys), ##__VA_ARGS__);                 \
      }                                                              \
    }                                                                \
  }
} // namespace facebook::velox
