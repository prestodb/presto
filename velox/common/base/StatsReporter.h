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

/// StatsReporter designed to assist in reporting various stats of the
/// application that uses velox library. The library itself does not implement
/// the StatsReporter and it should be implemented by the application.
///
/// To inialize the reporter singleton in your application use this pattern
/// (note that MyReporter should implement the abstract class
/// BaseStatsReporter):
///
///   folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
///     return new MyReporter();
///   });
///
/// Then, for every stat that needs to be reported, it is required to register
/// one (usually) or more types (StatType) before reporting the stat:
///
///   REPORT_ADD_STAT_EXPORT_TYPE("my_stat1", facebook::velox::StatType::COUNT);
///
/// To register one histogram, it requires the min and max value of
//  the range, the bucket width as well as the percentiles to be reported.
///   REPORT_ADD_HISTOGRAM_EXPORT_PERCENTILE("my_stat2", 10, 0, 100, 50, 99,
///   100);
///
/// The StatType controls how counter/stat is aggregated.
/// After that, every call to REPORT_ADD_STAT_VALUE increases the counter by the
/// given value:
///
///   By default the following will add 1 to the stat if not provided value
///   REPORT_ADD_STAT_VALUE("my_stat1");
///   REPORT_ADD_STAT_VALUE("my_stat2", 10);
///   REPORT_ADD_STAT_VALUE("my_stat1", numOfFailures);

namespace facebook::velox {

enum class StatType {
  AVG,
  SUM,
  RATE,
  COUNT,
};

/// This is the base stats reporter interface that should be extended by
/// different implementations.
class BaseStatsReporter {
 public:
  virtual ~BaseStatsReporter() {}

  /// Register a stat of the given stat type.
  /// @param key The key to identify the stat.
  /// @param statType How the stat is aggregated.
  virtual void addStatExportType(const char* key, StatType statType) const = 0;

  virtual void addStatExportType(folly::StringPiece key, StatType statType)
      const = 0;

  /// Register a histogram with a list of percentiles defined.
  /// @param key The key to identify the histogram.
  /// @param bucketWidth The width of the buckets.
  /// @param min The starting value of the buckets.
  /// @param max The ending value of the buckets.
  /// @param pcts The aggregated percentiles to be reported.
  virtual void addHistogramExportPercentiles(
      const char* key,
      int64_t bucketWidth,
      int64_t min,
      int64_t max,
      const std::vector<int32_t>& pcts) const = 0;

  virtual void addHistogramExportPercentiles(
      folly::StringPiece key,
      int64_t bucketWidth,
      int64_t min,
      int64_t max,
      const std::vector<int32_t>& pcts) const = 0;

  /// Add the given value to the stat.
  virtual void addStatValue(const std::string& key, size_t value = 1) const = 0;

  virtual void addStatValue(const char* key, size_t value = 1) const = 0;

  virtual void addStatValue(folly::StringPiece key, size_t value = 1) const = 0;

  /// Add the given value to the histogram.
  virtual void addHistogramValue(const std::string& key, size_t value)
      const = 0;

  virtual void addHistogramValue(const char* key, size_t value) const = 0;

  virtual void addHistogramValue(folly::StringPiece key, size_t value)
      const = 0;

  static bool registered;
};

// This is a dummy reporter that does nothing
class DummyStatsReporter : public BaseStatsReporter {
 public:
  void addStatExportType(const char* /*key*/, StatType /*statType*/)
      const override {}

  void addStatExportType(folly::StringPiece /*key*/, StatType /*statType*/)
      const override {}

  void addHistogramExportPercentiles(
      const char* /*key*/,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& /* pcts */) const override {}

  void addHistogramExportPercentiles(
      folly::StringPiece /* key */,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& /* pcts */) const override {}

  void addStatValue(const std::string& /* key */, size_t /* value */)
      const override {}

  void addStatValue(const char* /* key */, size_t /* value */) const override {}

  void addStatValue(folly::StringPiece /* key */, size_t /* value */)
      const override {}

  void addHistogramValue(const std::string& /* key */, size_t /* value */)
      const override {}

  void addHistogramValue(const char* /* key */, size_t /* value */)
      const override {}

  void addHistogramValue(folly::StringPiece /* key */, size_t /* value */)
      const override {}
};

#define REPORT_ADD_STAT_VALUE(key, ...)                        \
  {                                                            \
    if (::facebook::velox::BaseStatsReporter::registered) {    \
      auto reporter = folly::Singleton<                        \
          facebook::velox::BaseStatsReporter>::try_get_fast(); \
      if (FOLLY_LIKELY(reporter != nullptr)) {                 \
        reporter->addStatValue((key), ##__VA_ARGS__);          \
      }                                                        \
    }                                                          \
  }

#define REPORT_ADD_STAT_EXPORT_TYPE(key, type)                 \
  {                                                            \
    if (::facebook::velox::BaseStatsReporter::registered) {    \
      auto reporter = folly::Singleton<                        \
          facebook::velox::BaseStatsReporter>::try_get_fast(); \
      if (FOLLY_LIKELY(reporter != nullptr)) {                 \
        reporter->addStatExportType((key), (type));            \
      }                                                        \
    }                                                          \
  }

#define REPORT_ADD_HISTOGRAM_VALUE(key, ...)                   \
  {                                                            \
    if (::facebook::velox::BaseStatsReporter::registered) {    \
      auto reporter = folly::Singleton<                        \
          facebook::velox::BaseStatsReporter>::try_get_fast(); \
      if (FOLLY_LIKELY(reporter != nullptr)) {                 \
        reporter->addHistogramValue((key), ##__VA_ARGS__);     \
      }                                                        \
    }                                                          \
  }

#define REPORT_ADD_HISTOGRAM_EXPORT_PERCENTILE(key, bucket, min, max, ...) \
  {                                                                        \
    if (::facebook::velox::BaseStatsReporter::registered) {                \
      auto reporter = folly::Singleton<                                    \
          facebook::velox::BaseStatsReporter>::try_get_fast();             \
      if (FOLLY_LIKELY(reporter != nullptr)) {                             \
        reporter->addHistogramExportPercentiles(                           \
            (key),                                                         \
            (bucket),                                                      \
            (min),                                                         \
            (max),                                                         \
            (std::vector<int32_t>({__VA_ARGS__})));                        \
      }                                                                    \
    }                                                                      \
  }

} // namespace facebook::velox
