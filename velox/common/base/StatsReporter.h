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

// This is the base stats reporter interface that should be extended by
// different implementations
class BaseStatsReporter {
 public:
  virtual ~BaseStatsReporter() {}

  virtual void addStatExportType(const char* key, StatType statType) const = 0;

  virtual void addStatExportType(folly::StringPiece key, StatType statType)
      const = 0;

  virtual void addStatValue(const std::string& key, size_t value = 1) const = 0;

  virtual void addStatValue(const char* key, size_t value = 1) const = 0;

  virtual void addStatValue(folly::StringPiece key, size_t value = 1) const = 0;

  static bool registered;
};

// This is a dummy reporter that does nothing
class DummyStatsReporter : public BaseStatsReporter {
 public:
  void addStatExportType(const char* /*key*/, StatType /*statType*/)
      const override {}

  void addStatExportType(folly::StringPiece /*key*/, StatType /*statType*/)
      const override {}

  void addStatValue(const std::string& /* key */, size_t /* value */)
      const override {}

  void addStatValue(const char* /* key */, size_t /* value */) const override {}

  void addStatValue(folly::StringPiece /* key */, size_t /* value */)
      const override {}
};

#define REPORT_ADD_STAT_VALUE(k, ...)                          \
  {                                                            \
    if (::facebook::velox::BaseStatsReporter::registered) {    \
      auto reporter = folly::Singleton<                        \
          facebook::velox::BaseStatsReporter>::try_get_fast(); \
      if (LIKELY(reporter != nullptr)) {                       \
        reporter->addStatValue((k), ##__VA_ARGS__);            \
      }                                                        \
    }                                                          \
  }

#define REPORT_ADD_STAT_EXPORT_TYPE(k, t)                      \
  {                                                            \
    if (::facebook::velox::BaseStatsReporter::registered) {    \
      auto reporter = folly::Singleton<                        \
          facebook::velox::BaseStatsReporter>::try_get_fast(); \
      if (LIKELY(reporter != nullptr)) {                       \
        reporter->addStatExportType((k), (t));                 \
      }                                                        \
    }                                                          \
  }

} // namespace facebook::velox
