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

#include <fmt/args.h>
#include <cstdint>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include "velox/common/base/StatsReporter.h"

namespace facebook::velox {

/**
 * A test implementation of BaseStatsReporter for use in unit tests.
 * This class provides a mock implementation that captures all metric
 * registrations and values for verification in tests.
 */
class TestReporter : public BaseStatsReporter {
 public:
  mutable std::mutex m;
  mutable std::map<std::string, size_t> counterMap;
  mutable std::unordered_map<std::string, StatType> statTypeMap;
  mutable std::unordered_map<std::string, std::vector<int32_t>>
      histogramPercentilesMap;

  mutable std::unordered_map<std::string, std::vector<StatType>>
      quantileStatTypesMap;
  mutable std::unordered_map<std::string, std::vector<double>>
      quantilePercentilesMap;
  mutable std::unordered_map<std::string, std::vector<size_t>>
      quantileSlidingWindowsMap;

  mutable std::unordered_map<std::string, std::vector<StatType>>
      dynamicQuantileStatTypesMap;
  mutable std::unordered_map<std::string, std::vector<double>>
      dynamicQuantilePercentilesMap;
  mutable std::unordered_map<std::string, std::vector<size_t>>
      dynamicQuantileSlidingWindowsMap;

  void clear() {
    std::lock_guard<std::mutex> l(m);
    counterMap.clear();
    statTypeMap.clear();
    histogramPercentilesMap.clear();
    quantileStatTypesMap.clear();
    quantilePercentilesMap.clear();
    quantileSlidingWindowsMap.clear();
    dynamicQuantileStatTypesMap.clear();
    dynamicQuantilePercentilesMap.clear();
    dynamicQuantileSlidingWindowsMap.clear();
  }

  void registerMetricExportType(const char* key, StatType statType)
      const override {
    statTypeMap[key] = statType;
  }

  void registerMetricExportType(folly::StringPiece key, StatType statType)
      const override {
    statTypeMap[std::string(key)] = statType;
  }

  void registerHistogramMetricExportType(
      const char* key,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& pcts) const override {
    histogramPercentilesMap[key] = pcts;
  }

  void registerHistogramMetricExportType(
      folly::StringPiece key,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& pcts) const override {
    histogramPercentilesMap[std::string(key)] = pcts;
  }

  void registerQuantileMetricExportType(
      const char* key,
      const std::vector<StatType>& statTypes,
      const std::vector<double>& pcts,
      const std::vector<size_t>& slidingWindowsSeconds) const override {
    std::lock_guard<std::mutex> l(m);
    quantileStatTypesMap[key] = statTypes;
    quantilePercentilesMap[key] = pcts;
    quantileSlidingWindowsMap[key] = slidingWindowsSeconds;
  }

  void registerQuantileMetricExportType(
      folly::StringPiece key,
      const std::vector<StatType>& statTypes,
      const std::vector<double>& pcts,
      const std::vector<size_t>& slidingWindowsSeconds) const override {
    std::lock_guard<std::mutex> l(m);
    quantileStatTypesMap[std::string(key)] = statTypes;
    quantilePercentilesMap[std::string(key)] = pcts;
    quantileSlidingWindowsMap[std::string(key)] = slidingWindowsSeconds;
  }

  void addMetricValue(const std::string& key, const size_t value)
      const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] += value;
  }

  void addMetricValue(const char* key, const size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] += value;
  }

  void addMetricValue(folly::StringPiece key, size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[std::string(key)] += value;
  }

  void addHistogramMetricValue(const std::string& key, size_t value)
      const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramMetricValue(const char* key, size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramMetricValue(folly::StringPiece key, size_t value)
      const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[std::string(key)] =
        std::max(counterMap[std::string(key)], value);
  }

  void addQuantileMetricValue(const std::string& key, size_t value)
      const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] += value;
  }

  void addQuantileMetricValue(const char* key, size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] += value;
  }

  void addQuantileMetricValue(folly::StringPiece key, size_t value)
      const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[std::string(key)] += value;
  }

  void registerDynamicQuantileMetricExportType(
      const char* keyPattern,
      const std::vector<StatType>& statTypes,
      const std::vector<double>& pcts,
      const std::vector<size_t>& slidingWindowsSeconds) const override {
    std::lock_guard<std::mutex> l(m);
    dynamicQuantileStatTypesMap[keyPattern] = statTypes;
    dynamicQuantilePercentilesMap[keyPattern] = pcts;
    dynamicQuantileSlidingWindowsMap[keyPattern] = slidingWindowsSeconds;
  }

  void registerDynamicQuantileMetricExportType(
      folly::StringPiece keyPattern,
      const std::vector<StatType>& statTypes,
      const std::vector<double>& pcts,
      const std::vector<size_t>& slidingWindowsSeconds) const override {
    std::lock_guard<std::mutex> l(m);
    dynamicQuantileStatTypesMap[std::string(keyPattern)] = statTypes;
    dynamicQuantilePercentilesMap[std::string(keyPattern)] = pcts;
    dynamicQuantileSlidingWindowsMap[std::string(keyPattern)] =
        slidingWindowsSeconds;
  }

  void addDynamicQuantileMetricValue(
      const std::string& key,
      folly::Range<const folly::StringPiece*> subkeys,
      size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    // Check if the pattern was registered, if not silently ignore
    if (dynamicQuantileStatTypesMap.find(key) ==
        dynamicQuantileStatTypesMap.end()) {
      return;
    }

    // Substitute placeholders in the key pattern with subkeys using fmt::format
    std::string formattedKey;
    fmt::dynamic_format_arg_store<fmt::format_context> store;
    for (const auto& subkey : subkeys) {
      store.push_back(subkey);
    }
    formattedKey = fmt::vformat(key, store);
    counterMap[formattedKey] += value;
  }

  void addDynamicQuantileMetricValue(
      const char* key,
      folly::Range<const folly::StringPiece*> subkeys,
      size_t value) const override {
    addDynamicQuantileMetricValue(std::string(key), subkeys, value);
  }

  void addDynamicQuantileMetricValue(
      folly::StringPiece key,
      folly::Range<const folly::StringPiece*> subkeys,
      size_t value) const override {
    addDynamicQuantileMetricValue(std::string(key), subkeys, value);
  }

  std::string fetchMetrics() override {
    std::stringstream ss;
    ss << "[";
    auto sep = "";
    for (const auto& [key, value] : counterMap) {
      ss << sep << key << ":" << value;
      sep = ",";
    }
    ss << "]";
    return ss.str();
  }

  /**
   * Get the current counter value for a specific key.
   * Returns 0 if the key doesn't exist.
   */
  size_t getCounterValue(const std::string& key) const {
    std::lock_guard<std::mutex> l(m);
    auto it = counterMap.find(key);
    return it != counterMap.end() ? it->second : 0;
  }
};

} // namespace facebook::velox
