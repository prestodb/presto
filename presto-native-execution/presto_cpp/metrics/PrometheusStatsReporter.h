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

#pragma once

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/dynamic.h>
#include <fstream>
#include <iostream>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/velox/common/base/Exceptions.h"
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

namespace facebook::presto {

/**
 * @brief Reporter class for Prometheus metrics
 *
 * This class is responsible for reporting statistics and metrics
 * to Prometheus.
 */
class PrometheusStatsReporter : public facebook::velox::BaseStatsReporter {


 public:

  explicit PrometheusStatsReporter() {
    const char* odinCluster = std::getenv("ODIN_CLUSTER");
    const char* odinNode = std::getenv("ODIN_NODE");
    const char* hostName = std::getenv("HOST_NAME");
    cluster_ = (odinCluster != nullptr) ? odinCluster : "";
    node_ = (odinNode != nullptr) ? odinNode : "";
    host_ = (hostName != nullptr) ? hostName : "";
    // Get IP
    auto nodeConfig = facebook::presto::NodeConfig::instance();
    // Create the server
    std::string address_ = fmt::format("{}:{}", nodeConfig->nodeInternalAddress([this]() -> std::string {
      return default_ip;
    }), nodeConfig->nodeMetricPort(default_port));
    exposer = std::make_unique<::prometheus::Exposer>(address_);
    LOG(INFO) << "Started Prometheus Metrics Server at " << address_;
    registry = std::make_shared<prometheus::Registry>();
    exposer->RegisterCollectable(registry);
  }

  // /// Register a stat of the given stat type.
  // /// @param key The key to identify the stat.
  // /// @param statType How the stat is aggregated.
  void registerMetricExportType(
      folly::StringPiece key,
      facebook::velox::StatType statType) const override;

  void registerMetricExportType(
      const char* key,
      facebook::velox::StatType statType) const override;


  /// Add the given value to the stat.
  void addMetricValue(const std::string& key, size_t value = 1) const override;

  void addMetricValue(const char* key, size_t value = 1) const override;

  void addMetricValue(folly::StringPiece key, size_t value = 1) const override;


  /// Add the given value to the histogram.
  virtual void addHistogramMetricValue(const std::string& key, size_t value)
      const override {}

  virtual void addHistogramMetricValue(const char* key, size_t value) const override {}

  virtual void addHistogramMetricValue(folly::StringPiece key, size_t value)
      const override {}


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
      const  std::vector<int32_t>& pcts ) const override{};

  virtual void registerHistogramMetricExportType(
      folly::StringPiece key,
      int64_t bucketWidth,
      int64_t min,
      int64_t max,
      const   std::vector<int32_t>& pcts) const override {}

  // Getters for testing purposes
  const folly::ConcurrentHashMap<std::string, prometheus::Counter*>& getCounterMapForTest() const {
    return counterMap;
  }

  const folly::ConcurrentHashMap<std::string, velox::StatType>& getMetricTypesMapForTest() const {
    return statTypeMap;
  }

  // Visible for testing
  prometheus::Counter* getCounter(std::string key){
      return counterMap[key];
  }

  // Visible for testing
  velox::StatType getMetricType(std::string key){
    return statTypeMap[key];
  }

  // Visible for testing
  mutable folly::ConcurrentHashMap<std::string, velox::StatType> statTypeMap;
  mutable folly::ConcurrentHashMap<std::string,  prometheus::Counter*> counterMap;
  mutable folly::ConcurrentHashMap<std::string,  prometheus::Gauge*> gaugeMap;
  mutable folly::ConcurrentHashMap<std::string,  prometheus::Summary*> summaryMap;

 private:
  std::unique_ptr<prometheus::Exposer> exposer;
  std::shared_ptr<prometheus::Registry> registry;
  std::string cluster_;
  std::string host_;
  std::string node_;
  std::string default_port = "8082";
  std::string default_ip = "127.0.0.1";

  void registerSummaryMetricForAverageStatType(const std::string& keyStr) const;
  void registerGaugeMetric(const std::string& keyStr) const;
  void registerCounterMetric(const std::string& keyStr) const;
  void updateSummaryMetric(const std::string& keyStr, size_t value) const;
  void updateGaugeMetric(const std::string& keyStr, size_t value) const;
  void updateCounterMetric(const std::string& keyStr, size_t value) const;
  static std::string sanitizeMetricName(const std::string& name) ;
  std::string statTypeToString(velox::StatType statType) const;

}; // class PrometheusStatsReporter
} // namespace facebook::presto
