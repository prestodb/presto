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

#include "velox/dwio/common/FilterNode.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

class MetricsLog {
 public:
  static constexpr std::string_view LIB_VERSION_STRING{"1.1"};
  static constexpr folly::StringPiece WRITE_OPERATION{"WRITE"};

  enum class MetricsType {
    HEADER,
    FOOTER,
    FILE,
    STRIPE,
    STRIPE_INDEX,
    STRIPE_FOOTER,
    STREAM,
    STREAM_BUNDLE,
    GROUP,
    BLOCK,
    TEST
  };

  virtual ~MetricsLog() = default;

  // read path logging methods
  virtual void logRead(
      uint64_t duration, // milliseconds
      const std::string& category,
      uint64_t fileSize,
      uint64_t psSize,
      uint64_t footerSize,
      uint64_t readOffset,
      uint64_t readSize,
      MetricsType type,
      uint32_t numFileRead,
      uint32_t numStripeCache) const {}

  struct ReadMetrics {
    uint64_t duration; // milliseconds
    std::string category;
    uint64_t fileSize;
    uint64_t psSize;
    uint64_t footerSize;
    uint64_t readOffset;
    uint64_t readSize;
    std::string header;
    uint32_t numFileRead;
    uint32_t numStripeCache;
  };

  // read path logging methods
  virtual void logRead(const ReadMetrics& metrics) const {};

  virtual void logColumnFilter(
      const ColumnFilter& filter,
      uint64_t numColumns,
      uint64_t numNodes,
      bool hasSchema) const {}

  virtual void logMapKeyFilter(
      const FilterNode& /* mapNode */,
      std::string_view /* expr */) const {}

  // write path logging methods
  virtual void logWrite(size_t size, size_t duration) const {}

  virtual void
  logWrite(const std::string& operation, size_t size, size_t duration) const {}

  struct StripeFlushMetrics {
    std::string writerVersion;
    uint32_t stripeIndex;
    uint64_t rawStripeSize;
    uint64_t rowsInStripe;
    uint64_t stripeSize;
    uint64_t limit;
    uint64_t maxDictSize;
    uint64_t dictionaryMemory;
    uint64_t outputStreamMemoryEstimate;
    uint64_t availableMemory;
    size_t flushOverhead;
    float compressionRatio;
    float flushOverheadRatio;
    float averageRowSize;
    uint64_t stripeSizeEstimate;
    uint64_t groupSize;
    bool close;
  };

  virtual void logStripeFlush(const StripeFlushMetrics& /* metrics */) const {}

  struct FileCloseMetrics {
    std::string writerVersion;
    uint64_t footerLength;
    uint64_t fileSize;
    uint64_t cacheSize;
    uint64_t numCacheBlocks;
    int32_t cacheMode;
    uint64_t numOfStripes;
    uint64_t rowCount;
    uint64_t rawDataSize;
    uint64_t numOfStreams;
    // NOTE: these are memory footprints after the final flush.
    int64_t totalMemory;
    int64_t dictionaryMemory;
    int64_t generalMemory;
  };

  virtual void logFileClose(const FileCloseMetrics& /* metrics */) const {}

  static std::shared_ptr<const MetricsLog> voidLog() {
    static std::shared_ptr<const MetricsLog> log{new MetricsLog("")};
    return log;
  }

 protected:
  MetricsLog(const std::string& file) : file_{file} {}

  static std::string getMetricTypeName(MetricsType type) {
    switch (type) {
      case MetricsType::HEADER:
        return "HEADER";
      case MetricsType::FOOTER:
        return "FOOTER";
      case MetricsType::FILE:
        return "FILE";
      case MetricsType::STRIPE:
        return "STRIPE";
      case MetricsType::STRIPE_INDEX:
        return "STRIPE_INDEX";
      case MetricsType::STRIPE_FOOTER:
        return "STRIPE_FOOTER";
      case MetricsType::STREAM:
        return "STREAM";
      case MetricsType::STREAM_BUNDLE:
        return "STREAM_BUNDLE";
      case MetricsType::GROUP:
        return "GROUP";
      case MetricsType::BLOCK:
        return "BLOCK";
      case MetricsType::TEST:
        return "TEST";
    }
  }

  std::string file_;
};

using LogType = MetricsLog::MetricsType;
using MetricsLogPtr = std::shared_ptr<const MetricsLog>;

class DwioMetricsLogFactory {
 public:
  virtual ~DwioMetricsLogFactory() = default;
  virtual MetricsLogPtr create(const std::string& filePath, bool ioLogging) = 0;
};

void registerMetricsLogFactory(std::shared_ptr<DwioMetricsLogFactory> factory);

DwioMetricsLogFactory& getMetricsLogFactory();

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
