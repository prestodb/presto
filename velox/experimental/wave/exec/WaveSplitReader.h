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

#include "velox/common/io/IoStatistics.h"
#include "velox/common/time/Timer.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/exec/Task.h"
#include "velox/experimental/wave/exec/WaveOperator.h"

namespace facebook::velox::wave {

/// Parameters for a Wave Hive SplitReaderFactory.
struct SplitReaderParams {
  std ::shared_ptr<connector::hive::HiveTableHandle> hiveTableHandle;
  std::shared_ptr<common::ScanSpec> scanSpec;
  RowTypePtr readerOutputType;
  std::unordered_map<
      std::string,
      std::shared_ptr<connector::hive::HiveColumnHandle>>* partitionKeys;
  FileHandleFactory* fileHandleFactory;
  folly::Executor* executor;
  const connector::ConnectorQueryCtx* connectorQueryCtx;
  std::shared_ptr<connector::hive::HiveConfig> hiveConfig;
  std::shared_ptr<io::IoStatistics> ioStats;
};

class WaveSplitReaderFactory;

class WaveSplitReader {
 public:
  virtual ~WaveSplitReader() = default;

  static std::shared_ptr<WaveSplitReader> create(
      const std::shared_ptr<velox::connector::ConnectorSplit>& split,
      const SplitReaderParams& params,
      const DefinesMap* defines);

  virtual bool emptySplit() = 0;

  virtual int32_t canAdvance(WaveStream& stream) = 0;

  virtual void schedule(WaveStream& stream, int32_t maxRows) = 0;

  virtual bool isFinished() const = 0;

  virtual uint64_t getCompletedBytes() = 0;

  virtual uint64_t getCompletedRows() = 0;

  virtual std::unordered_map<std::string, RuntimeCounter> runtimeStats() = 0;

  virtual void configureReaderOptions() {}
  virtual void prepareSplit(
      std::shared_ptr<common::MetadataFilter> metadataFilter,
      dwio::common::RuntimeStatistics& runtimeStats) {}

  static void registerFactory(std::unique_ptr<WaveSplitReaderFactory> factory);

  static std::vector<std::unique_ptr<WaveSplitReaderFactory>> factories_;
};

class WaveSplitReaderFactory {
 public:
  virtual ~WaveSplitReaderFactory() = default;

  /// Returns a new split reader corresponding to 'split' if 'this' recognizes
  /// the split, otherwise returns nullptr.
  virtual std::shared_ptr<WaveSplitReader> create(
      const std::shared_ptr<connector::ConnectorSplit>& split,
      const SplitReaderParams& params,
      const DefinesMap* defines) = 0;
};

} // namespace facebook::velox::wave
