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
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/experimental/wave/exec/WaveDataSource.h"
#include "velox/experimental/wave/exec/WaveSplitReader.h"

namespace facebook::velox::wave {

class WaveHiveDataSource : public WaveDataSource {
 public:
  WaveHiveDataSource(
      const std::shared_ptr<connector::hive::HiveTableHandle>& hiveTableHandle,
      const std::shared_ptr<common::ScanSpec>& scanSpec,
      const RowTypePtr& readerOutputType,
      std::unordered_map<
          std::string,
          std::shared_ptr<connector::hive::HiveColumnHandle>>* partitionKeys,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const connector::ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<connector::hive::HiveConfig>& hiveConfig,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const exec::ExprSet* remainingFilter,
      std::shared_ptr<common::MetadataFilter> metadataFilter);

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

  void addSplit(std::shared_ptr<connector::ConnectorSplit> split) override;

  void setFromDataSource(std::shared_ptr<WaveDataSource> dataSource) override;

  int32_t canAdvance(WaveStream& stream) override;

  void schedule(WaveStream& stream, int32_t maxRows) override;

  vector_size_t outputSize(WaveStream& stream) const override;

  bool isFinished() override;

  uint64_t getCompletedBytes() override;

  uint64_t getCompletedRows() override;

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

  static void registerConnector();

 private:
  SplitReaderParams params_;
  std::shared_ptr<connector::ConnectorSplit> split_;
  std::unique_ptr<WaveSplitReader> splitReader_;
  std::shared_ptr<exec::Expr> remainingFilter_;
  dwio::common::RuntimeStatistics runtimeStats_;
  std::shared_ptr<common::MetadataFilter> metadataFilter_;
  int64_t completedRows_{0};
  int64_t completedBytes_{0};
  std::unordered_map<std::string, RuntimeCounter> splitReaderStats_;
};

} // namespace facebook::velox::wave
