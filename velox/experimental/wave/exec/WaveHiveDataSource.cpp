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

#include "velox/experimental/wave/exec/WaveHiveDataSource.h"

namespace facebook::velox::wave {

using namespace connector::hive;

WaveHiveDataSource::WaveHiveDataSource(
    const std::shared_ptr<HiveTableHandle>& hiveTableHandle,
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    const RowTypePtr& readerOutputType,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const connector::ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const exec::ExprSet* remainingFilter,
    std::shared_ptr<common::MetadataFilter> metadataFilter) {
  params_.hiveTableHandle = hiveTableHandle;
  params_.scanSpec = scanSpec;
  params_.readerOutputType = readerOutputType;
  params_.partitionKeys = partitionKeys;
  params_.fileHandleFactory = fileHandleFactory;
  params_.executor = executor;
  params_.connectorQueryCtx = connectorQueryCtx;
  params_.hiveConfig = hiveConfig;
  params_.ioStats = ioStats;
  remainingFilter_ = remainingFilter ? remainingFilter->exprs().at(0) : nullptr;
  metadataFilter_ = metadataFilter;
}

void WaveHiveDataSource::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  VELOX_NYI();
}

void WaveHiveDataSource::setFromDataSource(
    std::shared_ptr<WaveDataSource> sourceShared) {
  auto source = dynamic_cast<WaveHiveDataSource*>(sourceShared.get());
  VELOX_CHECK(source, "Bad DataSource type");

  split_ = std::move(source->split_);
  if (source->splitReader_ && source->splitReader_->emptySplit()) {
    runtimeStats_.skippedSplits += source->runtimeStats_.skippedSplits;
    runtimeStats_.skippedSplitBytes += source->runtimeStats_.skippedSplitBytes;
    return;
  }
  source->params_.scanSpec->moveAdaptationFrom(*params_.scanSpec);
  params_.scanSpec = std::move(source->params_.scanSpec);
  splitReader_ = std::move(source->splitReader_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->params_.ioStats->merge(*params_.ioStats);
  params_.ioStats = std::move(source->params_.ioStats);
}

void WaveHiveDataSource::addSplit(
    std::shared_ptr<connector::ConnectorSplit> split) {
  VELOX_CHECK(
      split_ == nullptr,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  VELOX_CHECK(split_, "Wrong type of split");

  VLOG(1) << "Adding split " << split_->toString();

  if (splitReader_) {
    splitReader_.reset();
  }

  splitReader_ = WaveSplitReader::create(split, params_, defines_);
  ;
  // Split reader subclasses may need to use the reader options in prepareSplit
  // so we initialize it beforehand.
  splitReader_->configureReaderOptions();
  splitReader_->prepareSplit(metadataFilter_, runtimeStats_);
}

int32_t WaveHiveDataSource::canAdvance() {
  return splitReader_ != nullptr ? splitReader_->canAdvance() : 0;
}

void WaveHiveDataSource::schedule(WaveStream& stream, int32_t maxRows) {
  splitReader_->schedule(stream, maxRows);
}

vector_size_t WaveHiveDataSource::outputSize(WaveStream& stream) const {
  return splitReader_->outputSize(stream);
}

bool WaveHiveDataSource::isFinished() {
  if (!splitReader_) {
    return false;
  }
  if (splitReader_->isFinished()) {
    auto stats = splitReader_->runtimeStats();
    for (const auto& [name, counter] : stats) {
      auto it = splitReaderStats_.find(name);
      if (it == splitReaderStats_.end()) {
        splitReaderStats_.insert(std::make_pair(name, counter));
      } else {
        splitReaderStats_.insert(std::make_pair(
            name, RuntimeCounter(it->second.value, counter.unit)));
      }
    }
    return true;
  }
  return false;
}

uint64_t WaveHiveDataSource::getCompletedBytes() {
  return 0;
}

uint64_t WaveHiveDataSource::getCompletedRows() {
  return completedRows_;
}

std::unordered_map<std::string, RuntimeCounter>
WaveHiveDataSource::runtimeStats() {
  auto map = runtimeStats_.toMap();
  for (const auto& [name, counter] : splitReaderStats_) {
    map.insert(std::make_pair(name, counter));
  }
  return map;
}

// static
void WaveHiveDataSource::registerConnector() {
  static bool registered = false;
  if (registered) {
    return;
  }
  registered = true;
  auto config = std::make_shared<const core::MemConfig>();

  // Create hive connector with config...
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector("wavemock", config, nullptr);
  connector::registerConnector(hiveConnector);
  connector::hive::HiveDataSource::registerWaveDelegateHook(
      [](const std::shared_ptr<HiveTableHandle>& hiveTableHandle,
         const std::shared_ptr<common::ScanSpec>& scanSpec,
         const RowTypePtr& readerOutputType,
         std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
             partitionKeys,
         FileHandleFactory* fileHandleFactory,
         folly::Executor* executor,
         const connector::ConnectorQueryCtx* connectorQueryCtx,
         const std::shared_ptr<HiveConfig>& hiveConfig,
         const std::shared_ptr<io::IoStatistics>& ioStats,
         const exec::ExprSet* remainingFilter,
         std::shared_ptr<common::MetadataFilter> metadataFilter) {
        return std::make_shared<WaveHiveDataSource>(
            hiveTableHandle,
            scanSpec,
            readerOutputType,

            partitionKeys,
            fileHandleFactory,
            executor,
            connectorQueryCtx,
            hiveConfig,
            ioStats,
            remainingFilter,
            metadataFilter);
      });
}

} // namespace facebook::velox::wave
