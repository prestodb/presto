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

#include "velox/experimental/wave/exec/TableScan.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Task.h"
#include "velox/experimental/wave/exec/WaveDriver.h"
#include "velox/experimental/wave/exec/WaveSplitReader.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::wave {

std::atomic<uint64_t> TableScan::ioWaitNanos_;

using exec::BlockingReason;

BlockingReason TableScan::isBlocked(ContinueFuture* future) {
  if (!dataSource_ || needNewSplit_) {
    nextSplit(future);
    isNewSplit_ = true;
  }
  if (blockingFuture_.valid()) {
    *future = std::move(blockingFuture_);
    return blockingReason_;
  }
  return BlockingReason::kNotBlocked;
}

std::vector<AdvanceResult> TableScan::canAdvance(WaveStream& stream) {
  if (!dataSource_ || needNewSplit_) {
    return {};
  }
  if (isNewSplit_) {
    isNewSplit_ = false;
    return {{.numRows = waveDataSource_->canAdvance(stream)}};
  }
  return {{.numRows = nextAvailableRows_}};
}

void TableScan::schedule(WaveStream& stream, int32_t maxRows) {
  waveDataSource_->schedule(stream, maxRows);
  nextAvailableRows_ = waveDataSource_->canAdvance(stream);
  if (nextAvailableRows_ == 0) {
    updateStats(
        waveDataSource_->splitReader()->runtimeStats(),
        waveDataSource_->splitReader().get());
    needNewSplit_ = true;
  }
}

void TableScan::updateStats(
    std::unordered_map<std::string, RuntimeCounter> connectorStats,
    WaveSplitReader* splitReader) {
  auto lockedStats = stats().wlock();
  if (splitReader) {
    lockedStats->rawInputPositions = splitReader->getCompletedRows();
    lockedStats->rawInputBytes = splitReader->getCompletedBytes();
  }
  for (const auto& [name, counter] : connectorStats) {
    if (name == "ioWaitNanos") {
      ioWaitNanos_ += counter.value - lastIoWaitNanos_;
      lastIoWaitNanos_ = counter.value;
    }
    if (UNLIKELY(lockedStats->runtimeStats.count(name) == 0)) {
      lockedStats->runtimeStats.insert(
          std::make_pair(name, RuntimeMetric(counter.unit)));
    } else {
      VELOX_CHECK_EQ(lockedStats->runtimeStats.at(name).unit, counter.unit);
    }
    lockedStats->runtimeStats.at(name).addValue(counter.value);
  }
}

BlockingReason TableScan::nextSplit(ContinueFuture* future) {
  exec::Split split;
  blockingReason_ = driverCtx_->task->getSplitOrFuture(
      driverCtx_->splitGroupId,
      planNodeId_,
      split,
      blockingFuture_,
      maxPreloadedSplits_,
      splitPreloader_);
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    return blockingReason_;
  }

  if (!split.hasConnectorSplit()) {
    noMoreSplits_ = true;
    if (dataSource_) {
      updateStats(dataSource_->runtimeStats());
    }
    return BlockingReason::kNotBlocked;
  }

  const auto& connectorSplit = split.connectorSplit;
  needNewSplit_ = false;

  VELOX_CHECK_EQ(
      connector_->connectorId(),
      connectorSplit->connectorId,
      "Got splits with different connector IDs");

  WithSubfieldMap subfields(driver_->subfields());
  if (!dataSource_) {
    connectorQueryCtx_ = driver_->operatorCtx()->createConnectorQueryCtx(
        connectorSplit->connectorId, planNodeId_, connectorPool_);
    dataSource_ = connector_->createDataSource(
        outputType_, tableHandle_, columnHandles_, connectorQueryCtx_.get());
    waveDataSource_ = dataSource_->toWaveDataSource();
    waveDataSource_->setOutputOperands(defines_);
    waveDataSource_->addSplit(connectorSplit);
  } else {
    if (connectorSplit->dataSource) {
      ++numPreloadedSplits_;
      // The AsyncSource returns a unique_ptr to a shared_ptr. The
      // unique_ptr will be nullptr if there was a cancellation.
      numReadyPreloadedSplits_ += connectorSplit->dataSource->hasValue();
      auto preparedDataSource = connectorSplit->dataSource->move();
      driver_->stats().wlock()->getOutputTiming.add(
          connectorSplit->dataSource->prepareTiming());
      if (!preparedDataSource) {
        // There must be a cancellation.
        VELOX_CHECK(driver_->operatorCtx()->task()->isCancelled());
        return BlockingReason::kNotBlocked;
      }
      auto preparedWaveSource = preparedDataSource->toWaveDataSource();
      waveDataSource_->setFromDataSource(std::move(preparedWaveSource));
    } else {
      waveDataSource_->addSplit(connectorSplit);
    }
  }
  ++stats().wlock()->numSplits;

  for (const auto& entry : pendingDynamicFilters_) {
    waveDataSource_->addDynamicFilter(entry.first, entry.second);
  }
  pendingDynamicFilters_.clear();
  return BlockingReason::kNotBlocked;
}

void TableScan::preload(std::shared_ptr<connector::ConnectorSplit> split) {
  // The AsyncSource returns a unique_ptr to the shared_ptr of the
  // DataSource. The callback may outlive the Task, hence it captures
  // a shared_ptr to it. This is required to keep memory pools live
  // for the duration. The callback checks for task cancellation to
  // avoid needless work.
  split->dataSource = std::make_unique<AsyncSource<connector::DataSource>>(
      [type = outputType_,
       source = waveDataSource_,
       table = tableHandle_,
       columns = columnHandles_,
       connector = connector_,
       defines = defines_,
       this,
       ctx = driver_->operatorCtx()->createConnectorQueryCtx(
           split->connectorId, planNodeId_, connectorPool_),
       task = driver_->operatorCtx()->task(),
       split]() -> std::unique_ptr<connector::DataSource> {
        if (task->isCancelled()) {
          return nullptr;
        }
        auto debugString =
            fmt::format("Split {} Task {}", split->toString(), task->taskId());
        ExceptionContextSetter exceptionContext(
            {[](VeloxException::Type /*exceptionType*/, auto* debugString) {
               return *static_cast<std::string*>(debugString);
             },
             &debugString});

        auto ptr = connector->createDataSource(type, table, columns, ctx.get());
        if (task->isCancelled()) {
          return nullptr;
        }
        WithSubfieldMap subfields(driver_->subfields());
        auto waveSource = ptr->toWaveDataSource();
        waveSource->setOutputOperands(defines);
        waveSource->addSplit(split);
        return ptr;
      });
}

void TableScan::checkPreload() {
  auto executor = connector_->executor();
  if (maxPreloadedSplits_ == 0 || !executor ||
      !connector_->supportsSplitPreload()) {
    return;
  }
  if (dataSource_->allPrefetchIssued()) {
    maxPreloadedSplits_ = driverCtx_->task->numDrivers(driverCtx_->driver) *
        maxSplitPreloadPerDriver_;
    if (!splitPreloader_) {
      splitPreloader_ =
          [executor, this](std::shared_ptr<connector::ConnectorSplit> split) {
            preload(split);

            executor->add(
                [taskHolder = driver_->operatorCtx()->task(), split]() mutable {
                  split->dataSource->prepare();
                  split.reset();
                });
          };
    }
  }
}

bool TableScan::isFinished() const {
  return noMoreSplits_;
}

void TableScan::addDynamicFilter(
    const core::PlanNodeId& producer,
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  if (dataSource_) {
    dataSource_->addDynamicFilter(outputChannel, filter);
  } else {
    pendingDynamicFilters_.emplace(outputChannel, filter);
  }
}

} // namespace facebook::velox::wave
