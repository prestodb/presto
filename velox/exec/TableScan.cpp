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
#include "velox/exec/TableScan.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Task.h"
#include "velox/expression/Expr.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

TableScan::TableScan(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::TableScanNode>& tableScanNode)
    : SourceOperator(
          driverCtx,
          tableScanNode->outputType(),
          operatorId,
          tableScanNode->id(),
          "TableScan"),
      tableHandle_(tableScanNode->tableHandle()),
      columnHandles_(tableScanNode->assignments()),
      driverCtx_(driverCtx),
      connectorPool_(driverCtx_->task->addConnectorPoolLocked(
          planNodeId(),
          driverCtx_->pipelineId,
          driverCtx_->driverId,
          operatorType(),
          tableHandle_->connectorId())),
      maxSplitPreloadPerDriver_(
          driverCtx_->queryConfig().maxSplitPreloadPerDriver()),
      readBatchSize_(driverCtx_->queryConfig().preferredOutputBatchRows()),
      maxReadBatchSize_(driverCtx_->queryConfig().maxOutputBatchRows()),
      getOutputTimeLimitMs_(
          driverCtx_->queryConfig().tableScanGetOutputTimeLimitMs()) {
  connector_ = connector::getConnector(tableHandle_->connectorId());
}

folly::dynamic TableScan::toJson() const {
  auto ret = SourceOperator::toJson();
  ret["status"] = curStatus_.load();
  return ret;
}

bool TableScan::shouldYield(StopReason taskStopReason, size_t startTimeMs)
    const {
  // Checks task-level yield signal, driver-level yield signal and table scan
  // output processing time limit.
  return taskStopReason == StopReason::kYield ||
      driverCtx_->driver->shouldYield() ||
      ((getOutputTimeLimitMs_ != 0) &&
       (getCurrentTimeMs() - startTimeMs) >= getOutputTimeLimitMs_);
}

bool TableScan::shouldStop(StopReason taskStopReason) const {
  return taskStopReason != StopReason::kNone &&
      taskStopReason != StopReason::kYield;
}

RowVectorPtr TableScan::getOutput() {
  auto exitCurStatusGuard = folly::makeGuard([this]() { curStatus_ = ""; });

  if (noMoreSplits_) {
    return nullptr;
  }

  curStatus_ = "getOutput: enter";
  const auto startTimeMs = getCurrentTimeMs();
  for (;;) {
    if (needNewSplit_) {
      // Check if our Task needs us to yield or we've been running for too long
      // w/o producing a result. In this case we return with the Yield blocking
      // reason and an already fulfilled future.
      curStatus_ = "getOutput: task->shouldStop";
      const StopReason taskStopReason = driverCtx_->task->shouldStop();
      if (shouldStop(taskStopReason) ||
          shouldYield(taskStopReason, startTimeMs)) {
        blockingReason_ = BlockingReason::kYield;
        blockingFuture_ = ContinueFuture{folly::Unit{}};
        // A point for test code injection.
        TestValue::adjust(
            "facebook::velox::exec::TableScan::getOutput::yield", this);
        return nullptr;
      }

      // A point for test code injection.
      TestValue::adjust("facebook::velox::exec::TableScan::getOutput", this);

      exec::Split split;
      curStatus_ = "getOutput: task->getSplitOrFuture";
      blockingReason_ = driverCtx_->task->getSplitOrFuture(
          driverCtx_->splitGroupId,
          planNodeId(),
          split,
          blockingFuture_,
          maxPreloadedSplits_,
          splitPreloader_);
      if (blockingReason_ != BlockingReason::kNotBlocked) {
        return nullptr;
      }

      if (!split.hasConnectorSplit()) {
        noMoreSplits_ = true;
        dynamicFilters_.clear();
        if (dataSource_) {
          curStatus_ = "getOutput: noMoreSplits_=1, updating stats_";
          const auto connectorStats = dataSource_->runtimeStats();
          auto lockedStats = stats_.wlock();
          for (const auto& [name, counter] : connectorStats) {
            if (FOLLY_UNLIKELY(lockedStats->runtimeStats.count(name) == 0)) {
              lockedStats->runtimeStats.emplace(
                  name, RuntimeMetric(counter.unit));
            } else {
              VELOX_CHECK_EQ(
                  lockedStats->runtimeStats.at(name).unit, counter.unit);
            }
            lockedStats->runtimeStats.at(name).addValue(counter.value);
          }
        }
        return nullptr;
      }

      const auto& connectorSplit = split.connectorSplit;
      currentSplitWeight_ = connectorSplit->splitWeight;
      needNewSplit_ = false;

      // A point for test code injection.
      TestValue::adjust(
          "facebook::velox::exec::TableScan::getOutput::gotSplit", this);

      VELOX_CHECK_EQ(
          connector_->connectorId(),
          connectorSplit->connectorId,
          "Got splits with different connector IDs");

      if (dataSource_ == nullptr) {
        curStatus_ = "getOutput: creating dataSource_";
        connectorQueryCtx_ = operatorCtx_->createConnectorQueryCtx(
            connectorSplit->connectorId, planNodeId(), connectorPool_);
        dataSource_ = connector_->createDataSource(
            outputType_,
            tableHandle_,
            columnHandles_,
            connectorQueryCtx_.get());
        for (const auto& entry : dynamicFilters_) {
          dataSource_->addDynamicFilter(entry.first, entry.second);
        }
      }

      debugString_ = fmt::format(
          "Split [{}] Task {}",
          connectorSplit->toString(),
          operatorCtx_->task()->taskId());

      ExceptionContextSetter exceptionContext(
          {[](VeloxException::Type /*exceptionType*/, auto* debugString) {
             return *static_cast<std::string*>(debugString);
           },
           &debugString_});

      if (connectorSplit->dataSource != nullptr) {
        curStatus_ = "getOutput: preloaded split";
        ++numPreloadedSplits_;
        // The AsyncSource returns a unique_ptr to a shared_ptr. The unique_ptr
        // will be nullptr if there was a cancellation.
        numReadyPreloadedSplits_ += connectorSplit->dataSource->hasValue();
        auto preparedDataSource = connectorSplit->dataSource->move();
        stats_.wlock()->getOutputTiming.add(
            connectorSplit->dataSource->prepareTiming());
        if (!preparedDataSource) {
          // There must be a cancellation.
          VELOX_CHECK(operatorCtx_->task()->isCancelled());
          return nullptr;
        }
        dataSource_->setFromDataSource(std::move(preparedDataSource));
      } else {
        curStatus_ = "getOutput: adding split";
        const auto addSplitStartMicros = getCurrentTimeMicro();
        dataSource_->addSplit(connectorSplit);
        stats_.wlock()->addRuntimeStat(
            "dataSourceAddSplitWallNanos",
            RuntimeCounter(
                (getCurrentTimeMicro() - addSplitStartMicros) * 1'000,
                RuntimeCounter::Unit::kNanos));
      }
      curStatus_ = "getOutput: updating stats_.numSplits";
      ++stats_.wlock()->numSplits;

      curStatus_ = "getOutput: dataSource_->estimatedRowSize";
      const auto estimatedRowSize = dataSource_->estimatedRowSize();
      readBatchSize_ =
          estimatedRowSize == connector::DataSource::kUnknownRowSize
          ? outputBatchRows()
          : outputBatchRows(estimatedRowSize);
    }

    const auto ioTimeStartMicros = getCurrentTimeMicro();
    // Check for  cancellation since scans that filter everything out will not
    // hit the check in Driver.
    curStatus_ = "getOutput: task->isCancelled";
    if (operatorCtx_->task()->isCancelled()) {
      return nullptr;
    }

    ExceptionContextSetter exceptionContext(
        {[](VeloxException::Type /*exceptionType*/, auto* debugString) {
           return *static_cast<std::string*>(debugString);
         },
         &debugString_});

    int readBatchSize = readBatchSize_;
    if (maxFilteringRatio_ > 0) {
      readBatchSize = std::min(
          maxReadBatchSize_,
          static_cast<int>(readBatchSize / maxFilteringRatio_));
    }
    curStatus_ = "getOutput: dataSource_->next";
    auto dataOptional = dataSource_->next(readBatchSize, blockingFuture_);
    curStatus_ = "getOutput: checkPreload";
    checkPreload();

    {
      curStatus_ = "getOutput: updating stats_.dataSourceReadWallNanos";
      auto lockedStats = stats_.wlock();
      lockedStats->addRuntimeStat(
          "dataSourceReadWallNanos",
          RuntimeCounter(
              (getCurrentTimeMicro() - ioTimeStartMicros) * 1'000,
              RuntimeCounter::Unit::kNanos));

      if (!dataOptional.has_value()) {
        blockingReason_ = BlockingReason::kWaitForConnector;
        return nullptr;
      }

      curStatus_ = "getOutput: updating stats_.rawInput";
      lockedStats->rawInputPositions = dataSource_->getCompletedRows();
      lockedStats->rawInputBytes = dataSource_->getCompletedBytes();
      RowVectorPtr data = std::move(dataOptional).value();
      if (data != nullptr) {
        if (data->size() > 0) {
          lockedStats->addInputVector(data->estimateFlatSize(), data->size());
          constexpr int kMaxSelectiveBatchSizeMultiplier = 4;
          maxFilteringRatio_ = std::max(
              {maxFilteringRatio_,
               1.0 * data->size() / readBatchSize,
               1.0 / kMaxSelectiveBatchSizeMultiplier});
          return data;
        }
        continue;
      }
    }

    {
      curStatus_ = "getOutput: updating stats_.preloadedSplits";
      auto lockedStats = stats_.wlock();
      if (numPreloadedSplits_ > 0) {
        lockedStats->addRuntimeStat(
            "preloadedSplits", RuntimeCounter(numPreloadedSplits_));
        numPreloadedSplits_ = 0;
      }
      if (numReadyPreloadedSplits_ > 0) {
        lockedStats->addRuntimeStat(
            "readyPreloadedSplits", RuntimeCounter(numReadyPreloadedSplits_));
        numReadyPreloadedSplits_ = 0;
      }
    }

    curStatus_ = "getOutput: task->splitFinished";
    driverCtx_->task->splitFinished(true, currentSplitWeight_);
    needNewSplit_ = true;
  }
}

void TableScan::preload(
    const std::shared_ptr<connector::ConnectorSplit>& split) {
  // The AsyncSource returns a unique_ptr to the shared_ptr of the
  // DataSource. The callback may outlive the Task, hence it captures
  // a shared_ptr to it. This is required to keep memory pools live
  // for the duration. The callback checks for task cancellation to
  // avoid needless work.
  split->dataSource = std::make_unique<AsyncSource<connector::DataSource>>(
      [type = outputType_,
       table = tableHandle_,
       columns = columnHandles_,
       connector = connector_,
       ctx = operatorCtx_->createConnectorQueryCtx(
           split->connectorId, planNodeId(), connectorPool_),
       task = operatorCtx_->task(),
       dynamicFilters = dynamicFilters_,
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

        auto dataSource =
            connector->createDataSource(type, table, columns, ctx.get());
        if (task->isCancelled()) {
          return nullptr;
        }
        for (const auto& entry : dynamicFilters) {
          dataSource->addDynamicFilter(entry.first, entry.second);
        }
        dataSource->addSplit(split);
        return dataSource;
      });
}

void TableScan::checkPreload() {
  auto* executor = connector_->executor();
  if (maxSplitPreloadPerDriver_ == 0 || !executor ||
      !connector_->supportsSplitPreload()) {
    return;
  }
  if (dataSource_->allPrefetchIssued()) {
    maxPreloadedSplits_ = driverCtx_->task->numDrivers(driverCtx_->driver) *
        maxSplitPreloadPerDriver_;
    if (!splitPreloader_) {
      splitPreloader_ =
          [executor,
           this](const std::shared_ptr<connector::ConnectorSplit>& split) {
            preload(split);

            executor->add([connectorSplit = split]() mutable {
              connectorSplit->dataSource->prepare();
              connectorSplit.reset();
            });
          };
    }
  }
}

bool TableScan::isFinished() {
  return noMoreSplits_;
}

void TableScan::addDynamicFilter(
    const core::PlanNodeId& producer,
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  if (dataSource_) {
    dataSource_->addDynamicFilter(outputChannel, filter);
  }
  dynamicFilters_.emplace(outputChannel, filter);
  stats_.wlock()->dynamicFilterStats.producerNodeIds.emplace(producer);
}

} // namespace facebook::velox::exec
