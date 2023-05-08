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
#include "velox/common/time/Timer.h"
#include "velox/exec/Task.h"
#include "velox/expression/Expr.h"

DEFINE_int32(split_preload_per_driver, 2, "Prefetch split metadata");

namespace facebook::velox::exec {

std::atomic<uint64_t> TableScan::ioWaitNanos_;

TableScan::TableScan(
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::shared_ptr<const core::TableScanNode> tableScanNode)
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
      readBatchSize_(driverCtx_->task->queryCtx()
                         ->queryConfig()
                         .preferredOutputBatchRows()) {
  connector_ = connector::getConnector(tableHandle_->connectorId());
}

RowVectorPtr TableScan::getOutput() {
  if (noMoreSplits_) {
    return nullptr;
  }

  for (;;) {
    if (needNewSplit_) {
      exec::Split split;
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
        if (dataSource_) {
          auto connectorStats = dataSource_->runtimeStats();
          auto lockedStats = stats_.wlock();
          for (const auto& [name, counter] : connectorStats) {
            if (name == "ioWaitNanos") {
              ioWaitNanos_ += counter.value - lastIoWaitNanos_;
              lastIoWaitNanos_ = counter.value;
            }
            if (UNLIKELY(lockedStats->runtimeStats.count(name) == 0)) {
              lockedStats->runtimeStats.insert(
                  std::make_pair(name, RuntimeMetric(counter.unit)));
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
      needNewSplit_ = false;

      VELOX_CHECK_EQ(
          connector_->connectorId(),
          connectorSplit->connectorId,
          "Got splits with different connector IDs");

      if (!dataSource_) {
        connectorQueryCtx_ = operatorCtx_->createConnectorQueryCtx(
            connectorSplit->connectorId, planNodeId(), connectorPool_);
        dataSource_ = connector_->createDataSource(
            outputType_,
            tableHandle_,
            columnHandles_,
            connectorQueryCtx_.get());
        for (const auto& entry : pendingDynamicFilters_) {
          dataSource_->addDynamicFilter(entry.first, entry.second);
        }
        pendingDynamicFilters_.clear();
      }

      debugString_ = fmt::format(
          "Split {} Task {}",
          connectorSplit->toString(),
          operatorCtx_->task()->taskId());

      ExceptionContextSetter exceptionContext(
          {[](VeloxException::Type /*exceptionType*/, auto* debugString) {
             return *static_cast<std::string*>(debugString);
           },
           &debugString_});

      if (connectorSplit->dataSource) {
        ++numPreloadedSplits_;
        // The AsyncSource returns a unique_ptr to a shared_ptr. The
        // unique_ptr will be nullptr if there was a cancellation.
        numReadyPreloadedSplits_ += connectorSplit->dataSource->hasValue();
        auto preparedPtr = connectorSplit->dataSource->move();
        if (!preparedPtr) {
          // There must be a cancellation.
          VELOX_CHECK(operatorCtx_->task()->isCancelled());
          return nullptr;
        }
        auto preparedDataSource = std::move(*preparedPtr);
        dataSource_->setFromDataSource(std::move(preparedDataSource));
      } else {
        dataSource_->addSplit(connectorSplit);
      }
      ++stats_.wlock()->numSplits;

      auto estimatedRowSize = dataSource_->estimatedRowSize();
      readBatchSize_ =
          estimatedRowSize == connector::DataSource::kUnknownRowSize
          ? outputBatchRows()
          : outputBatchRows(estimatedRowSize);
    }

    const auto ioTimeStartMicros = getCurrentTimeMicro();
    // Check for  cancellation since scans that filter everything out will not
    // hit the check in Driver.
    if (operatorCtx_->task()->isCancelled()) {
      return nullptr;
    }
    ExceptionContextSetter exceptionContext(
        {[](VeloxException::Type /*exceptionType*/, auto* debugString) {
           return *static_cast<std::string*>(debugString);
         },
         &debugString_});

    auto dataOptional = dataSource_->next(readBatchSize_, blockingFuture_);
    checkPreload();

    {
      auto lockedStats = stats_.wlock();
      lockedStats->addRuntimeStat(
          "dataSourceWallNanos",
          RuntimeCounter(
              (getCurrentTimeMicro() - ioTimeStartMicros) * 1'000,
              RuntimeCounter::Unit::kNanos));

      if (!dataOptional.has_value()) {
        blockingReason_ = BlockingReason::kWaitForConnector;
        return nullptr;
      }

      lockedStats->rawInputPositions = dataSource_->getCompletedRows();
      lockedStats->rawInputBytes = dataSource_->getCompletedBytes();
      auto data = dataOptional.value();
      if (data) {
        if (data->size() > 0) {
          lockedStats->addInputVector(data->estimateFlatSize(), data->size());
          return data;
        }
        continue;
      }
    }

    {
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

    driverCtx_->task->splitFinished();
    needNewSplit_ = true;
  }
}

void TableScan::preload(std::shared_ptr<connector::ConnectorSplit> split) {
  // The AsyncSource returns a unique_ptr to the shared_ptr of the
  // DataSource. The callback may outlive the Task, hence it captures
  // a shared_ptr to it. This is required to keep memory pools live
  // for the duration. The callback checks for task cancellation to
  // avoid needless work.
  using DataSourcePtr = std::shared_ptr<connector::DataSource>;
  split->dataSource = std::make_shared<AsyncSource<DataSourcePtr>>(
      [type = outputType_,
       table = tableHandle_,
       columns = columnHandles_,
       connector = connector_,
       ctx = operatorCtx_->createConnectorQueryCtx(
           split->connectorId, planNodeId(), connectorPool_),
       task = operatorCtx_->task(),
       split]() -> std::unique_ptr<DataSourcePtr> {
        if (task->isCancelled()) {
          return nullptr;
        }
        auto ptr = std::make_unique<DataSourcePtr>();
        auto debugString =
            fmt::format("Split {} Task {}", split->toString(), task->taskId());
        ExceptionContextSetter exceptionContext(
            {[](VeloxException::Type /*exceptionType*/, auto* debugString) {
               return *static_cast<std::string*>(debugString);
             },
             &debugString});

        *ptr = connector->createDataSource(type, table, columns, ctx.get());
        if (task->isCancelled()) {
          return nullptr;
        }
        (*ptr)->addSplit(split);
        return ptr;
      });
}

void TableScan::checkPreload() {
  auto executor = connector_->executor();
  if (FLAGS_split_preload_per_driver == 0 || !executor ||
      !connector_->supportsSplitPreload()) {
    return;
  }
  if (dataSource_->allPrefetchIssued()) {
    maxPreloadedSplits_ = driverCtx_->task->numDrivers(driverCtx_->driver) *
        FLAGS_split_preload_per_driver;
    if (!splitPreloader_) {
      splitPreloader_ =
          [executor, this](std::shared_ptr<connector::ConnectorSplit> split) {
            preload(split);

            executor->add([taskHolder = operatorCtx_->task(), split]() mutable {
              split->dataSource->prepare();
              split.reset();
            });
          };
    }
  }
}

bool TableScan::isFinished() {
  return noMoreSplits_;
}

void TableScan::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  if (dataSource_) {
    dataSource_->addDynamicFilter(outputChannel, filter);
  } else {
    pendingDynamicFilters_.emplace(outputChannel, filter);
  }
}

} // namespace facebook::velox::exec
