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

#include "velox/exec/TableWriter.h"

#include "HashAggregation.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

TableWriter::TableWriter(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::TableWriteNode>& tableWriteNode)
    : Operator(
          driverCtx,
          tableWriteNode->outputType(),
          operatorId,
          tableWriteNode->id(),
          "TableWrite",
          tableWriteNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      driverCtx_(driverCtx),
      connectorPool_(driverCtx_->task->addConnectorPoolLocked(
          planNodeId(),
          driverCtx_->pipelineId,
          driverCtx_->driverId,
          operatorType(),
          tableWriteNode->insertTableHandle()->connectorId())),
      insertTableHandle_(
          tableWriteNode->insertTableHandle()->connectorInsertTableHandle()),
      commitStrategy_(tableWriteNode->commitStrategy()) {
  setConnectorMemoryReclaimer();
  if (tableWriteNode->outputType()->size() == 1) {
    VELOX_USER_CHECK_NULL(tableWriteNode->aggregationNode());
  } else {
    VELOX_USER_CHECK(tableWriteNode->outputType()->equivalent(
        *(TableWriteTraits::outputType(tableWriteNode->aggregationNode()))));
  }

  if (tableWriteNode->aggregationNode() != nullptr) {
    aggregation_ = std::make_unique<HashAggregation>(
        operatorId, driverCtx, tableWriteNode->aggregationNode());
  }
  const auto& connectorId = tableWriteNode->insertTableHandle()->connectorId();
  connector_ = connector::getConnector(connectorId);
  connectorQueryCtx_ = operatorCtx_->createConnectorQueryCtx(
      connectorId,
      planNodeId(),
      connectorPool_,
      spillConfig_.has_value() ? &(spillConfig_.value()) : nullptr);

  auto names = tableWriteNode->columnNames();
  auto types = tableWriteNode->columns()->children();

  const auto& inputType = tableWriteNode->sources()[0]->outputType();

  inputMapping_.reserve(types.size());
  for (const auto& name : tableWriteNode->columns()->names()) {
    inputMapping_.emplace_back(inputType->getChildIdx(name));
  }

  mappedType_ = ROW(std::move(names), std::move(types));
}

void TableWriter::initialize() {
  Operator::initialize();
  VELOX_CHECK_NULL(dataSink_);
  createDataSink();
  if (aggregation_ != nullptr) {
    aggregation_->initialize();
  }
}

void TableWriter::createDataSink() {
  dataSink_ = connector_->createDataSink(
      mappedType_,
      insertTableHandle_,
      connectorQueryCtx_.get(),
      commitStrategy_);
}

void TableWriter::abortDataSink() {
  VELOX_CHECK(!closed_);
  auto abortGuard = folly::makeGuard([this]() { closed_ = true; });
  if (dataSink_ != nullptr) {
    try {
      dataSink_->abort();
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to abort data sink from table writer: "
                   << toString() << ", error: " << e.what();
    }
  }
}

std::vector<std::string> TableWriter::closeDataSink() {
  // We only expect closeDataSink called once.
  VELOX_CHECK(!closed_);
  VELOX_CHECK_NOT_NULL(dataSink_);
  auto closeGuard = folly::makeGuard([this]() { closed_ = true; });
  return dataSink_->close();
}

bool TableWriter::finishDataSink() {
  // We only expect finish on a non-closed data sink.
  VELOX_CHECK(!closed_);
  VELOX_CHECK_NOT_NULL(dataSink_);
  return dataSink_->finish();
}

void TableWriter::addInput(RowVectorPtr input) {
  if (input->size() == 0) {
    return;
  }
  traceInput(input);
  std::vector<VectorPtr> mappedChildren;
  mappedChildren.reserve(inputMapping_.size());
  for (const auto i : inputMapping_) {
    mappedChildren.emplace_back(input->childAt(i));
  }

  const auto mappedInput = std::make_shared<RowVector>(
      input->pool(),
      mappedType_,
      input->nulls(),
      input->size(),
      mappedChildren,
      input->getNullCount());

  dataSink_->appendData(mappedInput);
  numWrittenRows_ += input->size();
  updateStats(dataSink_->stats());

  if (aggregation_ != nullptr) {
    aggregation_->addInput(input);
  }
}

void TableWriter::noMoreInput() {
  Operator::noMoreInput();
  if (aggregation_ != nullptr) {
    aggregation_->noMoreInput();
  }
}

BlockingReason TableWriter::isBlocked(ContinueFuture* future) {
  if (blockingFuture_.valid()) {
    *future = std::move(blockingFuture_);
    return blockingReason_;
  }
  return BlockingReason::kNotBlocked;
}

RowVectorPtr TableWriter::getOutput() {
  // Making sure the output is read only once after the write is fully done.
  if (!noMoreInput_ || finished_) {
    return nullptr;
  }

  if (aggregation_ != nullptr && !aggregation_->isFinished()) {
    const std::string commitContext = createTableCommitContext(false);
    return TableWriteTraits::createAggregationStatsOutput(
        outputType_,
        aggregation_->getOutput(),
        StringView(commitContext),
        pool());
  }

  if (!finishDataSink()) {
    blockingReason_ = BlockingReason::kYield;
    blockingFuture_ = ContinueFuture{folly::Unit{}};
    return nullptr;
  }

  finished_ = true;
  const std::vector<std::string> fragments = closeDataSink();
  updateStats(dataSink_->stats());

  if (outputType_->size() == 1) {
    // NOTE: this is for non-prestissimo use cases.
    return std::make_shared<RowVector>(
        pool(),
        outputType_,
        nullptr,
        1,
        std::vector<VectorPtr>{std::make_shared<ConstantVector<int64_t>>(
            pool(), 1, false /*isNull*/, BIGINT(), numWrittenRows_)});
  }

  const vector_size_t numOutputRows = fragments.size() + 1;

  // Page layout:
  // row     fragments     context    [partition]     [stats]
  // X         null          X        [null]          [null]
  // null       X            X        [null]          [null]
  // null       X            X        [null]          [null]

  // 1. Set rows column.
  FlatVectorPtr<int64_t> writtenRowsVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), numOutputRows, pool());
  writtenRowsVector->set(0, (int64_t)numWrittenRows_);
  for (int idx = 1; idx < numOutputRows; ++idx) {
    writtenRowsVector->setNull(idx, true);
  }

  // 2. Set fragments column.
  FlatVectorPtr<StringView> fragmentsVector =
      BaseVector::create<FlatVector<StringView>>(
          VARBINARY(), numOutputRows, pool());
  fragmentsVector->setNull(0, true);
  for (int i = 1; i < numOutputRows; ++i) {
    fragmentsVector->set(i, StringView(fragments[i - 1]));
  }

  // 3. Set commitcontext column.
  const std::string commitContext = createTableCommitContext(true);
  auto commitContextVector = std::make_shared<ConstantVector<StringView>>(
      pool(),
      numOutputRows,
      false /*isNull*/,
      VARBINARY(),
      StringView(commitContext));

  std::vector<VectorPtr> columns = {
      writtenRowsVector, fragmentsVector, commitContextVector};

  // 4. Set null statistics columns.
  if (aggregation_ != nullptr) {
    for (int i = TableWriteTraits::kStatsChannel; i < outputType_->size();
         ++i) {
      columns.push_back(BaseVector::createNullConstant(
          outputType_->childAt(i), writtenRowsVector->size(), pool()));
    }
  }

  return std::make_shared<RowVector>(
      pool(), outputType_, nullptr, numOutputRows, columns);
}

std::string TableWriter::createTableCommitContext(bool lastOutput) {
  // clang-format off
    return folly::toJson(
      folly::dynamic::object
          (TableWriteTraits::kLifeSpanContextKey, "TaskWide")
          (TableWriteTraits::kTaskIdContextKey, connectorQueryCtx_->taskId())
          (TableWriteTraits::kCommitStrategyContextKey, commitStrategyToString(commitStrategy_))
          (TableWriteTraits::klastPageContextKey, lastOutput));
  // clang-format on
}

void TableWriter::updateStats(const connector::DataSink::Stats& stats) {
  {
    auto lockedStats = stats_.wlock();
    lockedStats->physicalWrittenBytes = stats.numWrittenBytes;
    if (!closed_) {
      // NOTE: the other stats is only set when hive data sink is closed.
      VELOX_CHECK_EQ(stats.numWrittenFiles, 0);
      VELOX_CHECK(stats.spillStats.empty());
      return;
    }
    lockedStats->addRuntimeStat(
        "numWrittenFiles", RuntimeCounter(stats.numWrittenFiles));
    lockedStats->addRuntimeStat(
        "writeIOTime",
        RuntimeCounter(
            stats.writeIOTimeUs * 1000, RuntimeCounter::Unit::kNanos));
  }
  if (!stats.spillStats.empty()) {
    *spillStats_.wlock() += stats.spillStats;
  }
}

void TableWriter::close() {
  Operator::close();
  if (!closed_) {
    // Abort the data sink if the query has already failed and no need for
    // regular close.
    abortDataSink();
  }
  if (aggregation_ != nullptr) {
    aggregation_->close();
  }
}

void TableWriter::setConnectorMemoryReclaimer() {
  VELOX_CHECK_NOT_NULL(connectorPool_);
  if (connectorPool_->parent()->reclaimer() != nullptr) {
    connectorPool_->setReclaimer(TableWriter::ConnectorReclaimer::create(
        spillConfig_, operatorCtx_->driverCtx(), this));
  }
}

std::unique_ptr<memory::MemoryReclaimer>
TableWriter::ConnectorReclaimer::create(
    const std::optional<common::SpillConfig>& spillConfig,
    DriverCtx* driverCtx,
    Operator* op) {
  return std::unique_ptr<memory::MemoryReclaimer>(
      new TableWriter::ConnectorReclaimer(
          spillConfig, driverCtx->driver->shared_from_this(), op));
}

bool TableWriter::ConnectorReclaimer::reclaimableBytes(
    const memory::MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  reclaimableBytes = 0;
  if (!canReclaim_) {
    return false;
  }
  std::shared_ptr<Driver> driver = ensureDriver();
  if (FOLLY_UNLIKELY(driver == nullptr)) {
    return false;
  }
  return memory::MemoryReclaimer::reclaimableBytes(pool, reclaimableBytes);
}

uint64_t TableWriter::ConnectorReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  if (!canReclaim_) {
    return 0;
  }
  std::shared_ptr<Driver> driver = ensureDriver();
  if (FOLLY_UNLIKELY(driver == nullptr)) {
    return 0;
  }
  VELOX_CHECK(
      !driver->state().isOnThread() || driver->state().suspended() ||
      driver->state().isTerminated);
  VELOX_CHECK(driver->task()->pauseRequested());

  auto* writer = dynamic_cast<TableWriter*>(op_);
  if (writer->closed_) {
    // TODO: reduce the log frequency if it is too verbose.
    ++stats.numNonReclaimableAttempts;
    LOG(WARNING) << "Can't reclaim from a closed writer connector pool: "
                 << pool->name()
                 << ", memory usage: " << succinctBytes(pool->reservedBytes());
    return 0;
  }

  if (writer->dataSink_ == nullptr) {
    // TODO: reduce the log frequency if it is too verbose.
    ++stats.numNonReclaimableAttempts;
    LOG(WARNING)
        << "Can't reclaim from a writer connector pool which hasn't initialized yet: "
        << pool->name()
        << ", memory usage: " << succinctBytes(pool->reservedBytes());
    return 0;
  }
  RuntimeStatWriterScopeGuard opStatsGuard(op_);
  return ParallelMemoryReclaimer::reclaim(pool, targetBytes, maxWaitMs, stats);
}

// static
RowVectorPtr TableWriteTraits::createAggregationStatsOutput(
    RowTypePtr outputType,
    RowVectorPtr aggregationOutput,
    StringView tableCommitContext,
    velox::memory::MemoryPool* pool) {
  // TODO: record aggregation stats output time.
  if (aggregationOutput == nullptr) {
    return nullptr;
  }
  VELOX_CHECK_GT(aggregationOutput->childrenSize(), 0);
  const vector_size_t numOutputRows = aggregationOutput->childAt(0)->size();
  std::vector<VectorPtr> columns;
  for (int channel = 0; channel < outputType->size(); channel++) {
    if (channel < TableWriteTraits::kContextChannel) {
      // 1. Set null rows column.
      // 2. Set null fragments column.
      columns.push_back(BaseVector::createNullConstant(
          outputType->childAt(channel), numOutputRows, pool));
      continue;
    }
    if (channel == TableWriteTraits::kContextChannel) {
      // 3. Set commitcontext column.
      columns.push_back(std::make_shared<ConstantVector<StringView>>(
          pool,
          numOutputRows,
          false /*isNull*/,
          VARBINARY(),
          std::move(tableCommitContext)));
      continue;
    }
    // 4. Set statistics columns.
    columns.push_back(
        aggregationOutput->childAt(channel - TableWriteTraits::kStatsChannel));
  }
  return std::make_shared<RowVector>(
      pool, outputType, nullptr, numOutputRows, columns);
}

std::string TableWriteTraits::rowCountColumnName() {
  static const std::string kRowCountName = "rows";
  return kRowCountName;
}

std::string TableWriteTraits::fragmentColumnName() {
  static const std::string kFragmentName = "fragments";
  return kFragmentName;
}

std::string TableWriteTraits::contextColumnName() {
  static const std::string kContextName = "commitcontext";
  return kContextName;
}

const TypePtr& TableWriteTraits::rowCountColumnType() {
  static const TypePtr kRowCountType = BIGINT();
  return kRowCountType;
}

const TypePtr& TableWriteTraits::fragmentColumnType() {
  static const TypePtr kFragmentType = VARBINARY();
  return kFragmentType;
}

const TypePtr& TableWriteTraits::contextColumnType() {
  static const TypePtr kContextType = VARBINARY();
  return kContextType;
}

const RowTypePtr TableWriteTraits::outputType(
    const std::shared_ptr<core::AggregationNode>& aggregationNode) {
  static const auto kOutputTypeWithoutStats =
      ROW({rowCountColumnName(), fragmentColumnName(), contextColumnName()},
          {rowCountColumnType(), fragmentColumnType(), contextColumnType()});
  if (aggregationNode == nullptr) {
    return kOutputTypeWithoutStats;
  }
  return kOutputTypeWithoutStats->unionWith(aggregationNode->outputType());
}

folly::dynamic TableWriteTraits::getTableCommitContext(
    const RowVectorPtr& input) {
  VELOX_CHECK_GT(input->size(), 0);
  auto* contextVector =
      input->childAt(kContextChannel)->as<SimpleVector<StringView>>();
  return folly::parseJson(contextVector->valueAt(input->size() - 1));
}

int64_t TableWriteTraits::getRowCount(const RowVectorPtr& output) {
  VELOX_CHECK_GT(output->size(), 0);
  auto rowCountVector =
      output->childAt(kRowCountChannel)->asFlatVector<int64_t>();
  VELOX_CHECK_NOT_NULL(rowCountVector);
  int64_t rowCount{0};
  for (int i = 0; i < output->size(); ++i) {
    if (!rowCountVector->isNullAt(i)) {
      rowCount += rowCountVector->valueAt(i);
    }
  }
  return rowCount;
}
} // namespace facebook::velox::exec
