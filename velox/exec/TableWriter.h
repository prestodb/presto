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

#include "OperatorUtils.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/// Defines table writer output related config properties that are shared
/// between TableWriter and TableWriteMerger.
///
/// TODO: the table write output processing is Prestissimo specific. Consider
/// move these part logic to Prestissimo and pass to Velox through a customized
/// output processing callback.
class TableWriteTraits {
 public:
  /// Defines the column names/types in table write output.
  static std::string rowCountColumnName();
  static std::string fragmentColumnName();
  static std::string contextColumnName();

  static const TypePtr& rowCountColumnType();
  static const TypePtr& fragmentColumnType();
  static const TypePtr& contextColumnType();

  /// Defines the column channels in table write output.
  /// Both the statistics and the row_count + fragments are transferred over the
  /// same communication link between the TableWriter and TableFinish. Thus the
  /// multiplexing is needed.
  ///
  ///  The transferred page layout looks like:
  /// [row_count_channel], [fragment_channel], [context_channel],
  /// [statistic_channel_1] ... [statistic_channel_N]]
  ///
  /// [row_count_channel] - contains number of rows processed by a TableWriter
  /// [fragment_channel] - contains data provided by the DataSink#finish
  /// [statistic_channel_1] ...[statistic_channel_N] -
  /// contain aggregated statistics computed by the statistics aggregation
  /// within the TableWriter
  ///
  /// For convenience, we never set both: [row_count_channel] +
  /// [fragment_channel] and the [statistic_channel_1] ...
  /// [statistic_channel_N].
  ///
  /// If this is a row that holds statistics - the [row_count_channel] +
  /// [fragment_channel] will be NULL.
  ///
  /// If this is a row that holds the row count
  /// or the fragment - all the statistics channels will be set to NULL.
  static constexpr int32_t kRowCountChannel = 0;
  static constexpr int32_t kFragmentChannel = 1;
  static constexpr int32_t kContextChannel = 2;
  static constexpr int32_t kStatsChannel = 3;

  /// Defines the names of metadata in commit context in table writer output.
  static constexpr std::string_view kLifeSpanContextKey = "lifespan";
  static constexpr std::string_view kTaskIdContextKey = "taskId";
  static constexpr std::string_view kCommitStrategyContextKey =
      "pageSinkCommitStrategy";
  static constexpr std::string_view klastPageContextKey = "lastPage";

  static const RowTypePtr outputType(
      const std::shared_ptr<core::AggregationNode>& aggregationNode = nullptr);

  /// Returns the parsed commit context from table writer 'output'.
  static folly::dynamic getTableCommitContext(const RowVectorPtr& output);

  /// Returns the sum of row counts from table writer 'output'.
  static int64_t getRowCount(const RowVectorPtr& output);

  /// Creates the statistics output.
  /// Statistics page layout (aggregate by partition):
  /// row     fragments     context     [partition]   stats1     stats2 ...
  /// null       null          X          [X]            X          X
  /// null       null          X          [X]            X          X
  static RowVectorPtr createAggregationStatsOutput(
      RowTypePtr outputType,
      RowVectorPtr aggregationOutput,
      StringView tableCommitContext,
      velox::memory::MemoryPool* pool);
};

class TableWriter : public Operator {
 public:
  TableWriter(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::TableWriteNode>& tableWriteNode);

  BlockingReason isBlocked(ContinueFuture* future) override;

  void initialize() override;

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  virtual bool needsInput() const override {
    return true;
  }

  void close() override;

  RowVectorPtr getOutput() override;

  bool isFinished() override {
    return finished_;
  }

  /// NOTE: we don't reclaim memory from table write operator directly but from
  /// its paired connector pool which reclaims memory from the file writers
  /// created inside the connector.
  bool canReclaim() const override {
    return false;
  }

  OperatorStats stats(bool clear) override {
    auto stats = Operator::stats(clear);
    // NOTE: file writers allocates memory through 'connectorPool_', not from
    // the table writer operator pool. So we report the memory usage from
    // 'connectorPool_'.
    stats.memoryStats = MemoryStats::memStatsFromPool(connectorPool_);
    return stats;
  }

 private:
  // The memory reclaimer customized for connector which interface with the
  // memory arbitrator to reclaim memory from the file writers created within
  // the connector.
  class ConnectorReclaimer : public exec::ParallelMemoryReclaimer {
   public:
    static std::unique_ptr<memory::MemoryReclaimer> create(
        const std::optional<common::SpillConfig>& spillConfig,
        DriverCtx* driverCtx,
        Operator* op);

    void enterArbitration() override {}

    void leaveArbitration() noexcept override {}

    bool reclaimableBytes(
        const memory::MemoryPool& pool,
        uint64_t& reclaimableBytes) const override;

    uint64_t reclaim(
        memory::MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        memory::MemoryReclaimer::Stats& stats) override;

    void abort(memory::MemoryPool* pool, const std::exception_ptr& /* error */)
        override {}

    std::shared_ptr<Driver> ensureDriver() const {
      return driver_.lock();
    }

   private:
    ConnectorReclaimer(
        const std::optional<common::SpillConfig>& spillConfig,
        const std::shared_ptr<Driver>& driver,
        Operator* op)
        : ParallelMemoryReclaimer(
              spillConfig.has_value() ? spillConfig.value().executor : nullptr),
          canReclaim_(spillConfig.has_value()),
          driver_(driver),
          op_(op) {}

    const bool canReclaim_{false};
    const std::weak_ptr<Driver> driver_;
    Operator* const op_;
  };

  void createDataSink();

  bool finishDataSink();

  std::vector<std::string> closeDataSink();

  void abortDataSink();

  void updateStats(const connector::DataSink::Stats& stats);

  std::string createTableCommitContext(bool lastOutput);

  void setConnectorMemoryReclaimer();

  const DriverCtx* const driverCtx_;
  memory::MemoryPool* const connectorPool_;
  const std::shared_ptr<connector::ConnectorInsertTableHandle>
      insertTableHandle_;
  const connector::CommitStrategy commitStrategy_;

  std::unique_ptr<Operator> aggregation_;
  std::shared_ptr<connector::Connector> connector_;
  std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  std::unique_ptr<connector::DataSink> dataSink_;
  std::vector<column_index_t> inputMapping_;
  std::shared_ptr<const RowType> mappedType_;

  // The blocking future might be set when finish data sink.
  ContinueFuture blockingFuture_{ContinueFuture::makeEmpty()};
  BlockingReason blockingReason_{BlockingReason::kNotBlocked};

  bool finished_{false};
  bool closed_{false};
  vector_size_t numWrittenRows_{0};
};
} // namespace facebook::velox::exec
