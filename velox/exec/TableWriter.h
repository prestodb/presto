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

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/// Defines table writer output related config properties that are shared
/// betweeen TableWriter and TableWriteMerger.
class TableWriterTraits {
 public:
  /// Defines the column channels in table writer output.
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

  /// Returns the parsed commit context from table writer 'output'.
  static folly::dynamic getTableCommitContext(const RowVectorPtr& output);

  /// Returns the sum of row counts from table writer 'output'.
  static int64_t getRowCount(const RowVectorPtr& output);
};

/**
 * The class implements a simple table writer VELOX operator
 */
class TableWriter : public Operator {
 public:
  TableWriter(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::TableWriteNode>& tableWriteNode);

  BlockingReason isBlocked(ContinueFuture* /* future */) override {
    return BlockingReason::kNotBlocked;
  }

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override {
    Operator::noMoreInput();
    close();
  }

  virtual bool needsInput() const override {
    return true;
  }

  void close() override {
    if (!closed_) {
      if (dataSink_) {
        dataSink_->close();
      }
      closed_ = true;
    }
  }

  RowVectorPtr getOutput() override;

  bool isFinished() override {
    return finished_;
  }

 private:
  void createDataSink();

  const DriverCtx* const driverCtx_;
  memory::MemoryPool* const connectorPool_;
  const std::shared_ptr<connector::ConnectorInsertTableHandle>
      insertTableHandle_;
  const connector::CommitStrategy commitStrategy_;
  std::shared_ptr<connector::Connector> connector_;
  std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  std::unique_ptr<connector::DataSink> dataSink_;
  std::vector<column_index_t> inputMapping_;
  std::shared_ptr<const RowType> mappedType_;

  bool finished_{false};
  bool closed_{false};
  vector_size_t numWrittenRows_{0};
};
} // namespace facebook::velox::exec
