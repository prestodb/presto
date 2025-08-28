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
#include "velox/exec/ColumnStatsCollector.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/// Implements a merge operator to aggregate the metadata outputs from multiple
/// table write operators and produces the aggregated result.
class TableWriteMerge : public Operator {
 public:
  TableWriteMerge(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::TableWriteMergeNode>&
          tableWriteMergeNode);

  void initialize() override;

  BlockingReason isBlocked(ContinueFuture* /* future */) override {
    return BlockingReason::kNotBlocked;
  }

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  virtual bool needsInput() const override {
    return true;
  }

  RowVectorPtr getOutput() override;

  bool isFinished() override {
    return finished_;
  }

  void close() override;

 private:
  // Creates non-last output with fragments and last commit context only.
  RowVectorPtr createFragmentsOutput();

  // Creates json encoded string of last commit context with specified
  // 'lastOutput' flag.
  std::string createTableCommitContext(bool lastOutput) const;

  // Creates the last output and fragment columns must be null.
  RowVectorPtr createLastOutput();

  // Check if the input is statistics input.
  bool isStatistics(RowVectorPtr input);

  std::unique_ptr<ColumnStatsCollector> statsCollector_;
  bool finished_{false};
  // The sum of written rows.
  int64_t numRows_{0};
  std::queue<VectorPtr> fragmentVectors_;
  folly::dynamic lastCommitContext_;
};
} // namespace facebook::velox::exec
