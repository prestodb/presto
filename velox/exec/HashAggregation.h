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

#include "velox/exec/GroupingSet.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class HashAggregation : public Operator {
 public:
  HashAggregation(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::AggregationNode>& aggregationNode);

  void initialize() override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && !partialFull_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

  void close() override;

 private:
  void updateRuntimeStats();

  void prepareOutput(vector_size_t size);

  // Invoked to reset partial aggregation state if it was full and has been
  // flushed.
  void resetPartialOutputIfNeed();

  // Invoked on partial output flush to try to bump up the partial aggregation
  // memory usage if it needs. 'aggregationPct' is the ratio between the number
  // of output rows and the number of input rows as a percentage. It is a
  // measure of the effectiveness of the partial aggregation.
  void maybeIncreasePartialAggregationMemoryUsage(double aggregationPct);

  // True if we have enough rows and not enough reduction, i.e. more than
  // 'abandonPartialAggregationMinRows_' rows and more than
  // 'abandonPartialAggregationMinPct_' % of rows are unique.
  bool abandonPartialAggregationEarly(int64_t numOutput) const;

  RowVectorPtr getDistinctOutput();

  // Invoked to record the spilling stats in operator stats after processing all
  // the inputs.
  void recordSpillStats();

  void updateEstimatedOutputRowSize();

  std::shared_ptr<const core::AggregationNode> aggregationNode_;

  const bool isPartialOutput_;
  const bool isGlobal_;
  const bool isDistinct_;
  const int64_t maxExtendedPartialAggregationMemoryUsage_;
  // Minimum number of rows to see before deciding to give up on partial
  // aggregation.
  const int32_t abandonPartialAggregationMinRows_;
  // Min unique rows pct for partial aggregation. If more than this many rows
  // are unique, the partial aggregation is not worthwhile.
  const int32_t abandonPartialAggregationMinPct_;

  int64_t maxPartialAggregationMemoryUsage_;
  std::unique_ptr<GroupingSet> groupingSet_;

  // Size of a single output row estimated using
  // 'groupingSet_->estimateRowSize()'. If spilling, this value is set to max
  // 'groupingSet_->estimateRowSize()' across all accumulated data set.
  std::optional<int64_t> estimatedOutputRowSize_;

  bool partialFull_ = false;
  bool newDistincts_ = false;
  bool finished_ = false;
  // True if partial aggregation has been found to be non-reducing.
  bool abandonedPartialAggregation_{false};

  RowContainerIterator resultIterator_;
  bool pushdownChecked_ = false;
  bool mayPushdown_ = false;

  // Count the number of input rows. It is reset on partial aggregation output
  // flush.
  int64_t numInputRows_ = 0;
  // Count the number of output rows. It is reset on partial aggregation output
  // flush.
  int64_t numOutputRows_ = 0;

  // Possibly reusable output vector.
  RowVectorPtr output_;
};

} // namespace facebook::velox::exec
