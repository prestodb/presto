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

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && !partialFull_;
  }

  void noMoreInput() override {
    groupingSet_->noMoreInput();
    Operator::noMoreInput();
  }

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override {
    Operator::close();
    groupingSet_.reset();
  }

 private:
  void prepareOutput(vector_size_t size);
  void flushPartialOutputIfNeed();

  /// Maximum number of rows in the output batch.
  const uint32_t outputBatchSize_;

  const int64_t maxPartialAggregationMemoryUsage_;

  const bool isPartialOutput_;
  const bool isDistinct_;
  const bool isGlobal_;

  std::unique_ptr<GroupingSet> groupingSet_;

  bool partialFull_ = false;
  bool newDistincts_ = false;
  bool finished_ = false;
  RowContainerIterator resultIterator_;
  bool pushdownChecked_ = false;
  bool mayPushdown_ = false;

  /// Possibly reusable output vector.
  RowVectorPtr output_;
};

} // namespace facebook::velox::exec
