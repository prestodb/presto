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

#include "velox/exec/AggregationMasks.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class Aggregate;
class RowContainer;

class StreamingAggregation : public Operator {
 public:
  StreamingAggregation(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::AggregationNode>& aggregationNode);

  ~StreamingAggregation();

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return true;
  }

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

 private:
  // Returns the rows to aggregate with masking applied if applicable.
  const SelectivityVector& getSelectivityVector(size_t aggregateIndex) const;

  // Allocate new group or re-use previously allocated group that has been fully
  // calculated and included in the output.
  char* startNewGroup(vector_size_t index);

  // Write grouping keys from the specified input row into specified group.
  void storeKeys(char* group, vector_size_t index);

  // Populate output_ vector using specified number of groups from the beginning
  // of the groups_ vector.
  RowVectorPtr createOutput(size_t numGroups);

  // Assign input rows to groups based on values of the grouping keys. Store the
  // assignments in inputGroups_.
  void assignGroups();

  // Add input data to accumulators.
  void evaluateAggregates();

  /// Maximum number of rows in the output batch.
  const uint32_t outputBatchSize_;

  const core::AggregationNode::Step step_;

  std::vector<column_index_t> groupingKeys_;
  std::vector<std::unique_ptr<Aggregate>> aggregates_;
  std::unique_ptr<AggregationMasks> masks_;
  std::vector<std::vector<column_index_t>> args_;
  std::vector<std::vector<VectorPtr>> constantArgs_;
  std::vector<DecodedVector> decodedKeys_;

  // Storage of grouping keys and accumulators.
  std::unique_ptr<RowContainer> rows_;

  // Previous input vector. Used to compare grouping keys for groups which span
  // batches.
  RowVectorPtr prevInput_;

  // Unique groups.
  std::vector<char*> groups_;

  // Number of active entries at the beginning of the groups_ vector. The
  // remaining entries are re-usable.
  size_t numGroups_{0};

  // Reusable memory.

  // Pointers to groups for all input rows.
  std::vector<char*> inputGroups_;

  // A subset of input rows to evaluate the aggregate function on. Rows
  // where aggregation mask is false are excluded.
  SelectivityVector inputRows_;
};

} // namespace facebook::velox::exec
