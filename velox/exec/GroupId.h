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

#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class GroupId : public Operator {
 public:
  GroupId(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::GroupIdNode>& groupIdNode);

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_ || (noMoreInput_ && input_ == nullptr);
  }

 private:
  static constexpr column_index_t kMissingGroupingKey =
      std::numeric_limits<column_index_t>::max();

  bool finished_{false};

  /// A grouping set contains a subset of all the grouping keys. This list
  /// contains one entry per grouping set and identifies the grouping keys that
  /// are part of the set as indices of the input columns. The position in the
  /// list identifies the grouping key column in the output. Positions with
  /// kMissingGroupingKey correspond to grouping keys which are not included in
  /// the set.
  std::vector<std::vector<column_index_t>> groupingKeyMappings_;

  /// A list of input column indices corresponding to aggregation inputs. The
  /// position in the list identifies the column in the output.
  std::vector<column_index_t> aggregationInputs_;

  /// 'getOutput()' returns 'input_' for one grouping set at a time.
  /// 'groupingSetIndex_' contains the index of the grouping set to output in
  /// the next 'getOutput' call. This index is used to generate groupId column
  /// and lookup the input-to-output column mappings in the
  /// groupingKeyMappings_.
  int32_t groupingSetIndex_{0};
};
} // namespace facebook::velox::exec
