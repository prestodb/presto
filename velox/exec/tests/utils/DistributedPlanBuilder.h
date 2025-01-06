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

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/runner/MultiFragmentPlan.h"

namespace facebook::velox::exec::test {

/// Builder for distributed plans. Adds a shuffle() and related
/// methods for building PartitionedOutput-Exchange pairs between
/// fragments. Not thread safe.
class DistributedPlanBuilder : public PlanBuilder {
 public:
  /// Constructs a top level DistributedPlanBuilder.
  DistributedPlanBuilder(
      const runner::MultiFragmentPlan::Options& options,
      std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator,
      memory::MemoryPool* pool = nullptr);

  /// Constructs a child builder. Used for branching plans, e.g. the subplan for
  /// a join build side.
  DistributedPlanBuilder(DistributedPlanBuilder& root);

  ~DistributedPlanBuilder() override;

  /// Returns the planned fragments. The builder will be empty after this. This
  /// is only called on the root builder.
  std::vector<runner::ExecutableFragment> fragments();

  PlanBuilder& shufflePartitioned(
      const std::vector<std::string>& keys,
      int numPartitions,
      bool replicateNullsAndAny,
      const std::vector<std::string>& outputLayout = {}) override;

  core::PlanNodePtr shufflePartitionedResult(
      const std::vector<std::string>& keys,
      int numPartitions,
      bool replicateNullsAndAny,
      const std::vector<std::string>& outputLayout = {}) override;

  core::PlanNodePtr shuffleBroadcastResult() override;

 private:
  void newFragment();

  void gatherScans(const core::PlanNodePtr& plan);

  const runner::MultiFragmentPlan::Options& options_;
  DistributedPlanBuilder* const root_;

  // Stack of outstanding builders. The last element is the immediately
  // enclosing one. When returning an ExchangeNode from returnShuffle, the stack
  // is used to establish the linkage between the fragment of the returning
  // builder and the fragment current in the calling builder. Only filled in the
  // root builder.
  std::vector<DistributedPlanBuilder*> stack_;

  // Fragment counter. Only used in root builder.
  int32_t fragmentCounter_{0};

  // The fragment being built. Will be moved to the root builder's 'fragments_'
  // when complete.
  std::unique_ptr<runner::ExecutableFragment> current_;

  // The fragments gathered under this builder. Moved to the root builder when
  // returning the subplan.
  std::vector<runner::ExecutableFragment> fragments_;
};

} // namespace facebook::velox::exec::test
