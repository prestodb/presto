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

#include "velox/core/PlanFragment.h"

namespace facebook::velox::runner {

/// Describes an exchange source for an ExchangeNode a non-leaf stage.
struct InputStage {
  // Id of ExchangeNode in the consumer fragment.
  core::PlanNodeId consumerNodeId;

  /// Task prefix of producer stage.
  std::string producerTaskPrefix;
};

/// Describes a fragment of a distributed plan. This allows a run
/// time to distribute fragments across workers and to set up
/// exchanges. A complete plan is a vector of these with the last
/// being the fragment that gathers results from the complete
/// plan. Different runtimes, e.g. local, streaming or
/// materialized shuffle can use this to describe exchange
/// parallel execution. Decisions on number of workers, location
/// of workers and mode of exchange are up to the runtime.
struct ExecutableFragment {
  ExecutableFragment() = default;

  explicit ExecutableFragment(const std::string& taskPrefix)
      : taskPrefix(taskPrefix) {}
  std::string taskPrefix;
  int32_t width{0};
  velox::core::PlanFragment fragment;

  /// Source fragments and Exchange node ids for remote shuffles producing input
  /// for 'this'.
  std::vector<InputStage> inputStages;

  /// Table scan nodes in 'this'.
  std::vector<std::shared_ptr<const core::TableScanNode>> scans;
  int32_t numBroadcastDestinations{0};
};

/// Describes a distributed plan handed to a Runner for parallel/distributed
/// execution. The last element of 'fragments' is by convention the stage that
/// gathers the query result. Otherwise the order of 'fragments' is not
/// important since the producer-consumer relations are given by 'inputStages'
/// in each fragment.
class MultiFragmentPlan {
 public:
  /// Describes options for running a MultiFragmentPlan.
  struct Options {
    /// Query id used as a prefix for tasks ids.
    std::string queryId;

    /// Maximum Number of independent Tasks for one stage of execution. If 1,
    /// there are no exchanges.
    int32_t numWorkers;

    /// Number of threads in a fragment in a worker. If 1, there are no local
    /// exchanges.
    int32_t numDrivers;
  };

  MultiFragmentPlan(std::vector<ExecutableFragment> fragments, Options options)
      : fragments_(std::move(fragments)), options_(std::move(options)) {}

  const std::vector<ExecutableFragment>& fragments() const {
    return fragments_;
  }

  const Options& options() const {
    return options_;
  }

  /// @param detailed If true, includes details of each plan node. Otherwise,
  /// only node types are included.
  std::string toString(bool detailed = true) const;

  /// Prints the summary of the plan using PlanNode::toSummaryString() API.
  std::string toSummaryString(core::PlanSummaryOptions options = {}) const;

 private:
  const std::vector<ExecutableFragment> fragments_;
  const Options options_;
};

using MultiFragmentPlanPtr = std::shared_ptr<const MultiFragmentPlan>;

} // namespace facebook::velox::runner
