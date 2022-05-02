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

#include "velox/common/time/CpuWallTimer.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {
struct TaskStats;
} // namespace facebook::velox::exec

namespace facebook::velox::exec {

/// Aggregated runtime statistics per plan node.
///
/// Runtime statistics are collected on a per-operator instance basis. There can
/// be multiple operator types and multiple instances of each operator type that
/// correspond to a given plan node. For example, ProjectNode corresponds to
/// a single operator type, FilterProject, but HashJoinNode corresponds to two
/// operator types, HashProbe and HashBuild. Each operator type may have
/// different runtime parallelism, e.g. there can be multiple instances of each
/// operator type. Plan node statistics are calculated by adding up
/// operator-level statistics for all corresponding operator instances.
struct PlanNodeStats {
  explicit PlanNodeStats() = default;

  PlanNodeStats(const PlanNodeStats&) = delete;
  PlanNodeStats& operator=(const PlanNodeStats&) = delete;

  PlanNodeStats(PlanNodeStats&&) = default;
  PlanNodeStats& operator=(PlanNodeStats&&) = default;

  /// Sum of input rows for all corresponding operators. Useful primarily for
  /// leaf plan nodes or plan nodes that correspond to a single operator type.
  vector_size_t inputRows{0};

  /// Sum of input bytes for all corresponding operators.
  uint64_t inputBytes{0};

  /// Sum of raw input rows for all corresponding operators. Applies primarily
  /// to TableScan operator which reports rows before pushed down filter as raw
  /// input.
  vector_size_t rawInputRows{0};

  /// Sum of raw input bytes for all corresponding operators.
  uint64_t rawInputBytes{0};

  /// Sum of output rows for all corresponding operators. When
  /// plan node corresponds to multiple operator types, operators of only one of
  /// these types report non-zero output rows.
  vector_size_t outputRows{0};

  /// Sum of output bytes for all corresponding operators.
  uint64_t outputBytes{0};

  /// Sum of CPU, scheduled and wall times for all corresponding operators. For
  /// each operator, timing of addInput, getOutput and finish calls are added
  /// up.
  CpuWallTiming cpuWallTiming;

  /// Sum of blocked wall time for all corresponding operators.
  uint64_t blockedWallNanos{0};

  /// Max of peak memory usage for all corresponding operators. Assumes that all
  /// operator instances were running concurrently.
  uint64_t peakMemoryBytes{0};

  uint64_t numMemoryAllocations{0};

  /// Operator-specific counters.
  std::unordered_map<std::string, RuntimeMetric> customStats;

  /// Breakdown of stats by operator type.
  std::unordered_map<std::string, std::unique_ptr<PlanNodeStats>> operatorStats;

  /// Number of drivers that executed the pipeline.
  int numDrivers{0};

  /// Number of total splits.
  int numSplits{0};

  /// Add stats for a single operator instance.
  void add(const OperatorStats& stats);

  std::string toString(bool includeInputStats = false) const;

  bool isMultiOperatorNode() const {
    return operatorStats.size() > 1;
  }

 private:
  void addTotals(const OperatorStats& stats);
};

std::unordered_map<core::PlanNodeId, PlanNodeStats> toPlanStats(
    const TaskStats& taskStats);

/// Returns human-friendly representation of the plan augmented with runtime
/// statistics. The result has the same plan representation as in
/// PlanNode::toString(true, true), but each plan node includes an additional
/// line with runtime statistics. Plan nodes that correspond to multiple
/// operator types, e.g. HashJoinNode, also include breakdown of runtime
/// statistics per operator type.
///
/// Note that input row counts and sizes are printed only for leaf plan nodes.
///
/// @param includeCustomStats If true, prints operator-specific counters.
std::string printPlanWithStats(
    const core::PlanNode& plan,
    const TaskStats& taskStats,
    bool includeCustomStats = false);
} // namespace facebook::velox::exec
