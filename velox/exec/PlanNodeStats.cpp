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
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Operator.h"
#include "velox/exec/TaskStats.h"

namespace facebook::velox::exec {

void PlanNodeStats::add(const OperatorStats& stats) {
  addTotals(stats);

  auto it = operatorStats.find(stats.operatorType);
  if (it != operatorStats.end()) {
    it->second->addTotals(stats);
  } else {
    auto opStats = std::make_unique<PlanNodeStats>();
    opStats->addTotals(stats);
    operatorStats.emplace(stats.operatorType, std::move(opStats));
  }
}

void PlanNodeStats::addTotals(const OperatorStats& stats) {
  inputRows += stats.inputPositions;
  inputBytes += stats.inputBytes;

  outputRows += stats.outputPositions;
  outputBytes += stats.outputBytes;

  cpuWallTiming.add(stats.addInputTiming);
  cpuWallTiming.add(stats.getOutputTiming);
  cpuWallTiming.add(stats.finishTiming);

  blockedWallNanos += stats.blockedWallNanos;

  peakMemoryBytes += stats.memoryStats.peakTotalMemoryReservation;
}

std::string PlanNodeStats::toString(bool includeInputStats) const {
  std::stringstream out;
  if (includeInputStats) {
    out << "Input: " << inputRows << " rows (" << inputBytes << " bytes"
        << "), ";
  }
  out << "Output: " << outputRows << " rows (" << outputBytes << " bytes)"
      << ", Cpu time: " << cpuWallTiming.cpuNanos << "ns"
      << ", Blocked wall time: " << blockedWallNanos << "ns"
      << ", Peak memory: " << peakMemoryBytes << " bytes";
  return out.str();
}

std::unordered_map<core::PlanNodeId, PlanNodeStats> toPlanStats(
    const TaskStats& taskStats) {
  std::unordered_map<core::PlanNodeId, PlanNodeStats> planStats;

  for (const auto& pipelineStats : taskStats.pipelineStats) {
    for (const auto& opStats : pipelineStats.operatorStats) {
      const auto& planNodeId = opStats.planNodeId;
      auto it = planStats.find(planNodeId);
      if (it != planStats.end()) {
        it->second.add(opStats);
      } else {
        PlanNodeStats nodeStats;
        nodeStats.add(opStats);
        planStats.emplace(planNodeId, std::move(nodeStats));
      }
    }
  }

  return planStats;
}

std::string printPlanWithStats(
    const core::PlanNode& plan,
    const TaskStats& taskStats) {
  auto planStats = toPlanStats(taskStats);
  auto leafPlanNodes = plan.leafPlanNodeIds();

  return plan.toString(
      true,
      true,
      [&](const auto& planNodeId, const auto& indentation, auto& stream) {
        const auto& stats = planStats[planNodeId];

        // Print input rows and sizes only for leaf plan nodes. Including this
        // information for other plan nodes is redundant as it is the same as
        // output of the source nodes.
        const bool includeInputStats = leafPlanNodes.count(planNodeId) > 0;
        stream << stats.toString(includeInputStats);

        // Include break down by operator type if there are more than one of
        // these.
        if (stats.operatorStats.size() > 1) {
          int cnt = 0;
          for (const auto& entry : stats.operatorStats) {
            stream << std::endl;
            stream << indentation << entry.first << ": "
                   << entry.second->toString(includeInputStats);
            ++cnt;
          }
        }
      });
}
} // namespace facebook::velox::exec
