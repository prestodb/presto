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
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/exec/TaskStats.h"

namespace facebook::velox::exec {

void PlanNodeStats::add(const OperatorStats& stats) {
  auto it = operatorStats.find(stats.operatorType);
  if (it != operatorStats.end()) {
    it->second->addTotals(stats);
  } else {
    auto opStats = std::make_unique<PlanNodeStats>();
    opStats->addTotals(stats);
    operatorStats.emplace(stats.operatorType, std::move(opStats));
  }
  addTotals(stats);
}

void PlanNodeStats::addTotals(const OperatorStats& stats) {
  inputRows += stats.inputPositions;
  inputBytes += stats.inputBytes;
  inputVectors += stats.inputVectors;

  rawInputRows += stats.rawInputPositions;
  rawInputBytes += stats.rawInputBytes;

  outputRows += stats.outputPositions;
  outputBytes += stats.outputBytes;
  outputVectors += stats.outputVectors;

  cpuWallTiming.add(stats.addInputTiming);
  cpuWallTiming.add(stats.getOutputTiming);
  cpuWallTiming.add(stats.finishTiming);

  blockedWallNanos += stats.blockedWallNanos;

  peakMemoryBytes += stats.memoryStats.peakTotalMemoryReservation;
  numMemoryAllocations += stats.memoryStats.numMemoryAllocations;

  for (const auto& [name, runtimeStats] : stats.runtimeStats) {
    if (UNLIKELY(customStats.count(name) == 0)) {
      customStats.insert(std::make_pair(name, runtimeStats));
    } else {
      customStats.at(name).merge(runtimeStats);
    }
  }

  // Populating number of drivers for plan nodes with multiple operators is not
  // useful. Each operator could have been executed in different pipelines with
  // different number of drivers.
  if (!isMultiOperatorTypeNode()) {
    numDrivers += stats.numDrivers;
  } else {
    numDrivers = 0;
  }

  numSplits += stats.numSplits;
}

std::string PlanNodeStats::toString(bool includeInputStats) const {
  std::stringstream out;
  if (includeInputStats) {
    out << "Input: " << inputRows << " rows (" << succinctBytes(inputBytes)
        << ", " << inputVectors << " batches), ";
    if ((rawInputRows > 0) && (rawInputRows != inputRows)) {
      out << "Raw Input: " << rawInputRows << " rows ("
          << succinctBytes(rawInputBytes) << "), ";
    }
  }
  out << "Output: " << outputRows << " rows (" << succinctBytes(outputBytes)
      << ", " << outputVectors << " batches)"
      << ", Cpu time: " << succinctNanos(cpuWallTiming.cpuNanos)
      << ", Blocked wall time: " << succinctNanos(blockedWallNanos)
      << ", Peak memory: " << succinctBytes(peakMemoryBytes)
      << ", Memory allocations: " << numMemoryAllocations;

  if (numDrivers > 0) {
    out << ", Threads: " << numDrivers;
  }

  if (numSplits > 0) {
    out << ", Splits: " << numSplits;
  }
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

folly::dynamic toPlanStatsJson(const facebook::velox::exec::TaskStats& stats) {
  folly::dynamic jsonStats = folly::dynamic::array;
  auto planStats = facebook::velox::exec::toPlanStats(stats);
  for (const auto& planStat : planStats) {
    for (const auto& operatorStat : planStat.second.operatorStats) {
      folly::dynamic stat = folly::dynamic::object;
      stat["planNodeId"] = planStat.first;
      stat["operatorType"] = operatorStat.first;
      stat["inputRows"] = operatorStat.second->inputRows;
      stat["inputVectors"] = operatorStat.second->inputVectors;
      stat["inputBytes"] = operatorStat.second->inputBytes;
      stat["rawInputRows"] = operatorStat.second->rawInputRows;
      stat["rawInputBytes"] = operatorStat.second->rawInputBytes;
      stat["outputRows"] = operatorStat.second->outputRows;
      stat["outputVectors"] = operatorStat.second->outputVectors;
      stat["outputBytes"] = operatorStat.second->outputBytes;
      stat["cpuWallTiming"] = operatorStat.second->cpuWallTiming.toString();
      stat["blockedWallNanos"] = operatorStat.second->blockedWallNanos;
      stat["peakMemoryBytes"] = operatorStat.second->peakMemoryBytes;
      stat["numMemoryAllocations"] = operatorStat.second->numMemoryAllocations;
      stat["numDrivers"] = operatorStat.second->numDrivers;
      stat["numSplits"] = operatorStat.second->numSplits;

      folly::dynamic cs = folly::dynamic::object;
      for (const auto& cstat : operatorStat.second->customStats) {
        cs[cstat.first] = cstat.second.toString();
      }
      stat["customStats"] = cs;

      jsonStats.push_back(stat);
    }
  }
  return jsonStats;
}

namespace {
void printCustomStats(
    const std::unordered_map<std::string, RuntimeMetric>& stats,
    const std::string& indentation,
    std::stringstream& stream) {
  int width = 0;
  for (const auto& entry : stats) {
    if (width < entry.first.size()) {
      width = entry.first.size();
    }
  }
  width += 3;

  // Copy to a map to get a deterministic output.
  std::map<std::string_view, RuntimeMetric> orderedStats;
  for (const auto& [name, metric] : stats) {
    orderedStats[name] = metric;
  }

  for (const auto& [name, metric] : orderedStats) {
    stream << std::endl;
    stream << indentation << std::left << std::setw(width) << name;
    metric.printMetric(stream);
  }
}
} // namespace

std::string printPlanWithStats(
    const core::PlanNode& plan,
    const TaskStats& taskStats,
    bool includeCustomStats) {
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

        // Include break down by operator type for plan nodes with multiple
        // operators. Print input rows and sizes for all such nodes.
        if (stats.isMultiOperatorTypeNode()) {
          for (const auto& entry : stats.operatorStats) {
            stream << std::endl;
            stream << indentation << entry.first << ": "
                   << entry.second->toString(true);

            if (includeCustomStats) {
              printCustomStats(
                  entry.second->customStats, indentation + "   ", stream);
            }
          }
        } else {
          if (includeCustomStats) {
            printCustomStats(stats.customStats, indentation + "   ", stream);
          }
        }
      });
}
} // namespace facebook::velox::exec
