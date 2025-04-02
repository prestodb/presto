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

PlanNodeStats& PlanNodeStats::operator+=(const PlanNodeStats& another) {
  inputRows += another.inputRows;
  inputBytes += another.inputBytes;
  inputVectors += another.inputVectors;

  rawInputRows += another.inputRows;
  rawInputBytes += another.rawInputBytes;

  dynamicFilterStats.add(another.dynamicFilterStats);

  outputRows += another.outputRows;
  outputBytes += another.outputBytes;
  outputVectors += another.outputVectors;

  isBlockedTiming.add(another.isBlockedTiming);
  addInputTiming.add(another.addInputTiming);
  getOutputTiming.add(another.getOutputTiming);
  finishTiming.add(another.finishTiming);
  cpuWallTiming.add(another.isBlockedTiming);
  cpuWallTiming.add(another.addInputTiming);
  cpuWallTiming.add(another.getOutputTiming);
  cpuWallTiming.add(another.finishTiming);
  cpuWallTiming.add(another.isBlockedTiming);

  backgroundTiming.add(another.backgroundTiming);

  blockedWallNanos += another.blockedWallNanos;

  peakMemoryBytes += another.peakMemoryBytes;
  numMemoryAllocations += another.numMemoryAllocations;

  physicalWrittenBytes += another.physicalWrittenBytes;

  for (const auto& [name, customStats] : another.customStats) {
    if (UNLIKELY(this->customStats.count(name) == 0)) {
      this->customStats.insert(std::make_pair(name, customStats));
    } else {
      this->customStats.at(name).merge(customStats);
    }
  }

  // Populating number of drivers for plan nodes with multiple operators is not
  // useful. Each operator could have been executed in different pipelines with
  // different number of drivers.
  if (!isMultiOperatorTypeNode()) {
    numDrivers += another.numDrivers;
  } else {
    numDrivers = 0;
  }

  numSplits += another.numSplits;

  spilledInputBytes += another.spilledInputBytes;
  spilledBytes += another.spilledBytes;
  spilledRows += another.spilledRows;
  spilledPartitions += another.spilledPartitions;
  spilledFiles += another.spilledFiles;

  return *this;
}

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

  dynamicFilterStats.add(stats.dynamicFilterStats);

  outputRows += stats.outputPositions;
  outputBytes += stats.outputBytes;
  outputVectors += stats.outputVectors;

  isBlockedTiming.add(stats.isBlockedTiming);
  addInputTiming.add(stats.addInputTiming);
  getOutputTiming.add(stats.getOutputTiming);
  finishTiming.add(stats.finishTiming);
  cpuWallTiming.add(stats.addInputTiming);
  cpuWallTiming.add(stats.getOutputTiming);
  cpuWallTiming.add(stats.finishTiming);
  cpuWallTiming.add(stats.isBlockedTiming);

  backgroundTiming.add(stats.backgroundTiming);

  blockedWallNanos += stats.blockedWallNanos;

  peakMemoryBytes += stats.memoryStats.peakTotalMemoryReservation;
  numMemoryAllocations += stats.memoryStats.numMemoryAllocations;

  physicalWrittenBytes += stats.physicalWrittenBytes;

  for (const auto& [name, customStats] : stats.runtimeStats) {
    if (UNLIKELY(this->customStats.count(name) == 0)) {
      this->customStats.insert(std::make_pair(name, customStats));
    } else {
      this->customStats.at(name).merge(customStats);
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

  spilledInputBytes += stats.spilledInputBytes;
  spilledBytes += stats.spilledBytes;
  spilledRows += stats.spilledRows;
  spilledPartitions += stats.spilledPartitions;
  spilledFiles += stats.spilledFiles;
}

std::string PlanNodeStats::toString(
    bool includeInputStats,
    bool includeRuntimeStats) const {
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
      << ", " << outputVectors << " batches)";
  if (physicalWrittenBytes > 0) {
    out << ", Physical written output: " << succinctBytes(physicalWrittenBytes);
  }
  out << ", Cpu time: " << succinctNanos(cpuWallTiming.cpuNanos)
      << ", Wall time: " << succinctNanos(cpuWallTiming.wallNanos)
      << ", Blocked wall time: " << succinctNanos(blockedWallNanos)
      << ", Peak memory: " << succinctBytes(peakMemoryBytes)
      << ", Memory allocations: " << numMemoryAllocations;

  if (numDrivers > 0) {
    out << ", Threads: " << numDrivers;
  }

  if (numSplits > 0) {
    out << ", Splits: " << numSplits;
  }

  if (spilledRows > 0) {
    out << ", Spilled: " << spilledRows << " rows ("
        << succinctBytes(spilledBytes) << ", " << spilledFiles << " files)";
  }

  if (!dynamicFilterStats.empty()) {
    out << ", DynamicFilter producer plan nodes: "
        << folly::join(',', dynamicFilterStats.producerNodeIds);
  }

  out << ", CPU breakdown: B/I/O/F "
      << fmt::format(
             "({}/{}/{}/{})",
             succinctNanos(isBlockedTiming.cpuNanos),
             succinctNanos(addInputTiming.cpuNanos),
             succinctNanos(getOutputTiming.cpuNanos),
             succinctNanos(finishTiming.cpuNanos));

  if (includeRuntimeStats) {
    out << ", Runtime stats: (";
    for (const auto& [name, metric] : customStats) {
      out << name << ": " << metric.toString() << ", ";
    }
    out << ")";
  }
  return out.str();
}

void appendOperatorStats(
    const OperatorStats& stats,
    std::unordered_map<core::PlanNodeId, PlanNodeStats>& planStats) {
  const auto& planNodeId = stats.planNodeId;
  auto it = planStats.find(planNodeId);
  if (it != planStats.end()) {
    it->second.add(stats);
  } else {
    PlanNodeStats nodeStats;
    nodeStats.add(stats);
    planStats.emplace(planNodeId, std::move(nodeStats));
  }
}

std::unordered_map<core::PlanNodeId, PlanNodeStats> toPlanStats(
    const TaskStats& taskStats) {
  std::unordered_map<core::PlanNodeId, PlanNodeStats> planStats;

  for (const auto& pipelineStats : taskStats.pipelineStats) {
    for (const auto& opStats : pipelineStats.operatorStats) {
      if (opStats.statsSplitter.has_value()) {
        const auto& multiNodeStats = opStats.statsSplitter.value()(opStats);
        for (const auto& stats : multiNodeStats) {
          appendOperatorStats(stats, planStats);
        }
      } else {
        appendOperatorStats(opStats, planStats);
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
      stat["isBlockedTiming"] = operatorStat.second->isBlockedTiming.toString();
      stat["addInputTiming"] = operatorStat.second->addInputTiming.toString();
      stat["getOutputTiming"] = operatorStat.second->getOutputTiming.toString();
      stat["finishTiming"] = operatorStat.second->finishTiming.toString();
      stat["cpuWallTiming"] = operatorStat.second->cpuWallTiming.toString();
      stat["blockedWallNanos"] = operatorStat.second->blockedWallNanos;
      stat["peakMemoryBytes"] = operatorStat.second->peakMemoryBytes;
      stat["numMemoryAllocations"] = operatorStat.second->numMemoryAllocations;
      stat["physicalWrittenBytes"] = operatorStat.second->physicalWrittenBytes;
      stat["numDrivers"] = operatorStat.second->numDrivers;
      stat["numSplits"] = operatorStat.second->numSplits;
      stat["spilledInputBytes"] = operatorStat.second->spilledInputBytes;
      stat["spilledBytes"] = operatorStat.second->spilledBytes;
      stat["spilledRows"] = operatorStat.second->spilledRows;
      stat["spilledFiles"] = operatorStat.second->spilledFiles;

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
    bool includeCustomStats,
    PlanNodeAnnotation annotation) {
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
        if (annotation) {
          stream << indentation << annotation(planNodeId) << std::endl;
        }
      });
}
} // namespace facebook::velox::exec
