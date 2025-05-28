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

#include "velox/runner/tests/PrestoQueryReplayRunner.h"

namespace facebook::velox::runner {

const std::string kHiveConnectorId = "test-hive";
const int32_t kWaitTimeoutUs = 5'000'000;
const int32_t kDefaultWidth = 2;
const int32_t kDefaultMaxDrivers = 4;

namespace {
std::shared_ptr<memory::MemoryPool> makeRootPool(const std::string& queryId) {
  static std::atomic_uint64_t poolId{0};
  return memory::memoryManager()->addRootPool(
      fmt::format("{}_{}", queryId, poolId++));
}

std::vector<RowVectorPtr> readCursor(
    std::shared_ptr<runner::LocalRunner>& runner,
    memory::MemoryPool* pool) {
  // We'll check the result after tasks are deleted, so copy the result vectors
  // to 'pool' that has longer lifetime.
  std::vector<RowVectorPtr> result;
  while (auto rows = runner->next()) {
    result.push_back(
        std::dynamic_pointer_cast<RowVector>(BaseVector::copy(*rows, pool)));
  }
  return result;
}
} // namespace

PrestoQueryReplayRunner::PrestoQueryReplayRunner(
    memory::MemoryPool* pool,
    TaskPrefixExtractor taskPrefixExtractor,
    int32_t width,
    int32_t maxDrivers,
    const std::unordered_map<std::string, std::string>& config,
    const std::unordered_map<std::string, std::string>& hiveConfig)
    : pool_{pool},
      taskPrefixExtractor_{taskPrefixExtractor},
      width_{width},
      maxDrivers_{maxDrivers},
      config_{config},
      hiveConfig_{hiveConfig},
      executor_{std::make_unique<folly::CPUThreadPoolExecutor>(maxDrivers)} {}

std::shared_ptr<core::QueryCtx> PrestoQueryReplayRunner::makeQueryCtx(
    const std::string& queryId,
    const std::shared_ptr<memory::MemoryPool>& rootPool) {
  auto& config = config_;
  auto hiveConfig = hiveConfig_;
  std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[kHiveConnectorId] =
      std::make_shared<config::ConfigBase>(std::move(hiveConfig));

  return core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig(config),
      std::move(connectorConfigs),
      cache::AsyncDataCache::getInstance(),
      rootPool,
      nullptr,
      queryId);
}

namespace {
std::vector<std::string> getStringListFromJson(const folly::dynamic& json) {
  std::vector<std::string> result;
  result.resize(json.size());
  std::transform(
      json.begin(), json.end(), result.begin(), [](const folly::dynamic& json) {
        return json.getString();
      });
  return result;
}

void getScanNodesImpl(
    const core::PlanNodePtr& plan,
    std::vector<core::TableScanNodePtr>& result) {
  if (auto tableScan =
          std::dynamic_pointer_cast<const core::TableScanNode>(plan)) {
    result.push_back(tableScan);
    return;
  }
  for (const auto& child : plan->sources()) {
    getScanNodesImpl(child, result);
  }
}

// Return all table scan nodes in 'plan'.
std::vector<core::TableScanNodePtr> getScanNodes(
    const core::PlanNodePtr& plan) {
  std::vector<core::TableScanNodePtr> result;
  getScanNodesImpl(plan, result);
  return result;
}

// If 'plan' is a broadcast partitioned output node, return the number of
// broadcast destinations. Otherwise return 0.
int getNumBroadcastDestinations(const core::PlanNodePtr& plan) {
  if (auto partitionedOutput =
          std::dynamic_pointer_cast<const core::PartitionedOutputNode>(plan)) {
    if (partitionedOutput->isBroadcast()) {
      return partitionedOutput->numPartitions();
    }
  }
  return 0;
}

// Return true if 'node' is a gathering PartitionedOutput node.
bool isGatheringPartition(const core::PlanNodePtr& node) {
  if (auto partitionedOutput =
          std::dynamic_pointer_cast<const core::PartitionedOutputNode>(node)) {
    return partitionedOutput->keys().empty();
  }
  return false;
}

// Return a new plan tree with the same structure as 'plan' but with the number
// of partitions of the root PartitionedOutputNode updated to 'numPartitions'.
// This method throws if the root node of 'plan' is not a PartitionedOutputNode
// or if it's a gathering PartitionedOutputNode.
core::PlanNodePtr updateNumOfPartitions(
    const core::PlanNodePtr& plan,
    int numPartitions) {
  auto partitionedOutput =
      std::dynamic_pointer_cast<const core::PartitionedOutputNode>(plan);
  VELOX_CHECK(partitionedOutput != nullptr);
  VELOX_CHECK(!partitionedOutput->keys().empty());
  return core::PartitionedOutputNode::Builder(*partitionedOutput)
      .numPartitions(numPartitions)
      .build();
}

// Deserialize each json string record in 'serializedJsonRecords' into a
// folly::dynamic and return them as a vector in the same order.
std::vector<folly::dynamic> getJsonRecords(
    const std::vector<std::string>& serializedJsonRecords) {
  std::vector<folly::dynamic> jsonRecords;
  jsonRecords.reserve(serializedJsonRecords.size());
  for (const auto& serializedJsonRecord : serializedJsonRecords) {
    jsonRecords.push_back(folly::parseJson(serializedJsonRecord));
  }
  return jsonRecords;
}

core::PlanNodePtr getDeserializedPlan(
    const folly::dynamic& json,
    memory::MemoryPool* pool) {
  const auto& jsonPlanFragment = json.at("plan_fragment");
  return core::PlanNode::deserialize<core::PlanNode>(jsonPlanFragment, pool);
}

struct PlanFragmentInfo {
  core::PlanNodePtr plan{nullptr};
  std::unordered_map<std::string, std::unordered_set<std::string>>
      remoteTaskIdMap{};
  std::vector<core::TableScanNodePtr> scans{};
  int numBroadcastDestinations{0};
  int numWorkers{0};
};

std::vector<ExecutableFragment> createExecutableFragments(
    const std::unordered_map<std::string, PlanFragmentInfo>& planFragments) {
  std::vector<ExecutableFragment> executableFragments;
  for (const auto& [taskPrefix, planFragmentInfo] : planFragments) {
    ExecutableFragment executableFragment{taskPrefix};
    executableFragment.width =
        (planFragmentInfo.numWorkers > 0) ? planFragmentInfo.numWorkers : 1;
    executableFragment.fragment = core::PlanFragment{planFragmentInfo.plan};

    executableFragment.scans = planFragmentInfo.scans;
    executableFragment.numBroadcastDestinations =
        planFragmentInfo.numBroadcastDestinations;

    std::vector<InputStage> inputStages;
    const auto& remoteTaskIdMap = planFragmentInfo.remoteTaskIdMap;
    for (const auto& [planNodeId, remoteTaskPrefixes] : remoteTaskIdMap) {
      for (const auto& remoteTaskPrefix : remoteTaskPrefixes) {
        inputStages.push_back(InputStage{planNodeId, remoteTaskPrefix});
      }
    }
    executableFragment.inputStages = std::move(inputStages);
    executableFragments.push_back(std::move(executableFragment));
  }
  return executableFragments;
}

} // namespace

std::vector<std::string> PrestoQueryReplayRunner::getTaskPrefixes(
    const std::vector<folly::dynamic>& jsonRecords) {
  std::vector<std::string> taskPrefixes;
  taskPrefixes.reserve(jsonRecords.size());
  for (const auto& json : jsonRecords) {
    taskPrefixes.push_back(
        taskPrefixExtractor_(json.at("task_id").getString()));
  }
  return taskPrefixes;
}

MultiFragmentPlanPtr PrestoQueryReplayRunner::deserializePlan(
    const std::string& queryId,
    const std::vector<std::string>& serializedPlanFragments) {
  auto jsonRecords = getJsonRecords(serializedPlanFragments);
  auto taskPrefixes = getTaskPrefixes(jsonRecords);
  VELOX_CHECK_EQ(jsonRecords.size(), serializedPlanFragments.size());
  VELOX_CHECK_EQ(taskPrefixes.size(), serializedPlanFragments.size());

  // Assuming that the plan fragment of the root stage comes with the least task
  // prefix, put the this root plan fragment at the end. This is because
  // LocalRunner implicitly expect the last plan fragment to be the
  // root stage.
  // TODO: extend this logic to be more general.
  std::unordered_map<std::string, PlanFragmentInfo> planFragments;
  for (auto i = 0; i < serializedPlanFragments.size(); ++i) {
    auto& taskPrefix = taskPrefixes[i];
    VELOX_CHECK_EQ(planFragments.count(taskPrefix), 0);

    const auto plan = getDeserializedPlan(jsonRecords[i], pool_);
    planFragments[taskPrefix].scans = getScanNodes(plan);
    planFragments[taskPrefix].numBroadcastDestinations =
        getNumBroadcastDestinations(plan);
    planFragments[taskPrefix].plan = plan;
  }

  for (auto i = 0; i < serializedPlanFragments.size(); ++i) {
    auto& taskPrefix = taskPrefixes[i];
    auto jsonRemoteTaskIdMaps = jsonRecords[i].at("remote_task_ids");
    std::unordered_map<std::string, std::unordered_set<std::string>>
        remoteTaskIdMap;
    for (const auto& [planNodeId, remoteTaskIds] :
         jsonRemoteTaskIdMaps.items()) {
      auto remoteTaskIdList = getStringListFromJson(remoteTaskIds);
      std::unordered_set<std::string> remoteTaskIdPrefixSet;
      for (const auto& remoteTaskId : remoteTaskIdList) {
        auto remoteTaskPrefix = taskPrefixExtractor_(remoteTaskId);
        const auto [_, inserted] =
            remoteTaskIdPrefixSet.insert(remoteTaskPrefix);

        VELOX_CHECK_GT(planFragments.count(remoteTaskPrefix), 0);
        if (inserted) {
          if (isGatheringPartition(planFragments.at(remoteTaskPrefix).plan)) {
            planFragments[taskPrefix].numWorkers = 1;
          } else {
            planFragments[taskPrefix].numWorkers = width_;
            planFragments[remoteTaskPrefix].plan = updateNumOfPartitions(
                planFragments[remoteTaskPrefix].plan, width_);
          }
        } else {
          // If remoteTaskPrefix already exists in the remoteTaskIdPrefixSet,
          // the number of workers should have already been updated when we
          // inserted this remote task prefix.
          if (isGatheringPartition(planFragments.at(remoteTaskPrefix).plan)) {
            VELOX_CHECK_EQ(planFragments[taskPrefix].numWorkers, 1);
          } else {
            VELOX_CHECK_EQ(planFragments[taskPrefix].numWorkers, width_);
          }
        }
      }
      remoteTaskIdMap[planNodeId.getString()] =
          std::move(remoteTaskIdPrefixSet);
    }
    planFragments[taskPrefix].remoteTaskIdMap = remoteTaskIdMap;
  }

  auto executableFragments = createExecutableFragments(planFragments);

  MultiFragmentPlan::Options options{queryId, width_, maxDrivers_};
  return std::make_shared<MultiFragmentPlan>(
      std::move(executableFragments), std::move(options));
}

std::vector<RowVectorPtr> PrestoQueryReplayRunner::run(
    const std::string& queryId,
    const std::vector<std::string>& serializedPlanFragments) {
  auto queryRootPool = makeRootPool(queryId);
  auto multiFragmentPlan = deserializePlan(queryId, serializedPlanFragments);
  // TODO: Populate nodeSplitMap with serialized connector splits for TableScan
  // nodes.
  std::unordered_map<
      core::PlanNodeId,
      std::vector<std::shared_ptr<connector::ConnectorSplit>>>
      nodeSplitMap;
  auto localRunner = std::make_shared<LocalRunner>(
      std::move(multiFragmentPlan),
      makeQueryCtx(queryId, queryRootPool),
      std::make_shared<runner::SimpleSplitSourceFactory>(nodeSplitMap));

  auto result = readCursor(localRunner, pool_);
  localRunner->waitForCompletion(kWaitTimeoutUs);

  return result;
}

} // namespace facebook::velox::runner
