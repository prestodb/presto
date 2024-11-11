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

#include "velox/tool/trace/TableScanReplayer.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::tool::trace {

RowVectorPtr TableScanReplayer::run() {
  const auto plan = createPlan();
  return exec::test::AssertQueryBuilder(plan)
      .maxDrivers(maxDrivers_)
      .configs(queryConfigs_)
      .connectorSessionProperties(connectorConfigs_)
      .splits(getSplits())
      .copyResults(memory::MemoryManager::getInstance()->tracePool());
}

core::PlanNodePtr TableScanReplayer::createPlanNode(
    const core::PlanNode* node,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& /*source*/) const {
  const auto scanNode = dynamic_cast<const core::TableScanNode*>(node);
  VELOX_CHECK_NOT_NULL(scanNode);
  return std::make_shared<core::TableScanNode>(
      nodeId,
      scanNode->outputType(),
      scanNode->tableHandle(),
      scanNode->assignments());
}

std::vector<exec::Split> TableScanReplayer::getSplits() const {
  std::vector<std::string> splitInfoDirs;
  if (driverId_ != -1) {
    splitInfoDirs.push_back(exec::trace::getOpTraceDirectory(
        nodeTraceDir_, pipelineIds_.front(), driverId_));
  } else {
    for (auto i = 0; i < maxDrivers_; ++i) {
      splitInfoDirs.push_back(exec::trace::getOpTraceDirectory(
          nodeTraceDir_, pipelineIds_.front(), i));
    }
  }
  const auto splitStrs =
      exec::trace::OperatorTraceSplitReader(
          splitInfoDirs, memory::MemoryManager::getInstance()->tracePool())
          .read();

  std::vector<exec::Split> splits;
  for (const auto& splitStr : splitStrs) {
    folly::dynamic splitInfoObj = folly::parseJson(splitStr);
    const auto split =
        ISerializable::deserialize<connector::hive::HiveConnectorSplit>(
            splitInfoObj);
    splits.emplace_back(
        std::const_pointer_cast<connector::hive::HiveConnectorSplit>(split));
  }
  return splits;
}
} // namespace facebook::velox::tool::trace
