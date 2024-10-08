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

#include "velox/tool/trace/TableWriterReplayer.h"
#include "velox/exec/QueryDataReader.h"
#include "velox/exec/QueryTraceUtil.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::tool::trace {

namespace {

std::shared_ptr<connector::hive::HiveInsertTableHandle>
makeHiveInsertTableHandle(
    const core::TableWriteNode* node,
    std::string targetDir) {
  const auto tracedHandle =
      std::dynamic_pointer_cast<connector::hive::HiveInsertTableHandle>(
          node->insertTableHandle()->connectorInsertTableHandle());
  const auto inputColumns = tracedHandle->inputColumns();
  const auto compressionKind =
      tracedHandle->compressionKind().value_or(common::CompressionKind_NONE);
  const auto storageFormat = tracedHandle->tableStorageFormat();
  const auto serdeParameters = tracedHandle->serdeParameters();
  const auto writerOptions = tracedHandle->writerOptions();
  return std::make_shared<connector::hive::HiveInsertTableHandle>(
      inputColumns,
      std::make_shared<connector::hive::LocationHandle>(
          targetDir,
          targetDir,
          connector::hive::LocationHandle::TableType::kNew),
      storageFormat,
      tracedHandle->bucketProperty() == nullptr
          ? nullptr
          : std::make_shared<connector::hive::HiveBucketProperty>(
                *tracedHandle->bucketProperty()),
      compressionKind,
      std::unordered_map<std::string, std::string>{},
      writerOptions);
}

std::shared_ptr<core::InsertTableHandle> createInsertTableHanlde(
    const std::string& connectorId,
    const core::TableWriteNode* node,
    std::string targetDir) {
  return std::make_shared<core::InsertTableHandle>(
      connectorId, makeHiveInsertTableHandle(node, std::move(targetDir)));
}

} // namespace

core::PlanNodePtr TableWriterReplayer::createPlanNode(
    const core::PlanNode* node,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& source) const {
  const auto* tableWriterNode = dynamic_cast<const core::TableWriteNode*>(node);
  const auto insertTableHandle =
      createInsertTableHanlde("test-hive", tableWriterNode, replayOutputDir_);
  return std::make_shared<core::TableWriteNode>(
      nodeId,
      tableWriterNode->columns(),
      tableWriterNode->columnNames(),
      tableWriterNode->aggregationNode(),
      insertTableHandle,
      tableWriterNode->hasPartitioningScheme(),
      TableWriteTraits::outputType(tableWriterNode->aggregationNode()),
      tableWriterNode->commitStrategy(),
      source);
}
} // namespace facebook::velox::tool::trace
