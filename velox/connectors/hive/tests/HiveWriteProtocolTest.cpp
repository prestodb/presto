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

#include "velox/connectors/hive/HiveWriteProtocol.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/core/QueryCtx.h"

#include "gtest/gtest.h"

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::core;

TEST(HiveWriteProtocolTest, writerParameters) {
  HiveNoCommitWriteProtocol::registerProtocol();
  HiveTaskCommitWriteProtocol::registerProtocol();

  auto queryCtx = std::make_shared<QueryCtx>();
  ConnectorQueryCtx connectorQueryCtx(
      queryCtx->pool(),
      queryCtx->getConnectorConfig(HiveConnectorFactory::kHiveConnectorName),
      nullptr,
      queryCtx->mappedMemory(),
      "test_task_id",
      "test_plan_node_id",
      0);
  std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns{
      std::make_shared<const HiveColumnHandle>(
          "col", HiveColumnHandle::ColumnType::kRegular, BIGINT())};
  auto insertTableHandle = std::make_shared<HiveInsertTableHandle>(
      inputColumns,
      std::make_shared<LocationHandle>(
          "test_table_directory",
          "test_table_directory",
          LocationHandle::TableType::kNew,
          LocationHandle::WriteMode::kDirectToTargetNewDirectory));

  auto noCommitWriteProtocol =
      WriteProtocol::getWriteProtocol(WriteProtocol::CommitStrategy::kNoCommit);
  auto noCommitWriterParameters =
      std::dynamic_pointer_cast<const HiveWriterParameters>(
          noCommitWriteProtocol->getWriterParameters(
              insertTableHandle, &connectorQueryCtx));
  ASSERT_EQ(
      noCommitWriterParameters->writeFileName(),
      noCommitWriterParameters->targetFileName());

  auto taskCommitWriteProtocol = WriteProtocol::getWriteProtocol(
      WriteProtocol::CommitStrategy::kTaskCommit);
  auto taskCommitWriterParameters =
      std::dynamic_pointer_cast<const HiveWriterParameters>(
          taskCommitWriteProtocol->getWriterParameters(
              insertTableHandle, &connectorQueryCtx));
  ASSERT_NE(
      taskCommitWriterParameters->writeFileName(),
      taskCommitWriterParameters->targetFileName());
}
