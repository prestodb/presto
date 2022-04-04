/*
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
#include "presto_cpp/main/types/PrestoToVeloxSplit.h"
#include <gtest/gtest.h>
#include "velox/connectors/hive/HiveConnectorSplit.h"

using namespace facebook::velox;
using namespace facebook::presto;

namespace {
protocol::ScheduledSplit makeHiveScheduledSplit() {
  protocol::ScheduledSplit scheduledSplit;
  scheduledSplit.sequenceId = 111;
  scheduledSplit.planNodeId = "planNodeId-0";

  protocol::Split split;
  split.connectorId = "split.connectorId-0";
  auto hiveTransactionHandle =
      std::make_shared<protocol::HiveTransactionHandle>();
  hiveTransactionHandle->uuid = "split.transactionHandle.uuid-0";
  split.transactionHandle = hiveTransactionHandle;

  auto hiveSplit = std::make_shared<protocol::HiveSplit>();
  hiveSplit->path = "/file/path";
  hiveSplit->storage.storageFormat.inputFormat =
      "com.facebook.hive.orc.OrcInputFormat";
  hiveSplit->start = 0;
  hiveSplit->length = 100;

  split.connectorSplit = hiveSplit;
  scheduledSplit.split = split;
  return scheduledSplit;
}
} // namespace

TEST(PrestoToVeloxSplitTest, nullPartitionKey) {
  auto scheduledSplit = makeHiveScheduledSplit();
  auto hiveSplit = std::dynamic_pointer_cast<protocol::HiveSplit>(
      scheduledSplit.split.connectorSplit);
  protocol::HivePartitionKey partitionKey{"nullPartitionKey", nullptr};
  hiveSplit->partitionKeys.push_back(partitionKey);
  auto veloxSplit = toVeloxSplit(scheduledSplit);
  std::shared_ptr<connector::hive::HiveConnectorSplit> veloxHiveSplit;
  ASSERT_NO_THROW({
    veloxHiveSplit =
        std::dynamic_pointer_cast<connector::hive::HiveConnectorSplit>(
            veloxSplit.connectorSplit);
  });
  ASSERT_EQ(
      hiveSplit->partitionKeys.size(), veloxHiveSplit->partitionKeys.size());
  ASSERT_FALSE(
      veloxHiveSplit->partitionKeys.at("nullPartitionKey").has_value());
}
