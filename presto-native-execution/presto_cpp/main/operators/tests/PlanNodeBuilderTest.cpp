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

#include <gtest/gtest.h>
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"

using namespace facebook::velox;

namespace facebook::presto::operators::test {
namespace {
// Most nodes need a source, and shuffle read is a good example of one.
// Providing a default instance here that can be reused by tests.
const std::shared_ptr<const ShuffleReadNode> kShuffleRead =
    ShuffleReadNode::Builder()
        .id("shuffle_read_id")
        .outputType(ROW({"c0", "c1"}, {INTEGER(), VARCHAR()}))
        .build();
} // namespace

TEST(PlanNodeBuilderTest, testBroadcastWrite) {
  const core::PlanNodeId id = "broadcast_write_id";
  const std::string basePath = "base_path";
  const RowTypePtr serdeRowType =
      ROW({"write_c0", "write_c1"}, {BIGINT(), DOUBLE()});

  const auto verify =
      [&](const std::shared_ptr<const BroadcastWriteNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->basePath(), basePath);
        EXPECT_EQ(node->serdeRowType(), serdeRowType);
        EXPECT_EQ(
            node->sources(), std::vector<core::PlanNodePtr>{kShuffleRead});
      };

  const auto node = BroadcastWriteNode::Builder()
                        .id(id)
                        .basePath(basePath)
                        .serdeRowType(serdeRowType)
                        .source(kShuffleRead)
                        .build();
  verify(node);

  const auto node2 = BroadcastWriteNode::Builder(*node).build();
  verify(node2);
}

TEST(PlanNodeBuilderTest, testPartitionAndSerialize) {
  const core::PlanNodeId id = "partition_and_serialize_id";
  const std::vector<core::TypedExprPtr> keys{
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "col0")};
  const uint32_t numPartitions = 10;
  const velox::RowTypePtr serializedRowType =
      ROW({"partition_c0", "partition_c1"}, {BIGINT(), DOUBLE()});
  const bool replicateNullsAndAny = true;
  const core::PartitionFunctionSpecPtr partitionFunctionFactory =
      std::make_shared<core::GatherPartitionFunctionSpec>();
  const std::vector<core::SortOrder> sortingOrders{core::kAscNullsFirst};
  const std::vector<core::FieldAccessTypedExprPtr> sortingKeys{
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "col1")};

  const auto verify =
      [&](const std::shared_ptr<const PartitionAndSerializeNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->keys(), keys);
        EXPECT_EQ(node->numPartitions(), numPartitions);
        EXPECT_EQ(node->serializedRowType(), serializedRowType);
        EXPECT_EQ(node->isReplicateNullsAndAny(), replicateNullsAndAny);
        EXPECT_EQ(node->partitionFunctionFactory(), partitionFunctionFactory);
        EXPECT_EQ(node->sortingOrders(), sortingOrders);
        EXPECT_EQ(node->sortingKeys(), sortingKeys);
        EXPECT_EQ(
            node->sources(), std::vector<core::PlanNodePtr>{kShuffleRead});
      };

  const auto node = PartitionAndSerializeNode::Builder()
                        .id(id)
                        .keys(keys)
                        .numPartitions(numPartitions)
                        .serializedRowType(serializedRowType)
                        .replicateNullsAndAny(replicateNullsAndAny)
                        .partitionFunctionFactory(partitionFunctionFactory)
                        .sortingOrders(sortingOrders)
                        .sortingKeys(sortingKeys)
                        .source(kShuffleRead)
                        .build();
  verify(node);

  const auto node2 = PartitionAndSerializeNode::Builder(*node).build();
  verify(node2);
}

TEST(PlanNodeBuilderTest, testShuffleRead) {
  const core::PlanNodeId id = "shuffle_read_id";
  const RowTypePtr outputType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});

  const auto verify = [&](const std::shared_ptr<const ShuffleReadNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->outputType(), outputType);
  };

  const auto node =
      ShuffleReadNode::Builder().id(id).outputType(outputType).build();
  verify(node);

  const auto node2 = ShuffleReadNode::Builder(*node).build();
  verify(node2);
}

TEST(PlanNodeBuilderTest, testShuffleWrite) {
  const core::PlanNodeId id = "shuffle_write_id";
  const uint32_t numPartitions = 10;
  const std::string shuffleName = "shuffle_name";
  const std::string serializedShuffleWriteInfo = "shuffle_write_info";

  const auto verify = [&](const std::shared_ptr<const ShuffleWriteNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->numPartitions(), numPartitions);
    EXPECT_EQ(node->shuffleName(), shuffleName);
    EXPECT_EQ(node->serializedShuffleWriteInfo(), serializedShuffleWriteInfo);
    EXPECT_EQ(node->sources(), std::vector<core::PlanNodePtr>{kShuffleRead});
  };

  const auto node = ShuffleWriteNode::Builder()
                        .id(id)
                        .numPartitions(numPartitions)
                        .shuffleName(shuffleName)
                        .serializedShuffleWriteInfo(serializedShuffleWriteInfo)
                        .source(kShuffleRead)
                        .build();
  verify(node);

  const auto node2 = ShuffleWriteNode::Builder(*node).build();
  verify(node2);
}
} // namespace facebook::presto::operators::test
