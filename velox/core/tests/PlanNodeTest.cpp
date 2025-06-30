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
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace ::facebook::velox;
using namespace ::facebook::velox::core;

namespace {
class PlanNodeTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  PlanNodeTest() {
    rowType_ = ROW({"c0", "c1", "c2"}, {BIGINT(), BIGINT(), BIGINT()});

    VectorFuzzer::Options opts;
    VectorFuzzer fuzzer(opts, pool_.get());
    rowData_.push_back(fuzzer.fuzzInputRow(rowType_));
  }

  RowTypePtr rowType_;
  std::vector<RowVectorPtr> rowData_;
};

TEST_F(PlanNodeTest, findFirstNode) {
  auto rowType = ROW({"name1"}, {BIGINT()});

  std::shared_ptr<connector::ConnectorTableHandle> tableHandle;
  connector::ColumnHandleMap assignments;

  std::shared_ptr<PlanNode> tableScan3 =
      std::make_shared<TableScanNode>("3", rowType, tableHandle, assignments);
  std::shared_ptr<PlanNode> tableScan2 =
      std::make_shared<TableScanNode>("2", rowType, tableHandle, assignments);

  std::vector<FieldAccessTypedExprPtr> sortingKeys;
  std::vector<SortOrder> sortingOrders;
  std::shared_ptr<PlanNode> localMerge1 = std::make_shared<LocalMergeNode>(
      "1",
      sortingKeys,
      sortingOrders,
      std::vector<PlanNodePtr>{tableScan2, tableScan3});

  std::vector<std::string> names;
  std::vector<TypedExprPtr> projections;
  std::shared_ptr<PlanNode> project0 =
      std::make_shared<ProjectNode>("0", names, projections, localMerge1);

  EXPECT_EQ(
      tableScan3.get(),
      PlanNode::findFirstNode(project0.get(), [](const PlanNode* node) {
        return node->id() == "3";
      }));

  EXPECT_EQ(
      project0.get(),
      PlanNode::findFirstNode(project0.get(), [](const PlanNode* node) {
        return node->name() == "Project";
      }));

  EXPECT_EQ(
      nullptr,
      PlanNode::findFirstNode(project0.get(), [](const PlanNode* node) {
        return node->name() == "Unknown";
      }));
}

TEST_F(PlanNodeTest, sortOrder) {
  struct {
    SortOrder order1;
    SortOrder order2;
    int expectedEqual;

    std::string debugString() const {
      return fmt::format(
          "order1 {} order2 {} expectedEqual {}",
          order1.toString(),
          order2.toString(),
          expectedEqual);
    }
  } testSettings[] = {
      {{true, true}, {true, true}, true},
      {{true, false}, {true, false}, true},
      {{false, true}, {false, true}, true},
      {{false, false}, {false, false}, true},
      {{true, true}, {true, false}, false},
      {{true, true}, {false, false}, false},
      {{true, true}, {false, true}, false},
      {{true, false}, {false, false}, false},
      {{true, false}, {false, true}, false},
      {{false, true}, {false, false}, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (testData.expectedEqual) {
      ASSERT_EQ(testData.order1, testData.order2);
    } else {
      ASSERT_NE(testData.order1, testData.order2);
    }
  }
}

TEST_F(PlanNodeTest, duplicateSortKeys) {
  auto sortingKeys = std::vector<FieldAccessTypedExprPtr>{
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0"),
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c1"),
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0"),
  };
  auto sortingOrders =
      std::vector<SortOrder>{{true, true}, {false, false}, {true, true}};
  VELOX_ASSERT_USER_THROW(
      std::make_shared<OrderByNode>(
          "orderBy", sortingKeys, sortingOrders, false, nullptr),
      "Duplicate sorting keys are not allowed: c0");
}
class TestIndexTableHandle : public connector::ConnectorTableHandle {
 public:
  TestIndexTableHandle()
      : connector::ConnectorTableHandle("TestIndexConnnector") {}

  ~TestIndexTableHandle() override = default;

  std::string toString() const override {
    return "TestIndexTableHandle";
  }

  const std::string& name() const override {
    static const std::string kName = "TestIndexTableHandle";
    return kName;
  }

  bool supportsIndexLookup() const override {
    return true;
  }

  folly::dynamic serialize() const override {
    return {};
  }

  static std::shared_ptr<TestIndexTableHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<TestIndexTableHandle>();
  }
};

TEST_F(PlanNodeTest, isIndexLookupJoin) {
  const auto rowType = ROW({"name"}, {BIGINT()});
  const auto valueNode = std::make_shared<ValuesNode>("orderBy", rowData_);
  ASSERT_FALSE(isIndexLookupJoin(valueNode.get()));

  const RowTypePtr probeType = ROW({"c0"}, {BIGINT()});
  const RowTypePtr buildType = ROW({"c1"}, {BIGINT()});
  const RowTypePtr outputType = ROW({"c0", "c1"}, {BIGINT(), BIGINT()});
  auto indexTableHandle = std::make_shared<TestIndexTableHandle>();
  const auto probeNode = std::make_shared<TableScanNode>(
      "tableScan-probe", probeType, nullptr, connector::ColumnHandleMap{});
  ASSERT_FALSE(isIndexLookupJoin(probeNode.get()));
  const auto buildNode = std::make_shared<TableScanNode>(
      "tableScan-build",
      buildType,
      indexTableHandle,
      connector::ColumnHandleMap{});
  ASSERT_FALSE(isIndexLookupJoin(buildNode.get()));
  const std::vector<FieldAccessTypedExprPtr> leftKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::vector<FieldAccessTypedExprPtr> rightKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c1")};
  const auto indexJoinNode = std::make_shared<IndexLookupJoinNode>(
      "indexJoinNode",
      core::JoinType::kInner,
      leftKeys,
      rightKeys,
      std::vector<IndexLookupConditionPtr>{},
      probeNode,
      buildNode,
      outputType);
  ASSERT_TRUE(isIndexLookupJoin(indexJoinNode.get()));
}
} // namespace
