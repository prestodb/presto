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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/Connector.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;

namespace {
class IndexLookupJoinTest : public HiveConnectorTestBase {
 protected:
  IndexLookupJoinTest() = default;

  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    core::PlanNode::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    Type::registerSerDe();
    core::ITypedExpr::registerSerDe();
  }

  void TearDown() override {
    HiveConnectorTestBase::TearDown();
  }

  void testSerde(const core::PlanNodePtr& plan) {
    auto serialized = plan->serialize();

    auto copy = ISerializable::deserialize<core::PlanNode>(serialized, pool());

    ASSERT_EQ(plan->toString(true, true), copy->toString(true, true));
  }
};

class IndexTableHandle : public connector::ConnectorTableHandle {
 public:
  explicit IndexTableHandle(std::string connectorId)
      : ConnectorTableHandle(std::move(connectorId)) {}

  ~IndexTableHandle() override = default;

  std::string toString() const override {
    static const std::string str{"IndexTableHandle"};
    return str;
  }

  const std::string& name() const override {
    static const std::string connectorName{"IndexTableHandle"};
    return connectorName;
  }

  bool supportsIndexLookup() const override {
    return true;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = name();
    obj["connectorId"] = connectorId();
    return obj;
  }

  static std::shared_ptr<IndexTableHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<IndexTableHandle>(obj["connectorId"].getString());
  }

  static void registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("IndexTableHandle", create);
  }
};

TEST_F(IndexLookupJoinTest, planNodeAndSerde) {
  IndexTableHandle::registerSerDe();

  auto indexConnectorHandle =
      std::make_shared<IndexTableHandle>("IndexConnector");

  auto left = makeRowVector(
      {"t0", "t1", "t2"},
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30}),
       makeFlatVector<int32_t>({10, 30, 20})});

  auto right = makeRowVector(
      {"u0", "u1", "u2"},
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30}),
       makeFlatVector<int32_t>({10, 30, 20})});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto planBuilder = exec::test::PlanBuilder();
  auto nonIndexTableScan = std::dynamic_pointer_cast<const core::TableScanNode>(
      exec::test::PlanBuilder::TableScanBuilder(planBuilder)
          .outputType(std::dynamic_pointer_cast<const RowType>(right->type()))
          .endTableScan()
          .planNode());
  VELOX_CHECK_NOT_NULL(nonIndexTableScan);

  auto indexTableScan = std::dynamic_pointer_cast<const core::TableScanNode>(
      exec::test::PlanBuilder::TableScanBuilder(planBuilder)
          .tableHandle(indexConnectorHandle)
          .outputType(std::dynamic_pointer_cast<const RowType>(right->type()))
          .endTableScan()
          .planNode());
  VELOX_CHECK_NOT_NULL(indexTableScan);

  for (const auto joinType : {core::JoinType::kLeft, core::JoinType::kInner}) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({left})
                    .indexLookupJoin(
                        {"t0"},
                        {"u0"},
                        indexTableScan,
                        {},
                        {"t0", "u1", "t2", "t1"},
                        joinType)
                    .planNode();
    auto indexLookupJoinNode =
        std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(plan);
    ASSERT_TRUE(indexLookupJoinNode->joinConditions().empty());
    ASSERT_EQ(
        indexLookupJoinNode->lookupSource()->tableHandle()->connectorId(),
        "IndexConnector");
    testSerde(plan);
  }

  // with join conditions.
  for (const auto joinType : {core::JoinType::kLeft, core::JoinType::kInner}) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({left})
                    .indexLookupJoin(
                        {"t0"},
                        {"u0"},
                        indexTableScan,
                        {"u1 > t2"},
                        {"t0", "u1", "t2", "t1"},
                        joinType)
                    .planNode();
    auto indexLookupJoinNode =
        std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(plan);
    ASSERT_EQ(indexLookupJoinNode->joinConditions().size(), 1);
    ASSERT_EQ(
        indexLookupJoinNode->lookupSource()->tableHandle()->connectorId(),
        "IndexConnector");
    testSerde(plan);
  }

  // bad join type.
  {
    VELOX_ASSERT_USER_THROW(
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .indexLookupJoin(
                {"t0"},
                {"u0"},
                indexTableScan,
                {},
                {"t0", "u1", "t2", "t1"},
                core::JoinType::kFull)
            .planNode(),
        "Unsupported index lookup join type FULL");
  }

  // bad table handle.
  {
    VELOX_ASSERT_USER_THROW(
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .indexLookupJoin(
                {"t0"}, {"u0"}, nonIndexTableScan, {}, {"t0", "u1", "t2", "t1"})
            .planNode(),
        "The lookup table handle hive_table from connector test-hive doesn't support index lookup");
  }

  // Non-matched join keys.
  {
    VELOX_ASSERT_THROW(
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .indexLookupJoin(
                {"t0", "t1"},
                {"u0"},
                indexTableScan,
                {"u1 > t2"},
                {"t0", "u1", "t2", "t1"})
            .planNode(),
        "JoinNode requires same number of join keys on left and right sides");
  }

  // No join keys.
  {
    VELOX_ASSERT_THROW(
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .indexLookupJoin(
                {}, {}, indexTableScan, {"u1 > t2"}, {"t0", "u1", "t2", "t1"})
            .planNode(),
        "JoinNode requires at least one join key");
  }
}
} // namespace
