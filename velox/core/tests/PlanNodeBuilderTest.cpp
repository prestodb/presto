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

#include "velox/common/memory/Memory.h"
#include "velox/core/PlanNode.h"
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/exec/tests/utils/AggregationResolver.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace ::facebook::velox;
using namespace ::facebook::velox::core;

// ArrowArrayStream is forward decared in PlanNode.h. We provide a dummy
// implementation here for testing purposes.
struct ArrowArrayStream {};

namespace {
class PlanNodeBuilderTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  core::ColumnStatsSpec createStatsSpec(
      const RowTypePtr& type,
      const std::vector<std::string>& groupingKeys,
      core::AggregationNode::Step step,
      const std::vector<std::string>& aggregates,
      const std::vector<std::vector<TypePtr>>& rawInputArgs = {}) {
    std::vector<core::AggregationNode::Aggregate> aggs;
    aggs.reserve(aggregates.size());
    std::vector<std::string> names;
    names.reserve(aggregates.size());

    duckdb::ParseOptions options;
    options.parseIntegerAsBigint = true;
    exec::test::AggregateTypeResolver resolver(step);

    for (auto i = 0; i < aggregates.size(); ++i) {
      const auto& aggregate = aggregates[i];
      const auto untypedExpr = duckdb::parseAggregateExpr(aggregate, options);

      if (!rawInputArgs.empty()) {
        resolver.setRawInputTypes(rawInputArgs[i]);
      }

      core::AggregationNode::Aggregate agg;
      agg.call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
          core::Expressions::inferTypes(untypedExpr.expr, type, pool()));

      if (step == core::AggregationNode::Step::kPartial ||
          step == core::AggregationNode::Step::kSingle) {
        VELOX_CHECK(rawInputArgs.empty());
        for (const auto& input : agg.call->inputs()) {
          agg.rawInputTypes.push_back(input->type());
        }
      } else {
        agg.rawInputTypes = rawInputArgs[i];
      }

      VELOX_CHECK_NULL(untypedExpr.maskExpr);
      VELOX_CHECK(!untypedExpr.distinct);
      VELOX_CHECK(untypedExpr.orderBy.empty());

      aggs.emplace_back(agg);

      if (untypedExpr.expr->alias().has_value()) {
        names.push_back(untypedExpr.expr->alias().value());
      } else {
        names.push_back(fmt::format("a{}", i));
      }
    }
    VELOX_CHECK_EQ(aggs.size(), names.size());

    std::vector<core::FieldAccessTypedExprPtr> groupingKeyExprs;
    groupingKeyExprs.reserve(groupingKeys.size());
    for (const auto& groupingKey : groupingKeys) {
      auto untypedGroupingKeyExpr = duckdb::parseExpr(groupingKey, options);
      auto groupingKeyExpr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              core::Expressions::inferTypes(
                  untypedGroupingKeyExpr, type, pool()));
      VELOX_CHECK_NOT_NULL(
          groupingKeyExpr,
          "Grouping key must use a column name, not an expression: {}",
          groupingKey);
      groupingKeyExprs.emplace_back(std::move(groupingKeyExpr));
    }

    return core::ColumnStatsSpec(
        std::move(groupingKeyExprs), step, std::move(names), std::move(aggs));
  }

  // A default source node, these are frequently needed when construting
  // PlanNodes, so providing one here.
  const std::shared_ptr<const ValuesNode> source_ =
      ValuesNode::Builder()
          .id("values_node_id")
          .values({makeRowVector({makeFlatVector<int32_t>({1, 2, 3})})})
          .build();
};

// A dummy implementation of ConnectorTableHandle that supports index lookup.
class TestConnectorTableHandleForLookupJoin
    : public connector::ConnectorTableHandle {
 public:
  explicit TestConnectorTableHandleForLookupJoin(std::string connectorId)
      : connector::ConnectorTableHandle(std::move(connectorId)) {}

  const std::string& name() const override {
    VELOX_NYI();
  }

  bool supportsIndexLookup() const override {
    return true;
  }
};
} // namespace

TEST_F(PlanNodeBuilderTest, valuesNode) {
  const PlanNodeId id = "values_node_id";
  const std::vector<RowVectorPtr> values{
      makeRowVector(
          {makeFlatVector<int32_t>(std::vector<int32_t>{1}),
           makeFlatVector<StringView>({"a"})}),
      makeRowVector(
          {makeFlatVector<int32_t>(std::vector<int32_t>{2}),
           makeFlatVector<StringView>({"b"})})};
  const bool parallelizable = true;
  const size_t repeatTimes = 3;

  const auto verify = [&](const std::shared_ptr<const ValuesNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->values(), values);
    EXPECT_EQ(node->testingIsParallelizable(), parallelizable);
    EXPECT_EQ(node->repeatTimes(), repeatTimes);
  };

  // Build ValuesNode using the builder.
  const auto node = ValuesNode::Builder()
                        .id(id)
                        .values(values)
                        .testingParallelizable(parallelizable)
                        .repeatTimes(repeatTimes)
                        .build();
  verify(node);

  const auto node2 = ValuesNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, arrowStreamNode) {
  const PlanNodeId id = "arrow_stream_node_id";
  const RowTypePtr outputType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});
  auto arrowStream = std::make_shared<ArrowArrayStream>();

  const auto verify = [&](const std::shared_ptr<const ArrowStreamNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->outputType(), outputType);
    EXPECT_EQ(node->arrowStream(), arrowStream);
  };

  const auto node = ArrowStreamNode::Builder()
                        .id(id)
                        .outputType(outputType)
                        .arrowStream(arrowStream)
                        .build();
  verify(node);

  const auto node2 = ArrowStreamNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, traceScanNode) {
  const PlanNodeId id = "trace_scan_node_id";
  const std::string traceDir = "/tmp/trace";
  const uint32_t pipelineId = 7;
  const std::vector<uint32_t> driverIds{0, 1, 2};
  const RowTypePtr rowType = ROW({"col0", "col1"}, {INTEGER(), REAL()});

  const auto verify = [&](const std::shared_ptr<const TraceScanNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->traceDir(), traceDir);
    EXPECT_EQ(node->pipelineId(), pipelineId);
    EXPECT_EQ(node->driverIds(), driverIds);
    EXPECT_EQ(node->outputType(), rowType);
  };

  const auto node = TraceScanNode::Builder()
                        .id(id)
                        .traceDir(traceDir)
                        .pipelineId(pipelineId)
                        .driverIds(driverIds)
                        .outputType(rowType)
                        .build();
  verify(node);

  const auto node2 = TraceScanNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, filterNode) {
  const PlanNodeId id = "filter_node_id";
  const auto filter =
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "col0");

  const auto verify = [&](const std::shared_ptr<const FilterNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->filter(), filter);
    EXPECT_EQ(node->sources(), std::vector<PlanNodePtr>{source_});
  };

  const auto node =
      FilterNode::Builder().id(id).filter(filter).source(source_).build();
  verify(node);

  const auto node2 = FilterNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, projectNode) {
  const PlanNodeId id = "project_node_id";
  const std::vector<std::string> names{"out_col"};
  const std::vector<TypedExprPtr> projections{
      std::make_shared<FieldAccessTypedExpr>(INTEGER(), "col0")};

  const auto verify = [&](const std::shared_ptr<const ProjectNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->names(), names);
    EXPECT_EQ(node->projections(), projections);
    EXPECT_EQ(node->sources(), std::vector<PlanNodePtr>{source_});
  };

  const auto node = ProjectNode::Builder()
                        .id(id)
                        .names(names)
                        .projections(projections)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = ProjectNode::Builder(*node).build();
  verify(node2);
}

class DummyTableHandle : public connector::ConnectorTableHandle {
 public:
  DummyTableHandle(const std::string& connectorId)
      : connector::ConnectorTableHandle(connectorId) {}

  const std::string& name() const override {
    VELOX_NYI();
  }
};

class DummyColumnHandle : public connector::ColumnHandle {
 public:
  const std::string& name() const override {
    VELOX_NYI();
  }
};

TEST_F(PlanNodeBuilderTest, tableScanNode) {
  const PlanNodeId id = "table_scan_node_id";
  const RowTypePtr outputType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});
  const auto tableHandle = std::make_shared<DummyTableHandle>("connector_id");
  const connector::ColumnHandleMap assignments{
      {"c0", std::make_shared<DummyColumnHandle>()},
      {"c1", std::make_shared<DummyColumnHandle>()}};

  const auto verify = [&](const std::shared_ptr<const TableScanNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->outputType(), outputType);
    EXPECT_EQ(node->tableHandle(), tableHandle);
    EXPECT_EQ(node->assignments(), assignments);
  };

  const auto node = TableScanNode::Builder()
                        .id(id)
                        .outputType(outputType)
                        .tableHandle(tableHandle)
                        .assignments(assignments)
                        .build();
  verify(node);

  const auto node2 = TableScanNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, aggregationNode) {
  const PlanNodeId id = "aggregation_node_id";
  const auto step = AggregationNode::Step::kSingle;
  const auto rowType = ROW({"c0"}, {INTEGER()});
  const std::vector<FieldAccessTypedExprPtr> groupingKeys{
      std::make_shared<FieldAccessTypedExpr>(INTEGER(), "c0"),
      std::make_shared<FieldAccessTypedExpr>(VARCHAR(), "c1")};
  const std::vector<FieldAccessTypedExprPtr> preGroupedKeys{
      std::make_shared<FieldAccessTypedExpr>(VARCHAR(), "c1")};
  const std::vector<std::string> aggregateNames{"a0"};
  const std::vector<AggregationNode::Aggregate> aggregates{
      AggregationNode::Aggregate{
          .call = std::make_shared<core::CallTypedExpr>(INTEGER(), "sum"),
          .rawInputTypes = {INTEGER()}}};
  const std::vector<vector_size_t> globalGroupingSets{0};
  const std::optional<FieldAccessTypedExprPtr> groupId{
      std::make_shared<FieldAccessTypedExpr>(INTEGER(), "c0")};
  const bool ignoreNullKeys = false;

  const auto verify = [&](const std::shared_ptr<const AggregationNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->step(), step);
    EXPECT_EQ(node->groupingKeys(), groupingKeys);
    EXPECT_EQ(node->preGroupedKeys(), preGroupedKeys);
    EXPECT_EQ(node->aggregateNames(), aggregateNames);
    EXPECT_EQ(node->aggregates().size(), 1);
    EXPECT_EQ(node->aggregates()[0].serialize(), aggregates[0].serialize());
    EXPECT_EQ(node->globalGroupingSets(), globalGroupingSets);
    EXPECT_EQ(node->groupId(), groupId);
    EXPECT_EQ(node->ignoreNullKeys(), ignoreNullKeys);
    EXPECT_EQ(node->sources(), std::vector<PlanNodePtr>{source_});
  };

  const auto node = AggregationNode::Builder()
                        .id(id)
                        .step(step)
                        .groupingKeys(groupingKeys)
                        .preGroupedKeys(preGroupedKeys)
                        .aggregateNames({aggregateNames})
                        .aggregates({aggregates})
                        .globalGroupingSets(globalGroupingSets)
                        .groupId(groupId)
                        .ignoreNullKeys(ignoreNullKeys)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = AggregationNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, tableWriteNode) {
  const PlanNodeId id = "table_write_node_id";
  const RowTypePtr columns = ROW({"c0"}, {INTEGER()});
  const std::vector<std::string> columnNames{"c0"};
  const RowTypePtr outputType = ROW({"c1"}, {BIGINT()});
  const bool hasPartitioningScheme = true;
  const auto commitStrategy = connector::CommitStrategy::kNoCommit;

  const auto statsSpec = createStatsSpec(
      columns,
      std::vector<std::string>{},
      AggregationNode::Step::kPartial,
      std::vector<std::string>{"sum(c0)"});

  const auto insertTableHandle =
      std::make_shared<InsertTableHandle>("connector_id", nullptr);

  const auto verify = [&](const std::shared_ptr<const TableWriteNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->columns(), columns);
    EXPECT_EQ(node->columnNames(), columnNames);
    EXPECT_EQ(node->insertTableHandle(), insertTableHandle);
    EXPECT_TRUE(node->hasColumnStatsSpec());
    EXPECT_EQ(node->hasPartitioningScheme(), hasPartitioningScheme);
    EXPECT_EQ(node->outputType(), outputType);
    EXPECT_EQ(node->commitStrategy(), commitStrategy);
    EXPECT_EQ(node->sources(), std::vector<PlanNodePtr>{source_});
  };

  const auto node = TableWriteNode::Builder()
                        .id(id)
                        .columns(columns)
                        .columnNames(columnNames)
                        .columnStatsSpec(statsSpec)
                        .insertTableHandle(insertTableHandle)
                        .hasPartitioningScheme(hasPartitioningScheme)
                        .outputType(outputType)
                        .commitStrategy(commitStrategy)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = TableWriteNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, tableWriteMergeNode) {
  const PlanNodeId id = "table_write_merge_node_id";
  const RowTypePtr outputType = ROW({"c0"}, {BIGINT()});

  const auto statsSpec = createStatsSpec(
      outputType,
      std::vector<std::string>{},
      AggregationNode::Step::kIntermediate,
      std::vector<std::string>{"sum(c0)"},
      {{BIGINT()}});

  const auto verify =
      [&](const std::shared_ptr<const TableWriteMergeNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->outputType(), outputType);
        EXPECT_TRUE(node->hasColumnStatsSpec());
        EXPECT_EQ(node->sources()[0], source_);
      };

  const auto node = TableWriteMergeNode::Builder()
                        .id(id)
                        .outputType(outputType)
                        .columnStatsSpec(statsSpec)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = TableWriteMergeNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, expandNode) {
  const PlanNodeId id = "expand_node_id";
  std::vector<std::vector<TypedExprPtr>> projections{
      {std::make_shared<FieldAccessTypedExpr>(INTEGER(), "col0")}};
  std::vector<std::string> names = {"x"};

  const auto verify = [&](const std::shared_ptr<const ExpandNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->projections(), projections);
    EXPECT_EQ(node->names(), names);
    EXPECT_EQ(node->sources(), std::vector<PlanNodePtr>{source_});
  };

  const auto node = ExpandNode::Builder()
                        .id(id)
                        .projections(projections)
                        .names(names)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = ExpandNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, groupIdNode) {
  const PlanNodeId id = "group_id_node_id";
  const std::vector<std::vector<std::string>> groupingSets{{"a"}, {"b"}};
  const std::vector<GroupIdNode::GroupingKeyInfo> groupingKeyInfos{
      GroupIdNode::GroupingKeyInfo{
          .output = "xyz",
          .input = std::make_shared<FieldAccessTypedExpr>(INTEGER(), "c0")}};
  const std::vector<FieldAccessTypedExprPtr> aggregationInputs{
      std::make_shared<FieldAccessTypedExpr>(INTEGER(), "c1")};
  std::string groupIdName = "group_id";

  const auto verify = [&](const std::shared_ptr<const GroupIdNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->groupingSets(), groupingSets);
    EXPECT_EQ(node->groupingKeyInfos().size(), 1);
    EXPECT_EQ(
        node->groupingKeyInfos()[0].serialize(),
        groupingKeyInfos[0].serialize());
    EXPECT_EQ(node->aggregationInputs(), aggregationInputs);
    EXPECT_EQ(node->groupIdName(), groupIdName);
    EXPECT_EQ(node->sources(), std::vector<PlanNodePtr>{source_});
  };

  const auto node = GroupIdNode::Builder()
                        .id(id)
                        .groupingSets(groupingSets)
                        .groupingKeyInfos(groupingKeyInfos)
                        .aggregationInputs(aggregationInputs)
                        .groupIdName(groupIdName)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = GroupIdNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, exchangeNode) {
  const PlanNodeId id = "exchange_node_id";
  const RowTypePtr type = ROW({"c0"}, {BIGINT()});
  const auto serdeKind = VectorSerde::Kind::kPresto;

  const auto verify = [&](const std::shared_ptr<const ExchangeNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->outputType(), type);
    EXPECT_EQ(node->serdeKind(), serdeKind);
  };

  const auto node = ExchangeNode::Builder()
                        .id(id)
                        .outputType(type)
                        .serdeKind(serdeKind)
                        .build();
  verify(node);

  const auto node2 = ExchangeNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, mergeExchangeNode) {
  const PlanNodeId id = "merge_exchange_node_id";
  const RowTypePtr type = ROW({"c0"}, {BIGINT()});
  const auto serdeKind = VectorSerde::Kind::kPresto;
  const std::vector<FieldAccessTypedExprPtr> sortingKeys = {
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c1")};
  const std::vector<SortOrder> sortingOrders = {SortOrder(true, false)};

  const auto verify =
      [&](const std::shared_ptr<const MergeExchangeNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->outputType(), type);
        EXPECT_EQ(node->sortingKeys(), sortingKeys);
        EXPECT_EQ(node->sortingOrders(), sortingOrders);
        EXPECT_EQ(node->serdeKind(), serdeKind);
      };

  const auto node = MergeExchangeNode::Builder()
                        .id(id)
                        .outputType(type)
                        .sortingKeys(sortingKeys)
                        .sortingOrders(sortingOrders)
                        .serdeKind(serdeKind)
                        .build();
  verify(node);

  const auto node2 = MergeExchangeNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, localMergeNode) {
  const PlanNodeId id = "local_merge_node_id";
  std::vector<FieldAccessTypedExprPtr> sortingKeys = {
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  std::vector<SortOrder> sortingOrders = {SortOrder(true, false)};

  const auto verify = [&](const std::shared_ptr<const LocalMergeNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->sortingKeys(), sortingKeys);
    EXPECT_EQ(node->sortingOrders(), sortingOrders);
    EXPECT_EQ(node->sources(), std::vector<PlanNodePtr>{source_});
  };

  const auto node = LocalMergeNode::Builder()
                        .id(id)
                        .sortingKeys(sortingKeys)
                        .sortingOrders(sortingOrders)
                        .sources({source_})
                        .build();
  verify(node);

  const auto node2 = LocalMergeNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, localPartitionNode) {
  const PlanNodeId id = "local_partition_node_id";
  const auto type = LocalPartitionNode::Type::kGather;
  const bool scaleWriter = true;
  const auto partitionFunctionSpec =
      std::make_shared<GatherPartitionFunctionSpec>();

  const auto verify =
      [&](const std::shared_ptr<const LocalPartitionNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->type(), type);
        EXPECT_EQ(node->scaleWriter(), scaleWriter);
        EXPECT_EQ(node->sources(), std::vector<PlanNodePtr>{source_});
        EXPECT_EQ(
            node->partitionFunctionSpec().serialize(),
            partitionFunctionSpec->serialize());
      };

  const auto node = LocalPartitionNode::Builder()
                        .id(id)
                        .type(type)
                        .scaleWriter(scaleWriter)
                        .partitionFunctionSpec(partitionFunctionSpec)
                        .sources({source_})
                        .build();
  verify(node);

  const auto node2 = LocalPartitionNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, partitionedOutputNode) {
  const PlanNodeId id = "partitioned_output_node_id";
  const auto kind = PartitionedOutputNode::Kind::kPartitioned;
  std::vector<TypedExprPtr> keys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  const int numPartitions = 10;
  const bool replicateNullsAndAny = true;
  const auto partitionFunctionSpec =
      std::make_shared<GatherPartitionFunctionSpec>();
  const RowTypePtr outputType = ROW({"c0"}, {BIGINT()});
  const auto serdeKind = VectorSerde::Kind::kPresto;

  const auto verify =
      [&](const std::shared_ptr<const PartitionedOutputNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->kind(), kind);
        EXPECT_EQ(node->keys(), keys);
        EXPECT_EQ(node->numPartitions(), numPartitions);
        EXPECT_EQ(node->isReplicateNullsAndAny(), replicateNullsAndAny);
        EXPECT_EQ(node->outputType(), outputType);
        EXPECT_EQ(node->serdeKind(), serdeKind);
        EXPECT_EQ(node->partitionFunctionSpecPtr(), partitionFunctionSpec);
        EXPECT_EQ(node->sources(), std::vector<PlanNodePtr>{source_});
      };

  const auto node = PartitionedOutputNode::Builder()
                        .id(id)
                        .kind(kind)
                        .keys(keys)
                        .numPartitions(numPartitions)
                        .replicateNullsAndAny(replicateNullsAndAny)
                        .partitionFunctionSpec(partitionFunctionSpec)
                        .outputType(outputType)
                        .serdeKind(serdeKind)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = PartitionedOutputNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, hashJoinNode) {
  const PlanNodeId id = "hash_join_node_id";
  const auto joinType = JoinType::kLeftSemiProject;
  const bool nullAware = true;
  const std::vector<FieldAccessTypedExprPtr> leftKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::vector<FieldAccessTypedExprPtr> rightKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c1")};
  const auto filter =
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "col0");
  const auto left =
      ValuesNode::Builder()
          .id("values_node_id_1")
          .values({makeRowVector(
              {"c0"}, {makeFlatVector<int64_t>(std::vector<int64_t>{1})})})
          .build();
  const auto right =
      ValuesNode::Builder()
          .id("values_node_id_2")
          .values({makeRowVector(
              {"c1"}, {makeFlatVector<int64_t>(std::vector<int64_t>{2})})})
          .build();
  const auto outputType = ROW({"c0", "match"}, {BIGINT(), BOOLEAN()});

  const auto verify = [&](const std::shared_ptr<const HashJoinNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->isNullAware(), nullAware);
    EXPECT_EQ(node->joinType(), joinType);
    EXPECT_EQ(node->leftKeys(), leftKeys);
    EXPECT_EQ(node->rightKeys(), rightKeys);
    EXPECT_EQ(node->filter(), filter);
    EXPECT_EQ(node->sources()[0], left);
    EXPECT_EQ(node->sources()[1], right);
    EXPECT_EQ(node->outputType(), outputType);
  };

  const auto node = HashJoinNode::Builder()
                        .id(id)
                        .joinType(joinType)
                        .leftKeys(leftKeys)
                        .rightKeys(rightKeys)
                        .filter(filter)
                        .left(left)
                        .right(right)
                        .outputType(outputType)
                        .nullAware(nullAware)
                        .build();
  verify(node);

  const auto node2 = HashJoinNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, mergeJoinNode) {
  const PlanNodeId id = "merge_join_node_id";
  const auto joinType = JoinType::kInner;
  const std::vector<FieldAccessTypedExprPtr> leftKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::vector<FieldAccessTypedExprPtr> rightKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c1")};
  const auto filter =
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "col0");
  const auto left =
      ValuesNode::Builder()
          .id("values_node_id_1")
          .values({makeRowVector(
              {"c0"}, {makeFlatVector<int64_t>(std::vector<int64_t>{1})})})
          .build();
  const auto right =
      ValuesNode::Builder()
          .id("values_node_id_2")
          .values({makeRowVector(
              {"c1"}, {makeFlatVector<int64_t>(std::vector<int64_t>{2})})})
          .build();
  const auto outputType = ROW({"c0"}, {BIGINT()});

  const auto verify = [&](const std::shared_ptr<const MergeJoinNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->joinType(), joinType);
    EXPECT_EQ(node->leftKeys(), leftKeys);
    EXPECT_EQ(node->rightKeys(), rightKeys);
    EXPECT_EQ(node->filter(), filter);
    EXPECT_EQ(node->sources()[0], left);
    EXPECT_EQ(node->sources()[1], right);
    EXPECT_EQ(node->outputType(), outputType);
  };

  const auto node = MergeJoinNode::Builder()
                        .id(id)
                        .joinType(joinType)
                        .leftKeys(leftKeys)
                        .rightKeys(rightKeys)
                        .filter(filter)
                        .left(left)
                        .right(right)
                        .outputType(outputType)
                        .build();
  verify(node);

  const auto node2 = MergeJoinNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, indexLookupJoinNode) {
  const PlanNodeId id = "index_lookup_join_node_id";
  const auto joinType = JoinType::kInner;
  const std::vector<FieldAccessTypedExprPtr> leftKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::vector<FieldAccessTypedExprPtr> rightKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c1")};
  const std::vector<IndexLookupConditionPtr> joinConditions{
      std::make_shared<BetweenIndexLookupCondition>(
          std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0"),
          std::make_shared<ConstantTypedExpr>(BIGINT(), variant(1)),
          std::make_shared<ConstantTypedExpr>(BIGINT(), variant(2)))};
  const auto left =
      ValuesNode::Builder()
          .id("values_node_id_1")
          .values({makeRowVector(
              {"c0"}, {makeFlatVector<int64_t>(std::vector<int64_t>{1})})})
          .build();
  const auto right =
      TableScanNode::Builder()
          .id("values_node_id_2")
          .outputType(ROW({"c1"}, {VARCHAR()}))
          .tableHandle(std::make_shared<TestConnectorTableHandleForLookupJoin>(
              "connector_id"))
          .assignments({{"c1", std::make_shared<DummyColumnHandle>()}})
          .build();
  const auto outputType = ROW({"c0"}, {BIGINT()});

  const auto verify =
      [&](const std::shared_ptr<const IndexLookupJoinNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->joinType(), joinType);
        EXPECT_EQ(node->leftKeys(), leftKeys);
        EXPECT_EQ(node->rightKeys(), rightKeys);
        EXPECT_EQ(node->joinConditions().size(), 1);
        EXPECT_EQ(
            node->joinConditions()[0]->serialize(),
            joinConditions[0]->serialize());
        EXPECT_EQ(node->sources()[0], left);
        EXPECT_EQ(node->sources()[1], right);
        EXPECT_EQ(node->outputType(), outputType);
      };

  const auto node = IndexLookupJoinNode::Builder()
                        .id(id)
                        .joinType(joinType)
                        .leftKeys(leftKeys)
                        .rightKeys(rightKeys)
                        .joinConditions(joinConditions)
                        .left(left)
                        .right(right)
                        .outputType(outputType)
                        .build();
  verify(node);

  const auto node2 = IndexLookupJoinNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, nestedLoopJoinNode) {
  const PlanNodeId id = "nested_loop_join_node_id";
  const auto joinType = JoinType::kLeft;
  const auto joinCondition =
      std::make_shared<ConstantTypedExpr>(BOOLEAN(), variant(true));
  const auto left =
      ValuesNode::Builder()
          .id("values_node_id_1")
          .values({makeRowVector(
              {"c0"}, {makeFlatVector<int64_t>(std::vector<int64_t>{1})})})
          .build();
  const auto right =
      ValuesNode::Builder()
          .id("values_node_id_2")
          .values({makeRowVector(
              {"c1"}, {makeFlatVector<int64_t>(std::vector<int64_t>{2})})})
          .build();
  const auto outputType = ROW({"c0"}, {BIGINT()});

  const auto verify =
      [&](const std::shared_ptr<const NestedLoopJoinNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->joinType(), joinType);
        EXPECT_EQ(node->joinCondition(), joinCondition);
        EXPECT_EQ(node->sources()[0], left);
        EXPECT_EQ(node->sources()[1], right);
        EXPECT_EQ(node->outputType(), outputType);
      };

  const auto node = NestedLoopJoinNode::Builder()
                        .id(id)
                        .joinType(joinType)
                        .joinCondition(joinCondition)
                        .left(left)
                        .right(right)
                        .outputType(outputType)
                        .build();
  verify(node);

  const auto node2 = NestedLoopJoinNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, orderByNode) {
  const PlanNodeId id = "order_by_node_id";
  const std::vector<FieldAccessTypedExprPtr> sortingKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::vector<SortOrder> sortingOrders{SortOrder(true, false)};
  const bool isPartial = false;

  const auto verify = [&](const std::shared_ptr<const OrderByNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->sortingKeys(), sortingKeys);
    EXPECT_EQ(node->sortingOrders(), sortingOrders);
    EXPECT_EQ(node->isPartial(), isPartial);
    EXPECT_EQ(node->sources().size(), 1);
    EXPECT_EQ(node->sources()[0], source_);
  };

  const auto node = OrderByNode::Builder()
                        .id(id)
                        .sortingKeys(sortingKeys)
                        .sortingOrders(sortingOrders)
                        .isPartial(isPartial)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = OrderByNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, spatialJoinNode) {
  const PlanNodeId id = "spatial_join_node_id";
  const auto joinType = JoinType::kInner;
  const auto joinCondition =
      std::make_shared<ConstantTypedExpr>(BOOLEAN(), variant(true));
  const auto left =
      ValuesNode::Builder()
          .id("values_node_id_1")
          .values({makeRowVector(
              {"c0"}, {makeFlatVector<int64_t>(std::vector<int64_t>{1})})})
          .build();
  const auto right =
      ValuesNode::Builder()
          .id("values_node_id_2")
          .values({makeRowVector(
              {"c1"}, {makeFlatVector<int64_t>(std::vector<int64_t>{2})})})
          .build();
  const auto outputType = ROW({"c0"}, {BIGINT()});

  const auto verify = [&](const std::shared_ptr<const SpatialJoinNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->joinType(), joinType);
    EXPECT_EQ(node->joinCondition(), joinCondition);
    EXPECT_EQ(node->sources()[0], left);
    EXPECT_EQ(node->sources()[1], right);
    EXPECT_EQ(node->outputType(), outputType);
  };

  const auto node = SpatialJoinNode::Builder()
                        .id(id)
                        .joinType(joinType)
                        .joinCondition(joinCondition)
                        .left(left)
                        .right(right)
                        .outputType(outputType)
                        .build();
  verify(node);

  const auto node2 = SpatialJoinNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, topNNode) {
  const PlanNodeId id = "topn_node_id";
  const std::vector<FieldAccessTypedExprPtr> sortingKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::vector<SortOrder> sortingOrders{SortOrder(true, false)};
  const int32_t count = 10;
  const bool isPartial = false;

  const auto verify = [&](const std::shared_ptr<const TopNNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->sortingKeys(), sortingKeys);
    EXPECT_EQ(node->sortingOrders(), sortingOrders);
    EXPECT_EQ(node->count(), count);
    EXPECT_EQ(node->isPartial(), isPartial);
    EXPECT_EQ(node->sources().size(), 1);
    EXPECT_EQ(node->sources()[0], source_);
  };

  const auto node = TopNNode::Builder()
                        .id(id)
                        .sortingKeys(sortingKeys)
                        .sortingOrders(sortingOrders)
                        .count(count)
                        .isPartial(isPartial)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = TopNNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, limitNode) {
  const PlanNodeId id = "limit_node_id";
  const int64_t offset = 1;
  const int64_t count = 5;
  const bool isPartial = true;

  const auto verify = [&](const std::shared_ptr<const LimitNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->offset(), offset);
    EXPECT_EQ(node->count(), count);
    EXPECT_EQ(node->isPartial(), isPartial);
    EXPECT_EQ(node->sources().size(), 1);
    EXPECT_EQ(node->sources()[0], source_);
  };

  const auto node = LimitNode::Builder()
                        .id(id)
                        .offset(offset)
                        .count(count)
                        .isPartial(isPartial)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = LimitNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, unnestNode) {
  const PlanNodeId id = "unnest_node_id";
  std::vector<FieldAccessTypedExprPtr> replicateVariables{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "a")};
  std::vector<FieldAccessTypedExprPtr> unnestVariables{
      std::make_shared<FieldAccessTypedExpr>(ARRAY(BIGINT()), "b")};
  std::vector<std::string> unnestNames{"b"};
  std::optional<std::string> ordinalityName =
      std::make_optional<std::string>("ord");

  const auto verify = [&](const std::shared_ptr<const UnnestNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->replicateVariables(), replicateVariables);
    EXPECT_EQ(node->unnestVariables(), unnestVariables);
    EXPECT_TRUE(node->hasOrdinality());
    EXPECT_EQ(node->sources()[0], source_);

    for (int i = 0; i < node->outputType()->size(); ++i) {
      if (i < replicateVariables.size()) {
        EXPECT_EQ(node->outputType()->nameOf(i), replicateVariables[i]->name());
      } else if (i < replicateVariables.size() + unnestVariables.size()) {
        EXPECT_EQ(
            node->outputType()->nameOf(i),
            unnestVariables[i - replicateVariables.size()]->name());
      } else {
        EXPECT_EQ(i, node->outputType()->size() - 1);
        EXPECT_EQ(node->outputType()->nameOf(i), ordinalityName.value());
      }
    }
  };

  const auto node = UnnestNode::Builder()
                        .id(id)
                        .replicateVariables(replicateVariables)
                        .unnestVariables(unnestVariables)
                        .unnestNames(unnestNames)
                        .ordinalityName(ordinalityName)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = UnnestNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, enforceSingleRowNode) {
  const PlanNodeId id = "enforce_single_row_id";

  const auto verify =
      [&](const std::shared_ptr<const EnforceSingleRowNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->sources().size(), 1);
        EXPECT_EQ(node->sources()[0], source_);
      };

  const auto node =
      EnforceSingleRowNode::Builder().id(id).source(source_).build();
  verify(node);

  const auto node2 = EnforceSingleRowNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, assignUniqueIdNode) {
  const PlanNodeId id = "assign_unique_id_id";
  const std::string idName = "unique_id";
  const int32_t taskUniqueId = 42;

  const auto verify =
      [&](const std::shared_ptr<const AssignUniqueIdNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->outputType()->names().back(), idName);
        EXPECT_EQ(node->taskUniqueId(), taskUniqueId);
        EXPECT_EQ(node->sources().size(), 1);
        EXPECT_EQ(node->sources()[0], source_);
      };

  const auto node = AssignUniqueIdNode::Builder()
                        .id(id)
                        .idName(idName)
                        .taskUniqueId(taskUniqueId)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = AssignUniqueIdNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, windowNode) {
  const PlanNodeId id = "window_node_id";
  const std::vector<FieldAccessTypedExprPtr> partitionKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "a")};
  const std::vector<FieldAccessTypedExprPtr> sortingKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "b")};
  const std::vector<SortOrder> sortingOrders{SortOrder(true, false)};
  const std::vector<std::string> windowColumnNames = {"rank_col"};
  const bool inputsSorted = true;

  // Create a dummy window function.
  const auto functionCall = std::make_shared<CallTypedExpr>(BIGINT(), "rank");
  WindowNode::Frame frame{
      WindowNode::WindowType::kRows,
      WindowNode::BoundType::kUnboundedPreceding,
      nullptr,
      WindowNode::BoundType::kCurrentRow,
      nullptr};

  std::vector<WindowNode::Function> windowFunctions{
      {functionCall, frame, false}};

  const auto verify = [&](const std::shared_ptr<const WindowNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->partitionKeys(), partitionKeys);
    EXPECT_EQ(node->sortingKeys(), sortingKeys);
    EXPECT_EQ(node->sortingOrders(), sortingOrders);

    EXPECT_EQ(
        node->outputType()->size(),
        source_->outputType()->size() + windowColumnNames.size());
    for (size_t i = source_->outputType()->size();
         i < node->outputType()->size();
         ++i) {
      EXPECT_EQ(
          node->outputType()->nameOf(i),
          windowColumnNames[i - source_->outputType()->size()]);
    }

    EXPECT_EQ(node->windowFunctions().size(), 1);
    EXPECT_EQ(
        node->windowFunctions()[0].serialize(), windowFunctions[0].serialize());
    EXPECT_EQ(node->inputsSorted(), inputsSorted);
    EXPECT_EQ(node->sources().size(), 1);
    EXPECT_EQ(node->sources()[0], source_);
  };

  const auto node = WindowNode::Builder()
                        .id(id)
                        .partitionKeys(partitionKeys)
                        .sortingKeys(sortingKeys)
                        .sortingOrders(sortingOrders)
                        .windowColumnNames(windowColumnNames)
                        .windowFunctions(windowFunctions)
                        .inputsSorted(inputsSorted)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = WindowNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, rowNumberNode) {
  const PlanNodeId id = "row_number_node_id";
  const std::vector<FieldAccessTypedExprPtr> partitionKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::string rowNumberColumnName = "row_number";
  const int32_t limit = 10;

  const auto verify = [&](const std::shared_ptr<const RowNumberNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->partitionKeys(), partitionKeys);
    EXPECT_EQ(node->limit(), limit);
    EXPECT_TRUE(node->generateRowNumber());
    EXPECT_EQ(node->outputType()->names().back(), rowNumberColumnName);
    EXPECT_EQ(node->sources()[0], source_);
  };

  const auto node = RowNumberNode::Builder()
                        .id(id)
                        .partitionKeys(partitionKeys)
                        .rowNumberColumnName(rowNumberColumnName)
                        .limit(limit)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = RowNumberNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, markDistinctNode) {
  const PlanNodeId id = "mark_distinct_node_id";
  const std::string markerName = "is_distinct";
  const std::vector<FieldAccessTypedExprPtr> distinctKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};

  const auto verify = [&](const std::shared_ptr<const MarkDistinctNode>& node) {
    EXPECT_EQ(node->id(), id);
    EXPECT_EQ(node->markerName(), markerName);
    EXPECT_EQ(node->distinctKeys(), distinctKeys);
    EXPECT_EQ(node->sources().size(), 1);
    EXPECT_EQ(node->sources()[0], source_);
  };

  const auto node = MarkDistinctNode::Builder()
                        .id(id)
                        .markerName(markerName)
                        .distinctKeys(distinctKeys)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = MarkDistinctNode::Builder(*node).build();
  verify(node2);
}

TEST_F(PlanNodeBuilderTest, topNRowNumberNode) {
  const PlanNodeId id = "topn_row_number_node_id";
  const std::vector<FieldAccessTypedExprPtr> partitionKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::vector<FieldAccessTypedExprPtr> sortingKeys{
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c1")};
  const std::vector<SortOrder> sortingOrders{SortOrder(true, false)};
  const std::string rowNumberColumnName = "row_number";
  const int32_t limit = 5;

  const auto verify =
      [&](const std::shared_ptr<const TopNRowNumberNode>& node) {
        EXPECT_EQ(node->id(), id);
        EXPECT_EQ(node->partitionKeys(), partitionKeys);
        EXPECT_EQ(node->sortingKeys(), sortingKeys);
        EXPECT_EQ(node->sortingOrders(), sortingOrders);
        EXPECT_EQ(node->limit(), limit);
        EXPECT_TRUE(node->generateRowNumber());
        EXPECT_EQ(node->outputType()->names().back(), rowNumberColumnName);
        EXPECT_EQ(node->sources().size(), 1);
        EXPECT_EQ(node->sources()[0], source_);
      };

  const auto node = TopNRowNumberNode::Builder()
                        .id(id)
                        .partitionKeys(partitionKeys)
                        .sortingKeys(sortingKeys)
                        .sortingOrders(sortingOrders)
                        .rowNumberColumnName(rowNumberColumnName)
                        .limit(limit)
                        .source(source_)
                        .build();
  verify(node);

  const auto node2 = TopNRowNumberNode::Builder(*node).build();
  verify(node2);
}
