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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.execution.TestingPageSourceProvider;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.procedure.BaseProcedure;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.DistributedProcedure.Argument;
import com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.ExpressionMatcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.RowNumberSymbolMatcher;
import com.facebook.presto.sql.planner.optimizations.AddLocalExchanges;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestProcedureRegistry;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingSplitManager;
import com.facebook.presto.tests.QueryTemplate;
import com.facebook.presto.util.MorePredicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_SORT;
import static com.facebook.presto.SystemSessionProperties.ENFORCE_FIXED_DISTRIBUTION_FOR_OUTPUT_OPERATOR;
import static com.facebook.presto.SystemSessionProperties.EXPLOIT_CONSTRAINTS;
import static com.facebook.presto.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.LEAF_NODE_LIMIT_ENABLED;
import static com.facebook.presto.SystemSessionProperties.MAX_LEAF_NODES_IN_PLAN;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.OFFSET_CLAUSE_ENABLED;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.SystemSessionProperties.PREFER_SORT_MERGE_JOIN;
import static com.facebook.presto.SystemSessionProperties.PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID;
import static com.facebook.presto.SystemSessionProperties.REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT;
import static com.facebook.presto.SystemSessionProperties.SIMPLIFY_PLAN_WITH_EMPTY_INPUT;
import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.SystemSessionProperties.getMaxLeafNodesInPlan;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_LIMIT_CLAUSE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.SCHEMA;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.TABLE_NAME;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.sql.TestExpressionInterpreter.AVG_UDAF_CPP;
import static com.facebook.presto.sql.TestExpressionInterpreter.SQUARE_UDF_CPP;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.apply;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.groupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.mergeJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.topN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.topNRowNumber;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.FIRST;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.LAST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.sql.tree.SortItem.Ordering.DESCENDING;
import static com.facebook.presto.tests.QueryTemplate.queryTemplate;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestLogicalPlanner
        extends BasePlanTest
{
    // TODO: Use com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder#tableScan with required node/stream
    // partitioning to properly test aggregation, window function and join.

    @BeforeClass
    public void setup()
    {
        setupJsonFunctionNamespaceManager(this.getQueryRunner());

        // Register catalog `test` with a distributed procedure `distributed_fun`
        this.getQueryRunner().createCatalog("test",
                new ConnectorFactory()
                {
                    @Override
                    public String getName()
                    {
                        return "test";
                    }

                    @Override
                    public ConnectorHandleResolver getHandleResolver()
                    {
                        return new TestingHandleResolver();
                    }

                    @Override
                    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                    {
                        List<Argument> arguments = new ArrayList<>();
                        arguments.add(new Argument(SCHEMA, VARCHAR));
                        arguments.add(new Argument(TABLE_NAME, VARCHAR));
                        Set<BaseProcedure<?>> procedures = new HashSet<>();
                        procedures.add(new TableDataRewriteDistributedProcedure("system", "distributed_fun",
                                arguments,
                                (session, transactionContext, procedureHandle, fragments) -> null,
                                (session, transactionContext, procedureHandle, fragments) -> {},
                                ignored -> new TestProcedureRegistry.TestProcedureContext()));

                        return new Connector()
                        {
                            private final ConnectorMetadata metadata = new TestingMetadata();

                            @Override
                            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                            {
                                return new ConnectorTransactionHandle()
                                {};
                            }

                            @Override
                            public ConnectorPageSourceProvider getPageSourceProvider()
                            {
                                return new TestingPageSourceProvider();
                            }

                            @Override
                            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
                            {
                                return metadata;
                            }

                            @Override
                            public ConnectorSplitManager getSplitManager()
                            {
                                return new TestingSplitManager(ImmutableList.of());
                            }

                            @Override
                            public Set<DistributedProcedure> getDistributedProcedures()
                            {
                                return procedures.stream().filter(DistributedProcedure.class::isInstance)
                                        .map(DistributedProcedure.class::cast)
                                        .collect(Collectors.toSet());
                            }
                        };
                    }
                }, ImmutableMap.of());
    }

    @Test
    public void testCallDistributedProcedure()
    {
        Session session = getQueryRunner().getDefaultSession();

        // Call non-existed distributed procedure
        assertPlanFailedWithException("call test.system.no_fun('a', 'b')", session,
                format("Distributed procedure not registered: test.system.no_fun", "test", "system", "no_fun"));

        // Call distributed procedure on non-existed target table
        assertPlanFailedWithException("call test.system.distributed_fun('tiny', 'notable')", session,
                format("Table %s.%s.%s does not exist", session.getCatalog().get(), "tiny", "notable"));

        // Call distributed procedure on partitioned target table
        assertDistributedPlan("call test.system.distributed_fun('tiny', 'orders')",
                anyTree(node(TableFinishNode.class,
                        exchange(REMOTE_STREAMING, GATHER,
                                node(CallDistributedProcedureNode.class,
                                        exchange(LOCAL, GATHER,
                                                tableScan("orders")))))));

        // Call distributed procedure on unPartitioned target table
        assertDistributedPlan("call test.system.distributed_fun('tiny', 'customer')",
                anyTree(node(TableFinishNode.class,
                        exchange(REMOTE_STREAMING, GATHER,
                                node(CallDistributedProcedureNode.class,
                                        exchange(LOCAL, GATHER,
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        tableScan("customer"))))))));
    }
    @Test
    public void testAnalyze()
    {
        assertDistributedPlan("ANALYZE orders",
                anyTree(
                        node(StatisticsWriterNode.class,
                                anyTree(
                                        exchange(REMOTE_STREAMING, GATHER,
                                                node(AggregationNode.class,
                                                        anyTree(
                                                                exchange(REMOTE_STREAMING, GATHER,
                                                                        node(AggregationNode.class,
                                                                                tableScan("orders", ImmutableMap.of()))))))))));
    }

    @Test
    public void testAggregation()
    {
        // simple group by
        assertDistributedPlan("SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus",
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", functionCall("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_sum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        anyTree(tableScan("orders", ImmutableMap.of("totalprice", "totalprice")))))))));

        // simple group by over filter that keeps at most one group
        assertDistributedPlan("SELECT orderstatus, sum(totalprice) FROM orders WHERE orderstatus='O' GROUP BY orderstatus",
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", functionCall("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_sum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        anyTree(tableScan("orders", ImmutableMap.of("totalprice", "totalprice")))))))));
    }

    @Test
    public void testGroupingSet()
    {
        Session session = Session.builder(this.getQueryRunner().getDefaultSession())
                // Disable push RemoteExchange through GroupId.
                .setSystemProperty(PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID, "false")
                .build();
        assertDistributedPlan(
                "SELECT sum(totalprice), orderstatus, orderpriority FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderstatus, orderpriority))",
                session,
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", functionCall("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_sum", functionCall("sum", ImmutableList.of("totalprice"))), PARTIAL,
                                                        anyTree(
                                                                groupingSet(ImmutableList.of(ImmutableList.of("orderstatus"), ImmutableList.of("orderstatus", "orderpriority")), ImmutableMap.of("totalprice", "totalprice"), "groupid",
                                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderstatus", "orderstatus", "orderpriority", "orderpriority"))))))))));
    }

    @Test
    public void testGroupingSetWithPushRemoteExchange()
    {
        Session session = Session.builder(this.getQueryRunner().getDefaultSession())
                // Enable push RemoteExchange through GroupId.
                .setSystemProperty(PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID, "true")
                .build();
        assertDistributedPlan(
                "SELECT sum(totalprice), orderstatus, orderpriority FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderstatus, orderpriority))",
                session,
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", functionCall("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        aggregation(
                                                ImmutableMap.of("partial_sum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                PARTIAL,
                                                anyTree(
                                                        groupingSet(ImmutableList.of(ImmutableList.of("orderstatus"), ImmutableList.of("orderstatus", "orderpriority")), ImmutableMap.of("totalprice", "totalprice"), "groupid",
                                                                anyTree(
                                                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                                                anyTree(
                                                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderstatus", "orderstatus", "orderpriority", "orderpriority"))))))))))));

        // Verify RemoteExchange is not pushed through GroupId if grouping sets have no common grouping column.
        assertDistributedPlan(
                "SELECT sum(totalprice), orderstatus, orderpriority FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderpriority))",
                session,
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", functionCall("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_sum", functionCall("sum", ImmutableList.of("totalprice"))), PARTIAL,
                                                        anyTree(
                                                                groupingSet(ImmutableList.of(ImmutableList.of("orderstatus"), ImmutableList.of("orderpriority")), ImmutableMap.of("totalprice", "totalprice"), "groupid",
                                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderstatus", "orderstatus", "orderpriority", "orderpriority"))))))))));
    }

    @Test
    public void testWindow()
    {
        // Window partition key is pre-bucketed.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY orderkey) FROM orders",
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(ImmutableList.of("orderkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                project(tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))));

        assertDistributedPlan("SELECT row_number() OVER (PARTITION BY orderkey) FROM orders",
                anyTree(
                        rowNumber(rowNumberMatcherBuilder -> rowNumberMatcherBuilder
                                        .partitionBy(ImmutableList.of("orderkey")),
                                project(tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))));

        assertDistributedPlan("SELECT orderkey FROM (SELECT orderkey, row_number() OVER (PARTITION BY orderkey ORDER BY custkey) n FROM orders) WHERE n = 1",
                anyTree(
                        topNRowNumber(topNRowNumber -> topNRowNumber
                                        .specification(
                                                ImmutableList.of("orderkey"),
                                                ImmutableList.of("custkey"),
                                                ImmutableMap.of("custkey", ASC_NULLS_LAST)),
                                project(tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey"))))));

        // Window partition key is not pre-bucketed.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY orderstatus) FROM orders",
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(ImmutableList.of("orderstatus"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                project(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus"))))))));

        assertDistributedPlan("SELECT row_number() OVER (PARTITION BY orderstatus) FROM orders",
                anyTree(
                        rowNumber(rowNumberMatcherBuilder -> rowNumberMatcherBuilder
                                        .partitionBy(ImmutableList.of("orderstatus")),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                project(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus"))))))));

        assertDistributedPlan("SELECT orderstatus FROM (SELECT orderstatus, row_number() OVER (PARTITION BY orderstatus ORDER BY custkey) n FROM orders) WHERE n = 1",
                anyTree(
                        topNRowNumber(topNRowNumber -> topNRowNumber
                                        .specification(
                                                ImmutableList.of("orderstatus"),
                                                ImmutableList.of("custkey"),
                                                ImmutableMap.of("custkey", ASC_NULLS_LAST))
                                        .partial(false),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                topNRowNumber(topNRowNumber -> topNRowNumber
                                                                .specification(
                                                                        ImmutableList.of("orderstatus"),
                                                                        ImmutableList.of("custkey"),
                                                                        ImmutableMap.of("custkey", ASC_NULLS_LAST))
                                                                .partial(true),
                                                        project(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "custkey", "custkey")))))))));
    }

    @Test
    public void testWindowAfterJoin()
    {
        // Window partition key is a super set of join key.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY o.orderstatus, o.orderkey) FROM orders o JOIN lineitem l ON o.orderstatus = l.linestatus",
                noJoinReordering(),
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(ImmutableList.of("orderstatus", "orderkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        project(
                                                join(INNER, ImmutableList.of(equiJoinClause("orderstatus", "linestatus")), Optional.empty(), Optional.of(PARTITIONED),
                                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                                anyTree(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "orderkey", "orderkey")))),
                                                        exchange(LOCAL, GATHER,
                                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                                        anyTree(tableScan("lineitem", ImmutableMap.of("linestatus", "linestatus")))))))))));

        // Window partition key is not a super set of join key.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY o.orderkey) FROM orders o JOIN lineitem l ON o.orderstatus = l.linestatus",
                noJoinReordering(),
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(ImmutableList.of("orderkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(join(INNER, ImmutableList.of(equiJoinClause("orderstatus", "linestatus")), Optional.empty(), Optional.of(PARTITIONED),
                                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                                anyTree(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "orderkey", "orderkey")))),
                                                        exchange(LOCAL, GATHER,
                                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                                        anyTree(tableScan("lineitem", ImmutableMap.of("linestatus", "linestatus"))))))))))));

        // Test broadcast join
        Session broadcastJoin = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.toString(false))
                .build();
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY o.custkey) FROM orders o JOIN lineitem l ON o.orderstatus = l.linestatus",
                broadcastJoin,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(ImmutableList.of("custkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                project(
                                                        join(INNER, ImmutableList.of(equiJoinClause("orderstatus", "linestatus")), Optional.empty(), Optional.of(REPLICATED),
                                                                anyTree(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "custkey", "custkey"))),
                                                                exchange(LOCAL, GATHER,
                                                                        exchange(REMOTE_STREAMING, REPLICATE,
                                                                                anyTree(tableScan("lineitem", ImmutableMap.of("linestatus", "linestatus"))))))))))));
    }

    @Test
    public void testWindowAfterAggregation()
    {
        // Window partition key is a super set of group by key.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY custkey) FROM orders GROUP BY custkey",
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(ImmutableList.of("custkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                project(aggregation(singleGroupingSet("custkey"), ImmutableMap.of(), ImmutableMap.of(), Optional.empty(), FINAL,
                                        exchange(LOCAL, GATHER,
                                                project(exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(tableScan("orders", ImmutableMap.of("custkey", "custkey")))))))))));

        // Window partition key is not a super set of group by key.
        assertDistributedPlan("SELECT rank() OVER (partition by custkey) FROM (SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY shippriority, custkey)",
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(ImmutableList.of("custkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                project(aggregation(singleGroupingSet("shippriority", "custkey"), ImmutableMap.of(), ImmutableMap.of(), Optional.empty(), FINAL,
                                                        exchange(LOCAL, GATHER,
                                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                                        anyTree(tableScan("orders", ImmutableMap.of("custkey", "custkey", "shippriority", "shippriority"))))))))))));
    }

    @Test
    public void testDistinctLimitOverInequalityJoin()
    {
        assertPlan("SELECT DISTINCT o.orderkey FROM orders o JOIN lineitem l ON o.orderkey < l.orderkey LIMIT 1",
                anyTree(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        filter("O_ORDERKEY < L_ORDERKEY",
                                                join(INNER, ImmutableList.of(), Optional.empty(),
                                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")),
                                                        any(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))
                                                        .withExactOutputs(ImmutableList.of("O_ORDERKEY", "L_ORDERKEY")))))));

        assertPlan("SELECT DISTINCT o.orderkey FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey LIMIT 1",
                noJoinReordering(),
                anyTree(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        join(INNER,
                                                ImmutableList.of(equiJoinClause("O_SHIPPRIORITY", "L_LINENUMBER")),
                                                Optional.of("O_ORDERKEY < L_ORDERKEY"),
                                                any(tableScan("orders", ImmutableMap.of(
                                                        "O_SHIPPRIORITY", "shippriority",
                                                        "O_ORDERKEY", "orderkey"))),
                                                anyTree(tableScan("lineitem", ImmutableMap.of(
                                                        "L_LINENUMBER", "linenumber",
                                                        "L_ORDERKEY", "orderkey"))))
                                                .withExactOutputs(ImmutableList.of("O_ORDERKEY"))))));
    }

    @Test
    public void testDistinctOverConstants()
    {
        assertPlan("SELECT count(*), count(distinct orderstatus) FROM (SELECT * FROM orders WHERE orderstatus = 'F')",
                anyTree(
                        markDistinct(
                                "is_distinct",
                                ImmutableList.of("orderstatus_35"),
                                "hash",
                                anyTree(
                                        project(ImmutableMap.of("hash", expression("combine_hash(bigint '0', coalesce(\"$operator$hash_code\"(orderstatus_35), 0))")),
                                                project(
                                                        ImmutableMap.of("orderstatus_35", expression("'F'")),
                                                        tableScan("orders", ImmutableMap.of())))))));
    }

    @Test
    public void testInnerInequalityJoinNoEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o JOIN lineitem l ON o.orderkey < l.orderkey",
                anyTree(
                        filter("O_ORDERKEY < L_ORDERKEY",
                                join(INNER, ImmutableList.of(), Optional.empty(),
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")),
                                        any(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testInnerInequalityJoinWithEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey",
                noJoinReordering(),
                anyTree(
                        anyNot(FilterNode.class,
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("O_SHIPPRIORITY", "L_LINENUMBER")),
                                        Optional.of("O_ORDERKEY < L_ORDERKEY"),
                                        any(tableScan("orders", ImmutableMap.of(
                                                "O_SHIPPRIORITY", "shippriority",
                                                "O_ORDERKEY", "orderkey"))),
                                        anyTree(tableScan("lineitem", ImmutableMap.of(
                                                "L_LINENUMBER", "linenumber",
                                                "L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testLeftConvertedToInnerInequalityJoinNoEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o LEFT JOIN lineitem l ON o.orderkey < l.orderkey WHERE l.orderkey IS NOT NULL",
                anyTree(
                        project(
                                filter("O_ORDERKEY < L_ORDERKEY",
                                        join(INNER, ImmutableList.of(), Optional.empty(),
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")),
                                                any(
                                                        filter("NOT (L_ORDERKEY IS NULL)",
                                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))))));
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                noJoinReordering(),
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testJoinWithOrderBySameKey()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey ORDER BY l.orderkey ASC, o.orderkey ASC",
                noJoinReordering(),
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testSortMergeJoin()
    {
        Session preferSortMergeJoin = Session.builder(noJoinReordering())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty(PREFER_SORT_MERGE_JOIN, "true")
                .setSystemProperty(DISTRIBUTED_SORT, "false")
                .build();

        // Both sides are not sorted.
        assertPlan("SELECT o.orderkey FROM orders o INNER JOIN lineitem l ON o.custkey = l.partkey",
                preferSortMergeJoin,
                anyTree(
                        mergeJoin(INNER, ImmutableList.of(equiJoinClause("ORDERS_CK", "LINEITEM_PK")), Optional.empty(),
                                sort(
                                        ImmutableList.of(sort("ORDERS_CK", ASCENDING, FIRST)),
                                        exchange(LOCAL, GATHER, ImmutableList.of(),
                                                tableScan("orders", ImmutableMap.of("ORDERS_CK", "custkey")))),
                                sort(
                                        ImmutableList.of(sort("LINEITEM_PK", ASCENDING, FIRST)),
                                        exchange(LOCAL, GATHER, ImmutableList.of(),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_PK", "partkey")))))));

        // Left side is sorted.
        assertPlan("SELECT o.orderkey FROM orders o INNER JOIN lineitem l ON o.orderkey = l.partkey",
                preferSortMergeJoin,
                anyTree(
                        mergeJoin(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_PK")), Optional.empty(),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                sort(
                                        ImmutableList.of(sort("LINEITEM_PK", ASCENDING, FIRST)),
                                        exchange(LOCAL, GATHER, ImmutableList.of(),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_PK", "partkey")))))));

        // Right side is sorted.
        assertPlan("SELECT o.orderkey FROM orders o INNER JOIN lineitem l ON o.custkey = l.orderkey",
                preferSortMergeJoin,
                anyTree(
                        mergeJoin(INNER, ImmutableList.of(equiJoinClause("ORDERS_CK", "LINEITEM_OK")), Optional.empty(),
                                sort(
                                        ImmutableList.of(sort("ORDERS_CK", ASCENDING, FIRST)),
                                        exchange(LOCAL, GATHER, ImmutableList.of(),
                                                tableScan("orders", ImmutableMap.of("ORDERS_CK", "custkey")))),
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));

        // Both sides are sorted.
        assertPlan("SELECT o.orderkey FROM orders o INNER JOIN lineitem l ON o.orderkey = l.orderkey",
                preferSortMergeJoin,
                anyTree(
                        mergeJoin(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")), Optional.empty(),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    @Test
    public void testUncorrelatedSubqueries()
    {
        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("X", "Y")),
                                project(
                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                project(
                                        node(EnforceSingleRowNode.class,
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan("SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                anyTree(
                        filter("S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan("SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter("NOT S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testPushDownJoinConditionConjunctsToInnerSideBasedOnInheritedPredicate()
    {
        assertPlan(
                "SELECT nationkey FROM nation LEFT OUTER JOIN region " +
                        "ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'",
                anyTree(
                        join(LEFT, ImmutableList.of(equiJoinClause("NATION_NAME", "REGION_NAME"), equiJoinClause("NATION_REGIONKEY", "REGION_REGIONKEY")),
                                anyTree(
                                        filter("NATION_NAME = CAST ('blah' AS VARCHAR(25))",
                                                constrainedTableScan(
                                                        "nation",
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(
                                                                "NATION_NAME", "name",
                                                                "NATION_REGIONKEY", "regionkey")))),
                                anyTree(
                                        filter("REGION_NAME = CAST ('blah' AS VARCHAR(25))",
                                                constrainedTableScan(
                                                        "region",
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(
                                                                "REGION_NAME", "name",
                                                                "REGION_REGIONKEY", "regionkey")))))));
    }

    @Test
    public void testScalarSubqueryJoinFilterPushdown()
    {
        assertPlan(
                "SELECT * FROM orders WHERE orderkey = (SELECT 1)",
                anyTree(
                        join(INNER, ImmutableList.of(),
                                project(
                                        filter("orderkey = BIGINT '1'",
                                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey")))),
                                anyTree(
                                        project(ImmutableMap.of("orderkey", expression("1")), any())))));
    }

    @Test
    public void testSameScalarSubqueryIsAppliedOnlyOnce()
    {
        // three subqueries with two duplicates (coerced to two different types), only two scalar joins should be in plan
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders WHERE CAST(orderkey AS INTEGER) = (SELECT 1) AND custkey = (SELECT 2) AND CAST(custkey as REAL) != (SELECT 1)"),
                        EnforceSingleRowNode.class::isInstance),
                2);
        // same query used for left, right and complex join condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey = (SELECT 1) AND o2.orderkey = (SELECT 1) AND o1.orderkey + o2.orderkey = (SELECT 2)"),
                        EnforceSingleRowNode.class::isInstance),
                2);
    }

    @Test
    public void testSameInSubqueryIsAppliedOnlyOnce()
    {
        // same IN query used for left, right and complex condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey IN (SELECT 1) AND (o1.orderkey IN (SELECT 1) OR o1.orderkey IN (SELECT 1))"),
                        SemiJoinNode.class::isInstance),
                1);

        // one subquery used for "1 IN (SELECT 1)", one subquery used for "2 IN (SELECT 1)"
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT 1 IN (SELECT 1), 2 IN (SELECT 1) WHERE 1 IN (SELECT 1)"),
                        SemiJoinNode.class::isInstance),
                2);
    }

    @Test
    public void testSameQualifiedSubqueryIsAppliedOnlyOnce()
    {
        // same ALL query used for left, right and complex condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey <= ALL(SELECT 1) AND (o1.orderkey <= ALL(SELECT 1) OR o1.orderkey <= ALL(SELECT 1))"),
                        AggregationNode.class::isInstance),
                1);

        // one subquery used for "1 <= ALL(SELECT 1)", one subquery used for "2 <= ALL(SELECT 1)"
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT 1 <= ALL(SELECT 1), 2 <= ALL(SELECT 1) WHERE 1 <= ALL(SELECT 1)"),
                        AggregationNode.class::isInstance),
                2);
    }

    private static int countOfMatchingNodes(Plan plan, Predicate<PlanNode> predicate)
    {
        return searchFrom(plan.getRoot()).where(predicate).count();
    }

    @Test
    public void testRemoveUnreferencedScalarInputApplyNodes()
    {
        assertPlanContainsNoApplyOrAnyJoin("SELECT (SELECT 1)");
    }

    @Test
    public void testSubqueryPruning()
    {
        List<QueryTemplate.Parameter> subqueries = QueryTemplate.parameter("subquery").of(
                "orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 2 = 0)",
                "EXISTS(SELECT orderkey FROM lineitem WHERE orderkey % 2 = 0)",
                "0 = (SELECT orderkey FROM lineitem WHERE orderkey % 2 = 0)");

        queryTemplate("SELECT COUNT(*) FROM (SELECT %subquery% FROM orders)")
                .replaceAll(subqueries)
                .forEach(this::assertPlanContainsNoApplyOrAnyJoin);

        queryTemplate("SELECT * FROM orders WHERE true OR %subquery%")
                .replaceAll(subqueries)
                .forEach(this::assertPlanContainsNoApplyOrAnyJoin);
    }

    @Test
    public void testJoinOutputPruning()
    {
        assertPlan("SELECT nationkey FROM nation JOIN region ON nation.regionkey = region.regionkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("REGIONKEY_LEFT", "REGIONKEY_RIGHT")),
                                anyTree(
                                        tableScan("nation", ImmutableMap.of("REGIONKEY_LEFT", "regionkey", "NATIONKEY", "nationkey"))),
                                anyTree(
                                        tableScan("region", ImmutableMap.of("REGIONKEY_RIGHT", "regionkey")))))
                        .withNumberOfOutputColumns(1)
                        .withOutputs(ImmutableList.of("NATIONKEY")));
    }

    private void assertPlanContainsNoApplyOrAnyJoin(String sql)
    {
        assertFalse(
                searchFrom(plan(sql, OPTIMIZED).getRoot())
                        .where(isInstanceOfAny(ApplyNode.class, JoinNode.class, IndexJoinNode.class, SemiJoinNode.class, LateralJoinNode.class))
                        .matches(),
                "Unexpected node for query: " + sql);
    }

    @Test
    public void testCorrelatedSubqueries()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE 3 = (SELECT orderkey)",
                OPTIMIZED,
                any(
                        project(
                                ImmutableMap.of("X", expression("3")),
                                filter(
                                        "X = BIGINT '3'",
                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))))));
    }

    @Test
    public void testCorrelatedJoinWithLimit()
    {
        // rewrite Limit to RowNumberNode
        assertPlan(
                "SELECT regionkey, n.name FROM region CROSS JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey LIMIT 2) n",
                any(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("nation_regionkey", "region_regionkey")),
                                any(rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of("nation_regionkey"))
                                                .maxRowCountPerPartition(Optional.of(2)),
                                        anyTree(tableScan("nation", ImmutableMap.of("nation_name", "name", "nation_regionkey", "regionkey"))))),
                                any(project(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey")))))));

        // rewrite Limit to decorrelated Limit
        assertPlan("SELECT regionkey, n.nationkey FROM region CROSS JOIN LATERAL (SELECT nationkey FROM nation WHERE region.regionkey = 3 LIMIT 2) n",
                any(
                        project(
                                join(
                                        INNER,
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        //Optional.of("region_regionkey = BIGINT '3'"),
                                        any(any(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey")))),
                                        limit(
                                                2,
                                                any(tableScan("nation", ImmutableMap.of("nation_nationkey", "nationkey"))))))));
    }

    @Test
    public void testCorrelatedJoinWithTopN()
    {
        // rewrite TopN to TopNRowNumberNode
        assertPlan(
                "SELECT regionkey, n.name FROM region CROSS JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey ORDER BY name LIMIT 2) n",
                any(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("region_regionkey", "nation_regionkey")),
                                any(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey"))),
                                any(topNRowNumber(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of("nation_regionkey"),
                                                        ImmutableList.of("nation_name"),
                                                        ImmutableMap.of("nation_name", SortOrder.ASC_NULLS_LAST))
                                                .maxRowCountPerPartition(2)
                                                .partial(false),
                                        anyTree(tableScan("nation", ImmutableMap.of("nation_name", "name", "nation_regionkey", "regionkey"))))))));

        // rewrite TopN to RowNumberNode
        assertPlan(
                "SELECT regionkey, n.name FROM region CROSS JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey ORDER BY regionkey LIMIT 2) n",
                any(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("nation_regionkey", "region_regionkey")),
                                any(rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of("nation_regionkey"))
                                                .maxRowCountPerPartition(Optional.of(2)),
                                        anyTree(tableScan("nation", ImmutableMap.of("nation_name", "name", "nation_regionkey", "regionkey"))))),
                                any(project(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey")))))));
    }

    @Test
    public void testCorrelatedScalarSubqueryInSelect()
    {
        assertDistributedPlan("SELECT name, (SELECT name FROM region WHERE regionkey = nation.regionkey) FROM nation",
                noJoinReordering(),
                anyTree(
                        markDistinct("is_distinct", ImmutableList.of("unique"),
                                join(LEFT, ImmutableList.of(equiJoinClause("n_regionkey", "r_regionkey")),
                                        assignUniqueId("unique",
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey"))))),
                                        anyTree(
                                                tableScan("region", ImmutableMap.of("r_regionkey", "regionkey")))))));
    }

    @Test
    public void testStreamingAggregationForCorrelatedSubquery()
    {
        // Use equi-clause to trigger hash partitioning of the join sources
        assertDistributedPlan(
                "SELECT name, (SELECT max(name) FROM region WHERE regionkey = nation.regionkey AND length(name) > length(nation.name)) FROM nation",
                noJoinReordering(),
                anyTree(
                        aggregation(
                                singleGroupingSet("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(Optional.of("max"), functionCall("max", ImmutableList.of("r_name"))),
                                ImmutableList.of("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                node(JoinNode.class,
                                        assignUniqueId("unique",
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation", ImmutableMap.of("n_name", "name", "n_regionkey", "regionkey"))))),
                                        anyTree(
                                                tableScan("region", ImmutableMap.of("r_name", "name")))))));

        // Don't use equi-clauses to trigger replicated join
        assertDistributedPlan(
                "SELECT name, (SELECT max(name) FROM region WHERE regionkey > nation.regionkey) FROM nation",
                anyTree(
                        aggregation(
                                singleGroupingSet("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(Optional.of("max"), functionCall("max", ImmutableList.of("r_name"))),
                                ImmutableList.of("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                node(JoinNode.class,
                                        assignUniqueId("unique",
                                                tableScan("nation", ImmutableMap.of("n_name", "name", "n_regionkey", "regionkey"))),
                                        anyTree(
                                                tableScan("region", ImmutableMap.of("r_name", "name")))))));
    }

    @Test
    public void testStreamingAggregationOverJoin()
    {
        // "orders" table is naturally grouped on orderkey
        // this grouping should survive inner and left joins and allow for streaming aggregation later
        // this grouping should not survive a cross join

        // inner join -> streaming aggregation
        assertPlan("SELECT o.orderkey, count(*) FROM orders o, lineitem l WHERE o.orderkey=l.orderkey GROUP BY 1",
                noJoinReordering(),
                anyTree(
                        aggregation(
                                singleGroupingSet("o_orderkey"),
                                ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                ImmutableList.of("o_orderkey"), // streaming
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                join(INNER, ImmutableList.of(equiJoinClause("o_orderkey", "l_orderkey")),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey"))),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey")))))));

        // left join -> streaming aggregation
        assertPlan("SELECT o.orderkey, count(*) FROM orders o LEFT JOIN lineitem l ON o.orderkey=l.orderkey GROUP BY 1",
                anyTree(
                        aggregation(
                                singleGroupingSet("o_orderkey"),
                                ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                ImmutableList.of("o_orderkey"), // streaming
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                join(LEFT, ImmutableList.of(equiJoinClause("o_orderkey", "l_orderkey")),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey"))),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey")))))));

        // cross join - no streaming
        assertPlan("SELECT o.orderkey, count(*) FROM orders o, lineitem l GROUP BY 1",
                anyTree(
                        aggregation(
                                singleGroupingSet("orderkey"),
                                ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                ImmutableList.of(), // not streaming
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                join(INNER, ImmutableList.of(),
                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey")),
                                        anyTree(
                                                node(TableScanNode.class))))));
    }

    /**
     * Handling of correlated IN pulls up everything possible to the generated outer join condition.
     * This test ensures uncorrelated conditions are pushed back down.
     */
    @Test
    public void testCorrelatedInUncorrelatedFiltersPushDown()
    {
        assertPlan(
                "SELECT orderkey, comment IN (SELECT clerk FROM orders s WHERE s.orderkey = o.orderkey AND s.orderkey < 7) FROM lineitem o",
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem")),
                                anyTree(
                                        filter("orderkey < BIGINT '7'", // pushed down
                                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey")))))));
    }

    /**
     * Handling of correlated in predicate involves group by over all symbols from source. Once aggregation is added to the plan,
     * it prevents pruning of the unreferenced symbols. However, the aggregation's result doesn't actually depended on those symbols
     * and this test makes sure the symbols are pruned first.
     */
    @Test
    public void testSymbolsPrunedInCorrelatedInPredicateSource()
    {
        assertPlan(
                "SELECT orderkey, comment IN (SELECT clerk FROM orders s WHERE s.orderkey = o.orderkey AND s.orderkey < 7) FROM lineitem o",
                anyTree(
                        node(JoinNode.class,
                                anyTree(strictTableScan("lineitem", ImmutableMap.of(
                                        "orderkey", "orderkey",
                                        "comment", "comment"))),
                                anyTree(tableScan("orders")))));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Unexpected UnresolvedSymbolExpression.*")
    public void testDoubleNestedCorrelatedSubqueries()
    {
        assertPlan(
                "SELECT orderkey FROM orders o " +
                        "WHERE 3 IN (SELECT o.custkey FROM lineitem l WHERE (SELECT l.orderkey = o.orderkey))",
                OPTIMIZED_AND_VALIDATED,
                anyTree(
                        filter("OUTER_FILTER",
                                apply(ImmutableList.of("C", "O"),
                                        ImmutableMap.of("OUTER_FILTER", expression("THREE IN (C)")),
                                        project(ImmutableMap.of("THREE", expression("BIGINT '3'")),
                                                tableScan("orders", ImmutableMap.of(
                                                        "O", "orderkey",
                                                        "C", "custkey"))),
                                        project(
                                                any(
                                                        any(
                                                                tableScan("lineitem", ImmutableMap.of("L", "orderkey")))))))),
                MorePredicates.<PlanOptimizer>isInstanceOfAny(AddLocalExchanges.class).negate());
    }

    @Test
    public void testCorrelatedScalarAggregationRewriteToLeftOuterJoin()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE EXISTS(SELECT 1 WHERE orderkey = 3)", // EXISTS maps to count(*) > 0
                anyTree(
                        filter("FINAL_COUNT > BIGINT '0'",
                                aggregation(ImmutableMap.of("FINAL_COUNT", functionCall("count", ImmutableList.of("NON_NULL"))),
                                        join(LEFT, ImmutableList.of(), Optional.of("BIGINT '3' = ORDERKEY"),
                                                any(
                                                        tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey"))),
                                                project(ImmutableMap.of("NON_NULL", expression("true")),
                                                        node(ValuesNode.class)))))));
    }

    @Test
    public void testRemovesTrivialFilters()
    {
        assertPlan(
                "SELECT * FROM nation WHERE 1 = 1",
                output(
                        tableScan("nation")));
        assertPlan(
                "SELECT * FROM nation WHERE 1 = 0",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
    }

    @Test
    public void testPruneCountAggregationOverScalar()
    {
        assertPlan(
                "SELECT count(*) FROM (SELECT sum(orderkey) FROM orders)",
                output(
                        values(ImmutableList.of("_col0"), ImmutableList.of(ImmutableList.of(new LongLiteral("1"))))));
        assertPlan(
                "SELECT count(s) FROM (SELECT sum(orderkey) AS s FROM orders)",
                anyTree(
                        tableScan("orders")));
        assertPlan(
                "SELECT count(*) FROM (SELECT sum(orderkey) FROM orders GROUP BY custkey)",
                anyTree(
                        tableScan("orders")));
    }

    @Test
    public void testPickTableLayoutWithFilter()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE orderkey=5",
                output(
                        project(
                                ImmutableMap.of("expr_2", expression("5")),
                                filter("orderkey = BIGINT '5'",
                                        constrainedTableScanWithTableLayout(
                                                "orders",
                                                ImmutableMap.of(),
                                                ImmutableMap.of("orderkey", "orderkey"))))));
        assertPlan(
                "SELECT orderkey FROM orders WHERE orderstatus='F'",
                output(
                        constrainedTableScanWithTableLayout(
                                "orders",
                                ImmutableMap.of("orderstatus", singleValue(createVarcharType(1), utf8Slice("F"))),
                                ImmutableMap.of("orderkey", "orderkey"))));
    }

    @Test
    public void testBroadcastCorrelatedSubqueryAvoidsRemoteExchangeBeforeAggregation()
    {
        Session broadcastJoin = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.toString(false))
                .build();

        // make sure there is a remote exchange on the build side
        PlanMatchPattern joinBuildSideWithRemoteExchange =
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        node(TableScanNode.class)),
                                anyTree(
                                        exchange(REMOTE_STREAMING, ExchangeNode.Type.REPLICATE,
                                                anyTree(
                                                        node(TableScanNode.class))))));

        // validates that there exists only one remote exchange
        Consumer<Plan> validateSingleRemoteExchange = plan -> assertEquals(
                countOfMatchingNodes(
                        plan,
                        node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isRemote()),
                1);

        Consumer<Plan> validateSingleStreamingAggregation = plan -> assertEquals(
                countOfMatchingNodes(
                        plan,
                        node -> node instanceof AggregationNode
                                && ((AggregationNode) node).getGroupingKeys().contains(new VariableReferenceExpression(Optional.empty(), "unique", BIGINT))
                                && ((AggregationNode) node).isStreamable()),
                1);

        // region is unpartitioned, AssignUniqueId should provide satisfying partitioning for count(*) after LEFT JOIN
        assertPlanWithSession(
                "SELECT (SELECT COUNT(*) FROM region r2 WHERE r2.regionkey > r1.regionkey) FROM region r1",
                broadcastJoin,
                false,
                joinBuildSideWithRemoteExchange,
                validateSingleRemoteExchange.andThen(validateSingleStreamingAggregation));

        // orders is naturally partitioned, AssignUniqueId should not overwrite its natural partitioning
        assertPlanWithSession(
                "SELECT COUNT(COUNT) " +
                        "FROM (SELECT o1.orderkey orderkey, (SELECT COUNT(*) FROM orders o2 WHERE o2.orderkey > o1.orderkey) COUNT FROM orders o1) " +
                        "GROUP BY orderkey",
                broadcastJoin,
                false,
                joinBuildSideWithRemoteExchange,
                validateSingleRemoteExchange.andThen(validateSingleStreamingAggregation));
    }

    @Test
    public void testUsesDistributedJoinIfNaturallyPartitionedOnProbeSymbols()
    {
        Session broadcastJoin = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.toString())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.toString(false))
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, Boolean.toString(false))
                .build();

        Session disableRemoveCrossJoin = Session.builder(broadcastJoin)
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "false")
                .build();

        Session enableRemoveCrossJoin = Session.builder(broadcastJoin)
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .build();

        // replicated join with naturally partitioned and distributed probe side is rewritten to partitioned join
        assertPlanWithSession(
                "SELECT r1.regionkey FROM (SELECT regionkey FROM region GROUP BY regionkey) r1, region r2 WHERE r2.regionkey = r1.regionkey",
                broadcastJoin,
                false,
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_REGIONKEY", "RIGHT_REGIONKEY")), Optional.empty(), Optional.of(PARTITIONED),
                                // the only remote exchange in probe side should be below aggregation
                                aggregation(ImmutableMap.of(),
                                        anyTree(
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                tableScan("region", ImmutableMap.of("LEFT_REGIONKEY", "regionkey")))))),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                tableScan("region", ImmutableMap.of("RIGHT_REGIONKEY", "regionkey")))))),
                plan -> // make sure there are only two remote exchanges (one in probe and one in build side)
                        assertEquals(
                                countOfMatchingNodes(
                                        plan,
                                        node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isRemote()),
                                2));

        // replicated join is preserved if probe side is single node
        assertPlanWithSession(
                "SELECT * FROM (SELECT * FROM (VALUES 1) t(a)) t, region r WHERE r.regionkey = t.a",
                disableRemoveCrossJoin,
                false,
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        node(ValuesNode.class)),
                                anyTree(
                                        exchange(REMOTE_STREAMING, GATHER,
                                                node(TableScanNode.class))))));

        assertPlanWithSession(
                "SELECT * FROM (SELECT * FROM (VALUES 1) t(a)) t, region r WHERE r.regionkey = t.a",
                enableRemoveCrossJoin,
                false,
                anyTree(node(TableScanNode.class)));

        // replicated join is preserved if there are no equality criteria
        assertPlanWithSession(
                "SELECT * FROM (SELECT regionkey FROM region GROUP BY regionkey) r1, region r2 WHERE r2.regionkey > r1.regionkey",
                broadcastJoin,
                false,
                anyTree(
                        join(INNER, ImmutableList.of(), Optional.empty(), Optional.of(REPLICATED),
                                anyTree(
                                        node(TableScanNode.class)),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPLICATE,
                                                node(TableScanNode.class))))));
    }

    @Test
    public void testDistributedSort()
    {
        ImmutableList<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("ORDERKEY", DESCENDING, LAST));
        assertDistributedPlan(
                "SELECT orderkey FROM orders ORDER BY orderkey DESC",
                output(
                        exchange(REMOTE_STREAMING, GATHER, orderBy,
                                exchange(LOCAL, GATHER, orderBy,
                                        sort(orderBy,
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY", "orderkey"))))))));

        assertDistributedPlan(
                "SELECT orderkey FROM orders ORDER BY orderkey DESC",
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(DISTRIBUTED_SORT, Boolean.toString(false))
                        .build(),
                output(
                        sort(orderBy,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, GATHER,
                                                tableScan("orders", ImmutableMap.of(
                                                        "ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testEqualityInference()
    {
        assertPlan("" +
                        "SELECT l.comment, p.partkey " +
                        "FROM lineitem l " +
                        "JOIN partsupp p " +
                        "ON l.suppkey = p.suppkey " +
                        "AND l.partkey = p.partkey " +
                        "WHERE l.partkey = 42",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("l_suppkey", "p_suppkey")),
                                anyTree(
                                        filter(
                                                "l_partkey = 42",
                                                tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey", "l_suppkey", "suppkey", "l_comment", "comment")))),
                                anyTree(
                                        filter(
                                                "p_partkey = 42",
                                                tableScan("partsupp", ImmutableMap.of("p_partkey", "partkey", "p_suppkey", "suppkey")))))));
        assertPlan("" +
                        "SELECT l.comment, p.partkey " +
                        "FROM lineitem l " +
                        "JOIN partsupp p " +
                        "ON l.suppkey = p.suppkey " +
                        "AND l.comment = p.comment " +
                        "WHERE l.comment = '42'",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("p_suppkey", "l_suppkey")),
                                anyTree(
                                        filter(
                                                "p_comment = '42' ",
                                                tableScan("partsupp", ImmutableMap.of("p_suppkey", "suppkey", "p_partkey", "partkey", "p_comment", "comment")))),
                                anyTree(
                                        filter(
                                                "l_comment = '42'",
                                                tableScan("lineitem", ImmutableMap.of("l_suppkey", "suppkey", "l_comment", "comment")))))));
    }

    @Test
    public void testSimplifyJoinWithEmptyInput()
    {
        Session applyEmptyJoinOptimization = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, Boolean.toString(true))
                .build();

        Session disableEmptyJoinOptimization = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, Boolean.toString(false))
                .build();

        // Right child empty.
        assertPlanWithSession(
                "SELECT orderkey FROM orders join (select custkey from orders where 1=0) on 1=1",
                applyEmptyJoinOptimization, true,
                output(
                        values("orderkey_0")));

        // Left child empty with empty predicate
        assertPlanWithSession(
                "SELECT orderkey FROM (select custkey from orders where 1=0) join orders on 1=1",
                applyEmptyJoinOptimization, true,
                output(
                        values("orderkey_0")));

        // Three way join with empty middle child.
        assertPlanWithSession(
                "SELECT O1.orderkey FROM orders O1 join (select custkey C from orders where 1=0) ON O1.custkey = C join orders on 1=1",
                applyEmptyJoinOptimization, true,
                output(
                        values("orderkey_0")));

        // Three way join with empty middle child and aggregate children.
        assertPlanWithSession(
                "SELECT O1 FROM (select orderkey O1 from orders group by orderkey) join (select custkey C from orders where 1=0) ON O1 = C join orders on 1=1",
                applyEmptyJoinOptimization, true,
                output(
                        values("orderkey_0")));

        // Three way join with empty right child.
        assertPlanWithSession(
                "SELECT O1.orderkey FROM orders O1 join orders O2 ON 1=1 join (select custkey C from orders where 1=0) ON O1.custkey = C",
                applyEmptyJoinOptimization, true,
                output(
                        values("orderkey_0")));

        // Limit query with empty left child.
        assertPlanWithSession(
                "WITH DT AS (SELECT orderkey FROM (select custkey from orders where 1=0) join orders on 1=1) SELECT * FROM DT LIMIT 2",
                applyEmptyJoinOptimization, true,
                output(values("orderkey_0")));

        // Left child empty with zero limit
        assertPlanWithSession(
                "SELECT orderkey FROM (select custkey from orders limit 0) join orders on 1=1",
                applyEmptyJoinOptimization, true,
                output(
                        values("orderkey_0")));

        // Left child empty with zero Sample
        assertPlanWithSession(
                "SELECT orderkey FROM (select custkey from orders TABLESAMPLE BERNOULLI (0)) join orders on 1=1",
                applyEmptyJoinOptimization, true,
                output(values("orderkey_0")));

        // Empty left child with left outer join
        assertPlanWithSession(
                "WITH DT AS (SELECT orderkey FROM (select custkey C from orders limit 0) left outer join orders on orderkey=C) SELECT * FROM DT LIMIT 2",
                applyEmptyJoinOptimization, true,
                output(values("orderkey_0")));

        // 3 way join with empty non-null producing side for outer join
        assertPlanWithSession(
                "WITH DT AS (SELECT orderkey FROM (select custkey C from orders limit 0) left outer join orders on orderkey=C  " +
                        " left outer join customer C2 on C2.custkey = C) " +
                        " SELECT * FROM DT LIMIT 2",
                applyEmptyJoinOptimization, true,
                output(values("orderkey_0")));

        // Empty right child with right outer join
        assertPlanWithSession(
                "WITH DT AS (SELECT orderkey FROM orders right outer join (select custkey C from orders limit 0) on orderkey=C) SELECT * FROM DT LIMIT 2",
                applyEmptyJoinOptimization, true,
                output(values("orderkey_0")));

        // Empty right child with no projections and left outer join
        assertPlanWithSession(
                "WITH DT AS (SELECT orderkey FROM orders left outer join (select custkey C from orders limit 0) on orderkey=C) SELECT * FROM DT",
                applyEmptyJoinOptimization, true,
                output(node(TableScanNode.class)));

        // Empty left child with projections and right outer join
        assertPlanWithSession(
                "WITH DT AS (SELECT C, orderkey FROM (select custkey C from orders limit 0) right outer join orders on orderkey=C) SELECT * FROM DT",
                applyEmptyJoinOptimization, true,
                output(project(node(TableScanNode.class))));

        // Empty right child with projections and left outer join
        assertPlanWithSession(
                "WITH DT AS (SELECT orderkey, C FROM orders left outer join (select custkey C from orders limit 0) on orderkey=C) SELECT * FROM DT",
                applyEmptyJoinOptimization, true,
                output(project(node(TableScanNode.class))));

        // Empty right child with projections and full outer join
        assertPlanWithSession(
                "WITH DT AS (SELECT orderkey, C FROM orders full outer join (select custkey C from orders limit 0) on orderkey=C) SELECT * FROM DT",
                applyEmptyJoinOptimization, true,
                output(project(node(TableScanNode.class))));

        // Both Left and Right child empty and full outer join.
        assertPlanWithSession(
                "SELECt orderkey,custkey FROM (SELECT orderkey FROM orders where 1=0) full outer join (select custkey from orders where 1=0) on orderkey=custkey",
                applyEmptyJoinOptimization, true,
                output(
                        values("orderkey_0", "custkey_0")));

        // Negative tests. Both children are not empty
        assertPlanWithSession(
                "SELECT orderkey FROM (select custkey as C from orders where 1>0) join orders on orderkey=C",
                applyEmptyJoinOptimization, true,
                output(
                        node(JoinNode.class,
                                anyTree(node(TableScanNode.class)),
                                anyTree(node(TableScanNode.class)))));
        assertPlanWithSession(
                "SELECT orderkey FROM (select custkey as C from orders TABLESAMPLE BERNOULLI (1)) join orders on orderkey=C",
                applyEmptyJoinOptimization, true,
                output(
                        node(JoinNode.class,
                                anyTree(node(TableScanNode.class)),
                                anyTree(node(TableScanNode.class)))));

        // Negative test with optimization off
        assertPlanWithSession(
                "SELECT C, orderkey FROM (select orderkey as C from orders where 1=0) join orders on 1=1",
                disableEmptyJoinOptimization, true,
                output(node(JoinNode.class, values("orders"), anyTree(tableScan("orders")))));
    }

    @Test
    public void testLimitZero()
    {
        assertPlan(
                "SELECT orderkey FROM orders LIMIT 0",
                output(
                        values("orderkey_0")));

        assertPlan(
                "SELECT orderkey FROM orders ORDER BY orderkey ASC LIMIT 0",
                output(
                        values("orderkey_0")));

        assertPlan(
                "SELECT orderkey FROM orders GROUP BY 1 ORDER BY 1 DESC LIMIT 0",
                output(
                        values("orderkey_0")));

        assertPlan(
                "SELECT DISTINCT orderkey FROM orders LIMIT 0",
                output(
                        values("orderkey_0")));

        assertPlan(
                "SELECT * FROM (SELECT regionkey FROM region GROUP BY regionkey) r1, region r2 WHERE r2.regionkey > r1.regionkey LIMIT 0",
                output(
                        values("expr_8", "expr_9", "expr_10", "expr_11")));
    }

    @Test
    public void testInvalidLimit()
    {
        try {
            assertPlan(
                    "SELECT orderkey FROM orders LIMIT 10000000000000000000000",
                    output(
                            values("NOOP")));
            fail("PrestoException not thrown for invalid limit");
        }
        catch (Exception e) {
            assertTrue(e instanceof PrestoException, format("Expected PrestoException but found %s", e));
            PrestoException prestoException = (PrestoException) e;
            assertEquals(prestoException.getErrorCode(), INVALID_LIMIT_CLAUSE.toErrorCode());
            assertEquals(prestoException.getMessage(), "Invalid limit: 10000000000000000000000");
        }
    }

    @Test
    public void testTopN()
    {
        ImmutableList<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("ORDERKEY", DESCENDING, LAST));
        assertDistributedPlan(
                "SELECT orderkey FROM orders ORDER BY orderkey DESC LIMIT 1",
                output(
                        topN(1, orderBy,
                                anyTree(
                                        topN(1, orderBy,
                                                tableScan("orders", ImmutableMap.of(
                                                        "ORDERKEY", "orderkey")))))));

        assertDistributedPlan(
                "SELECT orderkey FROM orders GROUP BY 1 ORDER BY 1 DESC LIMIT 1",
                output(
                        topN(1, orderBy,
                                anyTree(
                                        topN(1, orderBy,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testComplexOrderBy()
    {
        assertDistributedPlan("SELECT COUNT(*) " +
                        "FROM (values ARRAY['a', 'b']) as t(col1) " +
                        "ORDER BY " +
                        "  IF( " +
                        "    SUM(REDUCE(col1, ROW(0),(l, r) -> l, x -> 1)) > 0, " +
                        "    COUNT(*), " +
                        "    SUM(REDUCE(col1, ROW(0),(l, r) -> l, x -> 1)) " +
                        "  )",
                output(aggregation(ImmutableMap.of(),
                        values())));

        Session session = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, Boolean.toString(false))
                .build();
        assertDistributedPlan("SELECT COUNT(*) " +
                        "FROM (values ARRAY['a', 'b']) as t(col1) " +
                        "ORDER BY " +
                        "  IF( " +
                        "    SUM(REDUCE(col1, ROW(0),(l, r) -> l, x -> 1)) > 0, " +
                        "    COUNT(*), " +
                        "    SUM(REDUCE(col1, ROW(0),(l, r) -> l, x -> 1)) " +
                        "  )",
                session,
                output(
                        project(
                                exchange(
                                        exchange(
                                                sort(
                                                        exchange(
                                                                project(
                                                                        aggregation(ImmutableMap.of(),
                                                                                project(values("col1")))))))))));
    }

    @Test
    public void testSizeBasedJoin()
    {
        // both local.sf100000.nation and local.sf100000.orders don't provide stats, therefore no reordering happens
        assertDistributedPlan("SELECT custkey FROM local.\"sf42.5\".nation, local.\"sf42.5\".orders WHERE nation.nationkey = orders.custkey",
                output(
                        anyTree(
                                join(INNER, ImmutableList.of(equiJoinClause("NATIONKEY", "CUSTKEY")),
                                        anyTree(tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey"))),
                                        anyTree(tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey")))))));

        // values node provides stats
        assertDistributedPlan("SELECT custkey FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT)) t(a), local.\"sf42.5\".orders WHERE t.a = orders.custkey",
                output(
                        anyTree(
                                join(INNER, ImmutableList.of(equiJoinClause("CUSTKEY", "T_A")), Optional.empty(), Optional.of(REPLICATED),
                                        anyTree(tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey"))),
                                        anyTree(values("T_A"))))));
    }

    @Test
    public void testSizeBasedSemiJoin()
    {
        // both local.sf100000.nation and local.sf100000.orders don't provide stats, therefore no reordering happens
        assertDistributedPlan("SELECT custkey FROM local.\"sf42.5\".orders WHERE orders.custkey NOT IN (SELECT nationkey FROM local.\"sf42.5\".nation)",
                output(
                        anyTree(
                                semiJoin("CUSTKEY", "NATIONKEY", "OUT", Optional.of(SemiJoinNode.DistributionType.PARTITIONED),
                                        anyTree(tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey"))),
                                        anyTree(tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")))))));

        // values node provides stats
        assertDistributedPlan("SELECT custkey FROM local.\"sf42.5\".orders WHERE orders.custkey NOT IN (SELECT t.a FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT)) t(a))",
                output(
                        anyTree(
                                semiJoin("CUSTKEY", "T_A", "OUT", Optional.of(SemiJoinNode.DistributionType.REPLICATED),
                                        anyTree(tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey"))),
                                        anyTree(values("T_A"))))));
    }

    @Test
    public void testEnforceFixedDistributionForOutputOperator()
    {
        Session session = Session.builder(this.getQueryRunner().getDefaultSession())
                // enable concurrency (default is 1)
                .setSystemProperty(TASK_CONCURRENCY, "2")
                .setSystemProperty(ENFORCE_FIXED_DISTRIBUTION_FOR_OUTPUT_OPERATOR, "true")
                .build();

        // simple group by
        assertDistributedPlan(
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus",
                session,
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", functionCall("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, REPARTITION,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                exchange(LOCAL, REPARTITION,
                                                        aggregation(
                                                                ImmutableMap.of("partial_sum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                                PARTIAL,
                                                                project(tableScan("orders", ImmutableMap.of("totalprice", "totalprice"))))))))));

        assertDistributedPlan(
                "SELECT orderstatus FROM (SELECT orderstatus, row_number() OVER (PARTITION BY orderstatus ORDER BY custkey) n FROM orders) WHERE n = 1",
                session,
                anyTree(
                        topNRowNumber(topNRowNumber -> topNRowNumber
                                        .specification(
                                                ImmutableList.of("orderstatus"),
                                                ImmutableList.of("custkey"),
                                                ImmutableMap.of("custkey", ASC_NULLS_LAST))
                                        .partial(false),
                                exchange(LOCAL, REPARTITION,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                exchange(LOCAL, REPARTITION,
                                                        topNRowNumber(topNRowNumber -> topNRowNumber
                                                                        .specification(
                                                                                ImmutableList.of("orderstatus"),
                                                                                ImmutableList.of("custkey"),
                                                                                ImmutableMap.of("custkey", ASC_NULLS_LAST))
                                                                        .partial(true),

                                                                project(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "custkey", "custkey"))))))))));
    }

    @Test
    public void testOffset()
    {
        Session enableOffset = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .build();
        assertPlanWithSession("SELECT name FROM nation OFFSET 2 ROWS",
                enableOffset,
                true,
                any(
                        strictProject(
                                ImmutableMap.of("name", new ExpressionMatcher("name")),
                                filter(
                                        "(row_num > BIGINT '2')",
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                any(
                                                        tableScan("nation", ImmutableMap.of("NAME", "name"))))
                                                .withAlias("row_num", new RowNumberSymbolMatcher())))));
        assertPlanWithSession("SELECT name FROM nation ORDER BY regionkey OFFSET 2 ROWS",
                enableOffset,
                true,
                any(
                        strictProject(
                                ImmutableMap.of("name", new ExpressionMatcher("name")),
                                filter(
                                        "row_num > BIGINT '2'",
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                anyTree(
                                                        sort(
                                                                ImmutableList.of(sort("regionkey", ASCENDING, LAST)),
                                                                any(
                                                                        tableScan("nation", ImmutableMap.of("name", "name", "regionkey", "regionkey"))))))
                                                .withAlias("row_num", new RowNumberSymbolMatcher())))));
    }

    @Test
    public void testOffsetWithLimit()
    {
        Session enableOffsetWithConcurrency = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .setSystemProperty("task_concurrency", "2") // task_concurrency > 1 required to add possible local exchanges that fail this test for incorrect AddLocalExchanges
                .build();

        assertPlanWithSession("SELECT totalprice FROM orders ORDER BY totalprice OFFSET 1 LIMIT 512",
                enableOffsetWithConcurrency,
                false,
                any(
                        strictProject(
                                ImmutableMap.of("totalprice", new ExpressionMatcher("totalprice")),
                                limit(
                                        512,
                                        filter(
                                                "row_num > BIGINT '1'",
                                                rowNumber(
                                                        pattern -> pattern
                                                                .partitionBy(ImmutableList.of()),
                                                        anyTree(
                                                                sort(
                                                                        ImmutableList.of(sort("totalprice", ASCENDING, LAST)),
                                                                        any(
                                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice"))))))
                                                        .withAlias("row_num", new RowNumberSymbolMatcher()))))));
    }

    @Test
    public void testRewriteExcludeColumnsFunctionToProjection()
    {
        assertPlan("SELECT *\n" +
                        "FROM TABLE(system.builtin.exclude_columns(\n" +
                        "    INPUT => TABLE(orders),\n" +
                        "    COLUMNS => DESCRIPTOR(comment)))\n",
                output(tableScan("orders")));
    }

    private Session noJoinReordering()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .build();
    }

    @Test
    public void testRedundantLimitNodeRemoval()
    {
        String query = "SELECT count(*) FROM orders LIMIT 10";
        assertFalse(
                searchFrom(plan(query, OPTIMIZED).getRoot())
                        .where(LimitNode.class::isInstance)
                        .matches(),
                format("Unexpected limit node for query: '%s'", query));

        assertPlan(
                "SELECT orderkey, count(*) FROM orders GROUP BY orderkey LIMIT 10",
                output(
                        limit(10,
                                anyTree(
                                        tableScan("orders")))));

        assertPlan(
                "SELECT * FROM (VALUES 1,2,3,4,5,6) AS t1 LIMIT 10",
                output(
                        values(ImmutableList.of("x"))));
    }

    @Test
    public void testRemoveSingleRowSort()
    {
        Session exploitConstraints = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, Boolean.toString(true))
                .build();

        String query = "SELECT count(*) FROM orders ORDER BY 1";
        assertFalse(
                searchFrom(plan(query, OPTIMIZED, exploitConstraints).getRoot())
                        .where(isInstanceOfAny(SortNode.class))
                        .matches(),
                format("Unexpected sort node for query: '%s'", query));

        assertPlan(
                "SELECT orderkey, count(*) FROM orders GROUP BY orderkey ORDER BY 1",
                anyTree(
                        node(SortNode.class,
                                anyTree(
                                        tableScan("orders")))));
    }

    @Test
    public void testRedundantTopNNodeRemoval()
    {
        Session exploitConstraints = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, Boolean.toString(true))
                .build();

        String query = "SELECT count(*) FROM orders ORDER BY 1 LIMIT 10";
        assertFalse(
                searchFrom(plan(query, OPTIMIZED, exploitConstraints).getRoot())
                        .where(isInstanceOfAny(TopNNode.class, SortNode.class))
                        .matches(),
                format("Unexpected TopN node for query: '%s'", query));

        assertPlan(
                "SELECT orderkey, count(*) FROM orders GROUP BY orderkey ORDER BY 1 LIMIT 10",
                output(
                        node(TopNNode.class,
                                anyTree(
                                        tableScan("orders")))));

        assertPlan(
                "SELECT orderkey, count(*) FROM orders GROUP BY orderkey ORDER BY 1 LIMIT 0",
                output(
                        node(ValuesNode.class)));
    }

    @Test
    public void testRedundantDistinctLimitNodeRemoval()
    {
        Session exploitConstraints = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, Boolean.toString(true))
                .build();

        String query = "SELECT distinct(c) FROM (SELECT count(*) as c FROM orders) LIMIT 10";
        assertFalse(
                searchFrom(plan(query, OPTIMIZED, exploitConstraints).getRoot())
                        .where(isInstanceOfAny(DistinctLimitNode.class))
                        .matches(),
                format("Unexpected DistinctLimit node for query: '%s'", query));

        assertPlan(
                "SELECT distinct(c) FROM (SELECT count(*) as c FROM orders GROUP BY orderkey) LIMIT 10",
                output(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        tableScan("orders")))));
    }

    @Test
    public void testRedundantDistinctRemovalWithMultipleGroupingSets()
    {
        Session exploitConstraints = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, Boolean.toString(true))
                .build();

        assertPlan("SELECT orderkey FROM (SELECT * FROM (SELECT * FROM orders WHERE 1=0)) GROUP BY ROLLUP (orderkey)",
                exploitConstraints,
                anyTree(
                        node(AggregationNode.class,
                                node(ProjectNode.class,
                                        node(ValuesNode.class)))));

        assertPlan("with t as (select orderkey, count(1) cnt from (select * from (select * from orders where 1=0) left join (select partkey, suppkey from lineitem where 1=0) on partkey=10 where suppkey is not null) group by rollup(orderkey)) select t1.orderkey, t1.cnt from t t1 cross join t t2",
                exploitConstraints,
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        node(AggregationNode.class,
                                                node(ProjectNode.class,
                                                        node(ValuesNode.class)))),
                                anyTree(node(AggregationNode.class,
                                        node(ProjectNode.class,
                                                node(ValuesNode.class)))))));
    }

    @Test
    public void testLeafNodeInPlanExceedException()
    {
        Session enableLeafNodeInPlanExceedException = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_LEAF_NODES_IN_PLAN, Integer.toString(10))
                .setSystemProperty(LEAF_NODE_LIMIT_ENABLED, Boolean.toString(true))
                .build();

        String expectedMessageRegExp = format("Number of leaf nodes in logical plan exceeds threshold %s set in max_leaf_nodes_in_plan",
                getMaxLeafNodesInPlan(enableLeafNodeInPlanExceedException));

        String joinQuery = "WITH t1 AS " +
                "(" +
                "    SELECT * FROM ( VALUES (1, 'a'), (2, 'b'), (3, 'c') ) AS t (id, name) " +
                "), " +
                "t2 AS ( " +
                "    SELECT A.id, B.name FROM t1 A Join t1 B On A.id = B.id " +
                "), " +
                "t3 AS ( " +
                "    SELECT A.id, B.name FROM t2 A Join t2 B On A.id = B.id " +
                "), " +
                "t4 AS ( " +
                "    SELECT A.id, B.name FROM t3 A Join t3 B On A.id = B.id " +
                "), " +
                "t5 AS ( " +
                "    SELECT A.id, B.name FROM t4 A Join t4 B On A.id = B.id " +
                ") " +
                "SELECT * FROM t5";

        assertPlanFailedWithException(joinQuery, enableLeafNodeInPlanExceedException, expectedMessageRegExp);

        enableLeafNodeInPlanExceedException = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(MAX_LEAF_NODES_IN_PLAN, Integer.toString(100))
                .setSystemProperty(LEAF_NODE_LIMIT_ENABLED, Boolean.toString(true))
                .build();

        String joinQuery2 = "WITH t1 AS " +
                "(" +
                "    SELECT * FROM ( VALUES (1, 'a'), (2, 'b'), (3, 'c') ) AS t (id, name) " +
                "), " +
                "t2 AS ( " +
                "    SELECT A.id, B.name FROM t1 A Join t1 B On A.id = B.id " +
                "), " +
                "t3 AS ( " +
                "    SELECT A.id, B.name FROM t2 A Join t2 B On A.id = B.id " +
                "), " +
                "t4 AS ( " +
                "    SELECT A.id, B.name FROM t3 A Join t3 B On A.id = B.id " +
                "), " +
                "t5 AS ( " +
                "    SELECT A.id, B.name FROM t4 A Join t4 B On A.id = B.id " +
                ") " +
                "SELECT * FROM t5";

        assertPlanSucceeded(joinQuery2, enableLeafNodeInPlanExceedException);
    }

    @Test
    public void testJsonBasedFunctions()
    {
        this.getQueryRunner().getFunctionAndTypeManager().createFunction(SQUARE_UDF_CPP, false);

        assertPlan(
                "SELECT json.test_schema.square(orderkey) from orders",
                any(
                        project(
                                ImmutableMap.of("out", expression("json.test_schema.square(orderkey)")),
                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey")))));
    }

    @Test
    public void testJsonBasedAggregateFunctions()
    {
        this.getQueryRunner().getFunctionAndTypeManager().createFunction(AVG_UDAF_CPP, false);

        assertDistributedPlan("SELECT orderstatus, json.test_schema.avg(totalprice) FROM orders GROUP BY orderstatus",
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_result", functionCall("avg", ImmutableList.of("partial_result"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_result", functionCall("avg", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        anyTree(tableScan("orders", ImmutableMap.of("totalprice", "totalprice")))))))));
    }

    // Run this method exactly once.
    private void setupJsonFunctionNamespaceManager(QueryRunner queryRunner)
    {
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        queryRunner.loadFunctionNamespaceManager(
                JsonFileBasedFunctionNamespaceManagerFactory.NAME,
                "json",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP",
                        "json-based-function-manager.path-to-function-definition", ""));
    }

    @Test
    public void testDuplicateUnnestItem()
    {
        assertPlanSucceeded("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(array[2, 3], ARRAY[2, 3]) AS r(r1, r2)", this.getQueryRunner().getDefaultSession());
    }

    @Test
    public void testCorrelatedSubqueriesWithOuterJoins()
    {
        // Subquery pushed to partsupp and therefore final join order is (part LOJ (partsupp JOIN supplier))
        assertPlan("SELECT COUNT(*) FROM part p LEFT JOIN partsupp ps ON p.partkey=ps.partkey AND EXISTS (SELECT 1 FROM supplier s WHERE s.suppkey=ps.suppkey)",
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("partkey", "partkey_0")),
                                anyTree(
                                        tableScan("part", ImmutableMap.of("partkey", "partkey"))),
                                anyTree(
                                        join(INNER,
                                                ImmutableList.of(equiJoinClause("suppkey", "suppkey_4")),
                                                anyTree(
                                                        tableScan("partsupp", ImmutableMap.of("partkey_0", "partkey", "suppkey", "suppkey"))),
                                                anyTree(
                                                        tableScan("supplier", ImmutableMap.of("suppkey_4", "suppkey"))))))));

        // Same query with a right join fails with legacy error
        String expectedErrorMsg = "Unexpected UnresolvedSymbolExpression in logical plan: ";
        assertPlanFailedWithException("SELECT COUNT(*) FROM part p RIGHT JOIN partsupp ps ON p.partkey=ps.partkey AND EXISTS (SELECT 1 FROM supplier s WHERE s.suppkey=ps.suppkey)",
                this.getQueryRunner().getDefaultSession(),
                expectedErrorMsg + "ps.suppkey");

        // Subquery pushed to part and therefore final join order is ((part JOIN supplier) ROJ partsupp)
        assertPlan("SELECT COUNT(*) FROM part p RIGHT JOIN partsupp ps ON p.partkey=ps.partkey AND EXISTS (SELECT 1 FROM supplier s WHERE s.suppkey=p.partkey)",
                anyTree(
                        join(RIGHT,
                                ImmutableList.of(equiJoinClause("partkey", "partkey_0")),
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("partkey", "suppkey_4")),
                                        anyTree(
                                                tableScan("part", ImmutableMap.of("partkey", "partkey"))),
                                        anyTree(
                                                tableScan("supplier", ImmutableMap.of("suppkey_4", "suppkey")))),
                                anyTree(
                                        tableScan("partsupp", ImmutableMap.of("partkey_0", "partkey"))))));

        // subquery gets pushed to join of partsupp, supplier
        assertPlan("SELECT COUNT(*) FROM part p LEFT JOIN (SELECT * FROM partsupp ps, supplier s WHERE ps.suppkey=s.suppkey) sq ON sq.partkey=p.partkey AND NOT EXISTS (SELECT 1 FROM lineitem l WHERE sq.supplycost+sq.acctbal = l.extendedprice)",
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("partkey", "partkey_0")),
                                anyTree(
                                        tableScan("part", ImmutableMap.of("partkey", "partkey"))),
                                anyTree(
                                        join(LEFT,
                                                ImmutableList.of(equiJoinClause("add", "extendedprice")),
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("add", new ExpressionMatcher("supplycost + acctbal")),
                                                                join(INNER,
                                                                        ImmutableList.of(equiJoinClause("suppkey", "suppkey_3")),
                                                                        anyTree(
                                                                                tableScan("partsupp", ImmutableMap.of("suppkey", "suppkey", "supplycost", "supplycost", "partkey_0", "partkey"))),
                                                                        anyTree(
                                                                                tableScan("supplier", ImmutableMap.of("suppkey_3", "suppkey", "acctbal", "acctbal")))))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("extendedprice", "extendedprice"))))))));

        // Same query with a right join fails with legacy error
        assertPlanFailedWithException("SELECT COUNT(*) FROM part p RIGHT JOIN (SELECT * FROM partsupp ps, supplier s WHERE ps.suppkey=s.suppkey) sq ON sq.partkey=p.partkey AND NOT EXISTS (SELECT 1 FROM lineitem l WHERE sq.supplycost+sq.acctbal = l.extendedprice)",
                this.getQueryRunner().getDefaultSession(),
                expectedErrorMsg + "sq.supplycost");

        // Ensure subquery and filter get pushed to partsupp
        assertPlan("SELECT COUNT(*) FROM part p LEFT JOIN partsupp ps ON p.partkey=ps.partkey AND EXISTS (SELECT 1 FROM supplier s WHERE s.suppkey=ps.suppkey) AND ps.availqty<1000",
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("partkey", "partkey_0")),
                                anyTree(
                                        tableScan("part", ImmutableMap.of("partkey", "partkey"))),
                                anyTree(
                                        anyNot(FilterNode.class,
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("suppkey", "suppkey_4")),
                                                        anyTree(
                                                                tableScan("partsupp", ImmutableMap.of("partkey_0", "partkey", "suppkey", "suppkey"))),
                                                        anyTree(
                                                                tableScan("supplier", ImmutableMap.of("suppkey_4", "suppkey")))))))));
    }

    @Test
    public void testSubselectQualifiedObjectNameContainsDot()
    {
        String query = "SELECT min((SELECT totalprice FROM orders WHERE orderstatus = \"Outer.Table\".\"orderstatus\")) as min FROM orders AS \"Outer.Table\"";
        assertPlanSucceeded(query, this.getQueryRunner().getDefaultSession());
    }
}
