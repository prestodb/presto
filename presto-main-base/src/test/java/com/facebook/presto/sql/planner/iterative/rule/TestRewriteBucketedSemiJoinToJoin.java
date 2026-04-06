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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchNodePartitioningProvider;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.REWRITE_BUCKETED_SEMI_JOIN_TO_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestRewriteBucketedSemiJoinToJoin
        extends BaseRuleTest
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("local");
    private static final double TINY_SCALE_FACTOR = 0.01;

    @Override
    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                new BucketedMockConnectorFactory());
    }

    private static TableHandle tpchTableHandle(String tableName)
    {
        return new TableHandle(
                CONNECTOR_ID,
                new TpchTableHandle(tableName, TINY_SCALE_FACTOR),
                TestingTransactionHandle.create(),
                Optional.empty());
    }

    @Test
    public void testRewriteBucketedSemiJoinToLeftJoin()
    {
        tester().assertThat(new RewriteBucketedSemiJoinToJoin(tester().getMetadata()))
                .setSystemProperty(REWRITE_BUCKETED_SEMI_JOIN_TO_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression sourceOrderkey = p.variable("sourceOrderkey", BIGINT);
                    VariableReferenceExpression filteringOrderkey = p.variable("filteringOrderkey", BIGINT);
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            sourceOrderkey,
                            filteringOrderkey,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.tableScan(tpchTableHandle("orders"),
                                    ImmutableList.of(sourceOrderkey),
                                    ImmutableMap.of(sourceOrderkey, new TpchColumnHandle("orderkey", BIGINT))),
                            p.tableScan(tpchTableHandle("lineitem"),
                                    ImmutableList.of(filteringOrderkey),
                                    ImmutableMap.of(filteringOrderkey, new TpchColumnHandle("orderkey", BIGINT))));
                })
                .matches(
                        project(
                                join(JoinType.LEFT,
                                        ImmutableList.of(equiJoinClause("sourceOrderkey", "filteringOrderkey")),
                                        tableScan("orders", ImmutableMap.of("sourceOrderkey", "orderkey")),
                                        aggregation(
                                                singleGroupingSet("filteringOrderkey"),
                                                ImmutableMap.of(),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                AggregationNode.Step.SINGLE,
                                                tableScan("lineitem", ImmutableMap.of("filteringOrderkey", "orderkey"))))));
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new RewriteBucketedSemiJoinToJoin(tester().getMetadata()))
                .setSystemProperty(REWRITE_BUCKETED_SEMI_JOIN_TO_JOIN, "false")
                .on(p -> {
                    VariableReferenceExpression sourceOrderkey = p.variable("sourceOrderkey", BIGINT);
                    VariableReferenceExpression filteringOrderkey = p.variable("filteringOrderkey", BIGINT);
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            sourceOrderkey,
                            filteringOrderkey,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.tableScan(tpchTableHandle("orders"),
                                    ImmutableList.of(sourceOrderkey),
                                    ImmutableMap.of(sourceOrderkey, new TpchColumnHandle("orderkey", BIGINT))),
                            p.tableScan(tpchTableHandle("lineitem"),
                                    ImmutableList.of(filteringOrderkey),
                                    ImmutableMap.of(filteringOrderkey, new TpchColumnHandle("orderkey", BIGINT))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenSourceNotBucketed()
    {
        // customer table is NOT bucketed in the mock
        tester().assertThat(new RewriteBucketedSemiJoinToJoin(tester().getMetadata()))
                .setSystemProperty(REWRITE_BUCKETED_SEMI_JOIN_TO_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression sourceCustkey = p.variable("sourceCustkey", BIGINT);
                    VariableReferenceExpression filteringOrderkey = p.variable("filteringOrderkey", BIGINT);
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            sourceCustkey,
                            filteringOrderkey,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.tableScan(tpchTableHandle("customer"),
                                    ImmutableList.of(sourceCustkey),
                                    ImmutableMap.of(sourceCustkey, new TpchColumnHandle("custkey", BIGINT))),
                            p.tableScan(tpchTableHandle("lineitem"),
                                    ImmutableList.of(filteringOrderkey),
                                    ImmutableMap.of(filteringOrderkey, new TpchColumnHandle("orderkey", BIGINT))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenFilteringSourceNotBucketed()
    {
        // customer table is NOT bucketed in the mock
        tester().assertThat(new RewriteBucketedSemiJoinToJoin(tester().getMetadata()))
                .setSystemProperty(REWRITE_BUCKETED_SEMI_JOIN_TO_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression sourceOrderkey = p.variable("sourceOrderkey", BIGINT);
                    VariableReferenceExpression filteringCustkey = p.variable("filteringCustkey", BIGINT);
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            sourceOrderkey,
                            filteringCustkey,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.tableScan(tpchTableHandle("orders"),
                                    ImmutableList.of(sourceOrderkey),
                                    ImmutableMap.of(sourceOrderkey, new TpchColumnHandle("orderkey", BIGINT))),
                            p.tableScan(tpchTableHandle("customer"),
                                    ImmutableList.of(filteringCustkey),
                                    ImmutableMap.of(filteringCustkey, new TpchColumnHandle("custkey", BIGINT))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenBucketedByDifferentKey()
    {
        // nation table is bucketed by regionkey, but join key is nationkey
        tester().assertThat(new RewriteBucketedSemiJoinToJoin(tester().getMetadata()))
                .setSystemProperty(REWRITE_BUCKETED_SEMI_JOIN_TO_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression sourceNationkey = p.variable("sourceNationkey", BIGINT);
                    VariableReferenceExpression filteringNationkey = p.variable("filteringNationkey", BIGINT);
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            sourceNationkey,
                            filteringNationkey,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.tableScan(tpchTableHandle("nation"),
                                    ImmutableList.of(sourceNationkey),
                                    ImmutableMap.of(sourceNationkey, new TpchColumnHandle("nationkey", BIGINT))),
                            p.tableScan(tpchTableHandle("nation"),
                                    ImmutableList.of(filteringNationkey),
                                    ImmutableMap.of(filteringNationkey, new TpchColumnHandle("nationkey", BIGINT))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonTableScanSource()
    {
        // source is ValuesNode, not a TableScan
        tester().assertThat(new RewriteBucketedSemiJoinToJoin(tester().getMetadata()))
                .setSystemProperty(REWRITE_BUCKETED_SEMI_JOIN_TO_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression sourceVar = p.variable("sourceVar", BIGINT);
                    VariableReferenceExpression filteringOrderkey = p.variable("filteringOrderkey", BIGINT);
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            sourceVar,
                            filteringOrderkey,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.values(sourceVar),
                            p.tableScan(tpchTableHandle("lineitem"),
                                    ImmutableList.of(filteringOrderkey),
                                    ImmutableMap.of(filteringOrderkey, new TpchColumnHandle("orderkey", BIGINT))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonTableScanFilteringSource()
    {
        // filtering source is ValuesNode, not a TableScan
        tester().assertThat(new RewriteBucketedSemiJoinToJoin(tester().getMetadata()))
                .setSystemProperty(REWRITE_BUCKETED_SEMI_JOIN_TO_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression sourceOrderkey = p.variable("sourceOrderkey", BIGINT);
                    VariableReferenceExpression filteringVar = p.variable("filteringVar", BIGINT);
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            sourceOrderkey,
                            filteringVar,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.tableScan(tpchTableHandle("orders"),
                                    ImmutableList.of(sourceOrderkey),
                                    ImmutableMap.of(sourceOrderkey, new TpchColumnHandle("orderkey", BIGINT))),
                            p.values(filteringVar));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresThroughFilter()
    {
        // Verifies the rule walks through FilterNode to find the underlying TableScanNode
        tester().assertThat(new RewriteBucketedSemiJoinToJoin(tester().getMetadata()))
                .setSystemProperty(REWRITE_BUCKETED_SEMI_JOIN_TO_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression sourceOrderkey = p.variable("sourceOrderkey", BIGINT);
                    VariableReferenceExpression filteringOrderkey = p.variable("filteringOrderkey", BIGINT);
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            sourceOrderkey,
                            filteringOrderkey,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.filter(
                                    TRUE_CONSTANT,
                                    p.tableScan(tpchTableHandle("orders"),
                                            ImmutableList.of(sourceOrderkey),
                                            ImmutableMap.of(sourceOrderkey, new TpchColumnHandle("orderkey", BIGINT)))),
                            p.filter(
                                    TRUE_CONSTANT,
                                    p.tableScan(tpchTableHandle("lineitem"),
                                            ImmutableList.of(filteringOrderkey),
                                            ImmutableMap.of(filteringOrderkey, new TpchColumnHandle("orderkey", BIGINT)))));
                })
                .matches(
                        project(
                                join(JoinType.LEFT,
                                        ImmutableList.of(equiJoinClause("sourceOrderkey", "filteringOrderkey")),
                                        filter(
                                                tableScan("orders", ImmutableMap.of("sourceOrderkey", "orderkey"))),
                                        aggregation(
                                                singleGroupingSet("filteringOrderkey"),
                                                ImmutableMap.of(),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                AggregationNode.Step.SINGLE,
                                                filter(
                                                        tableScan("lineitem", ImmutableMap.of("filteringOrderkey", "orderkey")))))));
    }

    /**
     * Mock connector factory that extends TpchConnectorFactory to provide
     * table metadata with bucketed_by properties for certain tables.
     */
    private static class BucketedMockConnectorFactory
            extends TpchConnectorFactory
    {
        BucketedMockConnectorFactory()
        {
            super(1);
        }

        @Override
        public Connector create(String catalogName, Map<String, String> properties, ConnectorContext context)
        {
            int splitsPerNode = getSplitsPerNode(properties);
            ColumnNaming columnNaming = ColumnNaming.valueOf(
                    properties.getOrDefault(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.SIMPLIFIED.name()).toUpperCase());
            NodeManager nodeManager = context.getNodeManager();

            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                {
                    return TpchTransactionHandle.INSTANCE;
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
                {
                    return new BucketedMockMetadata(catalogName, columnNaming, isPredicatePushdownEnabled(), isPartitioningEnabled(properties));
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return new TpchSplitManager(nodeManager, splitsPerNode);
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider()
                {
                    return new TpchRecordSetProvider();
                }

                @Override
                public ConnectorNodePartitioningProvider getNodePartitioningProvider()
                {
                    return new TpchNodePartitioningProvider(nodeManager, splitsPerNode);
                }
            };
        }
    }

    /**
     * Mock metadata that extends TpchMetadata and adds bucketed_by properties.
     * - orders: bucketed by [orderkey]
     * - lineitem: bucketed by [orderkey]
     * - nation: bucketed by [regionkey]
     * - customer: NOT bucketed (no bucketed_by property)
     */
    private static class BucketedMockMetadata
            extends TpchMetadata
    {
        private static final Map<String, List<String>> BUCKETED_TABLES = ImmutableMap.of(
                "orders", ImmutableList.of("orderkey"),
                "lineitem", ImmutableList.of("orderkey"),
                "nation", ImmutableList.of("regionkey"));

        BucketedMockMetadata(String connectorId, ColumnNaming columnNaming, boolean predicatePushdownEnabled, boolean partitioningEnabled)
        {
            super(connectorId, columnNaming, predicatePushdownEnabled, partitioningEnabled);
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            ConnectorTableMetadata base = super.getTableMetadata(session, tableHandle);
            String tableName = ((TpchTableHandle) tableHandle).getTableName();
            List<String> bucketColumns = BUCKETED_TABLES.getOrDefault(tableName, ImmutableList.of());
            if (bucketColumns.isEmpty()) {
                return base;
            }
            Map<String, Object> properties = new HashMap<>(base.getProperties());
            properties.put("bucketed_by", bucketColumns);
            return new ConnectorTableMetadata(base.getTable(), base.getColumns(), properties);
        }
    }
}
