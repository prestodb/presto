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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.MetadataQueryOptimizer;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchNodePartitioningProvider;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.facebook.presto.tpch.TpchTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_METADATA_QUERIES;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.distinctFunctionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestMetadataQueryOptimizer
{
    private RuleTester tester;
    private Rule metadataQueryOptimizer;
    private static final FunctionCall COUNT_DISTINCT_FUNCTION_CALL = new FunctionCall(
            QualifiedName.of("count"),
            true,
            ImmutableList.of(new SymbolReference("orderstatus")));
    private static final TpchColumnHandle ORDER_STATUS = new TpchColumnHandle("orderstatus", VARCHAR);

    @BeforeClass
    public void setUp()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty(OPTIMIZE_METADATA_QUERIES, "true")
                .build();
        LocalQueryRunner queryRunner = new LocalQueryRunner(session);
        queryRunner.createCatalog(session.getCatalog().get(),
                new TestingConnectorFactory(),
                ImmutableMap.of());

        tester = new RuleTester(queryRunner);
        metadataQueryOptimizer = new MetadataQueryOptimizer(tester.getMetadata());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester = null;
        metadataQueryOptimizer = null;
    }

    @Test
    public void testDoesNotFireWhenNoAggregationNode()
    {
        tester.assertThat(metadataQueryOptimizer)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenNotAnAllowedFunction()
    {
        FunctionCall countFunctionCall = new FunctionCall(QualifiedName.of("count"), false, ImmutableList.of(new SymbolReference("key")));
        tester.assertThat(metadataQueryOptimizer)
                .on(p -> p.aggregation(s -> s.source(
                        p.values(p.symbol("key", BIGINT)))
                        .addAggregation(p.symbol("COUNT", BIGINT), countFunctionCall, ImmutableList.of(BIGINT))
                        .addGroupingSet(p.symbol("key", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenProjectionIsNonDeterministic()
    {
        tester.assertThat(metadataQueryOptimizer)
                .on(p -> p.aggregation(s -> s.source(
                        p.project(
                                Assignments.of(
                                        p.symbol("non_deterministic", BOOLEAN), p.expression("random()")),
                                p.values(p.symbol("key", BIGINT))))
                        .addAggregation(p.symbol("COUNT", BIGINT), COUNT_DISTINCT_FUNCTION_CALL, ImmutableList.of(BIGINT))
                        .addGroupingSet(p.symbol("key", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenNoTableScan()
    {
        tester.assertThat(metadataQueryOptimizer)
                .on(p -> p.aggregation(s -> s.source(
                        p.limit(5, p.values(p.symbol("key", BIGINT))))
                        .addAggregation(p.symbol("COUNT", BIGINT), COUNT_DISTINCT_FUNCTION_CALL, ImmutableList.of(BIGINT))
                        .addGroupingSet(p.symbol("key", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenThereIsSort()
    {
        tester.assertThat(metadataQueryOptimizer)
                .on(p -> p.aggregation(s -> s.source(
                        p.filter(p.expression("key > 1"), p.tableScan(
                                ImmutableList.of(p.symbol("key", BIGINT)),
                                ImmutableMap.of(p.symbol("key", BIGINT), new TestingColumnHandle("key")))))
                        .addAggregation(p.symbol("COUNT", BIGINT), COUNT_DISTINCT_FUNCTION_CALL, ImmutableList.of(BIGINT))
                        .addGroupingSet(p.symbol("key", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testMetadataQueryOptimization()
    {
        ConnectorId connectorId = tester.getCurrentConnectorId();
        TpchTableHandle orders = new TpchTableHandle(connectorId.toString(), "orders", 1.0);
        TableHandle ordersTable = new TableHandle(connectorId, orders);
        TableLayoutHandle ordersTableLayout = new TableLayoutHandle(
                tester.getCurrentConnectorId(),
                TestingTransactionHandle.create(),
                new TpchTableLayoutHandle(orders, Optional.empty()));

        tester.assertThat(metadataQueryOptimizer)
                .on(p -> p.aggregation(s -> s.source(
                        p.tableScanWithTableLayout(
                                ImmutableList.of(p.symbol("orderstatus", VARCHAR)),
                                ImmutableMap.of(p.symbol("orderstatus", VARCHAR), ORDER_STATUS),
                                ordersTable,
                                ordersTableLayout))
                        .addAggregation(p.symbol("COUNT", BIGINT), COUNT_DISTINCT_FUNCTION_CALL, ImmutableList.of(BIGINT))
                        .addGroupingSet(p.symbol("orderstatus", VARCHAR))))
                .matches(aggregation(ImmutableMap.of("COUNT", distinctFunctionCall("count", ImmutableList.of("orderstatus"))),
                        values(ImmutableMap.of("orderstatus", 0))));
    }

    private class TestingConnectorFactory
            extends TpchConnectorFactory
    {
        private TestingConnectorFactory()
        {
            super(1);
        }

        @Override
        public Connector create(String connectorId, Map<String, String> properties, ConnectorContext context)
        {
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
                    return new TestingMetadata(connectorId);
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return new TpchSplitManager(nodeManager, 1);
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider()
                {
                    return new TpchRecordSetProvider();
                }

                @Override
                public ConnectorNodePartitioningProvider getNodePartitioningProvider()
                {
                    return new TpchNodePartitioningProvider(nodeManager, 1);
                }
            };
        }

        private class TestingMetadata
                extends TpchMetadata
        {
            private TestingMetadata(String connectorId)
            {
                super(connectorId);
            }

            @Override
            public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
            {
                return layoutWithDiscretePredicates((TpchTableLayoutHandle) handle);
            }

            private ConnectorTableLayout layoutWithDiscretePredicates(TpchTableLayoutHandle handle)
            {
                TupleDomain<ColumnHandle> predicate = withColumnDomains(ImmutableMap.of(
                        ORDER_STATUS,
                        Domain.singleValue(ORDER_STATUS.getType(), Slices.utf8Slice("value"))));

                ConnectorTableLayout layout = new ConnectorTableLayout(
                        new TpchTableLayoutHandle(handle.getTable(), Optional.empty()),
                        Optional.empty(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new DiscretePredicates(ImmutableList.of(ORDER_STATUS), ImmutableList.of(predicate))),
                        ImmutableList.of());

                return new ConnectorTableLayoutResult(layout, Constraint.<ColumnHandle>alwaysTrue().getSummary())
                        .getTableLayout();
            }
        }
    }
}
