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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.tpch.TpchTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_TOP_N_USING_ROW_ID;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_TOP_N_USING_ROW_ID_MIN_COLUMN_SAVINGS;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.topN;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.LAST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestOptimizeTopNUsingRowId
        extends BasePlanTest
{
    public TestOptimizeTopNUsingRowId()
    {
        super(TestOptimizeTopNUsingRowId::createQueryRunner);
    }

    private static LocalQueryRunner createQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1")
                .build();

        LocalQueryRunner queryRunner = new LocalQueryRunner(
                session,
                new FeaturesConfig(),
                new FunctionsConfig(),
                new NodeSpillConfig(),
                false,
                false,
                createObjectMapper(),
                new TaskManagerConfig().setTaskConcurrency(1));

        queryRunner.createCatalog("local", new UniqueColumnMockConnectorFactory(), ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testBasicRewrite()
    {
        // lineitem has 16 columns, sorting by 1 gives savings of 15 >= 10 threshold.
        // With unique column available, the optimization should produce SemiJoin plan.
        assertPlanWithSession(
                "SELECT * FROM lineitem ORDER BY orderkey LIMIT 10",
                enabledSession(),
                true,
                output(
                        topN(10, ImmutableList.of(sort("ORDERKEY", ASCENDING, LAST)),
                                anyTree(
                                        node(FilterNode.class,
                                                anyTree(
                                                        semiJoin("ROW_ID", "ROW_ID_CLONE", "SEMI_JOIN_OUTPUT",
                                                                anyTree(
                                                                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey", "ROW_ID", "$row_id"))),
                                                                anyTree(
                                                                        tableScan("lineitem", ImmutableMap.of("ROW_ID_CLONE", "$row_id"))))))))));
    }

    @Test
    public void testNoRewriteWhenDisabled()
    {
        // With optimization disabled, should produce a regular TopN plan.
        assertPlanWithSession(
                "SELECT * FROM lineitem ORDER BY orderkey LIMIT 10",
                disabledSession(),
                true,
                output(
                        topN(10, ImmutableList.of(sort("ORDERKEY", ASCENDING, LAST)),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))))));
    }

    @Test
    public void testNoRewriteNarrowTable()
    {
        // nation has only 4 columns. With savings of 3 (4 - 1 sort key), below the default threshold of 10.
        assertPlanWithSession(
                "SELECT * FROM nation ORDER BY name LIMIT 5",
                enabledSession(),
                true,
                output(
                        topN(5, ImmutableList.of(sort("NAME", ASCENDING, LAST)),
                                anyTree(
                                        tableScan("nation", ImmutableMap.of("NAME", "name"))))));
    }

    @Test
    public void testNoRewriteAllSortKeys()
    {
        // When all columns are sort keys, there's no savings.
        assertPlanWithSession(
                "SELECT regionkey FROM nation ORDER BY regionkey LIMIT 5",
                enabledSession(),
                true,
                output(
                        topN(5, ImmutableList.of(sort("REGIONKEY", ASCENDING, LAST)),
                                anyTree(
                                        tableScan("nation", ImmutableMap.of("REGIONKEY", "regionkey"))))));
    }

    @Test
    public void testRewriteWithLowThreshold()
    {
        // Even for nation (4 columns), with min_column_savings = 2 and sorting by 1, savings = 3 >= 2.
        Session lowThreshold = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_TOP_N_USING_ROW_ID, "true")
                .setSystemProperty(OPTIMIZE_TOP_N_USING_ROW_ID_MIN_COLUMN_SAVINGS, "2")
                .build();

        assertPlanWithSession(
                "SELECT * FROM nation ORDER BY name LIMIT 5",
                lowThreshold,
                true,
                output(
                        topN(5, ImmutableList.of(sort("NAME", ASCENDING, LAST)),
                                anyTree(
                                        node(FilterNode.class,
                                                anyTree(
                                                        semiJoin("ROW_ID", "ROW_ID_CLONE", "SEMI_JOIN_OUTPUT",
                                                                anyTree(
                                                                        tableScan("nation", ImmutableMap.of("NAME", "name", "ROW_ID", "$row_id"))),
                                                                anyTree(
                                                                        tableScan("nation", ImmutableMap.of("ROW_ID_CLONE", "$row_id"))))))))));
    }

    private Session enabledSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_TOP_N_USING_ROW_ID, "true")
                .build();
    }

    private Session disabledSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_TOP_N_USING_ROW_ID, "false")
                .build();
    }

    /**
     * Mock TpchConnectorFactory that provides a unique column ($row_id) on all tables.
     */
    private static class UniqueColumnMockConnectorFactory
            extends TpchConnectorFactory
    {
        UniqueColumnMockConnectorFactory()
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
                    return new UniqueColumnMockMetadata(catalogName, columnNaming, isPredicatePushdownEnabled(), isPartitioningEnabled(properties));
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
            };
        }
    }

    /**
     * Mock metadata that adds a unique column ($row_id) to the table layout of all tables.
     */
    private static class UniqueColumnMockMetadata
            extends TpchMetadata
    {
        // Synthetic $row_id column handle for testing
        private static final TpchColumnHandle ROW_ID_COLUMN = new TpchColumnHandle("$row_id", VARBINARY);

        UniqueColumnMockMetadata(String connectorId, ColumnNaming columnNaming, boolean predicatePushdownEnabled, boolean partitioningEnabled)
        {
            super(connectorId, columnNaming, predicatePushdownEnabled, partitioningEnabled);
        }

        @Override
        public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
        {
            ConnectorTableLayout baseLayout = super.getTableLayout(session, handle);
            // Add the unique column to make the optimizer applicable
            return new ConnectorTableLayout(
                    baseLayout.getHandle(),
                    baseLayout.getColumns(),
                    baseLayout.getPredicate(),
                    baseLayout.getTablePartitioning(),
                    baseLayout.getStreamPartitioningColumns(),
                    baseLayout.getDiscretePredicates(),
                    baseLayout.getLocalProperties(),
                    Optional.empty(),
                    Optional.of(ROW_ID_COLUMN));
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            Map<String, ColumnHandle> base = super.getColumnHandles(session, tableHandle);
            return ImmutableMap.<String, ColumnHandle>builder()
                    .putAll(base)
                    .put("$row_id", ROW_ID_COLUMN)
                    .build();
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
        {
            TpchColumnHandle tpchColumn = (TpchColumnHandle) columnHandle;
            if (tpchColumn.getColumnName().equals("$row_id")) {
                return ColumnMetadata.builder().setName("$row_id").setType(VARBINARY).setHidden(true).build();
            }
            return super.getColumnMetadata(session, tableHandle, columnHandle);
        }
    }
}
