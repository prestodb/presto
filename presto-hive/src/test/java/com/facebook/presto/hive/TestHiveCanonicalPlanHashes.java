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
package com.facebook.presto.hive;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.block.TestingBlockJsonSerde;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_PERFECTLY_CONSISTENT_HISTORIES;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.CONNECTOR;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.REMOVE_SAFE_CONSTANTS;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.sql.planner.CanonicalPlanGenerator.generateCanonicalPlan;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static com.google.common.graph.Traverser.forTree;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveCanonicalPlanHashes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner(ImmutableList.of(ORDERS, LINE_ITEM));
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProviders()
            {
                return ImmutableList.of(new InMemoryHistoryBasedPlanStatisticsProvider());
            }
        });
        return queryRunner;
    }

    @Override
    protected ObjectMapper createObjectMapper()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
        return new JsonObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)))
                .configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    @Test
    public void testCanonicalizationStrategies()
            throws Exception
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['ds', 'ts']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds, '00:01' as ts FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-02' as ds, '00:02' as ts FROM orders WHERE orderkey < 1000");

            assertSamePlanHash(
                    "SELECT orderkey from test_orders",
                    "SELECT orderkey from test_orders",
                    CONNECTOR);
            assertSamePlanHash(
                    "SELECT orderkey from test_orders where ds > '2020-09-01'",
                    "SELECT orderkey from test_orders where ds = '2020-09-02'",
                    CONNECTOR);
            assertSamePlanHash(
                    "SELECT orderkey from test_orders where ds = '2020-09-01' AND orderkey < 10 AND ts >= '00:01'",
                    "SELECT orderkey from test_orders where ds = '2020-09-02' AND orderkey < 10 AND ts >= '00:02'",
                    CONNECTOR);

            assertDifferentPlanHash(
                    "SELECT orderkey from test_orders where ds = '2020-09-01' AND orderkey < 10",
                    "SELECT orderkey from test_orders where ds = '2020-09-02' AND orderkey < 20",
                    CONNECTOR);

            assertSamePlanHash(
                    "SELECT orderkey, CAST(1 AS VARCHAR) from test_orders where ds = '2020-09-01' AND orderkey < 10",
                    "SELECT orderkey, CAST(2 AS VARCHAR) from test_orders where ds = '2020-09-02' AND orderkey < 10",
                    REMOVE_SAFE_CONSTANTS);
            assertDifferentPlanHash(
                    "SELECT orderkey, CAST(1 AS VARCHAR) from test_orders where ds = '2020-09-01' AND orderkey < 10",
                    "SELECT orderkey, CAST(1 AS VARCHAR) from test_orders where ds = '2020-09-02' AND orderkey < 20",
                    REMOVE_SAFE_CONSTANTS);

            assertSamePlanHash(
                    "INSERT INTO test_orders select * from test_orders",
                    "INSERT INTO test_orders select * from test_orders",
                    CONNECTOR);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    @Test
    public void testStatsEquivalentNodeMarking()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_orders_2 WITH (partitioned_by = ARRAY['ds', 'ts']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds, '00:01' as ts FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-02' as ds, '00:02' as ts FROM orders WHERE orderkey < 1000");

            List<PlanNode> nodes = getStatsEquivalentPlanHashes("SELECT COUNT(comment) FROM test_orders_2 WHERE ds = '2020-09-01' and orderkey < 500 GROUP BY custkey");
            assertTrue(nodes.stream().anyMatch(node -> node instanceof AggregationNode));
            assertTrue(nodes.stream().anyMatch(node -> node instanceof TableScanNode));
            assertTrue(nodes.stream().anyMatch(node -> node instanceof ProjectNode));
            assertTrue(nodes.stream().noneMatch(node -> node instanceof FilterNode));
            assertEquals(nodes.size(), 4);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_orders_2");
        }
    }

    private void assertSamePlanHash(String sql1, String sql2, PlanCanonicalizationStrategy strategy)
            throws Exception
    {
        String hashes1 = getPlanHash(sql1, strategy);
        String hashes2 = getPlanHash(sql2, strategy);
        assertEquals(hashes1, hashes2);
    }

    private void assertDifferentPlanHash(String sql1, String sql2, PlanCanonicalizationStrategy strategy)
            throws Exception
    {
        String hashes1 = getPlanHash(sql1, strategy);
        String hashes2 = getPlanHash(sql2, strategy);
        assertNotEquals(hashes1, hashes2);
    }

    private String getPlanHash(String sql, PlanCanonicalizationStrategy strategy)
            throws Exception
    {
        Session session = createSession();
        PlanNode plan = plan(sql, session).getRoot();
        ObjectMapper objectMapper = createObjectMapper();
        assertTrue(plan.getStatsEquivalentPlanNode().isPresent());
        return objectMapper.writeValueAsString(generateCanonicalPlan(plan.getStatsEquivalentPlanNode().get(), strategy, objectMapper, session).get());
    }

    private List<PlanNode> getStatsEquivalentPlanHashes(String sql)
    {
        Session session = createSession();
        PlanNode root = plan(sql, session).getRoot();
        assertTrue(root.getStatsEquivalentPlanNode().isPresent());

        ImmutableList.Builder<PlanNode> result = ImmutableList.builder();
        forTree(PlanNode::getSources)
                .depthFirstPreOrder(root)
                .forEach(node -> node.getStatsEquivalentPlanNode().ifPresent(result::add));
        return result.build();
    }

    private Session createSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(USE_PERFECTLY_CONSISTENT_HISTORIES, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .setSystemProperty(RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY, "false")
                .build();
    }
}
