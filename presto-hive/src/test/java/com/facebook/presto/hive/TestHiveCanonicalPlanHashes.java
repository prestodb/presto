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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.USE_EXTERNAL_PLAN_STATISTICS;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.CONNECTOR;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.REMOVE_SAFE_CONSTANTS;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.sql.planner.CanonicalPlanGenerator.generateCanonicalPlan;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
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
        return HiveQueryRunner.createQueryRunner(ImmutableList.of(ORDERS, LINE_ITEM));
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
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_orders");
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
        assertTrue(plan.getStatsEquivalentPlanNode().isPresent());
        return createObjectMapper().writeValueAsString(generateCanonicalPlan(plan.getStatsEquivalentPlanNode().get(), strategy).get());
    }

    private Session createSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_EXTERNAL_PLAN_STATISTICS, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
    }
}
