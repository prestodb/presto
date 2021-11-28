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
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.planner.CanonicalPlanFragment;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.sql.planner.CanonicalPlanGenerator.generateCanonicalPlan;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveCanonicalPlanGenerator
        extends AbstractTestQueryFramework
{
    private ObjectMapper objectMapper;

    public TestHiveCanonicalPlanGenerator()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
        this.objectMapper = new JsonObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)))
                .configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(ImmutableList.of(ORDERS, LINE_ITEM));
    }

    @Test
    public void testColumnPredicates()
            throws Exception
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_column_predicates WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-02' as ds FROM orders WHERE orderkey < 1000");

            assertDifferentCanonicalLeafSubPlan(
                    getSession(),
                    "SELECT * FROM test_column_predicates WHERE ds IN ('2020-09-01', '2020-09-02')",
                    "SELECT * FROM test_column_predicates");

            // Enabling filter push down would extract partition column predicate from domainPredicate, which would
            // make partition column predicates irrelevant for canonical plan.
            assertSameCanonicalLeafSubPlan(
                    pushdownFilterEnabled(),
                    "SELECT * FROM test_column_predicates WHERE ds IN ('2020-09-01', '2020-09-02')",
                    "SELECT * FROM test_column_predicates");
            assertSameCanonicalLeafSubPlan(
                    pushdownFilterEnabled(),
                    "SELECT * FROM test_column_predicates WHERE ds = '2020-09-01'",
                    "SELECT * FROM test_column_predicates WHERE ds = '2020-09-02'");
            assertSameCanonicalLeafSubPlan(
                    pushdownFilterEnabled(),
                    "SELECT * FROM test_column_predicates WHERE ds = '2020-09-01' AND regexp_like(comment, '.*foo.*')");

            assertDifferentCanonicalLeafSubPlan(
                    pushdownFilterEnabled(),
                    "SELECT * FROM test_column_predicates WHERE ds = '2020-09-01' AND regexp_like(comment, '.*foo.*')",
                    "SELECT * FROM test_column_predicates WHERE ds = '2020-09-01' AND regexp_like(comment, '.*bar.*')");
            assertDifferentCanonicalLeafSubPlan(
                    pushdownFilterEnabled(),
                    "SELECT * FROM test_column_predicates WHERE ds = '2020-09-01' AND orderkey < 50",
                    "SELECT * FROM test_column_predicates WHERE ds = '2020-09-01' AND orderkey < 100");
            assertDifferentCanonicalLeafSubPlan(
                    pushdownFilterEnabled(),
                    "SELECT * FROM test_column_predicates WHERE ds = '2020-09-01' AND orderkey < 50",
                    "SELECT * FROM test_column_predicates WHERE ds = '2020-09-01' AND custkey < 50");
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_column_predicates");
        }
    }

    @Test
    public void testBucketFilter()
            throws Exception
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_bucket_filter WITH (partitioned_by = ARRAY['ds'], bucketed_by = ARRAY['orderkey'], bucket_count = 11) AS " +
                    "SELECT orderkey, orderpriority, comment, '2020-09-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, '2020-09-02' as ds FROM orders WHERE orderkey < 1000");

            assertSameCanonicalLeafSubPlan(
                    pushdownFilterEnabled(),
                    "SELECT * FROM test_bucket_filter WHERE ds = '2020-09-01' AND orderkey = 50");

            assertDifferentCanonicalLeafSubPlan(
                    pushdownFilterEnabled(),
                    "SELECT * FROM test_bucket_filter WHERE ds = '2020-09-01' AND orderkey = 50",
                    "SELECT * FROM test_bucket_filter WHERE ds = '2020-09-01' AND orderkey = 60");
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_bucket_filter");
        }
    }

    private Session pushdownFilterEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
    }

    private static List<SubPlan> getLeafSubPlans(SubPlan subPlan)
    {
        if (subPlan.getChildren().isEmpty()) {
            return ImmutableList.of(subPlan);
        }
        return subPlan.getChildren().stream()
                .map(TestHiveCanonicalPlanGenerator::getLeafSubPlans)
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    private void assertSameCanonicalLeafSubPlan(Session session, String sql)
            throws Exception
    {
        assertSameCanonicalLeafSubPlan(session, sql, sql);
    }

    // This helper method would check if the provided sql could generate the same leaf canonical plan fragment when it appears
    // at two sides of UNION ALL. The provided sql should only contain queries that don't have subplan fanout like JOIN.
    private void assertSameCanonicalLeafSubPlan(Session session, String sql2, String sql1)
            throws Exception
    {
        SubPlan subplan = subplan(format("( %s ) UNION ALL ( %s )", sql1, sql2), session);
        List<CanonicalPlanFragment> leafCanonicalPlans = getLeafSubPlans(subplan).stream()
                .map(SubPlan::getFragment)
                .map(fragment -> generateCanonicalPlan(fragment.getRoot(), fragment.getPartitioningScheme()))
                .map(Optional::get)
                .collect(Collectors.toList());
        assertEquals(leafCanonicalPlans.size(), 2);
        assertEquals(objectMapper.writeValueAsString(leafCanonicalPlans.get(0)), objectMapper.writeValueAsString(leafCanonicalPlans.get(1)));
    }

    private void assertDifferentCanonicalLeafSubPlan(Session session, String sql1, String sql2)
            throws Exception
    {
        PlanFragment fragment1 = getOnlyElement(getLeafSubPlans(subplan(sql1, session))).getFragment();
        PlanFragment fragment2 = getOnlyElement(getLeafSubPlans(subplan(sql2, session))).getFragment();
        Optional<CanonicalPlanFragment> canonicalPlan1 = generateCanonicalPlan(fragment1.getRoot(), fragment1.getPartitioningScheme());
        Optional<CanonicalPlanFragment> canonicalPlan2 = generateCanonicalPlan(fragment2.getRoot(), fragment2.getPartitioningScheme());
        assertTrue(canonicalPlan1.isPresent());
        assertTrue(canonicalPlan2.isPresent());
        assertNotEquals(objectMapper.writeValueAsString(canonicalPlan1), objectMapper.writeValueAsString(canonicalPlan2));
    }
}
