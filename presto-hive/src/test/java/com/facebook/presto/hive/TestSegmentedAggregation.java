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

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.SEGMENTED_AGGREGATION_ENABLED;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.ORDER_BASED_EXECUTION_ENABLED;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSegmentedAggregation
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, LINE_ITEM, CUSTOMER, NATION),
                ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"),
                Optional.empty());
    }

    @Test
    public void testSortedbyKeysPrefixNotASubsetOfGroupbyKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_segmented_aggregation_customer0 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['name', 'custkey'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can't enable segmented aggregation
            assertPlan(orderBasedExecutionEnabled(),
                    "SELECT custkey, count(name) FROM test_segmented_aggregation_customer0 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1",
                    anyTree(aggregation(
                            singleGroupingSet("custkey"),
                            ImmutableMap.of(Optional.of("count"), functionCall("count", ImmutableList.of("name"))),
                            ImmutableList.of(), // no segmented streaming
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            tableScan("test_segmented_aggregation_customer0", ImmutableMap.of("custkey", "custkey", "name", "name")))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_aggregation_customer0");
        }
    }

    @Test
    public void testAndSortedByKeysArePrefixOfGroupbyKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_segmented_aggregation_customer WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name'], \n" +
                    "  sorted_by = ARRAY['custkey', 'name'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT custkey, name, nationkey, COUNT(*) FROM test_segmented_aggregation_customer \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1, 2, 3",
                    anyTree(aggregation(
                            singleGroupingSet("custkey", "name", "nationkey"),
                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                            ImmutableList.of("custkey", "name"), // segmented streaming
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            tableScan("test_segmented_aggregation_customer", ImmutableMap.of("custkey", "custkey", "name", "name", "nationkey", "nationkey")))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_aggregation_customer");
        }
    }

    @Test
    public void testSortedByPrefixOfBucketedKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_segmented_aggregation_customer2 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can enable segmented aggregation
            assertPlan(orderBasedExecutionEnabled(),
                    "SELECT name, custkey, COUNT(*) FROM test_segmented_aggregation_customer2 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1, 2",
                    anyTree(aggregation(
                            singleGroupingSet("name", "custkey"),
                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                            ImmutableList.of("custkey"), // segmented aggregation
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            tableScan("test_segmented_aggregation_customer2", ImmutableMap.of("name", "name", "custkey", "custkey")))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_aggregation_customer2");
        }
    }

    @Test
    public void testGroupByKeysShareElementsAsSortedByKeysPrefix()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_segmented_aggregation_customer_share_elements WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name', 'nationkey'], \n" +
                    "  sorted_by = ARRAY['custkey', 'phone'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can enable segmented aggregation
            assertPlan(orderBasedExecutionEnabled(),
                    "SELECT name, custkey, nationkey, COUNT(*) FROM test_segmented_aggregation_customer_share_elements \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1, 2, 3",
                    anyTree(aggregation(
                            singleGroupingSet("name", "custkey", "nationkey"),
                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                            ImmutableList.of("custkey"), // segmented aggregation
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            tableScan("test_segmented_aggregation_customer_share_elements", ImmutableMap.of("name", "name", "custkey", "custkey", "nationkey", "nationkey")))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_aggregation_customer_share_elements");
        }
    }

    /**
     * SELECT agg FROM t1 JOIN t2 ON t1.k = t2.k GROUP BY t1.g1, t2.g2, where t1 and t2 are both bucketed
     * by k and t1 is sorted by g1.
     * <p>
     * The join is colocated, so no exchange sits under it and the join output keeps the probe side's sort
     * on g1. The grouping keys do not contain the bucket key, so the aggregation is repartitioned and split
     * into a final over a partial, and the partial is pushed below that exchange onto the join output where
     * g1 is still a pre-grouped prefix. Only the partial can be segmented: the exchange above it does not
     * preserve the sort order, which is why {@link com.facebook.presto.sql.planner.optimizations.AddLocalExchanges}
     * cannot compute this prefix.
     */
    @Test
    public void testPartialAggregationBelowExchangeIsSegmented()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_segmented_partial_agg_t1 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['name'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    // Selecting the needed columns rather than *: orders has a date column and DWRF
                    // does not support the date type.
                    "SELECT custkey, name, nationkey, '2021-07-11' as ds FROM customer\n");
            queryRunner.execute("CREATE TABLE test_segmented_partial_agg_t2 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT custkey, orderkey, orderstatus, '2021-07-11' as ds FROM orders\n");

            @Language("SQL") String sql = "SELECT t1.name, t2.orderstatus, COUNT(*) \n" +
                    "FROM test_segmented_partial_agg_t1 t1 JOIN test_segmented_partial_agg_t2 t2 ON t1.custkey = t2.custkey \n" +
                    "WHERE t1.ds = '2021-07-11' AND t2.ds = '2021-07-11' \n" +
                    "GROUP BY 1, 2";

            assertPlanText(segmentedAggregationEnabled(), sql, "Aggregate(PARTIAL)(SEGMENTED, [name])");
            assertNoPlanText(segmentedAggregationDisabled(), sql, "SEGMENTED");

            // Segmenting the partial aggregation must not change the result.
            assertQueryWithSameQueryRunner(segmentedAggregationEnabled(), sql, segmentedAggregationDisabled());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_partial_agg_t1");
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_partial_agg_t2");
        }
    }

    private void assertPlanText(Session session, @Language("SQL") String sql, String expected)
    {
        String plan = (String) computeActual(session, "EXPLAIN (TYPE DISTRIBUTED) " + sql).getOnlyValue();
        assertTrue(plan.contains(expected), format("Expected the plan to contain [%s] but it was:%n%s", expected, plan));
    }

    private void assertNoPlanText(Session session, @Language("SQL") String sql, String unexpected)
    {
        String plan = (String) computeActual(session, "EXPLAIN (TYPE DISTRIBUTED) " + sql).getOnlyValue();
        assertFalse(plan.contains(unexpected), format("Did not expect the plan to contain [%s] but it was:%n%s", unexpected, plan));
    }

    private Session orderBasedExecutionEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, ORDER_BASED_EXECUTION_ENABLED, "true")
                .setSystemProperty(SEGMENTED_AGGREGATION_ENABLED, "true")
                .build();
    }

    /**
     * Pins the join so the sorted table is the probe: the join output only carries the probe side's
     * local properties.
     */
    private Session segmentedAggregation(boolean enabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, ORDER_BASED_EXECUTION_ENABLED, "true")
                .setSystemProperty(SEGMENTED_AGGREGATION_ENABLED, enabled ? "true" : "false")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
    }

    private Session segmentedAggregationEnabled()
    {
        return segmentedAggregation(true);
    }

    private Session segmentedAggregationDisabled()
    {
        return segmentedAggregation(false);
    }
}
