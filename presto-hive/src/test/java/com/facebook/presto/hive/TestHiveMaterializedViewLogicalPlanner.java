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
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.MATERIALIZED_VIEW_DATA_CONSISTENCY_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PREFER_PARTIAL_AGGREGATION;
import static com.facebook.presto.SystemSessionProperties.QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED;
import static com.facebook.presto.SystemSessionProperties.SIMPLIFY_PLAN_WITH_EMPTY_INPUT;
import static com.facebook.presto.common.predicate.Domain.create;
import static com.facebook.presto.common.predicate.Domain.multipleValues;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.hive.HiveMetadata.REFERENCED_MATERIALIZED_VIEWS;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD;
import static com.facebook.presto.hive.TestHiveLogicalPlanner.replicateHiveMetastore;
import static com.facebook.presto.hive.TestHiveLogicalPlanner.utf8Slices;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.SUPPLIER;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveMaterializedViewLogicalPlanner
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, LINE_ITEM, CUSTOMER, NATION, SUPPLIER),
                ImmutableMap.of("experimental.allow-legacy-materialized-views-toggle", "true"),
                Optional.empty());
    }

    @Test
    public void testMaterializedViewPartitionFilteringThroughLogicalView()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_lv_test";
        String materializedView = "orders_mv_lv_test";
        String logicalView = "orders_lv_test";

        try {
            // Create a table partitioned by 'ds' (date string)
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2025-11-10' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2025-11-11' AS ds FROM orders WHERE orderkey >= 1000 AND orderkey < 2000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2025-11-12' AS ds FROM orders WHERE orderkey >= 2000 AND orderkey < 3000", table));

            // Create a materialized view partitioned by 'ds'
            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT max(totalprice) as max_price, orderkey, ds FROM %s GROUP BY orderkey, ds", materializedView, table));

            assertTrue(getQueryRunner().tableExists(getSession(), materializedView));

            // Only refresh partition for '2025-11-10', leaving '2025-11-11' and '2025-11-12' missing
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2025-11-10'", materializedView), 255);

            // Create a logical view on top of the materialized view
            queryRunner.execute(format("CREATE VIEW %s AS SELECT * FROM %s", logicalView, materializedView));

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(materializedView));

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(1))
                    .build();

            // Query the logical view with a predicate
            // The predicate should be pushed down to the materialized view
            // Since only ds='2025-11-10' is refreshed and that's what we're querying,
            // the materialized view should be used (not fall back to base table)
            String logicalViewQuery = format("SELECT max_price, orderkey FROM %s WHERE ds='2025-11-10' ORDER BY orderkey", logicalView);
            String directMvQuery = format("SELECT max_price, orderkey FROM %s WHERE ds='2025-11-10' ORDER BY orderkey", materializedView);
            String baseTableQuery = format("SELECT max(totalprice) as max_price, orderkey FROM %s " +
                    "WHERE ds='2025-11-10' " +
                    "GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult baseQueryResult = computeActual(session, baseTableQuery);
            MaterializedResult logicalViewResult = computeActual(session, logicalViewQuery);
            MaterializedResult directMvResult = computeActual(session, directMvQuery);

            // All three queries should return the same results
            assertEquals(baseQueryResult, logicalViewResult);
            assertEquals(baseQueryResult, directMvResult);

            // The plan for the logical view query should use the materialized view
            // (not fall back to base table) because we're only querying the refreshed partition
            assertPlan(session, logicalViewQuery, anyTree(
                    constrainedTableScan(
                            materializedView,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2025-11-10"))),
                            ImmutableMap.of())));

            // Test query for a missing partition through logical view
            // This should fall back to base table because ds='2025-11-11' is not refreshed
            String logicalViewQueryMissing = format("SELECT max_price, orderkey FROM %s WHERE ds='2025-11-11' ORDER BY orderkey", logicalView);
            String baseTableQueryMissing = format("SELECT max(totalprice) as max_price, orderkey FROM %s " +
                    "WHERE ds='2025-11-11' " +
                    "GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult baseQueryResultMissing = computeActual(session, baseTableQueryMissing);
            MaterializedResult logicalViewResultMissing = computeActual(session, logicalViewQueryMissing);

            assertEquals(baseQueryResultMissing, logicalViewResultMissing);

            // Should fall back to base table for missing partition
            assertPlan(session, logicalViewQueryMissing, anyTree(
                    constrainedTableScan(table, ImmutableMap.of(), ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP VIEW IF EXISTS " + logicalView);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + materializedView);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewPartitionFilteringThroughLogicalViewWithCTE()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_cte_test";
        String materializedView = "orders_mv_cte_test";
        String logicalView = "orders_lv_cte_test";

        try {
            // Create a table partitioned by 'ds' (date string)
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2025-11-10' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2025-11-11' AS ds FROM orders WHERE orderkey >= 1000 AND orderkey < 2000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2025-11-12' AS ds FROM orders WHERE orderkey >= 2000 AND orderkey < 3000", table));

            // Create a materialized view partitioned by 'ds'
            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT max(totalprice) as max_price, orderkey, ds FROM %s GROUP BY orderkey, ds", materializedView, table));

            assertTrue(getQueryRunner().tableExists(getSession(), materializedView));

            // Only refresh partition for '2025-11-11', leaving '2025-11-10' and '2025-11-12' missing
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2025-11-11'", materializedView), 248);

            // Create a logical view on top of the materialized view
            queryRunner.execute(format("CREATE VIEW %s AS SELECT * FROM %s", logicalView, materializedView));

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(materializedView));

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(1))
                    .build();

            // Query the logical view through a CTE with a predicate
            // The predicate should be pushed down to the materialized view
            String cteQuery = format("WITH PreQuery AS (SELECT * FROM %s WHERE ds='2025-11-11') " +
                    "SELECT max_price, orderkey FROM PreQuery ORDER BY orderkey", logicalView);
            String baseTableQuery = format("SELECT max(totalprice) as max_price, orderkey FROM %s " +
                    "WHERE ds='2025-11-11' " +
                    "GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult baseQueryResult = computeActual(session, baseTableQuery);
            MaterializedResult cteQueryResult = computeActual(session, cteQuery);

            // Both queries should return the same results
            assertEquals(baseQueryResult, cteQueryResult);

            // The plan for the CTE query should use the materialized view
            // (not fall back to base table) because we're only querying the refreshed partition
            assertPlan(session, cteQuery, anyTree(
                    constrainedTableScan(
                            materializedView,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2025-11-11"))),
                            ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP VIEW IF EXISTS " + logicalView);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + materializedView);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewPartitionFilteringInCTE()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_mv_cte_test";
        String materializedView = "orders_mv_direct_cte_test";

        try {
            // Create a table partitioned by 'ds' (date string)
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2025-11-10' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2025-11-11' AS ds FROM orders WHERE orderkey >= 1000 AND orderkey < 2000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2025-11-12' AS ds FROM orders WHERE orderkey >= 2000 AND orderkey < 3000", table));

            // Create a materialized view partitioned by 'ds'
            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT max(totalprice) as max_price, orderkey, ds FROM %s GROUP BY orderkey, ds", materializedView, table));

            assertTrue(getQueryRunner().tableExists(getSession(), materializedView));

            // Only refresh partition for '2025-11-10', leaving '2025-11-11' and '2025-11-12' missing
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2025-11-10'", materializedView), 255);

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(materializedView));

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(1))
                    .build();

            // Query the materialized view directly through a CTE with a predicate
            // The predicate should be used to determine which partitions are needed
            String cteQuery = format("WITH PreQuery AS (SELECT * FROM %s WHERE ds='2025-11-10') " +
                    "SELECT max_price, orderkey FROM PreQuery ORDER BY orderkey", materializedView);
            String baseTableQuery = format("SELECT max(totalprice) as max_price, orderkey FROM %s " +
                    "WHERE ds='2025-11-10' " +
                    "GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult baseQueryResult = computeActual(session, baseTableQuery);
            MaterializedResult cteQueryResult = computeActual(session, cteQuery);

            // Both queries should return the same results
            assertEquals(baseQueryResult, cteQueryResult);

            // The plan for the CTE query should use the materialized view
            // (not fall back to base table) because we're only querying the refreshed partition
            assertPlan(session, cteQuery, anyTree(
                    constrainedTableScan(
                            materializedView,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2025-11-10"))),
                            ImmutableMap.of())));

            // Test query for a missing partition through CTE
            // This should fall back to base table because ds='2025-11-11' is not refreshed
            String cteQueryMissing = format("WITH PreQuery AS (SELECT * FROM %s WHERE ds='2025-11-11') " +
                    "SELECT max_price, orderkey FROM PreQuery ORDER BY orderkey", materializedView);
            String baseTableQueryMissing = format("SELECT max(totalprice) as max_price, orderkey FROM %s " +
                    "WHERE ds='2025-11-11' " +
                    "GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult baseQueryResultMissing = computeActual(session, baseTableQueryMissing);
            MaterializedResult cteQueryResultMissing = computeActual(session, cteQueryMissing);

            assertEquals(baseQueryResultMissing, cteQueryResultMissing);

            // Should fall back to base table for missing partition
            assertPlan(session, cteQueryMissing, anyTree(
                    constrainedTableScan(table, ImmutableMap.of(), ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + materializedView);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewOptimization()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned";
        String view = "test_orders_view";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table));
            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM %s", view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate("REFRESH MATERIALIZED VIEW test_orders_view WHERE ds='2020-01-01'", 255);

            String viewQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    filter("orderkey < BIGINT'10000'", constrainedTableScan(table,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS test_orders_view");
            queryRunner.execute("DROP TABLE IF EXISTS orders_partitioned");
        }
    }

    @Test
    public void testMaterializedViewOptimizationWithClause()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "test_orders_partitioned_with_clause";
        String view = "test_view_orders_partitioned_with_clause";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table));

            String viewPart = format(
                    "WITH X AS (SELECT orderkey, orderpriority, ds FROM %s), " +
                            "Y AS (SELECT orderkey, orderpriority, ds FROM X), " +
                            "Z AS (SELECT orderkey, orderpriority, ds FROM Y) " +
                            "SELECT orderkey, orderpriority, ds FROM Z",
                    table);

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS %s", view, viewPart));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s where ds='2020-01-01'", view), 255);

            String viewQuery = format("SELECT orderkey, orderpriority, ds from %s where orderkey < 100 ORDER BY orderkey", view);
            String baseQuery = viewPart + " where orderkey < 100 ORDER BY orderkey";

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    project(
                            ImmutableMap.of("ds_61", expression("'2019-01-02'")),
                            filter("orderkey < BIGINT'100'", constrainedTableScan(table,
                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                                    ImmutableMap.of("orderkey", "orderkey")))),
                    filter("orderkey_62 < BIGINT'100'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_62", "orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewOptimizationFullyMaterialized()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_fully_materialized";
        String view = "orders_view_fully_materialized";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM %s", view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2020-01-01'", view), 255);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2019-01-02'", view), 14745);

            String viewQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            // Otherwise the empty values node will be optimized
            Session disableEmptyInputOptimization = Session.builder(getSession()).setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "false").build();
            assertPlan(disableEmptyInputOptimization, viewQuery, anyTree(
                    anyTree(values("orderkey")), // Alias for the filter column
                    anyTree(filter("orderkey_17 < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey"))))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewOptimizationNotMaterialized()
    {
        String table = "orders_partitioned_not_materialized";
        String view = "orders_partitioned_view_not_materialized";

        QueryRunner queryRunner = getQueryRunner();
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM %s", view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));

            String viewQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    filter("orderkey < BIGINT'10000'", constrainedTableScan(table, ImmutableMap.of(), ImmutableMap.of("orderkey", "orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedTooManyMissingPartitions()
    {
        String table = "orders_partitioned_not_materialized";
        String view = "orders_partitioned_view_not_materialized";

        QueryRunner queryRunner = getQueryRunner();
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey >= 1000 and orderkey < 2000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-02-02' as ds FROM orders WHERE orderkey >= 2000 and orderkey < 3000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-03-02' as ds FROM orders WHERE orderkey >= 3000 and orderkey < 4000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-04-02' as ds FROM orders WHERE orderkey >= 4000 and orderkey < 5000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-05-02' as ds FROM orders WHERE orderkey >= 5000 and orderkey < 6000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-06-02' as ds FROM orders WHERE orderkey >= 6000 and orderkey < 7000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-07-02' as ds FROM orders WHERE orderkey >= 7000 and orderkey < 8000 ", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM %s", view, table));
            assertTrue(getQueryRunner().tableExists(getQueryRunner().getDefaultSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2020-01-01'", view), 255);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2019-01-02'", view), 248);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2019-02-02'", view), 248);

            String viewQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            // assert that when missing partition count > threshold, fallback to using base table to satisfy view query
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(2))
                    .build();

            assertPlan(session, viewQuery, anyTree(
                    filter("orderkey < BIGINT'10000'",
                            constrainedTableScan(table, ImmutableMap.of(), ImmutableMap.of("orderkey", "orderkey")))));

            // assert that when count of missing partition  <= threshold, use available partitions from view
            session = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(100))
                    .build();

            assertPlan(session, viewQuery, anyTree(
                    filter("orderkey < BIGINT'10000'", constrainedTableScan(table,
                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2019-03-02", "2019-04-02", "2019-05-02", "2019-06-02", "2019-07-02"))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", constrainedTableScan(view,
                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2020-01-01", "2019-01-02", "2019-02-02"))),
                            ImmutableMap.of("orderkey_17", "orderkey")))));

            // if there are too many missing partitions, the optimization rewrite should not happen
            session = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(2))
                    .build();
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            assertPlan(session, baseQuery, anyTree(
                    filter("orderkey < BIGINT'10000'",
                            constrainedTableScan(table, ImmutableMap.of(), ImmutableMap.of("orderkey", "orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewOptimizationWithNullPartition()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_null_partition";
        String view = "orders_partitioned_view_null_partition";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 500 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 500 and orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, NULL as ds FROM orders WHERE orderkey > 1000 and orderkey < 1500", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, ds FROM %s", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2020-01-01'", view), 127);

            String viewQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", view);
            String baseQuery = format("SELECT orderkey from %s  where orderkey < 10000 ORDER BY orderkey", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    filter("orderkey < BIGINT'10000'", constrainedTableScan(table,
                            ImmutableMap.of("ds", create(ValueSet.of(createVarcharType(10), utf8Slice("2019-01-02")), true)),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewWithLessGranularity()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_less_granularity";
        String view = "orders_partitioned_view_less_granularity";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['orderpriority', 'ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, ds FROM %s", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2020-01-01'", view), 255);

            String viewQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    filter("orderkey < BIGINT'10000'", constrainedTableScan(table,
                            ImmutableMap.of(
                                    "ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02")),
                                    "orderpriority", multipleValues(createVarcharType(15), utf8Slices("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewForIntersect()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "test_customer_intersect1";
        String table2 = "test_customer_intersect2";
        String view = "test_customer_view_intersect";
        try {
            computeActual(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS SELECT custkey, name, address, nationkey FROM customer", table1));

            computeActual(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS SELECT custkey, name, address, nationkey FROM customer", table2));

            String baseQuery = format(
                    "SELECT name, custkey, nationkey FROM ( SELECT name, custkey, nationkey FROM %s WHERE custkey < 1000 INTERSECT " +
                            "SELECT name, custkey, nationkey FROM %s WHERE custkey <= 900 )", table1, table2);

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS %s", view, baseQuery));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE nationkey < 10", view), 380);

            String viewQuery = format("SELECT name, custkey, nationkey from %s ORDER BY name", view);
            baseQuery = format("%s ORDER BY name", baseQuery);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    anyTree(
                            anyTree(
                                    filter("custkey < BIGINT'1000'", constrainedTableScan(table1,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                                            ImmutableMap.of("custkey", "custkey")))),
                            anyTree(
                                    filter("custkey_21 <= BIGINT'900'", constrainedTableScan(table2,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                                            ImmutableMap.of("custkey_21", "custkey"))))),
                    constrainedTableScan(view, ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewForUnionAll()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "test_customer_union1";
        String table2 = "test_customer_union2";
        String view = "test_customer_view_union";
        try {
            computeActual(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS SELECT custkey, name, address, nationkey FROM customer", table1));

            computeActual(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS SELECT custkey, name, address, nationkey FROM customer", table2));

            String baseQuery = format(
                    "SELECT name, custkey, nationkey FROM ( SELECT name, custkey, nationkey FROM %s WHERE custkey < 1000 UNION ALL " +
                            "SELECT name, custkey, nationkey FROM %s WHERE custkey >= 1000 )", table1, table2);

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS %s", view, baseQuery));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE nationkey < 10", view), 599);

            String viewQuery = format("SELECT name, custkey, nationkey from %s ORDER BY name", view);
            baseQuery = format("%s ORDER BY name", baseQuery);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    filter("custkey < BIGINT'1000'", constrainedTableScan(table1,
                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                            ImmutableMap.of("custkey", "custkey"))),
                    filter("custkey_21 >= BIGINT'1000'", constrainedTableScan(table2,
                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                            ImmutableMap.of("custkey_21", "custkey"))),
                    constrainedTableScan(view, ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewForUnionAllWithOneSideMaterialized()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "orders_key_partitioned_1";
        String table2 = "orders_key_partitioned_2";
        String view = "orders_key_view_union";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds FROM orders WHERE orderkey < 1000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds FROM orders WHERE orderkey > 1000 and orderkey < 2000", table2));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, ds FROM %s UNION ALL SELECT orderkey, ds FROM %s", view, table1, table2));

            assertTrue(queryRunner.tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 510);

            String viewQuery = format("SELECT orderkey, ds FROM %s ORDER BY orderkey", view);
            String baseQuery = format("(SELECT orderkey, ds FROM %s UNION ALL SELECT orderkey, ds FROM %s) ORDER BY orderkey", table1, table2);
            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    project(
                            ImmutableMap.of("ds_27", expression("'2019-01-02'")),
                            constrainedTableScan(table2,
                                    ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2019-01-02"))))),
                    constrainedTableScan(view, ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewForExcept()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "test_customer_except1";
        String table2 = "test_customer_except2";
        String view = "test_customer_view_except";
        try {
            computeActual(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS SELECT custkey, name, address, nationkey FROM customer", table1));

            computeActual(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS SELECT custkey, name, address, nationkey FROM customer", table2));

            String baseQuery = format(
                    "SELECT name, custkey, nationkey FROM %s WHERE custkey < 1000 EXCEPT " +
                            "SELECT name, custkey, nationkey FROM %s WHERE custkey > 900", table1, table2);

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS %s", view, baseQuery));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE nationkey < 10", view), 380);

            String viewQuery = format("SELECT name, custkey, nationkey from %s ORDER BY name", view);
            baseQuery = format("%s ORDER BY name", baseQuery);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    anyTree(
                            anyTree(
                                    filter("custkey < BIGINT'1000'", constrainedTableScan(table1,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                                            ImmutableMap.of("custkey", "custkey")))),
                            anyTree(
                                    filter("custkey_21 > BIGINT'900'", constrainedTableScan(table2,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                                            ImmutableMap.of("custkey_21", "custkey"))))),
                    constrainedTableScan(view, ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewForUnionAllWithMultipleTables()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "orders_key_small_union";
        String table2 = "orders_key_large_union";
        String view = "orders_view_union";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds FROM orders WHERE orderkey > 1000 and orderkey < 2000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds FROM orders WHERE orderkey > 2000 and orderkey < 3000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds FROM orders WHERE orderkey > 3000 and orderkey < 4000", table2));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey AS view_orderkey, ds FROM ( " +
                    "SELECT orderkey, ds FROM %s " +
                    "UNION ALL " +
                    "SELECT orderkey, ds FROM %s ) ", view, table1, table2));

            assertTrue(queryRunner.tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 503);

            String viewQuery = format("SELECT view_orderkey, ds from %s where view_orderkey <  10000 ORDER BY view_orderkey", view);
            String baseQuery = format("SELECT orderkey AS view_orderkey, ds FROM ( " +
                    "SELECT orderkey, ds FROM %s " +
                    "UNION ALL " +
                    "SELECT orderkey, ds FROM %s ) " +
                    "WHERE orderkey < 10000 ORDER BY orderkey", table1, table2);
            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewForGroupingSet()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "test_lineitem_grouping_set";
        String view = "test_view_lineitem_grouping_set";
        try {
            computeActual(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['shipmode']) " +
                    "AS SELECT linenumber, quantity, shipmode FROM lineitem", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['shipmode']) " +
                            "AS SELECT linenumber, SUM(DISTINCT CAST(quantity AS BIGINT)) quantity, shipmode FROM %s " +
                            "GROUP BY GROUPING SETS ((linenumber, shipmode), (shipmode))",
                    view, table));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE shipmode='RAIL'", view), 8);

            String viewQuery = format("SELECT * FROM %s ORDER BY linenumber, shipmode", view);
            String baseQuery = format("SELECT linenumber, SUM(DISTINCT CAST(quantity AS BIGINT)) quantity, shipmode FROM %s " +
                    "GROUP BY GROUPING SETS ((linenumber, shipmode), (shipmode)) ORDER BY linenumber, shipmode", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    anyTree(constrainedTableScan(table,
                            ImmutableMap.of("shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "REG AIR", "SHIP", "TRUCK"))))),
                    constrainedTableScan(view, ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewWithDifferentPartitions()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_different_partitions";
        String view = "orders_partitioned_view_different_partitions";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderpriority']) AS " +
                    "SELECT orderkey, orderstatus, '2020-01-01' as ds, orderpriority FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderstatus, '2019-01-02' as ds, orderpriority FROM orders WHERE orderkey > 1000", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'orderstatus']) AS " +
                    "SELECT orderkey, orderpriority, ds, orderstatus FROM %s", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2020-01-01'", view), 255);

            String viewQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    filter("orderkey < BIGINT'10000'", constrainedTableScan(table,
                            ImmutableMap.of(
                                    "ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02")),
                                    "orderpriority", multipleValues(createVarcharType(15), utf8Slices("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_23 < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_23", "orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewJoinsWithOneTableAlias()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "view_join_with_one_alias";
        String table1 = "nation_partitioned_join_with_one_alias";
        String table2 = "customer_partitioned_join_with_one_alias";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey', 'regionkey']) AS " +
                    "SELECT name, nationkey, regionkey FROM nation", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) AS SELECT custkey," +
                    " name, mktsegment, nationkey FROM customer", table2));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['marketsegment', " +
                            "'nationkey', 'regionkey']) AS SELECT %s.name AS nationname, " +
                            "customer.custkey, customer.name AS customername, UPPER(customer.mktsegment) AS marketsegment, customer.nationkey, regionkey " +
                            "FROM %s JOIN %s customer ON (%s.nationkey = customer.nationkey)",
                    view, table1, table1, table2, table1));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE regionkey = 1", view), 300);

            String viewQuery = format("SELECT nationname, custkey from %s ORDER BY custkey", view);
            String baseQuery = format("SELECT %s.name AS nationname, customer.custkey FROM %s JOIN %s customer ON (%s.nationkey = customer.nationkey)" +
                    "ORDER BY custkey", table1, table1, table2, table1);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(
                    Session.builder(getSession())
                            .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                            .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.PARTITIONED.name())
                            .build(),
                    viewQuery,
                    anyTree(
                            join(INNER, ImmutableList.of(equiJoinClause("l_nationkey", "r_nationkey")),
                                    anyTree(constrainedTableScan(table1,
                                            ImmutableMap.of(
                                                    "regionkey", multipleValues(BIGINT, ImmutableList.of(0L, 2L, 3L, 4L)),
                                                    "nationkey", multipleValues(BIGINT,
                                                            ImmutableList.of(0L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 18L, 19L, 20L, 21L, 22L, 23L))),
                                            ImmutableMap.of("l_nationkey", "nationkey"))),
                                    anyTree(constrainedTableScan(table2,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT,
                                                    ImmutableList.of(0L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 18L, 19L, 20L, 21L, 22L, 23L))),
                                            ImmutableMap.of("r_nationkey", "nationkey")))),
                            constrainedTableScan(view, ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewSampledRelations()
    {
        QueryRunner queryRunner = getQueryRunner();
        String viewFull = "view_nation_sampled_100";
        String viewHalf = "view_nation_sampled_50";
        String table = "nation_partitioned";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey', 'regionkey']) AS " +
                    "SELECT name, nationkey, regionkey FROM nation", table));

            String viewFullDefinition = format("SELECT SUM(regionkey) AS sum_region_key, nationkey FROM %s TABLESAMPLE BERNOULLI (100) GROUP BY nationkey", table);
            String viewHalfDefinition = format("SELECT SUM(regionkey) AS sum_region_key, nationkey FROM %s TABLESAMPLE BERNOULLI (50) GROUP BY nationkey", table);

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS %s", viewFull, viewFullDefinition));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['nationkey']) " +
                    "AS %s", viewHalf, viewHalfDefinition));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE nationKey < 5", viewFull), 5);
            queryRunner.execute(format("REFRESH MATERIALIZED VIEW %s WHERE nationKey < 5", viewHalf));

            String viewFullQuery = format("SELECT * from %s ORDER BY nationkey", viewFull);
            String baseQuery = format("%s ORDER BY nationkey", viewFullDefinition);

            MaterializedResult viewFullTable = computeActual(viewFullQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewFullTable, baseTable);

            // With over 25 nations with multiple regions, it is very high probability that even with millions of runs, we never get the same result
            // from sampled table and full table
            String viewHalfQuery = format("SELECT * from %s ORDER BY nationkey", viewHalf);
            MaterializedResult viewHalfTable = computeActual(viewHalfQuery);
            assertNotEquals(viewFullTable, viewHalfTable);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + viewFull);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + viewHalf);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewWithValues()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "view_nation_values";
        String table = "nation_partitioned";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey', 'regionkey']) AS " +
                    "SELECT name, nationkey, regionkey FROM nation", table));

            String viewDefinition = format("SELECT name, nationkey, regionkey FROM %s JOIN (VALUES 1, 2, 3) t(a) ON t.a = %s.regionkey", table, table);

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['nationkey', 'regionkey']) " +
                    "AS  %s", view, viewDefinition));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE regionkey = 1", view), 5);

            String viewQuery = format("SELECT name, nationkey, regionkey from %s ORDER BY name", view);
            String baseQuery = format("%s ORDER BY name", viewDefinition);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewOptimizationWithDerivedFields()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "lineitem_partitioned_derived_fields";
        String view = "lineitem_partitioned_view_derived_fields";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                    "SELECT discount, extendedprice, '2020-01-01' as ds, shipmode FROM lineitem WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT discount, extendedprice, '2020-01-02' as ds, shipmode FROM lineitem WHERE orderkey > 1000", table));

            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                            "SELECT SUM(discount*extendedprice) as _discount_multi_extendedprice_, ds, shipmode FROM %s group by ds, shipmode",
                    view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 7);

            String viewQuery = format("SELECT sum(_discount_multi_extendedprice_) from %s group by ds, shipmode ORDER BY sum(_discount_multi_extendedprice_)", view);
            String baseQuery = format("SELECT sum(discount * extendedprice) as _discount_multi_extendedprice_ from %s group by ds, shipmode " +
                    "ORDER BY _discount_multi_extendedprice_", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    anyTree(constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    anyTree(constrainedTableScan(view, ImmutableMap.of()))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewOptimizationWithDerivedFieldsWithAlias()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "lineitem_partitioned_derived_fields_with_alias";
        String view = "lineitem_partitioned_view_derived_fields_with_alias";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                    "SELECT discount, extendedprice, '2020-01-01' as ds, shipmode FROM lineitem WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT discount, extendedprice, '2020-01-02' as ds, shipmode FROM lineitem WHERE orderkey > 1000 ", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'view_shipmode']) " +
                    "AS SELECT SUM(discount*extendedprice) as _discount_multi_extendedprice_, ds, shipmode as view_shipmode " +
                    "FROM %s group by ds, shipmode", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 7);

            String viewQuery = format("SELECT sum(_discount_multi_extendedprice_) from %s group by ds ORDER BY sum(_discount_multi_extendedprice_)", view);
            String baseQuery = format("SELECT sum(discount * extendedprice) as _discount_multi_extendedprice_ from %s group by ds " +
                    "ORDER BY _discount_multi_extendedprice_", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    anyTree(constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    anyTree(constrainedTableScan(view, ImmutableMap.of()))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testBaseToViewConversionWithDerivedFields()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String table = "lineitem_partitioned_derived_fields";
        String view = "lineitem_partitioned_view_derived_fields";
        try {
            queryRunner.execute(format(
                    "CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                            "SELECT discount, extendedprice, '2020-01-01' as ds, shipmode FROM lineitem WHERE orderkey < 1000 " +
                            "UNION ALL " +
                            "SELECT discount, extendedprice, '2020-01-02' as ds, shipmode FROM lineitem WHERE orderkey > 1000",
                    table));
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['mvds', 'shipmode']) AS " +
                            "SELECT SUM(discount * extendedprice) as _discount_multi_extendedprice_ , MAX(discount*extendedprice) as _max_discount_multi_extendedprice_ , " +
                            "ds as mvds, shipmode FROM %s group by ds, shipmode",
                    view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s where mvds='2020-01-01'", view), 7);
            String baseQuery = format(
                    "SELECT sum(discount * extendedprice) as _discount_multi_extendedprice_ , MAX(discount*extendedprice) as _max_discount_multi_extendedprice_ , " +
                            "ds, shipmode as method from %s group by ds, shipmode ORDER BY ds, shipmode", table);
            String viewQuery = format(
                    "SELECT _discount_multi_extendedprice_ , _max_discount_multi_extendedprice_ , " +
                            "mvds, shipmode as method from %s ORDER BY mvds, shipmode", view);
            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(optimizedQueryResult, baseQueryResult);

            assertPlan(getSession(), viewQuery, anyTree(
                    anyTree(constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    constrainedTableScan(view, ImmutableMap.of())));

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, anyTree(
                    anyTree(constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    anyTree(constrainedTableScan(view, ImmutableMap.of()))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testBaseToViewConversionWithMultipleCandidates()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "false")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned";
        String view1 = "test_orders_view1";
        String view2 = "test_orders_view2";
        String view3 = "test_orders_view3";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, orderdate, totalprice, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, orderdate, totalprice, '2020-01-02' as ds FROM orders WHERE orderkey > 1000", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM %s", view1, table));
            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderdate, ds FROM %s", view2, table));
            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, totalprice, ds FROM %s", view3, table));

            assertTrue(queryRunner.tableExists(getSession(), view1));
            assertTrue(queryRunner.tableExists(getSession(), view2));
            assertTrue(queryRunner.tableExists(getSession(), view3));

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view1, view2, view3));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view1), 255);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-02'", view1), 14745);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view2), 255);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-02'", view2), 14745);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view3), 255);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-02'", view3), 14745);

            String baseQuery = format("SELECT orderkey, orderdate from %s where orderkey < 1000 ORDER BY orderkey", table);
            String viewQuery = format("SELECT orderkey, orderdate from %s where orderkey < 1000 ORDER BY orderkey", view2);

            // Try optimizing the base query when there is one compatible candidate from the referenced materialized views
            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(optimizedQueryResult, baseQueryResult);

            PlanMatchPattern expectedPattern = anyTree(
                    anyTree(values("orderkey", "orderdate")),
                    anyTree(filter("orderkey_25 < BIGINT'1000'", constrainedTableScan(view2,
                            ImmutableMap.of(),
                            ImmutableMap.of("orderkey_25", "orderkey")))));

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);

            Session disableEmptyInputOptimization = Session.builder(getSession()).setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "false").build();
            assertPlan(disableEmptyInputOptimization, viewQuery, expectedPattern);

            // Try optimizing the base query when all candidates are incompatible
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view1, view3));
            assertPlan(queryOptimizationWithMaterializedView, baseQuery, anyTree(
                    filter("orderkey < BIGINT'1000'", constrainedTableScan(table,
                            ImmutableMap.of(),
                            ImmutableMap.of("orderkey", "orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view1);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view2);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view3);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testBaseToViewConversionWithGroupBy()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String table = "lineitem_partitioned_derived_fields";
        String view = "lineitem_partitioned_view_derived_fields";
        try {
            queryRunner.execute(format(
                    "CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                            "SELECT discount, extendedprice, '2020-01-01' as ds, shipmode FROM lineitem WHERE orderkey < 1000 " +
                            "UNION ALL " +
                            "SELECT discount, extendedprice, '2020-01-02' as ds, shipmode FROM lineitem WHERE orderkey > 1000",
                    table));
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                            "SELECT SUM(discount * extendedprice) as _discount_multi_extendedprice_ , MAX(discount*extendedprice) as _max_discount_multi_extendedprice_ , " +
                            "ds, shipmode FROM %s group by ds, shipmode",
                    view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s where ds='2020-01-01'", view), 7);
            String baseQuery = format(
                    "SELECT SUM(discount * extendedprice) as _discount_multi_extendedprice_, MAX(discount*extendedprice) as _max_discount_multi_extendedprice_ FROM %s", table);
            String viewQuery = format(
                    "SELECT SUM(_discount_multi_extendedprice_), MAX(_max_discount_multi_extendedprice_) FROM %s", view);
            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(optimizedQueryResult, baseQueryResult);

            PlanMatchPattern expectedPattern = anyTree(
                    anyTree(constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    anyTree(constrainedTableScan(view, ImmutableMap.of())));
            assertPlan(getSession(), viewQuery, expectedPattern);
            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testBaseToViewConversionCountOptimizationWithStitching()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                //disable partial aggregation to simplify plans
                .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false")
                .build();
        QueryRunner queryRunner = getQueryRunner();

        String table = "orders_partitioned";
        String view = "test_orders_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s " +
                            "WITH (partitioned_by = array['ds']) " +
                            "AS SELECT *, '2021-07-11' AS ds " +
                            "FROM orders " +
                            "WHERE orderkey < 10000 " +
                            "UNION ALL " +
                            "SELECT *, '2021-07-12' AS ds FROM orders WHERE orderkey < 10000",
                    table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT COUNT(*) AS a_count, orderkey, ds " +
                            "FROM %s " +
                            "GROUP BY ds, orderkey",
                    view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds= '2021-07-11'", view), 2503);

            //group by orderkey only, which will stitch any data with different ds but same orderkey
            String viewQuery = format("SELECT SUM(a_count), orderkey FROM %s GROUP BY orderkey ORDER BY orderkey", view);
            String baseQuery = format("SELECT COUNT(*) AS a_count, orderkey FROM %s GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, optimizedQueryResult);

            PlanMatchPattern expectedPattern = anyTree(
                    aggregation(
                            singleGroupingSet("orderkey"),
                            ImmutableMap.of(Optional.empty(), functionCall("sum", ImmutableList.of("count"))),
                            ImmutableList.of(),
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            node(
                                    ExchangeNode.class,
                                    anyTree(
                                            aggregation(
                                                    ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                                    SINGLE,
                                                    node(
                                                            ExchangeNode.class,
                                                            anyTree(
                                                                    node(
                                                                            ProjectNode.class, constrainedTableScan(
                                                                                    table,
                                                                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2021-07-12"))),
                                                                                    ImmutableMap.of("orderkey", "orderkey"))))))),
                                    anyTree(
                                            constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of())))));

            assertPlan(queryOptimizationWithMaterializedView, viewQuery, expectedPattern);
            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testBaseToViewConversionCountOptimizationWithFreshView()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false")
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "false")
                .build();
        QueryRunner queryRunner = getQueryRunner();

        String table = "orders_partitioned";
        String view = "test_orders_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s " +
                            "WITH (partitioned_by = array['ds']) " +
                            "AS SELECT *, '2021-07-11' AS ds " +
                            "FROM orders " +
                            "WHERE orderkey < 10000 " +
                            "UNION ALL " +
                            "SELECT *, '2021-07-12' AS ds FROM orders WHERE orderkey < 10000",
                    table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT COUNT(*) AS a_count, orderkey, ds " +
                            "FROM %s " +
                            "GROUP BY ds, orderkey",
                    view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds= '2021-07-11'", view), 2503);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds= '2021-07-12'", view), 2503);

            String viewQuery = format("SELECT SUM(a_count), orderkey FROM %s GROUP BY orderkey ORDER BY orderkey", view);
            String baseQuery = format("SELECT COUNT(*) AS a_count, orderkey FROM %s GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, optimizedQueryResult);

            PlanMatchPattern expectedPattern = anyTree(
                    aggregation(
                            singleGroupingSet("orderkey"),
                            ImmutableMap.of(Optional.empty(), functionCall("sum", ImmutableList.of("count"))),
                            ImmutableList.of(),
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            node(
                                    ExchangeNode.class,
                                    anyTree(
                                            aggregation(
                                                    ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                                    SINGLE,
                                                    node(
                                                            ExchangeNode.class,
                                                            node(
                                                                    ProjectNode.class,
                                                                    values("orderkey", "ds"))))),
                                    anyTree(
                                            //expect no scan to happen over the base table since materialized view is completely fresh
                                            constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of())))));

            assertPlan(queryOptimizationWithMaterializedView, viewQuery, expectedPattern);
            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testBaseToViewConversionCountOptimizationDoesNotOccurWithStaleView()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false")
                .build();
        QueryRunner queryRunner = getQueryRunner();

        String table = "orders_partitioned";
        String view = "test_orders_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s " +
                            "WITH (partitioned_by = array['ds']) " +
                            "AS SELECT *, '2021-07-11' AS ds " +
                            "FROM orders " +
                            "WHERE orderkey < 10000 " +
                            "UNION ALL " +
                            "SELECT *, '2021-07-12' AS ds FROM orders WHERE orderkey < 10000",
                    table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT COUNT(*) AS a_count, orderkey, ds " +
                            "FROM %s " +
                            "GROUP BY ds, orderkey",
                    view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            String baseQuery = format("SELECT COUNT(*) AS a_count, orderkey FROM %s GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, optimizedQueryResult);

            //expect a query on the base table to behave as if the materialized view doesn't exist when view is completely stale
            PlanMatchPattern expectedPatternWithoutCountOptimization = anyTree(
                    aggregation(
                            singleGroupingSet("orderkey"),
                            ImmutableMap.of(Optional.empty(), functionCall("count", false, ImmutableList.of())),
                            ImmutableList.of(),
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            node(
                                    ExchangeNode.class,
                                    anyTree(
                                            constrainedTableScan(
                                                    table,
                                                    ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11", "2021-07-12"))),
                                                    ImmutableMap.of("orderkey", "orderkey"))))));

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPatternWithoutCountOptimization);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testBaseToViewConversionCountOptimizationWithAllColumnsOnSamePartition()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false")
                .build();
        QueryRunner queryRunner = getQueryRunner();

        String table = "orders_partitioned";
        String view = "test_orders_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s " +
                            "WITH (partitioned_by = array['ds']) " +
                            "AS SELECT *, '2021-07-11' AS ds " +
                            "FROM orders " +
                            "WHERE orderkey < 10000 " +
                            "UNION ALL " +
                            "SELECT *, '2021-07-12' AS ds FROM orders WHERE orderkey < 10000",
                    table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT COUNT(*) AS a_count,  ds " +
                            "FROM %s " +
                            "GROUP BY ds",
                    view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            String viewQuery = format("SELECT SUM(a_count), ds FROM %s GROUP BY ds ORDER BY ds", view);
            String baseQuery = format("SELECT COUNT(*) AS a_count, ds FROM %s GROUP BY ds ORDER BY ds", table);

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds= '2021-07-11'", view), 1);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, optimizedQueryResult);

            PlanMatchPattern expectedPattern = anyTree(
                    aggregation(
                            ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("count"))),
                            SINGLE,
                            node(
                                    ExchangeNode.class,
                                    anyTree(
                                            project(
                                                    ImmutableMap.of("ds_43", expression("'2021-07-12'")),
                                                    aggregation(
                                                            ImmutableMap.of("count", functionCall("count", false, ImmutableList.of())),
                                                            SINGLE,
                                                            node(
                                                                    ExchangeNode.class,
                                                                    anyTree(
                                                                            node(
                                                                                    ProjectNode.class, constrainedTableScan(
                                                                                            table,
                                                                                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2021-07-12"))),
                                                                                            ImmutableMap.of()))))))),
                                    anyTree(
                                            constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of())))));

            assertPlan(queryOptimizationWithMaterializedView, viewQuery, expectedPattern);
            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test(enabled = false)
    public void testBaseToViewConversionWithFilterCondition()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String baseTable = "lineitem_partitioned_derived_fields";
        String view = "lineitem_partitioned_view_derived_fields";
        try {
            queryRunner.execute(format(
                    "CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'shipmode']) AS " +
                            "SELECT discount, extendedprice, orderkey, '2020-01-01' as ds, shipmode FROM lineitem WHERE orderkey < 1000 " +
                            "UNION ALL " +
                            "SELECT discount, extendedprice, orderkey, '2020-01-02' as ds, shipmode FROM lineitem WHERE orderkey > 1000",
                    baseTable));
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['mvds', 'shipmode']) AS " +
                            "SELECT discount, extendedprice, orderkey as ok, ds as mvds, shipmode " +
                            "FROM %s WHERE orderkey < 100 AND orderkey > 50",
                    view,
                    baseTable));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s where mvds='2020-01-01'", view), 50);
            String baseQuery = format(
                    "SELECT discount, extendedprice, orderkey, ds, shipmode FROM %s " +
                            "WHERE orderkey <= 70 AND orderkey >= 60 AND orderkey <> 65 ORDER BY extendedprice",
                    baseTable);
            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(optimizedQueryResult, baseQueryResult);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + baseTable);
        }
    }

    @Test
    public void testMaterializedViewForJoin()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "orders_key_partitioned_join";
        String table2 = "orders_price_partitioned_join";
        String view = "orders_view_join";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds FROM orders WHERE orderkey > 1000 and orderkey < 2000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2019-01-02' as ds FROM orders WHERE orderkey > 1000 and orderkey < 2000", table2));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, t1.ds " +
                    "FROM %s t1 inner join %s t2 ON (t1.ds=t2.ds AND t1.orderkey = t2.orderkey)", view, table1, table2));

            assertTrue(queryRunner.tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 255);

            String viewQuery = format("SELECT view_orderkey, view_totalprice, ds FROM %s WHERE view_orderkey <  10000 ORDER BY view_orderkey", view);
            String baseQuery = format("SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, t1.ds " +
                    "FROM %s t1 inner join  %s t2 ON (t1.ds=t2.ds AND t1.orderkey = t2.orderkey) " +
                    "WHERE t1.orderkey < 10000 ORDER BY t1.orderkey", table1, table2);
            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    project(
                            ImmutableMap.of("expr_33", expression("'2019-01-02'")),
                            join(INNER, ImmutableList.of(equiJoinClause("orderkey_7", "orderkey")),
                                    anyTree(constrainedTableScan(table2, ImmutableMap.of(), ImmutableMap.of("orderkey_7", "orderkey"))),
                                    anyTree(filter("orderkey < BIGINT'10000'", constrainedTableScan(table1,
                                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                                            ImmutableMap.of("orderkey", "orderkey")))))),
                    filter("view_orderkey < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testSubqueryMaterializedView()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();
        QueryRunner queryRunner = getQueryRunner();

        String table = "orders_partitioned";
        String view = "test_orders_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s " +
                            "WITH (partitioned_by = array['ds']) " +
                            "AS SELECT *, '2021-07-11' AS ds " +
                            "FROM orders " +
                            "WHERE orderkey < 10000 " +
                            "UNION ALL " +
                            "SELECT *, '2021-07-12' AS ds " +
                            "FROM orders " +
                            "WHERE orderkey < 10000",
                    table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT orderkey, ds " +
                            "FROM %s",
                    view, table));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-12'", view), 2503);

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            PlanMatchPattern expectedPattern = anyTree(
                    constrainedTableScan(table,
                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11"))),
                            ImmutableMap.of()),
                    constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_43", "orderkey")));

            String baseQuery = format("SELECT orderkey FROM %s ORDER BY orderkey", table);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, optimizedQueryResult);
            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);

            String queryWithSubquery = format("SELECT orderkey FROM (SELECT orderkey FROM %s) ORDER BY orderkey", table);

            MaterializedResult optimizedSubqueryResult = computeActual(queryOptimizationWithMaterializedView, queryWithSubquery);
            MaterializedResult subqueryResult = computeActual(queryWithSubquery);
            assertEquals(optimizedSubqueryResult, subqueryResult);
            assertPlan(queryOptimizationWithMaterializedView, queryWithSubquery, expectedPattern);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testSubqueryMaterializedViewWithMultipleViews()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();
        QueryRunner queryRunner = getQueryRunner();

        String lineItemTable = "lineitem_partitioned";
        String supplierTable = "suppliers_partitioned";
        String lineItemView1 = "test_lineitem_view";
        String lineItemView2 = "test_lineitem_view_2";
        String suppliersView = "test_suppliers_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s " +
                            "WITH (partitioned_by = array['ds']) " +
                            "AS SELECT *, '2021-07-11' AS ds " +
                            "FROM lineitem " +
                            "WHERE orderkey < 10000 " +
                            "UNION ALL " +
                            "SELECT *, '2021-07-12' AS ds " +
                            "FROM lineitem " +
                            "WHERE orderkey < 10000",
                    lineItemTable));

            queryRunner.execute(format("CREATE TABLE %s " +
                            "WITH (partitioned_by = array['ds']) " +
                            "AS SELECT *, '2021-07-11' AS ds " +
                            "FROM supplier " +
                            "WHERE suppkey < 10000 " +
                            "UNION ALL " +
                            "SELECT *, '2021-07-12' AS ds " +
                            "FROM supplier " +
                            "WHERE suppkey < 10000",
                    supplierTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT suppkey, SUM(quantity) as qty, ds " +
                            "FROM %s " +
                            "GROUP BY suppkey, ds",
                    lineItemView1, lineItemTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT suppkey, SUM(quantity) as returned_qty, ds " +
                            "FROM %s " +
                            "WHERE returnflag = 'R' " +
                            "GROUP BY suppkey, ds",
                    lineItemView2, lineItemTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT suppkey, name, ds\n " +
                            "FROM %s " +
                            "WHERE name != 'bob'",
                    suppliersView, supplierTable));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-12'", lineItemView1), 100);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-12'", lineItemView2), 100);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-11'", suppliersView), 100);

            assertTrue(getQueryRunner().tableExists(getSession(), lineItemView1));
            assertTrue(getQueryRunner().tableExists(getSession(), lineItemView2));
            assertTrue(getQueryRunner().tableExists(getSession(), suppliersView));

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, lineItemTable, ImmutableList.of(lineItemView1, lineItemView2));
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, supplierTable, ImmutableList.of(suppliersView));

            String baseQuery = format("WITH long_name_supp AS ( %n" +
                            "SELECT suppkey, name %n" +
                            "FROM %s %n" +
                            "WHERE name != 'bob'), " +
                            "supp_returns AS (%n" +
                            "SELECT suppkey, sum(quantity) AS returned_qty %n " +
                            "FROM %s %n" +
                            "WHERE returnflag = 'R' " +
                            "GROUP BY suppkey, ds), %n" +
                            "supp_sum AS (%n" +
                            "SELECT suppkey, SUM(quantity) AS qty %n " +
                            "FROM %s %n " +
                            "GROUP BY suppkey, ds) %n " +
                            "SELECT n.suppkey, n.name, r.returned_qty, s.qty %n " +
                            "FROM long_name_supp AS n %n " +
                            "LEFT JOIN supp_returns AS r ON n.suppkey = r.suppkey %n " +
                            "LEFT JOIN supp_sum AS s ON n.suppkey = s.suppkey %n " +
                            "ORDER BY suppkey, name",
                    supplierTable, lineItemTable, lineItemTable);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, optimizedQueryResult);

            PlanMatchPattern expectedPattern = anyTree(
                    node(JoinNode.class,
                            node(JoinNode.class,
                                    exchange(
                                            anyTree(
                                                    constrainedTableScan(supplierTable,
                                                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-12"))),
                                                            ImmutableMap.of())),
                                            anyTree(
                                                    constrainedTableScan(suppliersView,
                                                            ImmutableMap.of(),
                                                            ImmutableMap.of("suppkey_37", "suppkey", "name_38", "name")))),
                                    anyTree(
                                            constrainedTableScan(lineItemTable,
                                                    ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11", "2021-07-12"))),
                                                    ImmutableMap.of("ds_73", "ds", "suppkey_71", "suppkey")))),
                            exchange(
                                    anyTree(
                                            exchange(
                                                    anyTree(
                                                            constrainedTableScan(lineItemTable,
                                                                    ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11"))),
                                                                    ImmutableMap.of("suppkey_94", "suppkey", "quantity_96", "quantity"))),
                                                    anyTree(
                                                            constrainedTableScan(lineItemView1,
                                                                    ImmutableMap.of(),
                                                                    ImmutableMap.of("ds_193", "ds", "suppkey_192", "suppkey"))))))));

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + lineItemView1);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + lineItemView2);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + suppliersView);
            queryRunner.execute("DROP TABLE IF EXISTS " + lineItemTable);
            queryRunner.execute("DROP TABLE IF EXISTS " + supplierTable);
        }
    }

    @Test
    public void testSubqueryMaterializedViewAggregateWithAndJoin()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false")
                .build();
        QueryRunner queryRunner = getQueryRunner();

        String supplierTable = "supplier_partitioned";
        String lineItemTable = "lineitem_partitioned";
        String lineItemView = "test_lineitem_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s " +
                            "WITH (partitioned_by = array['ds']) " +
                            "AS SELECT *, '2021-07-11' AS ds " +
                            "FROM supplier " +
                            "WHERE suppkey < 10000 " +
                            "UNION ALL " +
                            "SELECT *, '2021-07-12' AS ds " +
                            "FROM supplier " +
                            "WHERE suppkey < 10000",
                    supplierTable));

            queryRunner.execute(format("CREATE TABLE %s " +
                            "WITH (partitioned_by = array['ds']) " +
                            "AS SELECT *, '2021-07-11' AS ds " +
                            "FROM lineitem " +
                            "WHERE quantity > 1 " +
                            "UNION ALL " +
                            "SELECT *, '2021-07-12' AS ds " +
                            "FROM lineitem " +
                            "WHERE quantity > 1",
                    lineItemTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT MIN(extendedprice) AS min_price, partkey, ds " +
                            "FROM %s " +
                            "GROUP BY partkey, ds ",
                    lineItemView, lineItemTable));

            assertTrue(getQueryRunner().tableExists(getSession(), lineItemView));

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, lineItemTable, ImmutableList.of(lineItemView));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-12'", lineItemView), 2000);

            String baseQuery = format(
                    "SELECT t1.name, t1.suppkey, low_cost.partkey %n" +
                            "FROM %s t1 %n" +
                            "LEFT JOIN (%n" +
                            "SELECT t2.partkey, t2.suppkey FROM %s t2 %n" +
                            "LEFT JOIN (SELECT MIN(extendedprice) AS min_price, partkey, ds FROM %s GROUP BY partkey, ds) mp %n" +
                            "ON mp.partkey = t2.partkey " +
                            "WHERE t2.extendedprice <= mp.min_price*1.05) " +
                            "low_cost " +
                            "ON low_cost.suppkey = t1.suppkey " +
                            "ORDER BY t1.name, t1.suppkey, low_cost.partkey", supplierTable, lineItemTable, lineItemTable);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, optimizedQueryResult);

            PlanMatchPattern expectedPattern = anyTree(
                    node(JoinNode.class,
                            anyTree(
                                    constrainedTableScan(supplierTable,
                                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11", "2021-07-12"))),
                                            ImmutableMap.of())),
                            anyTree(
                                    node(JoinNode.class,
                                            anyTree(
                                                    constrainedTableScan(lineItemTable,
                                                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11", "2021-07-12"))),
                                                            ImmutableMap.of("suppkey_0", "suppkey", "extendedprice", "extendedprice"))),
                                            anyTree(
                                                    exchange(
                                                            anyTree(
                                                                    constrainedTableScan(lineItemTable,
                                                                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2021-07-11"))),
                                                                            ImmutableMap.of("partkey_7", "partkey", "extendedprice_11", "extendedprice"))),
                                                            anyTree(
                                                                    constrainedTableScan(lineItemView,
                                                                            ImmutableMap.of(),
                                                                            ImmutableMap.of("ds_103", "ds", "partkey_102", "partkey")))))))));

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + lineItemView);
            queryRunner.execute("DROP TABLE IF EXISTS " + lineItemTable);
            queryRunner.execute("DROP TABLE IF EXISTS " + supplierTable);
        }
    }

    @Test
    public void TestMaterializedViewForMultiWayJoin()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "orders_key_partitioned_join";
        String table2 = "orders_price_partitioned_join";
        String table3 = "orders_status_partitioned_join";
        String view = "orders_view_join";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, '2020-01-01' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' AS ds FROM orders WHERE orderkey > 1000 AND orderkey < 2000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2020-01-01' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2019-01-02' AS ds FROM orders WHERE orderkey > 1000 AND orderkey < 2000", table2));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderstatus, '2020-01-01' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderstatus, '2019-01-02' AS ds FROM orders WHERE orderkey > 1000 AND orderkey < 2000", table3));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                            "SELECT t1.orderkey AS view_orderkey, t2.totalprice AS view_totalprice, t3.orderstatus AS view_orderstatus, t1.ds " +
                            "FROM %s t1 INNER JOIN %s t2 ON (t1.ds=t2.ds AND t1.orderkey = t2.orderkey) INNER JOIN %s t3 " +
                            "ON (t1.ds = t3.ds AND t1.orderkey = t3.orderkey)",
                    view, table1, table2, table3));

            assertQueryFails(format("CREATE MATERIALIZED VIEW should_fail WITH (partitioned_by = ARRAY['ds']) AS " +
                                    "SELECT t1.orderkey AS view_orderkey, t2.totalprice AS view_totalprice, t3.orderstatus AS view_orderstatus, t1.ds " +
                                    "FROM %s t1 INNER JOIN %s t2 ON (t1.ds=t2.ds AND t1.orderkey = t2.orderkey) INNER JOIN %s t3" +
                                    " ON (t1.orderkey = t3.orderkey)",
                            table1, table2, table3),
                    "Materialized view tpch.should_fail must have at least one partition column" +
                            " that exists in orders_status_partitioned_join as well");

            assertTrue(queryRunner.tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 255);

            String viewQuery = format("SELECT view_orderkey, view_totalprice, view_orderstatus, ds FROM %s WHERE view_orderkey <  10000 ORDER BY view_orderkey", view);

            String baseQuery = format("SELECT t1.orderkey AS view_orderkey, t2.totalprice AS view_totalprice, t3.orderstatus" +
                    " AS view_orderstatus, t1.ds " +
                    "FROM %s t1 INNER JOIN %s t2 ON (t1.ds=t2.ds AND t1.orderkey = t2.orderkey) INNER JOIN %s t3" +
                    " ON (t1.ds = t3.ds AND t1.orderkey = t3.orderkey) " +
                    "WHERE t1.orderkey < 10000 ORDER BY t1.orderkey", table1, table2, table3);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);

            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery,
                    anyTree(
                            project(
                                    ImmutableMap.of("expr_56", expression("'2019-01-02'")),
                                    join(INNER, ImmutableList.of(equiJoinClause("orderkey_7", "orderkey")),
                                            anyTree(
                                                    filter("orderkey_7 < BIGINT'10000'",
                                                            constrainedTableScan(table2, ImmutableMap.of(), ImmutableMap.of("orderkey_7", "orderkey")))),
                                            anyTree(
                                                    join(INNER, ImmutableList.of(equiJoinClause("orderkey_28", "orderkey")),
                                                            anyTree(
                                                                    filter("orderkey_28 < BIGINT'10000'",
                                                                            constrainedTableScan(table3, ImmutableMap.of(), ImmutableMap.of("orderkey_28", "orderkey")))),
                                                            anyTree(
                                                                    filter("orderkey < BIGINT'10000'",
                                                                            constrainedTableScan(table1,
                                                                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                                                                                    ImmutableMap.of("orderkey", "orderkey")))))))),
                            filter("view_orderkey < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
            queryRunner.execute("DROP TABLE IF EXISTS " + table3);
        }
    }

    @Test
    public void testMaterializedViewOptimizationWithDoublePartition()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_double_partition";
        String view = "orders_view_double_partition";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['totalprice']) AS " +
                    "SELECT orderkey, orderpriority, totalprice FROM orders WHERE orderkey < 10 ", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['totalprice']) " +
                    "AS SELECT orderkey, orderpriority, totalprice FROM %s", view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE totalprice<65000", view), 3);

            String viewQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", view);
            String baseQuery = format("SELECT orderkey from %s where orderkey < 10000 ORDER BY orderkey", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    filter("orderkey < BIGINT'10000'", constrainedTableScan(table,
                            ImmutableMap.of("totalprice", multipleValues(DOUBLE, ImmutableList.of(105367.67, 172799.49, 205654.3, 271885.66))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewOptimizationWithUnsupportedFunctionSubquery()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned";
        String table2 = "lineitem_partitioned";
        String view = "orders_view";
        String view2 = "lineitem_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, comment, '2020-01-01' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, comment, '2019-01-02' AS ds FROM orders WHERE orderkey > 1000 AND orderkey < 2000", table));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, quantity, '2020-01-01' AS ds FROM lineitem WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, quantity, '2019-01-02' AS ds FROM lineitem WHERE orderkey > 1000 AND orderkey < 2000", table2));

            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT max(length(comment)) as longest_comment, orderkey, ds FROM %s GROUP BY ds, orderkey", view, table));

            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT sum(quantity) as total_quantity, orderkey, ds FROM %s GROUP BY ds, orderkey", view2, table2));

            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertTrue(getQueryRunner().tableExists(getSession(), view2));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 255);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2019-01-02'", view2), 248);

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table2, ImmutableList.of(view2));

            String baseQuery = format("SELECT * FROM " +
                    "(SELECT ds, orderkey, max(length(comment)) as longest_comment FROM %s GROUP BY ds, orderkey) s1 " +
                    "INNER JOIN " +
                    "(SELECT ds, orderkey, sum(quantity) as total_quantity FROM %s GROUP BY ds, orderkey) s2 " +
                    "ON s1.orderkey = s2.orderkey " +
                    "ORDER BY s1.orderkey, longest_comment", table, table2);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, optimizedQueryResult);

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, anyTree(
                    node(JoinNode.class,
                            anyTree(
                                    exchange(
                                            anyTree(
                                                    constrainedTableScan(table2,
                                                            ImmutableMap.of(),
                                                            ImmutableMap.of("orderkey_13", "orderkey"))),
                                            anyTree(
                                                    constrainedTableScan(view2,
                                                            ImmutableMap.of(),
                                                            ImmutableMap.of("ds_42", "ds", "orderkey_41", "orderkey"))))),
                            exchange(
                                    anyTree(
                                            constrainedTableScan(table,
                                                    ImmutableMap.of(),
                                                    ImmutableMap.of()))))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view2);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewPartitionKeyFilter()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned";
        String view = "orders_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2020-01-01' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2020-01-02' AS ds FROM orders WHERE orderkey > 1000 AND orderkey < 2000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2020-01-03' AS ds FROM orders WHERE orderkey > 2000 AND orderkey < 3000", table));

            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT max(totalprice) as max_price, orderkey, ds FROM %s GROUP BY orderkey, ds", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 255);

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            String baseQuery = format("SELECT max(totalprice) as max_price, orderkey FROM %s GROUP BY orderkey ORDER BY orderkey", table);

            String viewQuery = format("SELECT max_price, orderkey FROM %s GROUP BY orderkey, max_price ORDER BY orderkey", view);

            MaterializedResult viewQueryResult = computeActual(viewQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, viewQueryResult);

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(1))
                    .build();

            assertPlan(session, viewQuery, anyTree(
                    constrainedTableScan(
                            table,
                            ImmutableMap.of(),
                            ImmutableMap.of())));

            // When filtering out a stale partition which sets missing partitions <= threshold, expect optimization to occur
            String viewQueryWithFilterOnPartitionKey = format("SELECT max_price, orderkey FROM %s WHERE ds < '2020-01-03' ORDER BY orderkey", view);
            String baseQueryWithFilterOnPartitionkey = format("SELECT max(totalprice) as max_price, orderkey FROM %s " +
                    "WHERE ds < '2020-01-03' " +
                    "GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult baseQueryResultWithFilter = computeActual(session, baseQueryWithFilterOnPartitionkey);
            MaterializedResult viewQueryResultWithFilter = computeActual(session, viewQueryWithFilterOnPartitionKey);

            assertEquals(baseQueryResultWithFilter, viewQueryResultWithFilter);

            assertPlan(session, viewQueryWithFilterOnPartitionKey, anyTree(exchange(
                    anyTree(constrainedTableScan(
                            table,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))),
                            ImmutableMap.of())),
                    constrainedTableScan(
                            view,
                            ImmutableMap.of(),
                            ImmutableMap.of()))));

            Session queryOptimizationWithMaterializedView = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(1))
                    .build();

            MaterializedResult baseQueryResultWithFilterAndOptimization = computeActual(queryOptimizationWithMaterializedView, baseQueryWithFilterOnPartitionkey);

            assertEquals(baseQueryResultWithFilterAndOptimization, viewQueryResultWithFilter);

            assertPlan(queryOptimizationWithMaterializedView, baseQueryWithFilterOnPartitionkey, anyTree(
                    exchange(
                            anyTree(constrainedTableScan(
                                    table,
                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))),
                                    ImmutableMap.of())),
                            anyTree(constrainedTableScan(
                                    view,
                                    ImmutableMap.of(),
                                    ImmutableMap.of())))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewPartitionKeyFilterWithRenamedFilterColumn()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned";
        String view = "orders_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2020-01-01' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2020-01-02' AS ds FROM orders WHERE orderkey > 1000 AND orderkey < 2000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2020-01-03' AS ds FROM orders WHERE orderkey > 2000 AND orderkey < 3000", table));

            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds_mv']) AS " +
                    "SELECT max(totalprice) as max_price, orderkey, ds AS ds_mv FROM %s GROUP BY orderkey, ds", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds_mv='2020-01-01'", view), 255);

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(1))
                    .build();

            String viewQueryWithFilterOnPartitionKey = format("SELECT max_price, orderkey FROM %s WHERE ds_mv < '2020-01-03' ORDER BY orderkey", view);
            String baseQueryWithFilterOnPartitionkey = format("SELECT max(totalprice) as max_price, orderkey FROM %s " +
                    "WHERE ds < '2020-01-03' " +
                    "GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult baseQueryResultWithFilter = computeActual(session, baseQueryWithFilterOnPartitionkey);
            MaterializedResult viewQueryResultWithFilter = computeActual(session, viewQueryWithFilterOnPartitionKey);

            assertEquals(baseQueryResultWithFilter, viewQueryResultWithFilter);

            assertPlan(session, viewQueryWithFilterOnPartitionKey, anyTree(exchange(
                    anyTree(constrainedTableScan(
                            table,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))),
                            ImmutableMap.of())),
                    constrainedTableScan(
                            view,
                            ImmutableMap.of(),
                            ImmutableMap.of()))));

            Session queryOptimizationWithMaterializedView = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(1))
                    .build();

            MaterializedResult baseQueryResultWithFilterAndOptimization = computeActual(queryOptimizationWithMaterializedView, baseQueryWithFilterOnPartitionkey);

            assertEquals(baseQueryResultWithFilterAndOptimization, viewQueryResultWithFilter);

            assertPlan(queryOptimizationWithMaterializedView, baseQueryWithFilterOnPartitionkey, anyTree(
                    exchange(
                            anyTree(constrainedTableScan(
                                    table,
                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))),
                                    ImmutableMap.of())),
                            anyTree(constrainedTableScan(
                                    view,
                                    ImmutableMap.of(),
                                    ImmutableMap.of())))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewAvgRewrite()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned";
        String view = "orders_view_sum_count";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2020-01-01' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2020-01-02' AS ds FROM orders WHERE orderkey > 1000 AND orderkey < 2000 ", table));

            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT sum(totalprice) AS sum_price, count(totalprice) AS price_count, orderkey, ds FROM %s GROUP BY orderkey, ds", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2020-01-01'", view), 255);
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            String baseQuery = format("SELECT avg(totalprice) as base_avg_price, orderkey FROM %s GROUP BY orderkey ORDER BY orderkey", table);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            MaterializedResult optimizedQueryResultSumCount = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            assertEquals(optimizedQueryResultSumCount, baseQueryResult);

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, anyTree(exchange(
                    anyTree(constrainedTableScan(
                            table,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))),
                            ImmutableMap.of())),
                    anyTree(constrainedTableScan(
                            view,
                            ImmutableMap.of(),
                            ImmutableMap.of())))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewPartitionFilteringCaseInsensitive()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_case_test";
        String view = "orders_view_case_test";

        try {
            // Create a table partitioned by 'country' (lowercase)
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['country']) AS " +
                    "SELECT orderkey, totalprice, 'US' AS country FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, 'UK' AS country FROM orders WHERE orderkey >= 1000 AND orderkey < 2000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, 'CA' AS country FROM orders WHERE orderkey >= 2000 AND orderkey < 3000", table));

            // Create a materialized view partitioned by 'country'
            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['country']) AS " +
                    "SELECT max(totalprice) as max_price, orderkey, country FROM %s GROUP BY orderkey, country", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            // Only refresh partitions for 'US' and 'UK', leaving 'CA' missing
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE country='US'", view), 255);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE country='UK'", view), 248);

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(1))
                    .build();

            // Query with UPPERCASE column name, filtering for countries that ARE in the MV
            // This tests that case-insensitive lookup works correctly
            String viewQueryWithUpperCaseFilter = format("SELECT max_price, orderkey FROM %s WHERE COUNTRY >= 'UK' AND COUNTRY <= 'US' ORDER BY orderkey", view);
            String viewQueryWithLowerCaseFilter = format("SELECT max_price, orderkey FROM %s WHERE country >= 'UK' AND country <= 'US' ORDER BY orderkey", view);
            String baseQueryWithFilter = format("SELECT max(totalprice) as max_price, orderkey FROM %s " +
                    "WHERE country >= 'UK' AND country <= 'US' " +
                    "GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult baseQueryResult = computeActual(session, baseQueryWithFilter);
            MaterializedResult viewQueryUpperCaseResult = computeActual(session, viewQueryWithUpperCaseFilter);
            MaterializedResult viewQueryLowerCaseResult = computeActual(session, viewQueryWithLowerCaseFilter);

            // Both queries should return the same results
            assertEquals(baseQueryResult, viewQueryUpperCaseResult);
            assertEquals(baseQueryResult, viewQueryLowerCaseResult);

            // The plan should use the materialized view for countries UK and US (both are refreshed)
            // and should NOT count the missing 'CA' partition because the query filter excludes it
            assertPlan(session, viewQueryWithUpperCaseFilter, anyTree(
                    constrainedTableScan(
                            view,
                            ImmutableMap.of("country", multipleValues(createVarcharType(2), utf8Slices("UK", "US"))),
                            ImmutableMap.of())));
            assertPlan(session, viewQueryWithLowerCaseFilter, anyTree(
                    constrainedTableScan(
                            view,
                            ImmutableMap.of("country", multipleValues(createVarcharType(2), utf8Slices("UK", "US"))),
                            ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewMissingPartitionsCountWithMultiplePartitionColumns()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_multi_partition_count_test";
        String view = "orders_view_multi_partition_count_test";

        try {
            // Create a table partitioned by TWO columns: 'country' and 'region'
            queryRunner.execute(format("CREATE TABLE %s (id BIGINT, price DOUBLE, country VARCHAR, region VARCHAR) " +
                    "WITH (partitioned_by = ARRAY['country', 'region'])", table));

            // Insert data into 4 different partitions
            assertUpdate(format("INSERT INTO %s VALUES (1, 100.0, 'US', 'West'), (2, 200.0, 'US', 'West')", table), 2);
            assertUpdate(format("INSERT INTO %s VALUES (3, 300.0, 'US', 'East'), (4, 400.0, 'US', 'East')", table), 2);
            assertUpdate(format("INSERT INTO %s VALUES (5, 500.0, 'UK', 'North'), (6, 600.0, 'UK', 'North')", table), 2);
            assertUpdate(format("INSERT INTO %s VALUES (7, 700.0, 'UK', 'South'), (8, 800.0, 'UK', 'South')", table), 2);

            // Create a materialized view partitioned by both columns
            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['country', 'region']) AS " +
                    "SELECT max(price) as max_price, id, country, region FROM %s GROUP BY id, country, region", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            // Only refresh 2 out of 4 partitions, leaving 2 missing (UK/North and UK/South are missing)
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE country='US' AND region='West'", view), 2);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE country='US' AND region='East'", view), 2);

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            // Set the threshold to 2 missing partitions to test that the counted missingPartitions is 2
            Session sessionWithThreshold2 = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(2))
                    .build();

            String baseQuery = format("SELECT max(price) as max_price, id FROM %s GROUP BY id ORDER BY id", table);

            // With threshold = 2 and 2 missing partitions, the materialized view should still be used
            assertPlan(sessionWithThreshold2, baseQuery, anyTree(exchange(
                    anyTree(constrainedTableScan(
                            table,
                            ImmutableMap.of(),
                            ImmutableMap.of())),
                    anyTree(constrainedTableScan(
                            view,
                            ImmutableMap.of(),
                            ImmutableMap.of())))));

            // Now set threshold to 1 - should fall back to base table since we have 2 missing partitions
            Session sessionWithThreshold1 = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(1))
                    .build();

            // With threshold = 1 and 2 missing partitions, should use only the base table
            assertPlan(sessionWithThreshold1, baseQuery, anyTree(
                    constrainedTableScan(table, ImmutableMap.of(), ImmutableMap.of())));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewApproxDistinctRewrite()
    {
        Session queryOptimizationWithMaterializedView = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned";
        String view = "orders_view";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, custkey, '2020-01-01' AS ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, custkey, '2020-01-02' AS ds FROM orders WHERE orderkey > 1000 AND orderkey < 2000 ", table));

            queryRunner.execute(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT CAST(approx_set(custkey) AS varbinary) AS customers, orderkey, ds FROM %s GROUP BY orderkey, ds", view, table));

            assertTrue(getQueryRunner().tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds = '2020-01-01'", view), 255);

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            String baseQuery = format("SELECT approx_distinct(custkey) as approx_customers, orderkey FROM %s GROUP BY orderkey ORDER BY orderkey", table);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(optimizedQueryResult, baseQueryResult);

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, anyTree(exchange(
                    anyTree(constrainedTableScan(
                            table,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))),
                            ImmutableMap.of())),
                    anyTree(constrainedTableScan(
                            view,
                            ImmutableMap.of(),
                            ImmutableMap.of())))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewForJoinWithMultiplePartitions()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "order_view_join_with_multiple_partitions";
        String table1 = "orders_key_partitioned_join_with_multiple_partitions";
        String table2 = "orders_price_partitioned_join_with_multiple_partitions";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderpriority']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds, orderpriority FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds , orderpriority FROM orders WHERE orderkey > 1000 and orderkey < 2000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderstatus']) AS " +
                    "SELECT totalprice, '2020-01-01' as ds, orderstatus FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT totalprice, '2019-01-02' as ds, orderstatus FROM orders WHERE orderkey > 1000 and orderkey < 2000", table2));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'view_orderpriority', 'view_orderstatus']) " +
                    "AS SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, " +
                    "t1.ds as ds, t1.orderpriority as view_orderpriority, t2.orderstatus as view_orderstatus " +
                    " FROM %s t1 inner join %s t2 ON t1.ds=t2.ds", view, table1, table2));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 65025);

            String viewQuery = format("SELECT view_orderkey from %s where view_orderkey < 10000 ORDER BY view_orderkey", view);
            String baseQuery = format("SELECT t1.orderkey FROM %s t1 inner join %s t2 ON t1.ds=t2.ds where t1.orderkey < 10000 ORDER BY t1.orderkey", table1, table2);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    join(INNER, ImmutableList.of(),
                            filter("orderkey < BIGINT'10000'", constrainedTableScan(table1,
                                    ImmutableMap.of(
                                            "ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02")),
                                            "orderpriority", multipleValues(createVarcharType(15), utf8Slices("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"))),
                                    ImmutableMap.of("orderkey", "orderkey"))),
                            anyTree(constrainedTableScan(table2, ImmutableMap.of()))),
                    filter("view_orderkey < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewInvalidLeftOuterJoin()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "view_invalid_left_outer_join";
        String table1 = "t1_invalid_left_outer_join";
        String table2 = "t2_invalid_left_outer_join";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS SELECT 1 as a, '2020-01-01' as ds", table1));
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS SELECT 1 as a, '2020-01-01' as ds", table2));

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['t1_ds', 't2_ds']) " +
                            "AS SELECT t1.a as t1_a, t2.a as t2_a, t1.ds as t1_ds, t2.ds as t2_ds FROM %s t1 LEFT JOIN %s t2 ON t1.a = t2.a", view, table1, table2),
                    ".*must have at least one common partition equality constraint.*");

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['t2_ds']) " +
                            "AS SELECT t1.a as t1_a, t2.a as t2_a, t2.ds as t2_ds FROM %s t1 LEFT JOIN %s t2 ON t1.ds = t2.ds", view, table1, table2),
                    ".*must have at least one partition column that exists in.*");
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewWithLimit()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "view_with_limit";
        String table = "t1_with_limit";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS SELECT 1 as a, '2020-01-01' as ds", table));

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT a, ds FROM %s t1 LIMIT 10000", view, table),
                    ".*LIMIT clause in materialized view is not supported.*");
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewForLeftOuterJoin()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "orders_key_partitioned_left_outer_join";
        String table2 = "orders_price_partitioned_left_outer_join";
        String view = "orders_view_left_outer_join";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, '2019-01-01' as ds FROM orders WHERE orderkey < 1500 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds FROM orders WHERE orderkey > 1500 and orderkey < 2000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, totalprice, '2019-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, totalprice, '2019-01-02' as ds FROM orders WHERE orderkey > 1000 and orderkey < 2000", table2));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['t1_ds', 't2_ds']) AS " +
                    "SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, t1.ds as t1_ds, t2.ds as t2_ds " +
                    "FROM %s t1 left join %s t2 ON (t1.ds=t2.ds AND t1.orderkey = t2.orderkey)", view, table1, table2));

            assertTrue(queryRunner.tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE t1_ds='2019-01-01'", view), 375);

            String viewQuery = format("SELECT view_orderkey, view_totalprice, t1_ds FROM %s WHERE view_orderkey <  10000 ORDER BY view_orderkey", view);
            String baseQuery = format("SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, t1.ds " +
                    "FROM %s t1 left join  %s t2 ON (t1.ds=t2.ds AND t1.orderkey = t2.orderkey) " +
                    "WHERE t1.orderkey < 10000 ORDER BY t1.orderkey", table1, table2);
            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    project(
                            join(LEFT, ImmutableList.of(equiJoinClause("expr_6", "expr_23"), equiJoinClause("orderkey", "orderkey_7")),
                                    anyTree(
                                            project(
                                                    ImmutableMap.of("expr_6", expression("'2019-01-02'")),
                                                    filter("orderkey < BIGINT'10000'",
                                                            constrainedTableScan(table1,
                                                                    ImmutableMap.of(
                                                                            "ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                                                                    ImmutableMap.of("orderkey", "orderkey"))))),
                                    anyTree(
                                            project(ImmutableMap.of("expr_23", expression("'2019-01-02'")),
                                                    anyTree(
                                                            constrainedTableScan(table2, ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))), ImmutableMap.of("totalprice", "totalprice", "orderkey_7", "orderkey"))))))),
                    filter("view_orderkey < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewFullOuterJoin()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "order_view_full_outer_join";
        String table1 = "orders_key_partitioned_full_outer_join";
        String table2 = "orders_price_partitioned_full_outer_join";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderpriority']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds, orderpriority FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds , orderpriority FROM orders WHERE orderkey > 1000 and orderkey < 2000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderstatus']) AS " +
                    "SELECT totalprice, '2020-01-01' as ds, orderstatus FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT totalprice, '2019-01-02' as ds, orderstatus FROM orders WHERE orderkey > 1000 and orderkey < 2000", table2));

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds', 'view_orderpriority', 'view_orderstatus']) " +
                            "AS SELECT t1.orderkey as view_orderkey, t2.totalprice as view_totalprice, " +
                            "t1.ds as ds, t1.orderpriority as view_orderpriority, t2.orderstatus as view_orderstatus " +
                            " FROM %s t1 full outer join %s t2 ON t1.ds=t2.ds", view, table1, table2),
                    ".*Only inner join, left join and cross join unnested are supported for materialized view.*");
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewSameTableTwice()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "same_table";
        String view = "same_table_twice";

        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS SELECT 1 as a, '2020-01-01' as ds", table));

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT t1.a, t1.ds FROM %s t1 UNION ALL SELECT t2.a, t2.ds FROM %s t2", view, table, table), ".*Materialized View definition does not support multiple instances of same table*");
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewOrderBy()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned";
        String view = "test_orders_view";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table));

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM %s order by orderkey", view, table), ".*OrderBy are not supported for materialized view.*");
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewSubqueryShapes()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view1 = "orders_key_view1";
        String view2 = "orders_key_view2";
        String view3 = "orders_key_view3";
        String view4 = "orders_key_view4";
        String table1 = "orders_key_partitioned_1";
        String table2 = "orders_key_partitioned_2";
        String table3 = "orders_key_partitioned_3";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS SELECT 1 as a, '2020-01-01' as ds", table1));
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS SELECT 1 as a, '2020-01-01' as ds", table2));
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS SELECT 1 as a, '2020-01-01' as ds", table3));

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT t1.a, t1.ds FROM %s t1 WHERE (t1.a IN (SELECT t2.a FROM %s t2 WHERE t1.ds = t2.ds))", view1, table1, table2),
                    ".*Subqueries are not supported for materialized view.*");

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT t1.a, t1.ds FROM %s t1 join (select t2.ds AS t2_ds, t2.a from %s t2 where (t2.a IN (SELECT t3.a FROM %s t3 WHERE t2.ds = t3.ds))) ON t1.ds = t2_ds", view2, table1, table2, table3),
                    ".*Subqueries are not supported for materialized view.*");

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT t1.a, t1.ds FROM %s t1 join (select t2.ds AS t2_ds, t2.a from %s t2 where t2.a <= 420) ON t1.ds = t2_ds", view3, table1, table2));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view1);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view2);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view3);
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view4);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
            queryRunner.execute("DROP TABLE IF EXISTS " + table3);
        }
    }

    @Test
    public void testMaterializedViewLateralJoin()
    {
        QueryRunner queryRunner = getQueryRunner();
        String view = "order_view_lateral_join";
        String table1 = "orders_key_partitioned_lateral_join";
        String table2 = "orders_price_partitioned_lateral_join";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderpriority']) AS " +
                    "SELECT orderkey, '2020-01-01' as ds, orderpriority FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, '2019-01-02' as ds , orderpriority FROM orders WHERE orderkey > 1000 and orderkey < 2000", table1));

            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds', 'orderstatus']) AS " +
                    "SELECT totalprice, '2020-01-01' as ds, orderstatus FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT totalprice, '2019-01-02' as ds, orderstatus FROM orders WHERE orderkey > 1000 and orderkey < 2000", table2));

            assertQueryFails(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT t1.ds FROM %s t1, LATERAL(SELECT t2.ds, t2.orderstatus AS view_orderstatus, t1.orderpriority AS view_orderpriority FROM %s t2 WHERE t1.ds = t2.ds)", view, table1, table2),
                    ".*Only inner join, left join and cross join unnested are supported for materialized view.*");
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewForCrossJoinUnnest()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_key_cross_join_unnest";
        String view = "orders_view_cross_join_unnest";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, ARRAY['MEDIUM', 'LOW'] as volume, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, ARRAY['HIGH'] as volume, '2019-01-02' as ds FROM orders WHERE orderkey > 1000 and orderkey < 2000", table));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey AS view_orderkey, unnested.view_volume, ds " +
                    "FROM %s CROSS JOIN UNNEST (volume) AS unnested(view_volume)", view, table));

            assertTrue(queryRunner.tableExists(getSession(), view));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 510);

            String viewQuery = format("SELECT view_orderkey, view_volume, ds from %s where view_orderkey < 10000 ORDER BY view_orderkey, view_volume", view);
            String baseQuery = format("SELECT orderkey AS view_orderkey, unnested.view_volume, ds " +
                    "FROM %s CROSS JOIN UNNEST (volume) AS unnested(view_volume) " +
                    "WHERE orderkey < 10000 ORDER BY orderkey, view_volume", table);
            assertEquals(computeActual(viewQuery), computeActual(baseQuery));

            assertPlan(getSession(), viewQuery, anyTree(
                    project(
                            ImmutableMap.of("ds_17", expression("'2019-01-02'")),
                            unnest(filter("orderkey < BIGINT'10000'", constrainedTableScan(table,
                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                                    ImmutableMap.of("orderkey", "orderkey"))))),
                    filter("view_orderkey < BIGINT'10000'", constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testInsertBySelectingFromMaterializedView()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table1 = "orders_partitioned_source";
        String table2 = "orders_partitioned_target";
        String table3 = "orders_from_mv";
        String view = "test_orders_view";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000", table1));
            assertTrue(getQueryRunner().tableExists(getSession(), table1));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) AS SELECT orderkey, orderpriority, ds FROM %s", view, table1));
            assertTrue(getQueryRunner().tableExists(getSession(), view));

            assertUpdate(format("CREATE TABLE %s AS SELECT * FROM %s WHERE 1=0", table2, table1), 0);
            assertTrue(getQueryRunner().tableExists(getSession(), table2));

            assertQueryFails(format("CREATE TABLE %s AS SELECT * FROM %s", table3, view),
                    ".*CreateTableAsSelect by selecting from a materialized view \\w+ is not supported.*");

            assertUpdate(format("INSERT INTO %s VALUES(99999, '1-URGENT', '2019-01-02')", table2), 1);
            assertUpdate(format("INSERT INTO %s SELECT * FROM %s WHERE ds = '2020-01-01'", table2, table1), 255);
            assertQueryFails(format("INSERT INTO %s SELECT * FROM %s WHERE ds = '2020-01-01'", table2, view),
                    ".*Insert by selecting from a materialized view \\w+ is not supported.*");
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table1);
            queryRunner.execute("DROP TABLE IF EXISTS " + table2);
        }
    }

    @Test
    public void testMaterializedViewQueryAccessControl()
    {
        QueryRunner queryRunner = getQueryRunner();

        Session invokerStichingSession = Session.builder(getSession())
                .setIdentity(new Identity("test_view_invoker", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .build();

        /* Non-stitching session test is needed as the query is not rewritten
        with base table. In this case the analyzer should process the materialized view
        definition sql to check all the base tables permissions.
        */
        Session invokerNonStichingSession = Session.builder(getSession())
                .setIdentity(new Identity("test_view_invoker2", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setSystemProperty(MATERIALIZED_VIEW_DATA_CONSISTENCY_ENABLED, "false")
                .build();

        Session ownerSession = getSession();

        queryRunner.execute(
                ownerSession,
                "CREATE TABLE test_orders_base WITH (partitioned_by = ARRAY['orderstatus']) " +
                        "AS SELECT orderkey, custkey, totalprice, orderstatus FROM orders LIMIT 10");
        queryRunner.execute(
                ownerSession,
                "CREATE MATERIALIZED VIEW test_orders_view " +
                        "WITH (partitioned_by = ARRAY['orderstatus']) " +
                        "AS SELECT SUM(totalprice) AS totalprice, orderstatus FROM test_orders_base GROUP BY orderstatus");
        setReferencedMaterializedViews((DistributedQueryRunner) getQueryRunner(), "test_orders_base", ImmutableList.of("test_orders_view"));

        try {
            // Check for both the direct materialized view query and the base table query optimization with materialized view
            // for direct materialized view query, check when stitching is enabled/disabled
            String queryMaterializedView = "SELECT totalprice, orderstatus FROM test_orders_view";
            String queryBaseTable = "SELECT SUM(totalprice) AS totalprice, orderstatus FROM test_orders_base GROUP BY orderstatus";

            assertAccessDenied(
                    invokerNonStichingSession,
                    queryBaseTable,
                    "Cannot select from columns \\[.*\\] in table .*test_orders_base.*",
                    privilege(invokerNonStichingSession.getUser(), "test_orders_base", SELECT_COLUMN));

            assertAccessDenied(
                    invokerStichingSession,
                    queryBaseTable,
                    "Cannot select from columns \\[.*\\] in table .*test_orders_base.*",
                    privilege(invokerStichingSession.getUser(), "test_orders_base", SELECT_COLUMN));

            assertAccessAllowed(
                    invokerStichingSession,
                    queryMaterializedView,
                    privilege(invokerStichingSession.getUser(), "test_orders_view", SELECT_COLUMN));

            assertAccessDenied(
                    invokerNonStichingSession,
                    queryMaterializedView,
                    "Cannot select from columns \\[.*\\] in table .*test_orders_base.*",
                    privilege(invokerNonStichingSession.getUser(), "test_orders_base", SELECT_COLUMN));

            // Test when the materialized view is partially materialized
            queryRunner.execute(ownerSession, "REFRESH MATERIALIZED VIEW test_orders_view WHERE orderstatus = 'F'");
            assertAccessAllowed(
                    invokerStichingSession,
                    queryMaterializedView,
                    privilege(invokerStichingSession.getUser(), "test_orders_view", SELECT_COLUMN));

            assertAccessDenied(
                    invokerNonStichingSession,
                    queryMaterializedView,
                    "Cannot select from columns \\[.*\\] in table .*test_orders_base.*",
                    privilege(invokerNonStichingSession.getUser(), "test_orders_base", SELECT_COLUMN));

            // Test when the materialized view is fully materialized
            queryRunner.execute(ownerSession, "REFRESH MATERIALIZED VIEW test_orders_view WHERE orderstatus <> 'F'");
            assertAccessAllowed(
                    invokerStichingSession,
                    queryMaterializedView,
                    privilege(invokerStichingSession.getUser(), "test_orders_view", SELECT_COLUMN));

            assertAccessDenied(
                    invokerNonStichingSession,
                    queryMaterializedView,
                    "Cannot select from columns \\[.*\\] in table .*test_orders_base.*",
                    privilege(invokerNonStichingSession.getUser(), "test_orders_base", SELECT_COLUMN));
        }
        finally {
            queryRunner.execute(ownerSession, "DROP MATERIALIZED VIEW test_orders_view");
            queryRunner.execute(ownerSession, "DROP TABLE test_orders_base");
        }
    }

    @Test
    public void testRefreshMaterializedViewAccessControl()
    {
        QueryRunner queryRunner = getQueryRunner();
        Session invokerSession = Session.builder(getSession())
                .setIdentity(new Identity("test_view_invoker", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();
        Session ownerSession = getSession();

        queryRunner.execute(
                ownerSession,
                "CREATE TABLE test_orders_base WITH (partitioned_by = ARRAY['orderstatus']) " +
                        "AS SELECT orderkey, custkey, totalprice, orderstatus FROM orders LIMIT 10");
        queryRunner.execute(
                ownerSession,
                "CREATE MATERIALIZED VIEW test_orders_view " +
                        "WITH (partitioned_by = ARRAY['orderstatus']) " +
                        "AS SELECT orderkey, totalprice, orderstatus FROM test_orders_base");

        String refreshMaterializedView = "REFRESH MATERIALIZED VIEW test_orders_view WHERE orderstatus = 'F'";

        try {
            // Verify that refresh checks the owner's permission instead of the invoker's permission on the base table
            assertAccessDenied(
                    invokerSession,
                    refreshMaterializedView,
                    "Cannot select from columns \\[.*\\] in table .*test_orders_base.*",
                    privilege(ownerSession.getUser(), "test_orders_base", SELECT_COLUMN));
            assertAccessAllowed(
                    invokerSession,
                    refreshMaterializedView,
                    privilege(invokerSession.getUser(), "test_orders_base", SELECT_COLUMN));

            // Verify that refresh checks owner's permission instead of the invokers permission on the materialized view.
            // Verify that refresh requires INSERT_TABLE permission instead of SELECT_COLUMN permission on the materialized view.
            assertAccessDenied(
                    invokerSession,
                    refreshMaterializedView,
                    "Cannot insert into table .*test_orders_view.*",
                    privilege(ownerSession.getUser(), "test_orders_view", INSERT_TABLE));
            assertAccessAllowed(
                    invokerSession,
                    refreshMaterializedView,
                    privilege(invokerSession.getUser(), "test_orders_view", INSERT_TABLE));
            assertAccessAllowed(
                    invokerSession,
                    refreshMaterializedView,
                    privilege(ownerSession.getUser(), "test_orders_view", SELECT_COLUMN));
            assertAccessAllowed(
                    invokerSession,
                    refreshMaterializedView,
                    privilege(invokerSession.getUser(), "test_orders_view", SELECT_COLUMN));

            // Verify for the owner invoking refresh
            assertAccessDenied(
                    ownerSession,
                    refreshMaterializedView,
                    "Cannot select from columns \\[.*\\] in table .*test_orders_base.*",
                    privilege(ownerSession.getUser(), "test_orders_base", SELECT_COLUMN));
            assertAccessDenied(
                    ownerSession,
                    refreshMaterializedView,
                    "Cannot insert into table .*test_orders_view.*",
                    privilege(ownerSession.getUser(), "test_orders_view", INSERT_TABLE));
            assertAccessAllowed(
                    ownerSession,
                    refreshMaterializedView);
        }
        finally {
            queryRunner.execute(ownerSession, "DROP MATERIALIZED VIEW test_orders_view");
            queryRunner.execute(ownerSession, "DROP TABLE test_orders_base");
        }
    }

    @Test
    public void testAutoRefreshMaterializedViewWithoutPredicates()
    {
        QueryRunner queryRunner = getQueryRunner();
        String table = "test_orders_auto_refresh_source";
        String view = "test_orders_auto_refresh_target_mv";
        String view2 = "test_orders_auto_refresh_target_mv2";

        Session nonFullRefreshSession = getSession();

        Session fullRefreshSession = Session.builder(getSession())
                .setSystemProperty("materialized_view_allow_full_refresh_enabled", "true")
                .setSystemProperty("materialized_view_data_consistency_enabled", "false")
                .build();

        queryRunner.execute(
                fullRefreshSession,
                format("CREATE TABLE %s WITH (partitioned_by = ARRAY['orderstatus']) " +
                        "AS SELECT orderkey, custkey, totalprice, orderstatus FROM orders WHERE orderkey < 100", table));

        queryRunner.execute(
                fullRefreshSession,
                format("CREATE MATERIALIZED VIEW %s " +
                        "WITH (partitioned_by = ARRAY['orderstatus']) " +
                        "AS SELECT SUM(totalprice) AS total, COUNT(*) AS cnt, orderstatus " +
                        "FROM %s GROUP BY orderstatus", view, table));

        queryRunner.execute(
                nonFullRefreshSession,
                format("CREATE MATERIALIZED VIEW %s " +
                        "WITH (partitioned_by = ARRAY['orderstatus']) " +
                        "AS SELECT SUM(totalprice) AS total, COUNT(*) AS cnt, orderstatus " +
                        "FROM %s GROUP BY orderstatus", view2, table));

        try {
            // Test that refresh without predicates succeeds when flag is enabled
            queryRunner.execute(fullRefreshSession, format("REFRESH MATERIALIZED VIEW %s", view));

            // Verify all partitions are refreshed
            MaterializedResult result = queryRunner.execute(fullRefreshSession,
                    format("SELECT COUNT(DISTINCT orderstatus) FROM %s", view));
            assertTrue(((Long) result.getOnlyValue()) > 0, "Materialized view should contain data after auto-refresh");

            // Test that refresh without predicates fails when flag is not enabled
            assertQueryFails(
                    nonFullRefreshSession,
                    format("REFRESH MATERIALIZED VIEW %s", view2),
                    ".*misses too many partitions or is never refreshed and may incur high cost.*");
        }
        finally {
            queryRunner.execute(fullRefreshSession, format("DROP MATERIALIZED VIEW %s", view));
            queryRunner.execute(fullRefreshSession, format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testAutoRefreshMaterializedViewWithJoinWithoutPredicates()
    {
        QueryRunner queryRunner = getQueryRunner();

        String table1 = "test_customer_auto_refresh";
        String table2 = "test_orders_join_auto_refresh";
        String view = "test_auto_refresh_join_target_mv";

        Session fullRefreshSession = Session.builder(getSession())
                .setSystemProperty("materialized_view_allow_full_refresh_enabled", "true")
                .setSystemProperty("materialized_view_data_consistency_enabled", "false")
                .build();
        Session ownerSession = getSession();

        queryRunner.execute(
                fullRefreshSession,
                format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) " +
                        "AS SELECT custkey, name, nationkey FROM customer WHERE custkey < 100", table1));
        queryRunner.execute(
                fullRefreshSession,
                format("CREATE TABLE %s WITH (partitioned_by = ARRAY['orderstatus']) " +
                        "AS SELECT orderkey, custkey, totalprice, orderstatus FROM orders WHERE orderkey < 100", table2));
        queryRunner.execute(
                fullRefreshSession,
                format("CREATE MATERIALIZED VIEW %s " +
                        "WITH (partitioned_by = ARRAY['nationkey', 'orderstatus']) " +
                        "AS SELECT c.name, SUM(o.totalprice) AS total, c.nationkey, o.orderstatus " +
                        "FROM %s c JOIN %s o ON c.custkey = o.custkey " +
                        "GROUP BY c.name, c.nationkey, o.orderstatus", view, table1, table2));

        try {
            queryRunner.execute(fullRefreshSession, format("REFRESH MATERIALIZED VIEW %s", view));

            MaterializedResult result = queryRunner.execute(fullRefreshSession,
                    format("SELECT COUNT(*) FROM %s", view));
            assertTrue(((Long) result.getOnlyValue()) > 0,
                    "Materialized view with join should contain data after auto-refresh");
        }
        finally {
            queryRunner.execute(ownerSession, format("DROP MATERIALIZED VIEW %s", view));
            queryRunner.execute(ownerSession, format("DROP TABLE %s", table1));
            queryRunner.execute(ownerSession, format("DROP TABLE %s", table2));
        }
    }

    @Test
    public void testAutoRefreshMaterializedViewFullyRefreshed()
    {
        QueryRunner queryRunner = getQueryRunner();

        String table = "test_customer_auto_refresh";
        String view = "test_auto_refresh_join_target_mv";

        Session fullRefreshSession = Session.builder(getSession())
                .setSystemProperty("materialized_view_allow_full_refresh_enabled", "true")
                .setSystemProperty("materialized_view_data_consistency_enabled", "false")
                .build();
        Session ownerSession = getSession();

        queryRunner.execute(
                fullRefreshSession,
                format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) " +
                        "AS SELECT custkey, name, nationkey FROM customer WHERE custkey < 100", table));

        queryRunner.execute(
                fullRefreshSession,
                format("CREATE MATERIALIZED VIEW %s " +
                        "WITH (partitioned_by = ARRAY['nationkey']) " +
                        "AS SELECT custkey, nationkey FROM %s", view, table));

        try {
            queryRunner.execute(fullRefreshSession, format("REFRESH MATERIALIZED VIEW %s", view));

            MaterializedResult result = queryRunner.execute(fullRefreshSession,
                    format("REFRESH MATERIALIZED VIEW %s", view));

            assertEquals(result.getWarnings().size(), 1);
            assertTrue(result.getWarnings().get(0).getMessage().matches("Materialized view .* is already fully refreshed"));
        }
        finally {
            queryRunner.execute(ownerSession, format("DROP MATERIALIZED VIEW %s", view));
            queryRunner.execute(ownerSession, format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testAutoRefreshMaterializedViewAfterInsertion()
    {
        QueryRunner queryRunner = getQueryRunner();

        String table = "test_auto_refresh";
        String view = "test_auto_refresh_mv";

        Session fullRefreshSession = Session.builder(getSession())
                .setSystemProperty("materialized_view_allow_full_refresh_enabled", "true")
                .setSystemProperty("materialized_view_data_consistency_enabled", "false")
                .build();
        Session ownerSession = getSession();

        queryRunner.execute(
                fullRefreshSession,
                format("CREATE TABLE %s (col1 bigint, col2 varchar, part_key varchar) " +
                        "WITH (partitioned_by = ARRAY['part_key'])", table));

        queryRunner.execute(
                fullRefreshSession,
                format("INSERT INTO %s VALUES (1, 'aaa', 'p1'), " +
                        "(2, 'bbb', 'p2'), (3, 'aaa', 'p1')", table));

        queryRunner.execute(
                fullRefreshSession,
                format("CREATE MATERIALIZED VIEW %s " +
                        "WITH (partitioned_by = ARRAY['part_key']) " +
                        "AS SELECT col1, part_key FROM %s", view, table));

        try {
            queryRunner.execute(fullRefreshSession, format("REFRESH MATERIALIZED VIEW %s", view));

            MaterializedResult result = queryRunner.execute(fullRefreshSession,
                    format("SELECT COUNT(DISTINCT part_key) FROM %s", view));
            assertEquals((long) ((Long) result.getOnlyValue()), 2, "Materialized view should contain all data after refreshes");

            queryRunner.execute(
                    fullRefreshSession,
                    format("INSERT INTO %s VALUES (1, 'aaa', 'p3'), " +
                            "(2, 'bbb', 'p4'), (3, 'aaa', 'p5')", table));

            queryRunner.execute(fullRefreshSession,
                    format("REFRESH MATERIALIZED VIEW %s", view));

            result = queryRunner.execute(fullRefreshSession,
                    format("SELECT COUNT(DISTINCT part_key) FROM %s", view));
            assertEquals((long) ((Long) result.getOnlyValue()), 5, "Materialized view should contain all data after refreshes");
        }
        finally {
            queryRunner.execute(ownerSession, format("DROP MATERIALIZED VIEW %s", view));
            queryRunner.execute(ownerSession, format("DROP TABLE %s", table));
        }
    }

    @Test
    public void testMVJoinQueryWithOtherTableColumnFiltering()
    {
        QueryRunner queryRunner = getQueryRunner();
        Session session = getSession();

        assertUpdate("CREATE TABLE mv_base (mv_col1 int, mv_col2 varchar, mv_col3 varchar) " +
                "WITH (partitioned_by=ARRAY['mv_col3'])");
        assertUpdate("CREATE TABLE join_table (table_col1 int, table_col2 varchar, table_col3 varchar) " +
                " WITH (partitioned_by=ARRAY['table_col3'])");

        assertUpdate("INSERT INTO mv_base VALUES (1, 'Alice', 'A'), (2, 'Bob', 'B'), (3, 'Charlie', 'C')", 3);
        assertUpdate("INSERT INTO join_table VALUES (1, 'CityA', 'A'), (21, 'CityA', 'B'), (32, 'CityB', 'C')", 3);

        assertUpdate("CREATE MATERIALIZED VIEW mv " +
                        "WITH (partitioned_by=ARRAY['mv_col3']) " +
                        "AS SELECT mv_col1, mv_col2, mv_col3 FROM mv_base");

        assertUpdate("REFRESH MATERIALIZED VIEW mv WHERE mv_col3>='A'", 3);

        // Query MV with JOIN and WHERE clause on column from joined table (not in MV)
        MaterializedResult result = queryRunner.execute(session,
                "SELECT mv_col2 FROM mv " +
                        "JOIN join_table ON mv_col3=table_col3 " +
                        "WHERE table_col1>10 ORDER BY mv_col1");
        assertEquals(result.getRowCount(), 2, "Materialized view join produced unexpected row counts");

        List<Object> expectedResults = List.of("Bob", "Charlie");
        List<Object> actualResults = result.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .collect(toList());
        assertEquals(actualResults, expectedResults, "Materialized view join returned unexpected row values");

        // WHERE clause on MV column
        result = queryRunner.execute(session, "SELECT mv_col2 FROM mv JOIN join_table " +
                        "ON mv_col3=table_col3 WHERE mv_col2>'Alice' ORDER BY mv_col2");
        assertEquals(result.getRowCount(), 2, "Materialized view join produced unexpected row counts");

        expectedResults = List.of("Bob", "Charlie");
        actualResults = result.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .collect(toList());
        assertEquals(actualResults, expectedResults, "Materialized view join returned unexpected row values");

        // Test with multiple conditions in WHERE clause (non-partition column)
        result = queryRunner.execute(session, "SELECT mv_col1 FROM mv JOIN join_table ON mv_col3=table_col3 " +
                        "WHERE table_col1>10 AND table_col3='B' AND mv_col1>1");
        assertEquals(result.getRowCount(), 1, "Materialized view join produced unexpected row counts");

        expectedResults = List.of(2);
        actualResults = result.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .collect(toList());
        assertEquals(actualResults, expectedResults, "Materialized view join returned unexpected row values");

        // Test with multiple conditions in WHERE clause (partition column)
        result = queryRunner.execute(session, "SELECT mv_col1 FROM mv JOIN join_table ON mv_col3=table_col3 " +
                "WHERE table_col1>10 AND table_col3='B' AND mv_col3='C'");
        assertEquals(result.getRowCount(), 0, "Materialized view join produced wrong results");

        assertUpdate("DROP MATERIALIZED VIEW mv");
        assertUpdate("DROP TABLE join_table");
        assertUpdate("DROP TABLE mv_base");
    }

    public void testMaterializedViewNotRefreshedInNonLegacyMode()
    {
        Session nonLegacySession = Session.builder(getSession())
                .setSystemProperty("legacy_materialized_views", "false")
                .build();
        try {
            assertUpdate("CREATE TABLE base_table (id BIGINT, name VARCHAR, part_key BIGINT) WITH (partitioned_by = ARRAY['part_key'])");
            assertUpdate("INSERT INTO base_table VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);
            assertUpdate("CREATE MATERIALIZED VIEW simple_mv WITH (partitioned_by = ARRAY['part_key']) AS SELECT id, name, part_key FROM base_table");

            assertPlan(nonLegacySession, "SELECT * FROM simple_mv",
                    anyTree(tableScan("base_table")));
        }
        finally {
            assertUpdate("DROP TABLE base_table");
            assertUpdate("DROP MATERIALIZED VIEW simple_mv");
        }
    }

    @Test
    public void testMaterializedViewRefreshedInNonLegacyMode()
    {
        Session nonLegacySession = Session.builder(getSession())
                .setSystemProperty("legacy_materialized_views", "false")
                .build();
        try {
            assertUpdate("CREATE TABLE base_table (id BIGINT, name VARCHAR, part_key BIGINT) WITH (partitioned_by = ARRAY['part_key'])");
            assertUpdate("INSERT INTO base_table VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);
            assertUpdate("CREATE MATERIALIZED VIEW simple_mv WITH (partitioned_by = ARRAY['part_key']) AS SELECT id, name, part_key FROM base_table");
            assertUpdate("REFRESH MATERIALIZED VIEW simple_mv where part_key > 0", 2);

            assertPlan(nonLegacySession, "SELECT * FROM simple_mv",
                    anyTree(tableScan("simple_mv")));
        }
        finally {
            assertUpdate("DROP TABLE base_table");
            assertUpdate("DROP MATERIALIZED VIEW simple_mv");
        }
    }

    private void setReferencedMaterializedViews(DistributedQueryRunner queryRunner, String tableName, List<String> referencedMaterializedViews)
    {
        appendTableParameter(replicateHiveMetastore(queryRunner),
                tableName,
                REFERENCED_MATERIALIZED_VIEWS,
                referencedMaterializedViews.stream().map(view -> format("%s.%s", getSession().getSchema().orElse(""), view)).collect(joining(",")));
    }

    private void appendTableParameter(ExtendedHiveMetastore metastore, String tableName, String parameterKey, String parameterValue)
    {
        MetastoreContext metastoreContext = new MetastoreContext(getSession().getUser(), getSession().getQueryId().getId(), Optional.empty(), Collections.emptySet(), Optional.empty(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, getSession().getWarningCollector(), getSession().getRuntimeStats());
        Optional<Table> table = metastore.getTable(metastoreContext, getSession().getSchema().get(), tableName);
        if (table.isPresent()) {
            Table originalTable = table.get();
            Table alteredTable = Table.builder(originalTable).setParameter(parameterKey, parameterValue).build();
            metastore.dropTable(metastoreContext, originalTable.getDatabaseName(), originalTable.getTableName(), false);
            metastore.createTable(metastoreContext, alteredTable, new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()), emptyList());
        }
    }
}
