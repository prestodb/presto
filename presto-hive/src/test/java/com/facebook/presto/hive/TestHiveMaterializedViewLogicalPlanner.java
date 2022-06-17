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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PREFER_PARTIAL_AGGREGATION;
import static com.facebook.presto.SystemSessionProperties.QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED;
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
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
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
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertFalse;
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
                ImmutableMap.of(),
                Optional.empty());
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
                    filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey")))));
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
                    filter("orderkey < BIGINT'100'", PlanMatchPattern.constrainedTableScan(table,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_62 < BIGINT'100'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_62", "orderkey")))));
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

            assertPlan(getSession(), viewQuery, anyTree(
                    anyTree(values("orderkey")), // Alias for the filter column
                    anyTree(filter("orderkey_17 < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey"))))));
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
                    filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table, ImmutableMap.of(), ImmutableMap.of("orderkey", "orderkey")))));
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
                            PlanMatchPattern.constrainedTableScan(table, ImmutableMap.of(), ImmutableMap.of("orderkey", "orderkey")))));

            // assert that when count of missing partition  <= threshold, use available partitions from view
            session = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty(HIVE_CATALOG, MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.toString(100))
                    .build();

            assertPlan(session, viewQuery, anyTree(
                    filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table,
                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2019-03-02", "2019-04-02", "2019-05-02", "2019-06-02", "2019-07-02"))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view,
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
                            PlanMatchPattern.constrainedTableScan(table, ImmutableMap.of(), ImmutableMap.of("orderkey", "orderkey")))));
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
                    filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table,
                            ImmutableMap.of("ds", create(ValueSet.of(createVarcharType(10), utf8Slice("2019-01-02")), true)),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey")))));
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
                    filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table,
                            ImmutableMap.of(
                                    "ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02")),
                                    "orderpriority", multipleValues(createVarcharType(15), utf8Slices("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey")))));
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
                                    filter("custkey < BIGINT'1000'", PlanMatchPattern.constrainedTableScan(table1,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                                            ImmutableMap.of("custkey", "custkey")))),
                            anyTree(
                                    filter("custkey_21 <= BIGINT'900'", PlanMatchPattern.constrainedTableScan(table2,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                                            ImmutableMap.of("custkey_21", "custkey"))))),
                    PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of())));
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

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE nationkey < 10", view, baseQuery), 599);

            String viewQuery = format("SELECT name, custkey, nationkey from %s ORDER BY name", view);
            baseQuery = format("%s ORDER BY name", baseQuery);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    filter("custkey < BIGINT'1000'", PlanMatchPattern.constrainedTableScan(table1,
                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                            ImmutableMap.of("custkey", "custkey"))),
                    filter("custkey_21 >= BIGINT'1000'", PlanMatchPattern.constrainedTableScan(table2,
                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                            ImmutableMap.of("custkey_21", "custkey"))),
                    PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of())));
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

            assertPlan(getSession(), viewQuery, anyTree(PlanMatchPattern.values("ds", "orderkey"), anyTree(
                    PlanMatchPattern.constrainedTableScan(table2,
                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2019-01-02")))),
                    PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of()))));
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
                                    filter("custkey < BIGINT'1000'", PlanMatchPattern.constrainedTableScan(table1,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                                            ImmutableMap.of("custkey", "custkey")))),
                            anyTree(
                                    filter("custkey_21 > BIGINT'900'", PlanMatchPattern.constrainedTableScan(table2,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT, ImmutableList.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L))),
                                            ImmutableMap.of("custkey_21", "custkey"))))),
                    PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of())));
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
                    anyTree(PlanMatchPattern.constrainedTableScan(table,
                            ImmutableMap.of("shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "REG AIR", "SHIP", "TRUCK"))))),
                    PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of())));
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
                    filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table,
                            ImmutableMap.of(
                                    "ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02")),
                                    "orderpriority", multipleValues(createVarcharType(15), utf8Slices("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_23 < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_23", "orderkey")))));
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
                                    anyTree(PlanMatchPattern.constrainedTableScan(table1,
                                            ImmutableMap.of(
                                                    "regionkey", multipleValues(BIGINT, ImmutableList.of(0L, 2L, 3L, 4L)),
                                                    "nationkey", multipleValues(BIGINT,
                                                            ImmutableList.of(0L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 18L, 19L, 20L, 21L, 22L, 23L))),
                                            ImmutableMap.of("l_nationkey", "nationkey"))),
                                    anyTree(PlanMatchPattern.constrainedTableScan(table2,
                                            ImmutableMap.of("nationkey", multipleValues(BIGINT,
                                                    ImmutableList.of(0L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 18L, 19L, 20L, 21L, 22L, 23L))),
                                            ImmutableMap.of("r_nationkey", "nationkey")))),
                            PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of())));
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
            assertFalse(viewFullTable.equals(viewHalfTable));
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
            String baseQuery = format("%s ORDER BY name", viewDefinition, table);

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
                    anyTree(PlanMatchPattern.constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    anyTree(PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of()))));
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
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view, table), 7);

            String viewQuery = format("SELECT sum(_discount_multi_extendedprice_) from %s group by ds ORDER BY sum(_discount_multi_extendedprice_)", view);
            String baseQuery = format("SELECT sum(discount * extendedprice) as _discount_multi_extendedprice_ from %s group by ds " +
                    "ORDER BY _discount_multi_extendedprice_", table);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    anyTree(PlanMatchPattern.constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    anyTree(PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of()))));
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
                    anyTree(PlanMatchPattern.constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of())));

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, anyTree(
                    anyTree(PlanMatchPattern.constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    anyTree(PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of()))));
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
                    anyTree(filter("orderkey_25 < BIGINT'1000'", PlanMatchPattern.constrainedTableScan(view2,
                            ImmutableMap.of(),
                            ImmutableMap.of("orderkey_25", "orderkey")))));

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);
            assertPlan(getSession(), viewQuery, expectedPattern);

            // Try optimizing the base query when all candidates are incompatible
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view1, view3));
            assertPlan(queryOptimizationWithMaterializedView, baseQuery, anyTree(
                    filter("orderkey < BIGINT'1000'", PlanMatchPattern.constrainedTableScan(table,
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
                    anyTree(PlanMatchPattern.constrainedTableScan(table, ImmutableMap.of(
                            "shipmode", multipleValues(createVarcharType(10), utf8Slices("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")),
                            "ds", singleValue(createVarcharType(10), utf8Slice("2020-01-02"))))),
                    anyTree(PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of())));
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
                                                                                    ImmutableMap.of("orderkey", "orderkey", "ds", "ds"))))))),
                                    anyTree(
                                            constrainedTableScan(
                                                    view,
                                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2021-07-11"))),
                                                    ImmutableMap.of())))));

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
                                            constrainedTableScan(
                                                    view,
                                                    ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11", "2021-07-12"))),
                                                    ImmutableMap.of())))));

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
                            singleGroupingSet("ds"),
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
                                                                                    ImmutableMap.of("ds", "ds"))))))),
                                    anyTree(
                                            constrainedTableScan(
                                                    view,
                                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2021-07-11"))),
                                                    ImmutableMap.of())))));

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
                    join(INNER, ImmutableList.of(equiJoinClause("orderkey", "orderkey_7")),
                            anyTree(filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table1,
                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                                    ImmutableMap.of("orderkey", "orderkey")))),
                            anyTree(PlanMatchPattern.constrainedTableScan(table2, ImmutableMap.of(), ImmutableMap.of("orderkey_7", "orderkey")))),
                    filter("view_orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
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

            String baseQuery = format("SELECT orderkey FROM %s ORDER BY orderkey", table);
            String queryWithSubquery = format("SELECT orderkey FROM (SELECT orderkey FROM %s) ORDER BY orderkey", table);

            MaterializedResult optimizedQueryResult = computeActual(queryOptimizationWithMaterializedView, baseQuery);
            MaterializedResult baseQueryResult = computeActual(baseQuery);
            assertEquals(baseQueryResult, optimizedQueryResult);

            PlanMatchPattern expectedPattern = anyTree(
                    constrainedTableScan(table,
                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11"))),
                            ImmutableMap.of()),
                    constrainedTableScan(view,
                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-12"))),
                            ImmutableMap.of("orderkey_43", "orderkey")));

            assertPlan(queryOptimizationWithMaterializedView, baseQuery, expectedPattern);
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
                            "AS SELECT suppkey, MAX(quantity) as max_qty, ds " +
                            "FROM %s " +
                            "GROUP BY suppkey, ds",
                    lineItemView2, lineItemTable));

            assertUpdate(format("CREATE MATERIALIZED VIEW %s " +
                            "WITH (partitioned_by = ARRAY['ds']) " +
                            "AS SELECT suppkey, name, ds\n " +
                            "FROM %s " +
                            "WHERE name != 'bob'",
                    suppliersView, supplierTable));

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-11'", lineItemView1), 100);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-12'", lineItemView1), 100);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-11'", lineItemView2), 100);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-12'", lineItemView2), 100);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-11'", suppliersView), 100);
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2021-07-12'", suppliersView), 100);

            assertTrue(getQueryRunner().tableExists(getSession(), lineItemView1));
            assertTrue(getQueryRunner().tableExists(getSession(), lineItemView2));
            assertTrue(getQueryRunner().tableExists(getSession(), suppliersView));

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, lineItemTable, ImmutableList.of(lineItemView1, lineItemView2));
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, supplierTable, ImmutableList.of(suppliersView));

            String baseQuery = format("WITH long_name_supp AS ( \n" +
                    "SELECT suppkey, name \n" +
                    "FROM %s \n" +
                    "WHERE name != 'bob'), " +
                    "supp_max AS (\n" +
                    "SELECT suppkey, MAX(quantity) AS max_qty \n" +
                    "FROM %s \n" +
                    "GROUP BY suppkey, ds), \n" +
                    "supp_sum AS (\n" +
                    "SELECT suppkey, SUM(quantity) AS qty \n" +
                    "FROM %s \n" +
                    "GROUP BY suppkey, ds) \n" +
                    "SELECT n.suppkey, n.name, m.max_qty, s.qty\n " +
                    "FROM long_name_supp AS n \n" +
                    "LEFT JOIN supp_max AS m ON n.suppkey = m.suppkey \n" +
                    "LEFT JOIN supp_sum AS s ON n.suppkey = s.suppkey \n" +
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
                                                    values("suppkey", "name")),
                                            anyTree(
                                                    constrainedTableScan(suppliersView,
                                                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11", "2021-07-12"))),
                                                            ImmutableMap.of("suppkey_37", "suppkey", "name_38", "name")))),
                                    exchange(
                                            anyTree(
                                                    exchange(
                                                            anyTree(
                                                                    values("suppkey_71", "quantity", "ds_73")),
                                                            anyTree(
                                                                    constrainedTableScan(lineItemView2,
                                                                            ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11", "2021-07-12"))),
                                                                            ImmutableMap.of("ds_155", "ds", "suppkey_154", "suppkey"))))))),
                            exchange(
                                    anyTree(
                                            exchange(
                                                    anyTree(
                                                            values("suppkey_197", "quantity_199", "ds_211")),
                                                    anyTree(
                                                            constrainedTableScan(lineItemView1,
                                                                    ImmutableMap.of("ds", multipleValues(createVarcharType(10), utf8Slices("2021-07-11", "2021-07-12"))),
                                                                    ImmutableMap.of("ds_295", "ds", "suppkey_294", "suppkey"))))))));

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
                    "SELECT t1.name, t1.suppkey, low_cost.partkey \n" +
                    "FROM %s t1 \n" +
                    "LEFT JOIN (\n" +
                    "SELECT t2.partkey, t2.suppkey FROM %s t2 \n" +
                    "LEFT JOIN (SELECT MIN(extendedprice) AS min_price, partkey, ds FROM %s GROUP BY partkey, ds) mp \n" +
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
                                                                                    ImmutableMap.of("ds_22", "ds", "partkey_7", "partkey", "extendedprice_11", "extendedprice"))),
                                                                    anyTree(
                                                                            constrainedTableScan(lineItemView,
                                                                                    ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2021-07-12"))),
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
                            join(INNER, ImmutableList.of(equiJoinClause("orderkey", "orderkey_28")),
                                    anyTree(
                                            filter("orderkey < BIGINT'10000'",
                                                    PlanMatchPattern.constrainedTableScan(table1,
                                                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                                                            ImmutableMap.of("orderkey", "orderkey")))),
                                    anyTree(
                                            join(INNER, ImmutableList.of(equiJoinClause("orderkey_7", "orderkey_28")),
                                                    anyTree(
                                                            filter("orderkey_7 < BIGINT'10000'",
                                                                    PlanMatchPattern.constrainedTableScan(table2, ImmutableMap.of(), ImmutableMap.of("orderkey_7", "orderkey")))),
                                                    anyTree(
                                                            filter("orderkey_28 < BIGINT'10000'",
                                                                    PlanMatchPattern.constrainedTableScan(table3, ImmutableMap.of(), ImmutableMap.of("orderkey_28", "orderkey"))))))),
                            filter("view_orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
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
                    filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table,
                            ImmutableMap.of("totalprice", multipleValues(DOUBLE, ImmutableList.of(105367.67, 172799.49, 205654.3, 271885.66))),
                            ImmutableMap.of("orderkey", "orderkey"))),
                    filter("orderkey_17 < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("orderkey_17", "orderkey")))));
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

            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view, table1, table2), 65025);

            String viewQuery = format("SELECT view_orderkey from %s where view_orderkey < 10000 ORDER BY view_orderkey", view);
            String baseQuery = format("SELECT t1.orderkey FROM %s t1" +
                    " inner join %s t2 ON t1.ds=t2.ds where t1.orderkey < 10000 ORDER BY t1.orderkey", table1, table2);

            MaterializedResult viewTable = computeActual(viewQuery);
            MaterializedResult baseTable = computeActual(baseQuery);
            assertEquals(viewTable, baseTable);

            assertPlan(getSession(), viewQuery, anyTree(
                    join(INNER, ImmutableList.of(),
                            filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table1,
                                    ImmutableMap.of(
                                            "ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02")),
                                            "orderpriority", multipleValues(createVarcharType(15), utf8Slices("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"))),
                                    ImmutableMap.of("orderkey", "orderkey"))),
                            anyTree(PlanMatchPattern.constrainedTableScan(table2, ImmutableMap.of()))),
                    filter("view_orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
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
                    join(LEFT, ImmutableList.of(equiJoinClause("ds", "ds_8"), equiJoinClause("orderkey", "orderkey_7")),
                            anyTree(filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table1,
                                    ImmutableMap.of(
                                            "ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                                    ImmutableMap.of("orderkey", "orderkey", "ds", "ds")))),
                            anyTree(PlanMatchPattern.constrainedTableScan(table2, ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))), ImmutableMap.of("orderkey_7", "orderkey", "ds_8", "ds")))),
                    filter("view_orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
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
                    unnest(filter("orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(table,
                            ImmutableMap.of("ds", singleValue(createVarcharType(10), utf8Slice("2019-01-02"))),
                            ImmutableMap.of("orderkey", "orderkey")))),
                    filter("view_orderkey < BIGINT'10000'", PlanMatchPattern.constrainedTableScan(view, ImmutableMap.of(), ImmutableMap.of("view_orderkey", "view_orderkey")))));
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
        Session invokerSession = Session.builder(getSession())
                .setIdentity(new Identity("test_view_invoker", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
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

        Consumer<String> testQueryWithDeniedPrivilege = query -> {
            // Verify checking the base table instead of the materialized view for SELECT permission
            assertAccessDenied(
                    invokerSession,
                    query,
                    "Cannot select from columns \\[.*\\] in table .*test_orders_base.*",
                    privilege(invokerSession.getUser(), "test_orders_base", SELECT_COLUMN));
            assertAccessAllowed(
                    invokerSession,
                    query,
                    privilege(invokerSession.getUser(), "test_orders_view", SELECT_COLUMN));
        };

        try {
            // Check for both the direct materialized view query and the base table query optimization with materialized view
            String directMaterializedViewQuery = "SELECT totalprice, orderstatus FROM test_orders_view";
            String queryWithMaterializedViewOptimization = "SELECT SUM(totalprice) AS totalprice, orderstatus FROM test_orders_base GROUP BY orderstatus";

            // Test when the materialized view is not materialized yet
            testQueryWithDeniedPrivilege.accept(directMaterializedViewQuery);
            testQueryWithDeniedPrivilege.accept(queryWithMaterializedViewOptimization);

            // Test when the materialized view is partially materialized
            queryRunner.execute(ownerSession, "REFRESH MATERIALIZED VIEW test_orders_view WHERE orderstatus = 'F'");
            testQueryWithDeniedPrivilege.accept(directMaterializedViewQuery);
            testQueryWithDeniedPrivilege.accept(queryWithMaterializedViewOptimization);

            // Test when the materialized view is fully materialized
            queryRunner.execute(ownerSession, "REFRESH MATERIALIZED VIEW test_orders_view WHERE orderstatus <> 'F'");
            testQueryWithDeniedPrivilege.accept(directMaterializedViewQuery);
            testQueryWithDeniedPrivilege.accept(queryWithMaterializedViewOptimization);
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

    private void setReferencedMaterializedViews(DistributedQueryRunner queryRunner, String tableName, List<String> referencedMaterializedViews)
    {
        appendTableParameter(replicateHiveMetastore(queryRunner),
                tableName,
                REFERENCED_MATERIALIZED_VIEWS,
                referencedMaterializedViews.stream().map(view -> format("%s.%s", getSession().getSchema().orElse(""), view)).collect(joining(",")));
    }

    private void appendTableParameter(ExtendedHiveMetastore metastore, String tableName, String parameterKey, String parameterValue)
    {
        MetastoreContext metastoreContext = new MetastoreContext(getSession().getUser(), getSession().getQueryId().getId(), Optional.empty(), Optional.empty(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        Optional<Table> table = metastore.getTable(metastoreContext, getSession().getSchema().get(), tableName);
        if (table.isPresent()) {
            Table originalTable = table.get();
            Table alteredTable = Table.builder(originalTable).setParameter(parameterKey, parameterValue).build();
            metastore.dropTable(metastoreContext, originalTable.getDatabaseName(), originalTable.getTableName(), false);
            metastore.createTable(metastoreContext, alteredTable, new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()));
        }
    }
}
