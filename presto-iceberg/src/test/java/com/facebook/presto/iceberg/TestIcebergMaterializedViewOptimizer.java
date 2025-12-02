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
package com.facebook.presto.iceberg;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.common.predicate.Domain.create;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.predicate.Range.greaterThan;
import static com.facebook.presto.common.predicate.Range.lessThan;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.Slices.utf8Slice;

@Test(singleThreaded = true)
public class TestIcebergMaterializedViewOptimizer
        extends AbstractTestQueryFramework
{
    private static final String MV_STORAGE = "__mv_storage__";
    private File warehouseLocation;
    private TestingHttpServer restServer;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        if (warehouseLocation != null) {
            deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(restConnectorProperties(restServer.getBaseUrl().toString()))
                        .put("iceberg.materialized-view-storage-prefix", MV_STORAGE)
                        .build())
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(false)
                .setExtraProperties(ImmutableMap.of(
                        "experimental.legacy-materialized-views", "false",
                        "optimizer.optimize-hash-generation", "false",
                        "materialized-view-stale-read-behavior", "USE_STITCHING"))
                .build().getQueryRunner();
    }

    @Test
    public void testBasicOptimization()
    {
        try {
            assertUpdate("CREATE TABLE base_no_parts (id BIGINT, value BIGINT)");
            assertUpdate("INSERT INTO base_no_parts VALUES (1, 100), (2, 200)", 2);

            assertUpdate("CREATE MATERIALIZED VIEW mv_no_parts AS SELECT id, value FROM base_no_parts");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_no_parts");

            assertUpdate("INSERT INTO base_no_parts VALUES (3, 300)", 1);

            assertPlan("SELECT * FROM mv_no_parts",
                    anyTree(tableScan("base_no_parts")));

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_no_parts");

            assertPlan("SELECT * FROM mv_no_parts",
                    anyTree(tableScan("__mv_storage__mv_no_parts")));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_no_parts");
            assertUpdate("DROP TABLE IF EXISTS base_no_parts");
        }
    }

    @Test
    public void testUnionStitchingWithStalePartition()
    {
        try {
            assertUpdate("CREATE TABLE base_table (id BIGINT, ds VARCHAR) WITH (partitioning = ARRAY['ds'])");
            assertUpdate("INSERT INTO base_table VALUES (1, '2024-01-01'), (2, '2024-01-02')", 2);

            assertUpdate("CREATE MATERIALIZED VIEW test_mv " +
                    "WITH (partitioning = ARRAY['ds']) AS SELECT id, ds FROM base_table");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW test_mv");

            assertUpdate("INSERT INTO base_table VALUES (3, '2024-01-03')", 1);

            assertPlan("SELECT * FROM test_mv",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__test_mv",
                                            ImmutableMap.of("ds", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-0T3")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-03")))), false)),
                                            ImmutableMap.of("ds", "ds", "id", "id")),
                                    project(constrainedTableScan("base_table",
                                            ImmutableMap.of("ds", singleValue(VARCHAR, utf8Slice("2024-01-03"))),
                                            ImmutableMap.of("id_1", "id"))))));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS test_mv");
            assertUpdate("DROP TABLE IF EXISTS base_table");
        }
    }

    @Test
    public void testFallbackForNonPartitionedTable()
    {
        try {
            assertUpdate("CREATE TABLE base_no_parts (id BIGINT, value BIGINT)");
            assertUpdate("INSERT INTO base_no_parts VALUES (1, 100), (2, 200)", 2);

            assertUpdate("CREATE MATERIALIZED VIEW mv_no_parts AS SELECT id, value FROM base_no_parts");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_no_parts");

            assertUpdate("INSERT INTO base_no_parts VALUES (3, 300)", 1);

            assertPlan("SELECT * FROM mv_no_parts",
                    anyTree(tableScan("base_no_parts")));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_no_parts");
            assertUpdate("DROP TABLE IF EXISTS base_no_parts");
        }
    }

    @Test
    public void testMultiTableStaleness()
    {
        try {
            assertUpdate("CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, ds VARCHAR) " +
                    "WITH (partitioning = ARRAY['ds'])");
            assertUpdate("CREATE TABLE customers (customer_id BIGINT, name VARCHAR, reg_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['reg_date'])");

            assertUpdate("INSERT INTO orders VALUES (1, 100, '2024-01-01')", 1);
            assertUpdate("INSERT INTO customers VALUES (100, 'Alice', '2024-01-01')", 1);

            assertUpdate("CREATE MATERIALIZED VIEW mv_join " +
                    "WITH (partitioning = ARRAY['ds', 'reg_date']) AS " +
                    "SELECT o.order_id, c.name, o.ds, c.reg_date " +
                    "FROM orders o JOIN customers c ON o.customer_id = c.customer_id");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_join");

            assertUpdate("INSERT INTO orders VALUES (2, 200, '2024-01-02')", 1);
            assertUpdate("INSERT INTO customers VALUES (200, 'Bob', '2024-01-02')", 1);

            PlanMatchPattern staleBranchPattern = join(
                    anyTree(tableScan("orders")),
                    anyTree(tableScan("customers")));

            assertPlan("SELECT * FROM mv_join",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_join",
                                            ImmutableMap.of(
                                                    "ds", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false),
                                                    "reg_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("ds", "ds", "reg_date", "reg_date", "order_id", "order_id", "name", "name")),
                                    exchange(
                                            project(staleBranchPattern),
                                            project(staleBranchPattern)))));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            assertPlan(skipStorageSession, "SELECT * FROM mv_join",
                    output(exchange(staleBranchPattern)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_join");
            assertUpdate("DROP TABLE IF EXISTS customers");
            assertUpdate("DROP TABLE IF EXISTS orders");
        }
    }

    @Test
    public void testAggregationMV()
    {
        try {
            assertUpdate("CREATE TABLE sales (product_id BIGINT, amount DOUBLE, sale_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['sale_date'])");

            assertUpdate("INSERT INTO sales VALUES (1, 100.0, '2024-01-01'), (2, 200.0, '2024-01-01')", 2);

            assertUpdate("CREATE MATERIALIZED VIEW mv_sales_agg " +
                    "WITH (partitioning = ARRAY['sale_date']) AS " +
                    "SELECT product_id, sale_date, SUM(amount) as total_amount " +
                    "FROM sales GROUP BY product_id, sale_date");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_sales_agg");

            assertUpdate("INSERT INTO sales VALUES (3, 150.0, '2024-01-02')", 1);

            PlanMatchPattern staleBranchPattern = aggregation(
                    ImmutableMap.of(),
                    anyTree(tableScan("sales")));
            PlanMatchPattern stalePlan = project(staleBranchPattern);

            assertPlan("SELECT * FROM mv_sales_agg",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_sales_agg",
                                            ImmutableMap.of("sale_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("sale_date", "sale_date", "product_id", "product_id", "total_amount", "total_amount")),
                                    stalePlan)));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            assertPlan(skipStorageSession, "SELECT * FROM mv_sales_agg",
                    output(exchange(staleBranchPattern)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_sales_agg");
            assertUpdate("DROP TABLE IF EXISTS sales");
        }
    }

    @Test
    public void testThreeTableJoinMV()
    {
        try {
            assertUpdate("CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, product_id BIGINT, order_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['order_date'])");
            assertUpdate("CREATE TABLE customers (customer_id BIGINT, customer_name VARCHAR, region VARCHAR) " +
                    "WITH (partitioning = ARRAY['region'])");
            assertUpdate("CREATE TABLE products (product_id BIGINT, product_name VARCHAR, category VARCHAR) " +
                    "WITH (partitioning = ARRAY['category'])");

            assertUpdate("INSERT INTO orders VALUES (1, 100, 1000, '2024-01-01')", 1);
            assertUpdate("INSERT INTO customers VALUES (100, 'Alice', 'US')", 1);
            assertUpdate("INSERT INTO products VALUES (1000, 'Widget', 'tools')", 1);

            assertUpdate("CREATE MATERIALIZED VIEW mv_order_details " +
                    "WITH (partitioning = ARRAY['order_date', 'region', 'category']) AS " +
                    "SELECT o.order_id, c.customer_name, p.product_name, o.order_date, c.region, p.category " +
                    "FROM orders o " +
                    "JOIN customers c ON o.customer_id = c.customer_id " +
                    "JOIN products p ON o.product_id = p.product_id");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_order_details");

            assertUpdate("INSERT INTO orders VALUES (2, 200, 2000, '2024-01-02')", 1);
            assertUpdate("INSERT INTO customers VALUES (200, 'Bob', 'EU')", 1);
            assertUpdate("INSERT INTO products VALUES (2000, 'Gadget', 'electronics')", 1);

            // For a 3-table join (orders JOIN customers JOIN products), with 3 stale partitions
            // from 3 different tables, the delta algebra creates complex nested unions.
            // Actual structure: LocalExchange[ROUND_ROBIN] with 2 children:
            //   1. InnerJoin(LocalExchange[ROUND_ROBIN] -> [2 orders/customers delta branches], products)
            //   2. Project -> InnerJoin((orders JOIN customers) JOIN products with category constraint)

            // First stale branch: orders/customers delta joined with products
            // LocalExchange[ROUND_ROBIN] has two project branches:
            //   - Project -> InnerJoin(orders with order_date='2024-01-02', customers full)
            //   - Project -> InnerJoin(orders with order_date != '2024-01-02', customers with region='EU')
            PlanMatchPattern orderCustomerDelta1 = project(
                    join(
                            exchange(constrainedTableScan("orders",
                                    ImmutableMap.of("order_date", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                                    ImmutableMap.of())),
                            exchange(exchange(tableScan("customers")))));

            PlanMatchPattern orderCustomerDelta2 = project(
                    join(
                            exchange(constrainedTableScan("orders",
                                    ImmutableMap.of("order_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                            lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                    ImmutableMap.of())),
                            exchange(exchange(constrainedTableScan("customers",
                                    ImmutableMap.of("region", singleValue(VARCHAR, utf8Slice("EU"))),
                                    ImmutableMap.of())))));

            // First branch: InnerJoin with nested LocalExchange for orders/customers deltas, then products
            PlanMatchPattern firstBranch = join(
                    exchange(
                            exchange(orderCustomerDelta1),
                            exchange(orderCustomerDelta2)),
                    exchange(exchange(tableScan("products"))));

            // Second branch: orders JOIN customers JOIN products with category='electronics'
            PlanMatchPattern ordersCustomersForCategoryDelta = join(
                    exchange(constrainedTableScan("orders",
                            ImmutableMap.of("order_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                    lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                    greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                            ImmutableMap.of())),
                    exchange(exchange(constrainedTableScan("customers",
                            ImmutableMap.of("region", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                    lessThan(VARCHAR, utf8Slice("EU")),
                                    greaterThan(VARCHAR, utf8Slice("EU")))), false)),
                            ImmutableMap.of()))));

            PlanMatchPattern secondBranch = project(
                    join(
                            exchange(ordersCustomersForCategoryDelta),
                            exchange(exchange(constrainedTableScan("products",
                                    ImmutableMap.of("category", singleValue(VARCHAR, utf8Slice("electronics"))),
                                    ImmutableMap.of())))));

            assertPlan("SELECT * FROM mv_order_details",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_order_details",
                                            ImmutableMap.of(
                                                    "order_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false),
                                                    "region", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("EU")),
                                                            greaterThan(VARCHAR, utf8Slice("EU")))), false),
                                                    "category", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("electronics")),
                                                            greaterThan(VARCHAR, utf8Slice("electronics")))), false)),
                                            ImmutableMap.of("order_date", "order_date", "region", "region", "category", "category",
                                                    "order_id", "order_id", "customer_name", "customer_name", "product_name", "product_name")),
                                    exchange(
                                            firstBranch,
                                            secondBranch))));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            PlanMatchPattern nestedJoinPattern = join(
                    exchange(join(
                            exchange(tableScan("orders")),
                            exchange(exchange(tableScan("customers"))))),
                    exchange(exchange(tableScan("products"))));
            assertPlan(skipStorageSession, "SELECT * FROM mv_order_details",
                    output(exchange(nestedJoinPattern)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_order_details");
            assertUpdate("DROP TABLE IF EXISTS products");
            assertUpdate("DROP TABLE IF EXISTS customers");
            assertUpdate("DROP TABLE IF EXISTS orders");
        }
    }

    @Test
    public void testJoinWithFilterMV()
    {
        try {
            assertUpdate("CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['order_date'])");
            assertUpdate("CREATE TABLE customers (customer_id BIGINT, name VARCHAR, status VARCHAR, reg_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['reg_date'])");

            assertUpdate("INSERT INTO orders VALUES (1, 100, 50.0, '2024-01-01')", 1);
            assertUpdate("INSERT INTO customers VALUES (100, 'Alice', 'active', '2024-01-01')", 1);

            assertUpdate("CREATE MATERIALIZED VIEW mv_active_orders " +
                    "WITH (partitioning = ARRAY['order_date', 'reg_date']) AS " +
                    "SELECT o.order_id, c.name, o.amount, o.order_date, c.reg_date " +
                    "FROM orders o JOIN customers c ON o.customer_id = c.customer_id " +
                    "WHERE c.status = 'active' AND o.amount > 10.0");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_active_orders");

            assertUpdate("INSERT INTO orders VALUES (2, 200, 100.0, '2024-01-02')", 1);
            assertUpdate("INSERT INTO customers VALUES (200, 'Bob', 'active', '2024-01-02')", 1);

            PlanMatchPattern staleBranchPattern = join(
                    anyTree(tableScan("orders")),
                    anyTree(tableScan("customers")));

            assertPlan("SELECT * FROM mv_active_orders",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_active_orders",
                                            ImmutableMap.of(
                                                    "order_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false),
                                                    "reg_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("order_date", "order_date", "reg_date", "reg_date",
                                                    "order_id", "order_id", "name", "name", "amount", "amount")),
                                    exchange(
                                            project(staleBranchPattern),
                                            project(staleBranchPattern)))));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            assertPlan(skipStorageSession, "SELECT * FROM mv_active_orders",
                    output(exchange(staleBranchPattern)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_active_orders");
            assertUpdate("DROP TABLE IF EXISTS customers");
            assertUpdate("DROP TABLE IF EXISTS orders");
        }
    }

    @Test
    public void testSingleTableStaleness()
    {
        try {
            assertUpdate("CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, ds VARCHAR) " +
                    "WITH (partitioning = ARRAY['ds'])");
            assertUpdate("CREATE TABLE customers (customer_id BIGINT, name VARCHAR, reg_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['reg_date'])");

            assertUpdate("INSERT INTO orders VALUES (1, 100, '2024-01-01')", 1);
            assertUpdate("INSERT INTO customers VALUES (100, 'Alice', '2024-01-01')", 1);

            assertUpdate("CREATE MATERIALIZED VIEW mv_partial_stale " +
                    "WITH (partitioning = ARRAY['ds', 'reg_date']) AS " +
                    "SELECT o.order_id, c.name, o.ds, c.reg_date " +
                    "FROM orders o JOIN customers c ON o.customer_id = c.customer_id");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_partial_stale");

            assertUpdate("INSERT INTO orders VALUES (2, 100, '2024-01-02')", 1);

            PlanMatchPattern staleBranchPattern = join(
                    anyTree(tableScan("orders")),
                    anyTree(tableScan("customers")));
            PlanMatchPattern stalePlan = project(staleBranchPattern);

            assertPlan("SELECT * FROM mv_partial_stale",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_partial_stale",
                                            ImmutableMap.of(
                                                    "ds", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("ds", "ds", "reg_date", "reg_date", "order_id", "order_id", "name", "name")),
                                    stalePlan)));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            assertPlan(skipStorageSession, "SELECT * FROM mv_partial_stale",
                    output(exchange(staleBranchPattern)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_partial_stale");
            assertUpdate("DROP TABLE IF EXISTS customers");
            assertUpdate("DROP TABLE IF EXISTS orders");
        }
    }

    @Test
    public void testUnionPredicatePushdown()
    {
        try {
            assertUpdate("CREATE TABLE union_table1 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE union_table2 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");

            assertUpdate("INSERT INTO union_table1 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02')", 2);
            assertUpdate("INSERT INTO union_table2 VALUES (3, 'c', '2024-01-01'), (4, 'd', '2024-01-02')", 2);

            assertUpdate("CREATE MATERIALIZED VIEW mv_union " +
                    "WITH (partitioning = ARRAY['dt']) AS " +
                    "SELECT id, value, dt FROM union_table1 " +
                    "UNION " +
                    "SELECT id, value, dt FROM union_table2");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_union");

            assertUpdate("INSERT INTO union_table1 VALUES (5, 'e', '2024-01-03')", 1);

            assertPlan("SELECT * FROM mv_union",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_union",
                                            ImmutableMap.of("dt", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-03")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-03")))), false)),
                                            ImmutableMap.of("dt", "dt", "id", "id", "value", "value")),
                                    aggregation(
                                            ImmutableMap.of(),
                                            FINAL,
                                            anyTree(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            anyTree(constrainedTableScan("union_table1",
                                                                    ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-03"))),
                                                                    ImmutableMap.of("id_1", "id", "value_1", "value")))))))));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            assertPlan(skipStorageSession, "SELECT * FROM mv_union",
                    output(exchange(
                            aggregation(
                                    ImmutableMap.of(),
                                    FINAL,
                                    exchange(
                                            project(
                                                    exchange(
                                                            aggregation(
                                                                    ImmutableMap.of(),
                                                                    PARTIAL,
                                                                    tableScan("union_table1", ImmutableMap.of("dt", "dt"))))),
                                            project(
                                                    exchange(
                                                            aggregation(
                                                                    ImmutableMap.of(),
                                                                    PARTIAL,
                                                                    tableScan("union_table2")))))))));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_union");
            assertUpdate("DROP TABLE IF EXISTS union_table2");
            assertUpdate("DROP TABLE IF EXISTS union_table1");
        }
    }

    @Test
    public void testJoinPredicatePushdown()
    {
        try {
            assertUpdate("CREATE TABLE join_orders (order_id BIGINT, customer_id BIGINT, order_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['order_date'])");
            assertUpdate("CREATE TABLE join_customers (customer_id BIGINT, name VARCHAR, reg_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['reg_date'])");

            assertUpdate("INSERT INTO join_orders VALUES (1, 100, '2024-01-01')", 1);
            assertUpdate("INSERT INTO join_customers VALUES (100, 'Alice', '2024-01-01')", 1);
            assertUpdate("INSERT INTO join_customers VALUES (300, 'Candace', '2024-01-02')", 1);
            assertUpdate("INSERT INTO join_orders VALUES (3, 300, '2024-01-02')", 1);
            assertUpdate("INSERT INTO join_customers VALUES (400, 'Billy', '2024-01-03')", 1);
            assertUpdate("INSERT INTO join_orders VALUES (4, 400, '2024-01-03')", 1);

            assertUpdate("CREATE MATERIALIZED VIEW mv_join " +
                    "WITH (partitioning = ARRAY['order_date']) AS " +
                    "SELECT o.order_id, c.name, o.order_date, c.reg_date " +
                    "FROM join_orders o " +
                    "JOIN join_customers c ON o.customer_id = c.customer_id");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_join");

            assertUpdate("INSERT INTO join_orders VALUES (2, 200, '2024-01-02')", 1);
            assertUpdate("INSERT INTO join_customers VALUES (200, 'Bob', '2024-01-01')", 1);

            // MV is partitioned by order_date only. The filter for reg_date becomes a residual
            // filter predicate (ScanFilter node) rather than just a domain constraint, because
            // reg_date is not a partition column in the MV storage table.
            assertPlan("SELECT * FROM mv_join",
                    output(
                            exchange(
                                    filter("(reg_date) <> (VARCHAR '2024-01-01')",
                                            constrainedTableScan("__mv_storage__mv_join",
                                                    ImmutableMap.of(
                                                            "order_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(lessThan(VARCHAR, utf8Slice("2024-01-02")), greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false),
                                                            "reg_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(lessThan(VARCHAR, utf8Slice("2024-01-01")), greaterThan(VARCHAR, utf8Slice("2024-01-01")))), false)),
                                                    ImmutableMap.of("order_date", "order_date", "reg_date", "reg_date", "order_id", "order_id", "name", "name"))),
                                    exchange(
                                            project(join(
                                                    exchange(constrainedTableScan("join_orders",
                                                            ImmutableMap.of("order_date", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                                                            ImmutableMap.of())),
                                                    exchange(exchange(tableScan("join_customers"))))),
                                            project(join(
                                                    exchange(constrainedTableScan("join_orders",
                                                            // R (unchanged) = all non-stale partitions: order_date != '2024-01-02'
                                                            ImmutableMap.of("order_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(lessThan(VARCHAR, utf8Slice("2024-01-02")), greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                                            ImmutableMap.of())),
                                                    exchange(exchange(constrainedTableScan("join_customers",
                                                            ImmutableMap.of("reg_date", singleValue(VARCHAR, utf8Slice("2024-01-01"))),
                                                            ImmutableMap.of())))))))));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            assertPlan(skipStorageSession, "SELECT * FROM mv_join",
                    output(exchange(join(
                            anyTree(tableScan("join_orders")),
                            anyTree(tableScan("join_customers"))))));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_join");
            assertUpdate("DROP TABLE IF EXISTS join_customers");
            assertUpdate("DROP TABLE IF EXISTS join_orders");
        }
    }

    @Test
    public void testJoinPassthroughPartition()
    {
        try {
            assertUpdate("CREATE TABLE passthrough_orders (order_id BIGINT, customer_id BIGINT, order_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['order_date'])");
            assertUpdate("CREATE TABLE passthrough_customers (customer_id BIGINT, name VARCHAR, reg_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['reg_date'])");

            assertUpdate("INSERT INTO passthrough_orders VALUES (1, 100, '2024-01-01')", 1);
            assertUpdate("INSERT INTO passthrough_customers VALUES (100, 'Alice', '2024-01-01')", 1);
            assertUpdate("INSERT INTO passthrough_customers VALUES (300, 'Candace', '2024-01-02')", 1);
            assertUpdate("INSERT INTO passthrough_orders VALUES (3, 300, '2024-01-02')", 1);
            assertUpdate("INSERT INTO passthrough_customers VALUES (400, 'Billy', '2024-01-03')", 1);
            assertUpdate("INSERT INTO passthrough_orders VALUES (4, 400, '2024-01-03')", 1);

            assertUpdate("CREATE MATERIALIZED VIEW mv_passthrough " +
                    "WITH (partitioning = ARRAY['order_date']) AS " +
                    "SELECT o.order_id, c.name, o.order_date " +
                    "FROM passthrough_orders o " +
                    "JOIN passthrough_customers c ON o.customer_id = c.customer_id AND o.order_date = c.reg_date");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_passthrough");

            assertUpdate("INSERT INTO passthrough_orders VALUES (2, 200, '2024-01-02')", 1);
            assertUpdate("INSERT INTO passthrough_customers VALUES (200, 'Bob', '2024-01-01')", 1);

            assertPlan("SELECT * FROM mv_passthrough",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_passthrough",
                                            ImmutableMap.of(
                                                    "order_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("2024-01-01")),
                                                            Range.range(VARCHAR, utf8Slice("2024-01-01"), false, utf8Slice("2024-01-02"), false),
                                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("order_date", "order_date", "order_id", "order_id", "name", "name")),
                                    exchange(
                                            project(join(
                                                    anyTree(constrainedTableScan("passthrough_orders",
                                                            ImmutableMap.of("order_date", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                                                            ImmutableMap.of("order_id_1", "order_id", "customer_id_1", "customer_id"))),
                                                    anyTree(constrainedTableScan("passthrough_customers",
                                                            ImmutableMap.of("reg_date", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                                                            ImmutableMap.of("customer_id_2", "customer_id", "name_2", "name"))))),
                                            project(join(
                                                    anyTree(constrainedTableScan("passthrough_orders",
                                                            ImmutableMap.of("order_date", singleValue(VARCHAR, utf8Slice("2024-01-01"))),
                                                            ImmutableMap.of("order_id_3", "order_id", "customer_id_3", "customer_id"))),
                                                    anyTree(constrainedTableScan("passthrough_customers",
                                                            ImmutableMap.of("reg_date", singleValue(VARCHAR, utf8Slice("2024-01-01"))),
                                                            ImmutableMap.of("customer_id_4", "customer_id", "name_4", "name")))))))));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            assertPlan(skipStorageSession, "SELECT * FROM mv_passthrough",
                    output(exchange(join(
                            anyTree(tableScan("passthrough_orders", ImmutableMap.of("order_date", "order_date"))),
                            anyTree(tableScan("passthrough_customers", ImmutableMap.of("reg_date", "reg_date")))))));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_passthrough");
            assertUpdate("DROP TABLE IF EXISTS passthrough_customers");
            assertUpdate("DROP TABLE IF EXISTS passthrough_orders");
        }
    }

    @Test
    public void testIntersectPredicatePushdown()
    {
        try {
            assertUpdate("CREATE TABLE intersect_table1 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE intersect_table2 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");

            assertUpdate("INSERT INTO intersect_table1 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01')", 2);
            assertUpdate("INSERT INTO intersect_table2 VALUES (2, 'b', '2024-01-01'), (3, 'c', '2024-01-01')", 2);

            assertUpdate("CREATE MATERIALIZED VIEW mv_intersect " +
                    "WITH (partitioning = ARRAY['dt']) AS " +
                    "SELECT id, value, dt FROM intersect_table1 " +
                    "INTERSECT " +
                    "SELECT id, value, dt FROM intersect_table2");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_intersect");

            assertUpdate("INSERT INTO intersect_table1 VALUES (4, 'd', '2024-01-02'), (5, 'e', '2024-01-02')", 2);
            assertUpdate("INSERT INTO intersect_table2 VALUES (5, 'e', '2024-01-02')", 1);

            // For INTERSECT with stale data on both tables, the plan creates TWO union branches.
            // Branch 1: FilterProject -> Aggregation(FINAL) -> LocalExchange ->
            //            [Project -> ... -> Aggregation(PARTIAL) -> table1,
            //             Project -> ... -> Aggregation(PARTIAL) -> table2]
            // Branch 2: FilterProject -> Aggregation(FINAL) -> LocalExchange ->
            //            [Project -> ... -> Aggregation(PARTIAL) -> table2]  (only table2!)
            // This is because table1 has 2 new rows while table2 has 1 new row, creating
            // different delta algebra terms.
            PlanMatchPattern intersectBranchBoth = project(
                    filter(
                            aggregation(
                                    ImmutableMap.of(),
                                    FINAL,
                                    exchange(
                                            anyTree(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            anyTree(constrainedTableScan("intersect_table1",
                                                                    ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                                                                    ImmutableMap.of())))),
                                            anyTree(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            anyTree(constrainedTableScan("intersect_table2",
                                                                    ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                                                                    ImmutableMap.of()))))))));

            PlanMatchPattern intersectBranchTable2Only = project(
                    filter(
                            aggregation(
                                    ImmutableMap.of(),
                                    FINAL,
                                    exchange(
                                            anyTree(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            anyTree(constrainedTableScan("intersect_table2",
                                                                    ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                                                                    ImmutableMap.of()))))))));

            assertPlan("SELECT * FROM mv_intersect",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_intersect",
                                            ImmutableMap.of("dt", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("dt", "dt", "id", "id", "value", "value")),
                                    intersectBranchBoth,
                                    intersectBranchTable2Only)));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            // When forcing stale read, the full query scans both tables WITHOUT partition constraints
            PlanMatchPattern fullIntersectPattern = project(
                    filter(
                            aggregation(
                                    ImmutableMap.of(),
                                    FINAL,
                                    exchange(
                                            anyTree(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            anyTree(tableScan("intersect_table1")))),
                                            anyTree(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            anyTree(tableScan("intersect_table2"))))))));
            assertPlan(skipStorageSession, "SELECT * FROM mv_intersect",
                    output(exchange(fullIntersectPattern)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_intersect");
            assertUpdate("DROP TABLE IF EXISTS intersect_table2");
            assertUpdate("DROP TABLE IF EXISTS intersect_table1");
        }
    }

    /**
     * Test INTERSECT where only the LEFT side becomes stale.
     * Verifies that the RIGHT side gets filtered to the left's stale partitions
     * via predicate propagation using column equivalences.
     */
    @Test
    public void testIntersectWithOnlyLeftSideStale()
    {
        try {
            assertUpdate("CREATE TABLE intersect_left_only1 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE intersect_left_only2 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");

            // Initial data - same rows in both tables for intersection
            assertUpdate("INSERT INTO intersect_left_only1 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01')", 2);
            assertUpdate("INSERT INTO intersect_left_only2 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01'), (3, 'c', '2024-01-01')", 3);

            assertUpdate("CREATE MATERIALIZED VIEW mv_intersect_left " +
                    "WITH (partitioning = ARRAY['dt']) AS " +
                    "SELECT id, value, dt FROM intersect_left_only1 " +
                    "INTERSECT " +
                    "SELECT id, value, dt FROM intersect_left_only2");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_intersect_left");

            // Make ONLY left side stale
            assertUpdate("INSERT INTO intersect_left_only1 VALUES (4, 'd', '2024-01-02')", 1);
            // Add matching data to right side so intersection has results
            assertUpdate("INSERT INTO intersect_left_only2 VALUES (4, 'd', '2024-01-02')", 1);
            // Refresh to make right side fresh again
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_intersect_left");

            // Now make left stale again with a new partition
            assertUpdate("INSERT INTO intersect_left_only1 VALUES (5, 'e', '2024-01-03')", 1);
            assertUpdate("INSERT INTO intersect_left_only2 VALUES (5, 'e', '2024-01-03')", 1);

            // The optimization should filter intersect_left_only2 to dt='2024-01-03'
            // even though it's not directly stale.
            // NOTE: There are TWO union branches because only one row is inserted into each table,
            // creating different delta algebra terms: one branch scans both tables, another scans only table2
            // Structure for each branch:
            // - FilterProject (combined filter+project node = project(filter(...)))
            //     - Aggregate(FINAL)
            //         - LocalExchange[HASH] (partitioning on grouping keys)
            //             - Project (first child)
            //                 - RemoteStreamingExchange[REPARTITION]
            //                     - Aggregate(PARTIAL)
            //                         - ScanProject[table1] (= project(tableScan(...)))
            //             - Project (second child)
            //                 - RemoteStreamingExchange[REPARTITION]
            //                     - Aggregate(PARTIAL)
            //                         - ScanProject[table2] (= project(tableScan(...)))
            PlanMatchPattern intersectBranchBoth = project(
                    filter(
                            aggregation(
                                    ImmutableMap.of(),
                                    FINAL,
                                    exchange(
                                            project(
                                                    exchange(
                                                            aggregation(
                                                                    ImmutableMap.of(),
                                                                    PARTIAL,
                                                                    project(tableScan("intersect_left_only1"))))),
                                            project(
                                                    exchange(
                                                            aggregation(
                                                                    ImmutableMap.of(),
                                                                    PARTIAL,
                                                                    project(tableScan("intersect_left_only2")))))))));

            PlanMatchPattern intersectBranchTable2Only = project(
                    filter(
                            aggregation(
                                    ImmutableMap.of(),
                                    FINAL,
                                    exchange(
                                            exchange(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            project(tableScan("intersect_left_only2"))))))));

            assertPlan("SELECT * FROM mv_intersect_left",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_intersect_left",
                                            ImmutableMap.of("dt", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-03")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-03")))), false)),
                                            ImmutableMap.of("dt", "dt", "id", "id", "value", "value")),
                                    intersectBranchBoth,
                                    intersectBranchTable2Only)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_intersect_left");
            assertUpdate("DROP TABLE IF EXISTS intersect_left_only2");
            assertUpdate("DROP TABLE IF EXISTS intersect_left_only1");
        }
    }

    /**
     * Test INTERSECT where only the RIGHT side becomes stale.
     * Verifies that the LEFT side gets filtered to the right's stale partitions
     * via predicate propagation using column equivalences.
     */
    @Test
    public void testIntersectWithOnlyRightSideStale()
    {
        try {
            assertUpdate("CREATE TABLE intersect_right_only1 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE intersect_right_only2 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");

            // Initial data
            assertUpdate("INSERT INTO intersect_right_only1 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01')", 2);
            assertUpdate("INSERT INTO intersect_right_only2 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01')", 2);

            assertUpdate("CREATE MATERIALIZED VIEW mv_intersect_right " +
                    "WITH (partitioning = ARRAY['dt']) AS " +
                    "SELECT id, value, dt FROM intersect_right_only1 " +
                    "INTERSECT " +
                    "SELECT id, value, dt FROM intersect_right_only2");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_intersect_right");

            // Make ONLY right side stale
            assertUpdate("INSERT INTO intersect_right_only2 VALUES (3, 'c', '2024-01-02')", 1);
            // Add matching data to left side
            assertUpdate("INSERT INTO intersect_right_only1 VALUES (3, 'c', '2024-01-02')", 1);
            // Refresh to make left side fresh
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_intersect_right");

            // Now make right stale again
            assertUpdate("INSERT INTO intersect_right_only2 VALUES (4, 'd', '2024-01-03')", 1);
            assertUpdate("INSERT INTO intersect_right_only1 VALUES (4, 'd', '2024-01-03')", 1);

            // The optimization should filter intersect_right_only1 (left side) to dt='2024-01-03'
            // even though it's not directly stale, because we're computing R ∩ ∆S
            PlanMatchPattern intersectBranch1 = project(
                    filter(
                            aggregation(
                                    ImmutableMap.of(),
                                    FINAL,
                                    exchange(
                                            project(exchange(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            project(constrainedTableScan("intersect_right_only1",
                                                                    ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-03"))),
                                                                    ImmutableMap.of()))))),
                                            project(exchange(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            project(constrainedTableScan("intersect_right_only2",
                                                                    ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-03"))),
                                                                    ImmutableMap.of())))))))));

            // Second intersect branch: only table2 (simpler structure since table1 is pruned)
            PlanMatchPattern intersectBranch2 = project(
                    filter(
                            aggregation(
                                    ImmutableMap.of(),
                                    FINAL,
                                    exchange(
                                            exchange(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            project(constrainedTableScan("intersect_right_only2",
                                                                    ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-03"))),
                                                                    ImmutableMap.of()))))))));

            assertPlan("SELECT * FROM mv_intersect_right",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_intersect_right",
                                            ImmutableMap.of("dt", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-03")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-03")))), false)),
                                            ImmutableMap.of("dt", "dt", "id", "id", "value", "value")),
                                    intersectBranch1,
                                    intersectBranch2)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_intersect_right");
            assertUpdate("DROP TABLE IF EXISTS intersect_right_only2");
            assertUpdate("DROP TABLE IF EXISTS intersect_right_only1");
        }
    }

    @Test
    public void testDeeplyNestedJoins()
    {
        try {
            assertUpdate("CREATE TABLE dnj_orders (order_id BIGINT, customer_id BIGINT, product_id BIGINT, order_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['order_date'])");
            assertUpdate("CREATE TABLE dnj_customers (customer_id BIGINT, customer_name VARCHAR, region VARCHAR, reg_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['reg_date'])");
            assertUpdate("CREATE TABLE dnj_products (product_id BIGINT, product_name VARCHAR, category_id BIGINT, product_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['product_date'])");
            assertUpdate("CREATE TABLE dnj_categories (category_id BIGINT, category_name VARCHAR, cat_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['cat_date'])");

            assertUpdate("INSERT INTO dnj_orders VALUES " +
                    "(1, 100, 1000, '2024-01-01'), " +
                    "(2, 200, 2000, '2024-01-01')", 2);
            assertUpdate("INSERT INTO dnj_customers VALUES " +
                    "(100, 'Alice', 'US', '2024-01-01'), " +
                    "(200, 'Bob', 'EU', '2024-01-01')", 2);
            assertUpdate("INSERT INTO dnj_products VALUES " +
                    "(1000, 'Laptop', 10, '2024-01-01'), " +
                    "(2000, 'Phone', 20, '2024-01-01')", 2);
            assertUpdate("INSERT INTO dnj_categories VALUES " +
                    "(10, 'Electronics', '2024-01-01'), " +
                    "(20, 'Mobile', '2024-01-01')", 2);

            assertUpdate("CREATE MATERIALIZED VIEW mv_dnj " +
                    "WITH (partitioning = ARRAY['order_date']) AS " +
                    "SELECT oc.order_id, oc.customer_name, pc.product_name, pc.category_name, oc.order_date " +
                    "FROM " +
                    "  (SELECT o.order_id, c.customer_name, o.product_id, o.order_date FROM dnj_orders o " +
                    "   JOIN dnj_customers c ON o.customer_id = c.customer_id AND o.order_date = c.reg_date) oc " +
                    "  JOIN " +
                    "  (SELECT p.product_id, p.product_name, cat.category_name, p.product_date FROM dnj_products p " +
                    "   JOIN dnj_categories cat ON p.category_id = cat.category_id AND p.product_date = cat.cat_date) pc " +
                    "  ON oc.product_id = pc.product_id AND oc.order_date = pc.product_date");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_dnj");

            assertUpdate("INSERT INTO dnj_customers VALUES (300, 'Charlie', 'US', '2024-01-02')", 1);
            assertUpdate("INSERT INTO dnj_products VALUES (3000, 'Tablet', 10, '2024-01-02')", 1);
            assertUpdate("INSERT INTO dnj_categories VALUES (10, 'Electronics', '2024-01-02')", 1);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_dnj");

            assertUpdate("INSERT INTO dnj_orders VALUES (3, 300, 3000, '2024-01-02')", 1);

            // The stale branch for a 4-table deeply nested join has this structure:
            // InnerJoin[(orders-customers) x (products-categories)]
            // Actual structure:
            // - Left branch: exchange -> project -> innerJoin(orders, customers)
            // - Right branch: exchange -> exchange -> innerJoin(products, categories) - NO project wrapper!
            PlanMatchPattern innerJoin1 = join(
                    exchange(constrainedTableScan("dnj_orders",
                            ImmutableMap.of("order_date", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                            ImmutableMap.of())),
                    exchange(exchange(constrainedTableScan("dnj_customers",
                            ImmutableMap.of("reg_date", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                            ImmutableMap.of()))));
            PlanMatchPattern innerJoin2 = join(
                    exchange(constrainedTableScan("dnj_products",
                            ImmutableMap.of("product_date", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                            ImmutableMap.of())),
                    exchange(exchange(constrainedTableScan("dnj_categories",
                            ImmutableMap.of("cat_date", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                            ImmutableMap.of()))));
            // Outer join structure: left has project wrapper, right does NOT
            PlanMatchPattern nestedJoinPattern = join(
                    exchange(project(innerJoin1)),
                    exchange(exchange(innerJoin2)));

            assertPlan("SELECT * FROM mv_dnj",
                    output(
                            exchange(
                                    tableScan("__mv_storage__mv_dnj"),
                                    nestedJoinPattern)));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            // For skipStorageSession, tables are scanned without partition constraints (full scan)
            PlanMatchPattern innerJoin1Full = join(
                    exchange(tableScan("dnj_orders")),
                    exchange(exchange(tableScan("dnj_customers"))));
            PlanMatchPattern innerJoin2Full = join(
                    exchange(tableScan("dnj_products")),
                    exchange(exchange(tableScan("dnj_categories"))));
            PlanMatchPattern nestedJoinPatternFull = join(
                    exchange(innerJoin1Full),
                    exchange(exchange(innerJoin2Full)));
            assertPlan(skipStorageSession, "SELECT * FROM mv_dnj",
                    output(exchange(nestedJoinPatternFull)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_dnj");
            assertUpdate("DROP TABLE IF EXISTS dnj_categories");
            assertUpdate("DROP TABLE IF EXISTS dnj_products");
            assertUpdate("DROP TABLE IF EXISTS dnj_customers");
            assertUpdate("DROP TABLE IF EXISTS dnj_orders");
        }
    }

    @Test
    public void testAggregationMVSkipsMarkDistinct()
    {
        try {
            assertUpdate("CREATE TABLE agg_orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['order_date'])");
            assertUpdate("CREATE TABLE agg_customers (customer_id BIGINT, name VARCHAR, reg_date VARCHAR) " +
                    "WITH (partitioning = ARRAY['reg_date'])");

            assertUpdate("INSERT INTO agg_orders VALUES (1, 100, 50.0, '2024-01-01')", 1);
            assertUpdate("INSERT INTO agg_customers VALUES (100, 'Alice', '2024-01-01')", 1);

            assertUpdate("CREATE MATERIALIZED VIEW mv_agg_join " +
                    "WITH (partitioning = ARRAY['order_date', 'reg_date']) AS " +
                    "SELECT c.name, o.order_date, c.reg_date, SUM(o.amount) as total_amount, COUNT(*) as order_count " +
                    "FROM agg_orders o JOIN agg_customers c ON o.customer_id = c.customer_id " +
                    "GROUP BY c.name, o.order_date, c.reg_date");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_agg_join");

            assertUpdate("INSERT INTO agg_orders VALUES (2, 200, 100.0, '2024-01-02')", 1);
            assertUpdate("INSERT INTO agg_customers VALUES (200, 'Bob', '2024-01-02')", 1);

            PlanMatchPattern aggregationBranch =
                    aggregation(
                            ImmutableMap.of(),
                            anyTree(
                                    join(
                                            anyTree(tableScan("agg_orders")),
                                            anyTree(tableScan("agg_customers")))));

            assertPlan("SELECT * FROM mv_agg_join",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_agg_join",
                                            ImmutableMap.of(
                                                    "order_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false),
                                                    "reg_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                            lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                            greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("order_date", "order_date", "reg_date", "reg_date",
                                                    "name", "name", "total_amount", "total_amount", "order_count", "order_count")),
                                    aggregation(ImmutableMap.of(), anyTree(join(anyTree(tableScan("agg_orders")), anyTree(tableScan("agg_customers"))))))));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            assertPlan(skipStorageSession, "SELECT * FROM mv_agg_join",
                    output(exchange(aggregationBranch)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_agg_join");
            assertUpdate("DROP TABLE IF EXISTS agg_customers");
            assertUpdate("DROP TABLE IF EXISTS agg_orders");
        }
    }

    @Test
    public void testExceptPredicatePushdown()
    {
        try {
            assertUpdate("CREATE TABLE except_table1 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE except_table2 (id BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");

            assertUpdate("INSERT INTO except_table1 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01')", 2);
            assertUpdate("INSERT INTO except_table2 VALUES (2, 'b', '2024-01-01')", 1);

            assertUpdate("CREATE MATERIALIZED VIEW mv_except " +
                    "WITH (partitioning = ARRAY['dt']) AS " +
                    "SELECT id, value, dt FROM except_table1 " +
                    "EXCEPT " +
                    "SELECT id, value, dt FROM except_table2");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_except");

            assertUpdate("INSERT INTO except_table1 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02')", 2);

            // First stale branch: FilterProject -> Aggregate(FINAL) -> LocalExchange[HASH]
            //   with two children: Project -> RemoteExchange -> Aggregate(PARTIAL) -> ScanProject for each table
            PlanMatchPattern firstStaleBranch = project(filter(
                    aggregation(
                            ImmutableMap.of(),
                            FINAL,
                            exchange(
                                    project(exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(constrainedTableScan("except_table1",
                                                            ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                                                            ImmutableMap.of()))))),
                                    project(exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(tableScan("except_table2")))))))));

            // Second stale branch: FilterProject -> Aggregate(FINAL) -> LocalExchange[HASH]
            //   with single child: RemoteExchange -> Aggregate(PARTIAL) -> ScanProject[except_table2]
            PlanMatchPattern secondStaleBranch = project(filter(
                    aggregation(
                            ImmutableMap.of(),
                            FINAL,
                            exchange(
                                    exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(tableScan("except_table2"))))))));

            assertPlan("SELECT * FROM mv_except",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_except",
                                            ImmutableMap.of("dt", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("dt", "dt", "id", "id", "value", "value")),
                                    firstStaleBranch,
                                    secondStaleBranch)));

            Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("materialized_view_force_stale", "true")
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                    .build();
            PlanMatchPattern fullExceptPattern = project(
                    filter(
                            aggregation(
                                    ImmutableMap.of(),
                                    FINAL,
                                    exchange(
                                            anyTree(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            project(tableScan("except_table1")))),
                                            anyTree(
                                                    aggregation(
                                                            ImmutableMap.of(),
                                                            PARTIAL,
                                                            project(tableScan("except_table2"))))))));
            assertPlan(skipStorageSession, "SELECT * FROM mv_except",
                    output(exchange(fullExceptPattern)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_except");
            assertUpdate("DROP TABLE IF EXISTS except_table2");
            assertUpdate("DROP TABLE IF EXISTS except_table1");
        }
    }

    @Test
    public void testPartialPassthroughColumns()
    {
        try {
            assertUpdate("CREATE TABLE partial_orders (order_id BIGINT, amount DOUBLE, order_date VARCHAR, region VARCHAR) " +
                    "WITH (partitioning = ARRAY['order_date', 'region'])");

            assertUpdate("INSERT INTO partial_orders VALUES (1, 100.0, '2024-01-01', 'US'), (2, 200.0, '2024-01-01', 'EU')", 2);

            assertUpdate("CREATE MATERIALIZED VIEW mv_partial_passthrough " +
                    "WITH (partitioning = ARRAY['order_date']) AS " +
                    "SELECT order_id, amount, order_date " +
                    "FROM partial_orders");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_partial_passthrough");

            assertUpdate("INSERT INTO partial_orders VALUES (3, 150.0, '2024-01-02', 'US')", 1);

            assertPlan("SELECT * FROM mv_partial_passthrough",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_partial_passthrough",
                                            ImmutableMap.of("order_date", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("order_date", "order_date", "order_id", "order_id", "amount", "amount")),
                                    project(constrainedTableScan("partial_orders",
                                            ImmutableMap.of("order_date", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                                            ImmutableMap.of("order_id_1", "order_id", "amount_1", "amount"))))));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_partial_passthrough");
            assertUpdate("DROP TABLE IF EXISTS partial_orders");
        }
    }

    @Test
    public void testSelectDistinctMVWithMultipleStaleTables()
    {
        try {
            assertUpdate("CREATE TABLE test_distinct_t1 (" +
                    "id BIGINT, " +
                    "category VARCHAR, " +
                    "value BIGINT, " +
                    "event_date DATE) " +
                    "WITH (partitioning = ARRAY['event_date'])");

            assertUpdate("CREATE TABLE test_distinct_t2 (" +
                    "id BIGINT, " +
                    "region VARCHAR, " +
                    "code VARCHAR, " +
                    "reg_date DATE) " +
                    "WITH (partitioning = ARRAY['reg_date'])");

            assertUpdate("INSERT INTO test_distinct_t1 VALUES " +
                    "(1, 'A', 100, DATE '2024-01-01'), " +
                    "(2, 'B', 200, DATE '2024-01-01')", 2);

            assertUpdate("INSERT INTO test_distinct_t2 VALUES " +
                    "(1, 'US', 'X', DATE '2024-01-01'), " +
                    "(2, 'EU', 'Y', DATE '2024-01-01')", 2);

            assertUpdate("CREATE MATERIALIZED VIEW test_distinct_mv " +
                    "WITH (partitioning = ARRAY['event_date', 'reg_date']) AS " +
                    "SELECT DISTINCT t1.category, t2.region, t1.event_date, t2.reg_date " +
                    "FROM test_distinct_t1 t1 " +
                    "JOIN test_distinct_t2 t2 ON t1.id = t2.id");

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW test_distinct_mv");

            assertUpdate("INSERT INTO test_distinct_t1 VALUES " +
                    "(1, 'A', 150, DATE '2024-01-02'), " +
                    "(3, 'C', 300, DATE '2024-01-02')", 2);

            assertUpdate("INSERT INTO test_distinct_t2 VALUES " +
                    "(3, 'APAC', 'Z', DATE '2024-01-02')", 1);

            long jan2InDays = java.time.LocalDate.of(2024, 1, 2).toEpochDay();
            // The actual plan structure without ORDER BY:
            // Output -> exchange -> [MV scan, Aggregate(FINAL) -> LocalExchange -> two stale branches]
            // Branch 1: t1 constrained to event_date='2024-01-02', t2 with all rows
            // Branch 2: t1 with event_date != '2024-01-02', t2 constrained to reg_date='2024-01-02'

            // Branch 1: JOIN(t1 constrained to event_date=jan2, t2 full scan)
            PlanMatchPattern joinBranch1 = join(
                    exchange(constrainedTableScan("test_distinct_t1",
                            ImmutableMap.of("event_date", singleValue(DATE, jan2InDays)),
                            ImmutableMap.of())),
                    exchange(exchange(tableScan("test_distinct_t2"))));

            // Branch 2: JOIN(t1 constrained to event_date != jan2, t2 constrained to reg_date=jan2)
            PlanMatchPattern joinBranch2 = join(
                    exchange(constrainedTableScan("test_distinct_t1",
                            ImmutableMap.of("event_date", create(SortedRangeSet.copyOf(DATE, ImmutableList.of(
                                    lessThan(DATE, jan2InDays),
                                    greaterThan(DATE, jan2InDays))), false)),
                            ImmutableMap.of())),
                    exchange(exchange(constrainedTableScan("test_distinct_t2",
                            ImmutableMap.of("reg_date", singleValue(DATE, jan2InDays)),
                            ImmutableMap.of()))));

            // The Aggregate(FINAL) with both branches under LocalExchange
            PlanMatchPattern aggregationPattern = aggregation(
                    ImmutableMap.of(),
                    FINAL,
                    exchange(
                            project(exchange(aggregation(ImmutableMap.of(), PARTIAL, project(joinBranch1)))),
                            project(exchange(aggregation(ImmutableMap.of(), PARTIAL, project(joinBranch2))))));

            // Query without ORDER BY to simplify plan (testing MV stitching, not sorting)
            assertPlan("SELECT * FROM test_distinct_mv",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__test_distinct_mv",
                                            ImmutableMap.of(
                                                    "event_date", create(SortedRangeSet.copyOf(DATE, ImmutableList.of(
                                                            lessThan(DATE, jan2InDays),
                                                            greaterThan(DATE, jan2InDays))), false),
                                                    "reg_date", create(SortedRangeSet.copyOf(DATE, ImmutableList.of(
                                                            lessThan(DATE, jan2InDays),
                                                            greaterThan(DATE, jan2InDays))), false)),
                                            ImmutableMap.of()),
                                    aggregationPattern)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS test_distinct_mv");
            assertUpdate("DROP TABLE IF EXISTS test_distinct_t2");
            assertUpdate("DROP TABLE IF EXISTS test_distinct_t1");
        }
    }

    /**
     * Test (A JOIN B) EXCEPT (C JOIN D) where A (left side of EXCEPT) becomes stale.
     * Verifies that the MV storage reads fresh partitions while the stale branch
     * reads from base tables. The stale predicate on A may propagate to B via the
     * join condition (a.dt = b.dt), and tables C/D may be pruned to Values if they
     * have no data matching the stale partition.
     */
    @Test
    public void testJoinExceptJoinWithLeftSideStale()
    {
        try {
            // Create 4 tables for (A JOIN B) EXCEPT (C JOIN D)
            assertUpdate("CREATE TABLE jexj_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE jexj_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE jexj_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE jexj_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");

            // Initial data
            assertUpdate("INSERT INTO jexj_a VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-01')", 2);
            assertUpdate("INSERT INTO jexj_b VALUES (10, 'x', '2024-01-01'), (20, 'y', '2024-01-01')", 2);
            assertUpdate("INSERT INTO jexj_c VALUES (1, 10, '2024-01-01')", 1);
            assertUpdate("INSERT INTO jexj_d VALUES (10, 'x', '2024-01-01')", 1);

            // Create MV: (A JOIN B) EXCEPT (C JOIN D)
            assertUpdate("CREATE MATERIALIZED VIEW mv_jexj " +
                    "WITH (partitioning = ARRAY['dt']) AS " +
                    "SELECT a.id, b.value, a.dt " +
                    "FROM jexj_a a JOIN jexj_b b ON a.key = b.key AND a.dt = b.dt " +
                    "EXCEPT " +
                    "SELECT c.id, d.value, c.dt " +
                    "FROM jexj_c c JOIN jexj_d d ON c.key = d.key AND c.dt = d.dt");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jexj");

            // Make A stale by inserting new partition
            assertUpdate("INSERT INTO jexj_a VALUES (3, 30, '2024-01-02')", 1);
            assertUpdate("INSERT INTO jexj_b VALUES (30, 'z', '2024-01-02')", 1);

            // The actual plan has:
            // 1. MV storage scan with constraint excluding stale partition
            // 2. First FilterProject branch: computes EXCEPT with A JOIN B and C JOIN D
            //    - Left side (A JOIN B) constrained to stale partition
            //    - Right side (C JOIN D) scans all rows (no constraint since C/D have data for '2024-01-01')
            // 3. Second FilterProject branch: computes EXCEPT with only C JOIN D
            // Join structure for A JOIN B:
            PlanMatchPattern leftJoinBranch = join(
                    exchange(constrainedTableScan("jexj_a",
                            ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                            ImmutableMap.of())),
                    exchange(exchange(constrainedTableScan("jexj_b",
                            ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                            ImmutableMap.of()))));

            // C JOIN D scans the full table (data exists for other partitions)
            PlanMatchPattern rightJoinBranch = join(
                    exchange(tableScan("jexj_c")),
                    exchange(exchange(tableScan("jexj_d"))));

            // First EXCEPT branch with both sides
            PlanMatchPattern exceptBranch1 = project(filter(
                    aggregation(
                            ImmutableMap.of(),
                            FINAL,
                            exchange(
                                    project(exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(leftJoinBranch)))),
                                    project(exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(rightJoinBranch))))))));

            // Second EXCEPT branch with only right side (C JOIN D)
            PlanMatchPattern exceptBranch2 = project(filter(
                    aggregation(
                            ImmutableMap.of(),
                            FINAL,
                            exchange(
                                    exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(rightJoinBranch)))))));

            assertPlan("SELECT * FROM mv_jexj",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_jexj",
                                            ImmutableMap.of("dt", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("dt", "dt", "id", "id", "value", "value")),
                                    exceptBranch1,
                                    exceptBranch2)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_jexj");
            assertUpdate("DROP TABLE IF EXISTS jexj_d");
            assertUpdate("DROP TABLE IF EXISTS jexj_c");
            assertUpdate("DROP TABLE IF EXISTS jexj_b");
            assertUpdate("DROP TABLE IF EXISTS jexj_a");
        }
    }

    /**
     * Test (A JOIN B) EXCEPT (C JOIN D) where C (right side of EXCEPT) becomes stale.
     * Verifies that the MV storage reads fresh partitions while the stale branch
     * computes the EXCEPT for the stale partition. C gets the stale predicate,
     * D may get it via join condition, and A/B tables scan the matching partition.
     */
    @Test
    public void testJoinExceptJoinWithRightSideStale()
    {
        try {
            // Create 4 tables for (A JOIN B) EXCEPT (C JOIN D)
            assertUpdate("CREATE TABLE jexjr_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE jexjr_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE jexjr_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE jexjr_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");

            // Initial data
            assertUpdate("INSERT INTO jexjr_a VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-01')", 2);
            assertUpdate("INSERT INTO jexjr_b VALUES (10, 'x', '2024-01-01'), (20, 'y', '2024-01-01')", 2);
            assertUpdate("INSERT INTO jexjr_c VALUES (1, 10, '2024-01-01')", 1);
            assertUpdate("INSERT INTO jexjr_d VALUES (10, 'x', '2024-01-01')", 1);

            // Create MV: (A JOIN B) EXCEPT (C JOIN D)
            assertUpdate("CREATE MATERIALIZED VIEW mv_jexjr " +
                    "WITH (partitioning = ARRAY['dt']) AS " +
                    "SELECT a.id, b.value, a.dt " +
                    "FROM jexjr_a a JOIN jexjr_b b ON a.key = b.key AND a.dt = b.dt " +
                    "EXCEPT " +
                    "SELECT c.id, d.value, c.dt " +
                    "FROM jexjr_c c JOIN jexjr_d d ON c.key = d.key AND c.dt = d.dt");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jexjr");

            // Make C stale (right side of EXCEPT)
            assertUpdate("INSERT INTO jexjr_c VALUES (2, 20, '2024-01-02')", 1);
            assertUpdate("INSERT INTO jexjr_d VALUES (20, 'y', '2024-01-02')", 1);

            // The actual plan has:
            // 1. MV storage scan with constraint excluding stale partition
            // 2. First FilterProject branch: Aggregate(FINAL) -> LocalExchange -> RemoteExchange -> Aggregate(PARTIAL) -> Project -> C JOIN D
            // 3. Second FilterProject branch: Same structure as first
            // Each branch has only ONE Aggregate(PARTIAL) child under LocalExchange

            // C JOIN D pattern
            PlanMatchPattern rightJoinBranch = join(
                    exchange(tableScan("jexjr_c")),
                    exchange(exchange(tableScan("jexjr_d"))));

            // Both EXCEPT branches have the same structure: single Aggregate(PARTIAL) child
            PlanMatchPattern exceptBranch = project(filter(
                    aggregation(
                            ImmutableMap.of(),
                            FINAL,
                            exchange(
                                    exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(rightJoinBranch)))))));

            assertPlan("SELECT * FROM mv_jexjr",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_jexjr",
                                            ImmutableMap.of("dt", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-02")))), false)),
                                            ImmutableMap.of("dt", "dt", "id", "id", "value", "value")),
                                    exceptBranch,
                                    exceptBranch)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_jexjr");
            assertUpdate("DROP TABLE IF EXISTS jexjr_d");
            assertUpdate("DROP TABLE IF EXISTS jexjr_c");
            assertUpdate("DROP TABLE IF EXISTS jexjr_b");
            assertUpdate("DROP TABLE IF EXISTS jexjr_a");
        }
    }

    /**
     * Test (A JOIN B) EXCEPT (C JOIN D) where both A and C become stale.
     * This should create two union branches (one for A's stale partition, one for C's).
     */
    @Test
    public void testJoinExceptJoinWithBothSidesStale()
    {
        try {
            // Create 4 tables for (A JOIN B) EXCEPT (C JOIN D)
            assertUpdate("CREATE TABLE jexjb_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE jexjb_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE jexjb_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");
            assertUpdate("CREATE TABLE jexjb_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                    "WITH (partitioning = ARRAY['dt'])");

            // Initial data
            assertUpdate("INSERT INTO jexjb_a VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-01')", 2);
            assertUpdate("INSERT INTO jexjb_b VALUES (10, 'x', '2024-01-01'), (20, 'y', '2024-01-01')", 2);
            assertUpdate("INSERT INTO jexjb_c VALUES (1, 10, '2024-01-01')", 1);
            assertUpdate("INSERT INTO jexjb_d VALUES (10, 'x', '2024-01-01')", 1);

            // Create MV: (A JOIN B) EXCEPT (C JOIN D)
            assertUpdate("CREATE MATERIALIZED VIEW mv_jexjb " +
                    "WITH (partitioning = ARRAY['dt']) AS " +
                    "SELECT a.id, b.value, a.dt " +
                    "FROM jexjb_a a JOIN jexjb_b b ON a.key = b.key AND a.dt = b.dt " +
                    "EXCEPT " +
                    "SELECT c.id, d.value, c.dt " +
                    "FROM jexjb_c c JOIN jexjb_d d ON c.key = d.key AND c.dt = d.dt");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jexjb");

            // Make both A and C stale with different partitions
            assertUpdate("INSERT INTO jexjb_a VALUES (3, 30, '2024-01-02')", 1);
            assertUpdate("INSERT INTO jexjb_b VALUES (30, 'z', '2024-01-02')", 1);
            assertUpdate("INSERT INTO jexjb_c VALUES (4, 40, '2024-01-03')", 1);
            assertUpdate("INSERT INTO jexjb_d VALUES (40, 'w', '2024-01-03')", 1);

            // The actual plan has:
            // 1. MV storage scan excluding both stale partitions
            // 2. First FilterProject: A JOIN B (constrained to 2024-01-02) + C JOIN D (full scan)
            // 3. Second FilterProject: only C JOIN D (full scan)
            // The optimizer doesn't produce Values nodes - C and D are scanned fully
            PlanMatchPattern joinABForBranch1 = join(
                    exchange(constrainedTableScan("jexjb_a",
                            ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                            ImmutableMap.of())),
                    exchange(exchange(constrainedTableScan("jexjb_b",
                            ImmutableMap.of("dt", singleValue(VARCHAR, utf8Slice("2024-01-02"))),
                            ImmutableMap.of()))));

            // C JOIN D scans full table (data exists for '2024-01-01')
            PlanMatchPattern joinCD = join(
                    exchange(tableScan("jexjb_c")),
                    exchange(exchange(tableScan("jexjb_d"))));

            // First branch with A JOIN B and C JOIN D
            PlanMatchPattern exceptBranch1 = project(filter(
                    aggregation(
                            ImmutableMap.of(),
                            FINAL,
                            exchange(
                                    project(exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(joinABForBranch1)))),
                                    project(exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(joinCD))))))));

            // Second branch with only C JOIN D
            PlanMatchPattern exceptBranch2 = project(filter(
                    aggregation(
                            ImmutableMap.of(),
                            FINAL,
                            exchange(
                                    exchange(
                                            aggregation(
                                                    ImmutableMap.of(),
                                                    PARTIAL,
                                                    project(joinCD)))))));

            assertPlan("SELECT * FROM mv_jexjb",
                    output(
                            exchange(
                                    constrainedTableScan("__mv_storage__mv_jexjb",
                                            ImmutableMap.of("dt", create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                                                    lessThan(VARCHAR, utf8Slice("2024-01-02")),
                                                    Range.range(VARCHAR, utf8Slice("2024-01-02"), false, utf8Slice("2024-01-03"), false),
                                                    greaterThan(VARCHAR, utf8Slice("2024-01-03")))), false)),
                                            ImmutableMap.of("dt", "dt", "id", "id", "value", "value")),
                                    exceptBranch1,
                                    exceptBranch2)));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS mv_jexjb");
            assertUpdate("DROP TABLE IF EXISTS jexjb_d");
            assertUpdate("DROP TABLE IF EXISTS jexjb_c");
            assertUpdate("DROP TABLE IF EXISTS jexjb_b");
            assertUpdate("DROP TABLE IF EXISTS jexjb_a");
        }
    }
}
