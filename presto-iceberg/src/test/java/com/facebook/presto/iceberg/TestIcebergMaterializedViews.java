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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

@Test(singleThreaded = true)
public class TestIcebergMaterializedViews
        extends AbstractTestQueryFramework
{
    private File warehouseLocation;
    private TestingHttpServer restServer;
    private String serverUri;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();

        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();

        serverUri = restServer.getBaseUrl().toString();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(restConnectorProperties(serverUri))
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(false)
                .build().getQueryRunner();
    }

    @Test
    public void testCreateMaterializedView()
    {
        assertUpdate("CREATE TABLE test_mv_base (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO test_mv_base VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_simple AS SELECT id, name, value FROM test_mv_base");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_simple\"", "SELECT 0");

        assertQuery("SELECT COUNT(*) FROM test_mv_simple", "SELECT 3");
        assertQuery("SELECT * FROM test_mv_simple ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_simple");
        assertUpdate("DROP TABLE test_mv_base");
    }

    @Test
    public void testCreateMaterializedViewWithFilter()
    {
        assertUpdate("CREATE TABLE test_mv_filtered_base (id BIGINT, status VARCHAR, amount BIGINT)");
        assertUpdate("INSERT INTO test_mv_filtered_base VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'active', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_filtered AS SELECT id, amount FROM test_mv_filtered_base WHERE status = 'active'");

        assertQuery("SELECT COUNT(*) FROM test_mv_filtered", "SELECT 2");
        assertQuery("SELECT * FROM test_mv_filtered ORDER BY id",
                "VALUES (1, 100), (3, 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_filtered");
        assertUpdate("DROP TABLE test_mv_filtered_base");
    }

    @Test
    public void testCreateMaterializedViewWithAggregation()
    {
        assertUpdate("CREATE TABLE test_mv_sales (product_id BIGINT, category VARCHAR, revenue BIGINT)");
        assertUpdate("INSERT INTO test_mv_sales VALUES (1, 'Electronics', 1000), (2, 'Electronics', 1500), (3, 'Books', 500), (4, 'Books', 300)", 4);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_category_sales AS " +
                "SELECT category, COUNT(*) as product_count, SUM(revenue) as total_revenue " +
                "FROM test_mv_sales GROUP BY category");

        assertQuery("SELECT COUNT(*) FROM test_mv_category_sales", "SELECT 2");
        assertQuery("SELECT * FROM test_mv_category_sales ORDER BY category",
                "VALUES ('Books', 2, 800), ('Electronics', 2, 2500)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_category_sales");
        assertUpdate("DROP TABLE test_mv_sales");
    }

    @Test
    public void testMaterializedViewStaleness()
    {
        assertUpdate("CREATE TABLE test_mv_stale_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_mv_stale_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_stale AS SELECT id, value FROM test_mv_stale_base");

        assertQuery("SELECT COUNT(*) FROM test_mv_stale", "SELECT 2");
        assertQuery("SELECT * FROM test_mv_stale ORDER BY id", "VALUES (1, 100), (2, 200)");

        assertUpdate("INSERT INTO test_mv_stale_base VALUES (3, 300)", 1);

        assertQuery("SELECT COUNT(*) FROM test_mv_stale", "SELECT 3");
        assertQuery("SELECT * FROM test_mv_stale ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_stale");
        assertUpdate("DROP TABLE test_mv_stale_base");
    }

    @Test
    public void testDropMaterializedView()
    {
        assertUpdate("CREATE TABLE test_mv_drop_base (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO test_mv_drop_base VALUES (1, 'test')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_drop AS SELECT id, value FROM test_mv_drop_base");

        assertQuery("SELECT COUNT(*) FROM test_mv_drop", "SELECT 1");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_drop\"", "SELECT 0");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_drop");

        assertQueryFails("SELECT * FROM \"__mv_storage__test_mv_drop\"", ".*does not exist.*");

        assertQuery("SELECT COUNT(*) FROM test_mv_drop_base", "SELECT 1");

        assertUpdate("DROP TABLE test_mv_drop_base");
    }

    @Test
    public void testMaterializedViewMetadata()
    {
        assertUpdate("CREATE TABLE test_mv_metadata_base (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_mv_metadata_base VALUES (1, 'test')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_metadata AS SELECT id, name FROM test_mv_metadata_base WHERE id > 0");

        assertQueryReturnsEmptyResult("SELECT table_name FROM information_schema.tables " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_mv_metadata' AND table_type = 'MATERIALIZED VIEW'");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_metadata");
        assertUpdate("DROP TABLE test_mv_metadata_base");
    }

    @Test
    public void testRefreshMaterializedView()
    {
        assertUpdate("CREATE TABLE test_mv_refresh_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_mv_refresh_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_refresh AS SELECT id, value FROM test_mv_refresh_base");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 0");

        assertQuery("SELECT COUNT(*) FROM test_mv_refresh", "SELECT 2");
        assertQuery("SELECT * FROM test_mv_refresh ORDER BY id", "VALUES (1, 100), (2, 200)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_refresh", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 2");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_refresh\" ORDER BY id",
                "VALUES (1, 100), (2, 200)");

        assertQuery("SELECT COUNT(*) FROM test_mv_refresh", "SELECT 2");
        assertQuery("SELECT * FROM test_mv_refresh ORDER BY id", "VALUES (1, 100), (2, 200)");

        assertUpdate("INSERT INTO test_mv_refresh_base VALUES (3, 300)", 1);

        assertQuery("SELECT COUNT(*) FROM test_mv_refresh", "SELECT 3");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 2");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_refresh", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_refresh\" ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_refresh");
        assertUpdate("DROP TABLE test_mv_refresh_base");
    }

    @Test
    public void testRefreshMaterializedViewWithAggregation()
    {
        assertUpdate("CREATE TABLE test_mv_agg_refresh_base (category VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO test_mv_agg_refresh_base VALUES ('A', 10), ('B', 20), ('A', 15)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_agg_refresh AS " +
                "SELECT category, SUM(value) as total FROM test_mv_agg_refresh_base GROUP BY category");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_agg_refresh\"", "SELECT 0");

        assertQuery("SELECT * FROM test_mv_agg_refresh ORDER BY category",
                "VALUES ('A', 25), ('B', 20)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_agg_refresh", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_agg_refresh\"", "SELECT 2");

        assertUpdate("INSERT INTO test_mv_agg_refresh_base VALUES ('A', 5), ('C', 30)", 2);

        assertQuery("SELECT * FROM test_mv_agg_refresh ORDER BY category",
                "VALUES ('A', 30), ('B', 20), ('C', 30)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_agg_refresh", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_agg_refresh\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_agg_refresh\" ORDER BY category",
                "VALUES ('A', 30), ('B', 20), ('C', 30)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_agg_refresh");
        assertUpdate("DROP TABLE test_mv_agg_refresh_base");
    }

    @Test
    public void testPartitionedMaterializedViewWithStaleDataConstraints()
    {
        assertUpdate("CREATE TABLE test_mv_partitioned_base (" +
                "id BIGINT, " +
                "event_date DATE, " +
                "value BIGINT) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_mv_partitioned_base VALUES " +
                "(1, DATE '2024-01-01', 100), " +
                "(2, DATE '2024-01-01', 200), " +
                "(3, DATE '2024-01-02', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_partitioned AS " +
                "SELECT id, event_date, value FROM test_mv_partitioned_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_partitioned", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_partitioned\"", "SELECT 3");

        assertQuery("SELECT COUNT(*) FROM test_mv_partitioned", "SELECT 3");
        assertQuery("SELECT * FROM test_mv_partitioned ORDER BY id",
                "VALUES (1, DATE '2024-01-01', 100), (2, DATE '2024-01-01', 200), (3, DATE '2024-01-02', 300)");

        assertUpdate("INSERT INTO test_mv_partitioned_base VALUES " +
                "(4, DATE '2024-01-03', 400), " +
                "(5, DATE '2024-01-03', 500)", 2);

        assertQuery("SELECT COUNT(*) FROM test_mv_partitioned", "SELECT 5");
        assertQuery("SELECT * FROM test_mv_partitioned ORDER BY id",
                "VALUES (1, DATE '2024-01-01', 100), " +
                        "(2, DATE '2024-01-01', 200), " +
                        "(3, DATE '2024-01-02', 300), " +
                        "(4, DATE '2024-01-03', 400), " +
                        "(5, DATE '2024-01-03', 500)");

        assertUpdate("INSERT INTO test_mv_partitioned_base VALUES " +
                "(6, DATE '2024-01-04', 600)", 1);

        assertQuery("SELECT COUNT(*) FROM test_mv_partitioned", "SELECT 6");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_partitioned");
        assertUpdate("DROP TABLE test_mv_partitioned_base");
    }

    @Test
    public void testMinimalRefresh()
    {
        assertUpdate("CREATE TABLE minimal_table (id BIGINT)");
        assertUpdate("INSERT INTO minimal_table VALUES (1)", 1);
        assertUpdate("CREATE MATERIALIZED VIEW minimal_mv AS SELECT id FROM minimal_table");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__minimal_mv\"", "SELECT 0");

        try {
            assertUpdate("REFRESH MATERIALIZED VIEW minimal_mv", 1);
        }
        catch (Exception e) {
            System.err.println("REFRESH failed with: " + e.getMessage());
        }

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__minimal_mv\"", "SELECT 1");
        assertQuery("SELECT * FROM \"__mv_storage__minimal_mv\"", "SELECT 1");

        assertUpdate("DROP MATERIALIZED VIEW minimal_mv");
        assertUpdate("DROP TABLE minimal_table");
    }

    @Test
    public void testJoinMaterializedViewLifecycle()
    {
        assertUpdate("CREATE TABLE test_mv_orders (order_id BIGINT, customer_id BIGINT, amount BIGINT)");
        assertUpdate("CREATE TABLE test_mv_customers (customer_id BIGINT, customer_name VARCHAR)");

        assertUpdate("INSERT INTO test_mv_orders VALUES (1, 100, 50), (2, 200, 75), (3, 100, 25)", 3);
        assertUpdate("INSERT INTO test_mv_customers VALUES (100, 'Alice'), (200, 'Bob')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_order_details AS " +
                "SELECT o.order_id, c.customer_name, o.amount " +
                "FROM test_mv_orders o JOIN test_mv_customers c ON o.customer_id = c.customer_id");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_order_details\"", "SELECT 0");

        assertQuery("SELECT COUNT(*) FROM test_mv_order_details", "SELECT 3");
        assertQuery("SELECT * FROM test_mv_order_details ORDER BY order_id",
                "VALUES (1, 'Alice', 50), (2, 'Bob', 75), (3, 'Alice', 25)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_order_details", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_order_details\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_order_details\" ORDER BY order_id",
                "VALUES (1, 'Alice', 50), (2, 'Bob', 75), (3, 'Alice', 25)");

        assertQuery("SELECT COUNT(*) FROM test_mv_order_details", "SELECT 3");

        assertUpdate("INSERT INTO test_mv_orders VALUES (4, 200, 100)", 1);

        assertQuery("SELECT COUNT(*) FROM test_mv_order_details", "SELECT 4");
        assertQuery("SELECT * FROM test_mv_order_details ORDER BY order_id",
                "VALUES (1, 'Alice', 50), (2, 'Bob', 75), (3, 'Alice', 25), (4, 'Bob', 100)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_order_details", 4);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_order_details\"", "SELECT 4");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_order_details");
        assertUpdate("DROP TABLE test_mv_customers");
        assertUpdate("DROP TABLE test_mv_orders");
    }

    @Test
    public void testPartitionedJoinMaterializedView()
    {
        assertUpdate("CREATE TABLE test_mv_part_orders (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "order_date DATE, " +
                "amount BIGINT) " +
                "WITH (partitioning = ARRAY['order_date'])");

        assertUpdate("CREATE TABLE test_mv_part_customers (customer_id BIGINT, customer_name VARCHAR)");


        assertUpdate("INSERT INTO test_mv_part_orders VALUES " +
                "(1, 100, DATE '2024-01-01', 50), " +
                "(2, 200, DATE '2024-01-01', 75), " +
                "(3, 100, DATE '2024-01-02', 25)", 3);
        assertUpdate("INSERT INTO test_mv_part_customers VALUES (100, 'Alice'), (200, 'Bob')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_part_join AS " +
                "SELECT o.order_id, c.customer_name, o.order_date, o.amount " +
                "FROM test_mv_part_orders o JOIN test_mv_part_customers c ON o.customer_id = c.customer_id");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_part_join", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_part_join\"", "SELECT 3");

        assertQuery("SELECT COUNT(*) FROM test_mv_part_join", "SELECT 3");
        assertQuery("SELECT * FROM test_mv_part_join ORDER BY order_id",
                "VALUES (1, 'Alice', DATE '2024-01-01', 50), " +
                        "(2, 'Bob', DATE '2024-01-01', 75), " +
                        "(3, 'Alice', DATE '2024-01-02', 25)");

        assertUpdate("INSERT INTO test_mv_part_orders VALUES (4, 200, DATE '2024-01-03', 100)", 1);

        assertQuery("SELECT COUNT(*) FROM test_mv_part_join", "SELECT 4");
        assertQuery("SELECT * FROM test_mv_part_join ORDER BY order_id",
                "VALUES (1, 'Alice', DATE '2024-01-01', 50), " +
                        "(2, 'Bob', DATE '2024-01-01', 75), " +
                        "(3, 'Alice', DATE '2024-01-02', 25), " +
                        "(4, 'Bob', DATE '2024-01-03', 100)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_part_join", 4);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_part_join\"", "SELECT 4");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_part_join");
        assertUpdate("DROP TABLE test_mv_part_customers");
        assertUpdate("DROP TABLE test_mv_part_orders");
    }

    @Test
    public void testMultiTableStaleness_TwoTablesBothStale()
    {
        assertUpdate("CREATE TABLE test_mv_orders (" +
                "order_id BIGINT, " +
                "order_date DATE, " +
                "amount BIGINT) " +
                "WITH (partitioning = ARRAY['order_date'])");

        assertUpdate("CREATE TABLE test_mv_customers (" +
                "customer_id BIGINT, " +
                "reg_date DATE, " +
                "name VARCHAR) " +
                "WITH (partitioning = ARRAY['reg_date'])");

        assertUpdate("INSERT INTO test_mv_orders VALUES " +
                "(1, DATE '2024-01-01', 100), " +
                "(2, DATE '2024-01-02', 200)", 2);
        assertUpdate("INSERT INTO test_mv_customers VALUES " +
                "(1, DATE '2024-01-01', 'Alice'), " +
                "(2, DATE '2024-01-02', 'Bob')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_multi_stale AS " +
                "SELECT o.order_id, c.name, o.order_date, c.reg_date, o.amount " +
                "FROM test_mv_orders o JOIN test_mv_customers c ON o.order_id = c.customer_id");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_multi_stale", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_multi_stale\"", "SELECT 2");

        assertQuery("SELECT COUNT(*) FROM test_mv_multi_stale", "SELECT 2");

        assertUpdate("INSERT INTO test_mv_orders VALUES (3, DATE '2024-01-03', 300)", 1);
        assertUpdate("INSERT INTO test_mv_customers VALUES (3, DATE '2024-01-03', 'Charlie')", 1);

        assertQuery("SELECT COUNT(*) FROM test_mv_multi_stale", "SELECT 3");
        assertQuery("SELECT order_id, name, order_date, reg_date, amount FROM test_mv_multi_stale ORDER BY order_id",
                "VALUES (1, 'Alice', DATE '2024-01-01', DATE '2024-01-01', 100), " +
                        "(2, 'Bob', DATE '2024-01-02', DATE '2024-01-02', 200), " +
                        "(3, 'Charlie', DATE '2024-01-03', DATE '2024-01-03', 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_multi_stale");
        assertUpdate("DROP TABLE test_mv_customers");
        assertUpdate("DROP TABLE test_mv_orders");
    }

    @Test
    public void testMultiTableStaleness_ThreeTablesWithTwoStale()
    {
        assertUpdate("CREATE TABLE test_mv_t1 (" +
                "id BIGINT, " +
                "date1 DATE, " +
                "value1 BIGINT) " +
                "WITH (partitioning = ARRAY['date1'])");

        assertUpdate("CREATE TABLE test_mv_t2 (" +
                "id BIGINT, " +
                "date2 DATE, " +
                "value2 BIGINT) " +
                "WITH (partitioning = ARRAY['date2'])");

        assertUpdate("CREATE TABLE test_mv_t3 (" +
                "id BIGINT, " +
                "date3 DATE, " +
                "value3 BIGINT) " +
                "WITH (partitioning = ARRAY['date3'])");

        assertUpdate("INSERT INTO test_mv_t1 VALUES (1, DATE '2024-01-01', 100)", 1);
        assertUpdate("INSERT INTO test_mv_t2 VALUES (1, DATE '2024-01-01', 200)", 1);
        assertUpdate("INSERT INTO test_mv_t3 VALUES (1, DATE '2024-01-01', 300)", 1);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_three_tables AS " +
                "SELECT t1.id, t1.date1, t2.date2, t3.date3, " +
                "       t1.value1, t2.value2, t3.value3 " +
                "FROM test_mv_t1 t1 " +
                "JOIN test_mv_t2 t2 ON t1.id = t2.id " +
                "JOIN test_mv_t3 t3 ON t1.id = t3.id");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_three_tables", 1);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_three_tables\"", "SELECT 1");

        assertUpdate("INSERT INTO test_mv_t1 VALUES (2, DATE '2024-01-02', 150)", 1);
        assertUpdate("INSERT INTO test_mv_t2 VALUES (2, DATE '2024-01-01', 250)", 1);
        assertUpdate("INSERT INTO test_mv_t3 VALUES (2, DATE '2024-01-02', 350)", 1);

        assertQuery("SELECT COUNT(*) FROM test_mv_three_tables", "SELECT 2");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_three_tables");
        assertUpdate("DROP TABLE test_mv_t3");
        assertUpdate("DROP TABLE test_mv_t2");
        assertUpdate("DROP TABLE test_mv_t1");
    }

    @Test
    public void testMultiTableStaleness_DifferentPartitionCounts()
    {
        assertUpdate("CREATE TABLE test_mv_table_a (" +
                "id BIGINT, " +
                "date_a DATE, " +
                "value BIGINT) " +
                "WITH (partitioning = ARRAY['date_a'])");

        assertUpdate("CREATE TABLE test_mv_table_b (" +
                "id BIGINT, " +
                "date_b DATE, " +
                "status VARCHAR) " +
                "WITH (partitioning = ARRAY['date_b'])");

        assertUpdate("INSERT INTO test_mv_table_a VALUES " +
                "(1, DATE '2024-01-01', 100), " +
                "(2, DATE '2024-01-02', 200)", 2);
        assertUpdate("INSERT INTO test_mv_table_b VALUES " +
                "(1, DATE '2024-01-01', 'active'), " +
                "(2, DATE '2024-01-02', 'inactive')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_diff_partitions AS " +
                "SELECT a.id, a.date_a, b.date_b, a.value, b.status " +
                "FROM test_mv_table_a a JOIN test_mv_table_b b ON a.id = b.id");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_diff_partitions", 2);

        assertUpdate("INSERT INTO test_mv_table_a VALUES " +
                "(3, DATE '2024-01-03', 300), " +
                "(4, DATE '2024-01-04', 400), " +
                "(5, DATE '2024-01-05', 500)", 3);

        assertUpdate("INSERT INTO test_mv_table_b VALUES " +
                "(3, DATE '2024-01-03', 'active'), " +
                "(4, DATE '2024-01-04', 'active'), " +
                "(5, DATE '2024-01-05', 'pending')", 3);

        assertQuery("SELECT COUNT(*) FROM test_mv_diff_partitions", "SELECT 5");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_diff_partitions");
        assertUpdate("DROP TABLE test_mv_table_b");
        assertUpdate("DROP TABLE test_mv_table_a");
    }

    @Test
    public void testMultiTableStaleness_NonPartitionedAndPartitionedBothStale()
    {
        assertUpdate("CREATE TABLE test_mv_non_part (id BIGINT, category VARCHAR)");

        assertUpdate("CREATE TABLE test_mv_part_sales (" +
                "id BIGINT, " +
                "sale_date DATE, " +
                "amount BIGINT) " +
                "WITH (partitioning = ARRAY['sale_date'])");

        assertUpdate("INSERT INTO test_mv_non_part VALUES (1, 'Electronics'), (2, 'Books')", 2);
        assertUpdate("INSERT INTO test_mv_part_sales VALUES " +
                "(1, DATE '2024-01-01', 500), " +
                "(2, DATE '2024-01-02', 300)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_mixed_stale AS " +
                "SELECT c.id, c.category, s.sale_date, s.amount " +
                "FROM test_mv_non_part c JOIN test_mv_part_sales s ON c.id = s.id");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_mixed_stale", 2);

        assertUpdate("INSERT INTO test_mv_non_part VALUES (3, 'Toys')", 1);
        assertUpdate("INSERT INTO test_mv_part_sales VALUES (3, DATE '2024-01-03', 700)", 1);

        assertQuery("SELECT COUNT(*) FROM test_mv_mixed_stale", "SELECT 3");
        assertQuery("SELECT id, category, sale_date, amount FROM test_mv_mixed_stale ORDER BY id",
                "VALUES (1, 'Electronics', DATE '2024-01-01', 500), " +
                        "(2, 'Books', DATE '2024-01-02', 300), " +
                        "(3, 'Toys', DATE '2024-01-03', 700)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_mixed_stale");
        assertUpdate("DROP TABLE test_mv_part_sales");
        assertUpdate("DROP TABLE test_mv_non_part");
    }

    @Test
    public void testPartitionAlignment_MatchingColumns()
    {
        assertUpdate("CREATE TABLE test_pa_matching_base (" +
                "id BIGINT, " +
                "event_date DATE, " +
                "amount BIGINT) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_pa_matching_base VALUES " +
                "(1, DATE '2024-01-01', 100), " +
                "(2, DATE '2024-01-02', 200), " +
                "(3, DATE '2024-01-03', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_pa_matching_mv AS " +
                "SELECT id, event_date, amount FROM test_pa_matching_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_pa_matching_mv", 3);

        assertUpdate("INSERT INTO test_pa_matching_base VALUES (4, DATE '2024-01-04', 400)", 1);

        assertQuery("SELECT COUNT(*) FROM test_pa_matching_mv", "SELECT 4");
        assertQuery("SELECT id, event_date, amount FROM test_pa_matching_mv ORDER BY id",
                "VALUES " +
                        "(1, DATE '2024-01-01', 100), " +
                        "(2, DATE '2024-01-02', 200), " +
                        "(3, DATE '2024-01-03', 300), " +
                        "(4, DATE '2024-01-04', 400)");

        assertUpdate("DROP MATERIALIZED VIEW test_pa_matching_mv");
        assertUpdate("DROP TABLE test_pa_matching_base");
    }

    @Test
    public void testPartitionAlignment_MissingConstraintColumn()
    {
        // Test: Base partition columns missing from storage schema → complete recompute (falls back)
        //
        // Setup:
        // - Base table partitioned by event_date
        // - MV does NOT include event_date in output → storage doesn't have event_date column
        // - Stale partition constraint: event_date = '2024-01-04'
        //
        // Expected: event_date missing from storage → cannot project constraints → falls back to full recompute

        // Create partitioned base table
        assertUpdate("CREATE TABLE test_pa_missing_base (" +
                "id BIGINT, " +
                "event_date DATE, " +
                "amount BIGINT) " +
                "WITH (partitioning = ARRAY['event_date'])");

        // Insert initial data
        assertUpdate("INSERT INTO test_pa_missing_base VALUES " +
                "(1, DATE '2024-01-01', 100), " +
                "(2, DATE '2024-01-02', 200), " +
                "(3, DATE '2024-01-03', 300)", 3);

        // Create MV that EXCLUDES the partition column from output
        // This means storage table won't have event_date column
        assertUpdate("CREATE MATERIALIZED VIEW test_pa_missing_mv AS " +
                "SELECT id, amount FROM test_pa_missing_base");

        // REFRESH to populate storage and record snapshot IDs
        assertUpdate("REFRESH MATERIALIZED VIEW test_pa_missing_mv", 3);

        // Insert stale partition
        assertUpdate("INSERT INTO test_pa_missing_base VALUES (4, DATE '2024-01-04', 400)", 1);

        // Query MV - should fall back to full recompute because:
        // 1. Stale constraint: event_date = '2024-01-04'
        // 2. Storage table does NOT have event_date column
        // 3. Cannot project constraint to storage (column doesn't exist)
        // 4. Falls back to conjunctiveConstraints = TupleDomain.all()
        // 5. Complete recompute from base table
        assertQuery("SELECT COUNT(*) FROM test_pa_missing_mv", "SELECT 4");
        assertQuery("SELECT id, amount FROM test_pa_missing_mv ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300), (4, 400)");

        // Cleanup
        assertUpdate("DROP MATERIALIZED VIEW test_pa_missing_mv");
        assertUpdate("DROP TABLE test_pa_missing_base");
    }

    @Test
    public void testPartitionAlignment_OverSpecifiedStorage()
    {
        // Test: Storage has extra columns beyond constraint columns → partial recompute (still works)
        //
        // Setup:
        // - Two base tables with different partitions: table_a by event_date, table_b by region
        // - MV joins both, includes both partition columns in output
        // - Storage has both event_date and region columns
        // - Only table_a has stale data: constraint is event_date = '2024-01-04'
        //
        // Expected: Storage has required column (event_date) plus extra (region) → constraints projected → UNION stitching

        // Create first partitioned table
        assertUpdate("CREATE TABLE test_pa_over_table_a (" +
                "id BIGINT, " +
                "event_date DATE, " +
                "amount BIGINT) " +
                "WITH (partitioning = ARRAY['event_date'])");

        // Create second partitioned table
        assertUpdate("CREATE TABLE test_pa_over_table_b (" +
                "customer_id BIGINT, " +
                "region VARCHAR, " +
                "name VARCHAR) " +
                "WITH (partitioning = ARRAY['region'])");

        // Insert initial data
        assertUpdate("INSERT INTO test_pa_over_table_a VALUES " +
                "(1, DATE '2024-01-01', 100), " +
                "(2, DATE '2024-01-02', 200), " +
                "(3, DATE '2024-01-03', 300)", 3);

        assertUpdate("INSERT INTO test_pa_over_table_b VALUES " +
                "(1, 'US', 'Alice'), " +
                "(2, 'US', 'Bob'), " +
                "(3, 'UK', 'Charlie')", 3);

        // Create MV that includes BOTH partition columns
        // Storage will have both event_date and region
        assertUpdate("CREATE MATERIALIZED VIEW test_pa_over_mv AS " +
                "SELECT a.id, a.event_date, a.amount, b.region, b.name " +
                "FROM test_pa_over_table_a a " +
                "JOIN test_pa_over_table_b b ON a.id = b.customer_id");

        // REFRESH to populate storage and record snapshot IDs
        assertUpdate("REFRESH MATERIALIZED VIEW test_pa_over_mv", 3);

        // Insert stale partition in table_a only (table_b remains fresh)
        assertUpdate("INSERT INTO test_pa_over_table_a VALUES (1, DATE '2024-01-04', 150)", 1);

        // Query MV - should use UNION stitching because:
        // 1. Only table_a has stale data: constraint is event_date = '2024-01-04'
        // 2. Storage has event_date column (and also has region, but that doesn't matter)
        // 3. Constraint can be projected to storage
        // 4. UNION: storage WHERE event_date != '2024-01-04' (fresh) + recomputed stale partition
        assertQuery("SELECT COUNT(*) FROM test_pa_over_mv", "SELECT 4");
        assertQuery("SELECT id, event_date, amount, region, name FROM test_pa_over_mv ORDER BY id, event_date",
                "VALUES " +
                        "(1, DATE '2024-01-01', 100, 'US', 'Alice'), " +
                        "(1, DATE '2024-01-04', 150, 'US', 'Alice'), " +
                        "(2, DATE '2024-01-02', 200, 'US', 'Bob'), " +
                        "(3, DATE '2024-01-03', 300, 'UK', 'Charlie')");

        // Cleanup
        assertUpdate("DROP MATERIALIZED VIEW test_pa_over_mv");
        assertUpdate("DROP TABLE test_pa_over_table_b");
        assertUpdate("DROP TABLE test_pa_over_table_a");
    }

    // NOTE: Phase 1B implements partition-level UNION stitching for queries.
    // When a partitioned MV becomes partially stale, the optimizer automatically:
    // 1. Reads fresh partitions from storage table
    // 2. Recomputes stale partitions from base table
    // 3. UNIONs both sides to produce complete results
    //
    // REFRESH MATERIALIZED VIEW always performs full refresh (no WHERE clause support).
    // Partition-level refresh planning is deferred to Phase 2.
}
