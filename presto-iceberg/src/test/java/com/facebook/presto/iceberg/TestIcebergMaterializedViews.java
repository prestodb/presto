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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
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
                .setExtraProperties(ImmutableMap.of("experimental.legacy-materialized-views", "false"))
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

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_stale", 3);
        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_stale\"", "SELECT 3");

        assertUpdate("TRUNCATE TABLE test_mv_stale_base");
        assertQuery("SELECT COUNT(*) FROM test_mv_stale_base", "SELECT 0");
        assertQuery("SELECT COUNT(*) FROM test_mv_stale", "SELECT 0");
        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_stale\"", "SELECT 3");

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

        assertQuery("SELECT table_name, table_type FROM information_schema.tables " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_mv_metadata'",
                "VALUES ('test_mv_metadata', 'MATERIALIZED VIEW')");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_metadata");
        assertUpdate("DROP TABLE test_mv_metadata_base");
    }

    @DataProvider(name = "baseTableNames")
    public Object[][] baseTableNamesProvider()
    {
        return new Object[][] {
                {"tt1"},
                {"\"tt2\""},
                {"\"tt.3\""},
                {"\"tt,4.5\""},
                {"\"tt\"\"tt,123\"\".123\""}
        };
    }

    @Test(dataProvider = "baseTableNames")
    public void testMaterializedViewWithSpecialBaseTableName(String tableName)
    {
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_refresh AS SELECT id, value FROM " + tableName);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 0");

        assertQuery("SELECT COUNT(*) FROM test_mv_refresh", "SELECT 2");
        assertQuery("SELECT * FROM test_mv_refresh ORDER BY id", "VALUES (1, 100), (2, 200)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_refresh", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 2");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_refresh\" ORDER BY id",
                "VALUES (1, 100), (2, 200)");

        assertQuery("SELECT COUNT(*) FROM test_mv_refresh", "SELECT 2");
        assertQuery("SELECT * FROM test_mv_refresh ORDER BY id", "VALUES (1, 100), (2, 200)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (3, 300)", 1);

        assertQuery("SELECT COUNT(*) FROM test_mv_refresh", "SELECT 3");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 2");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_refresh", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_refresh\" ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_refresh");
        assertUpdate("DROP TABLE " + tableName);
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

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_partitioned\"", "SELECT 0");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_partitioned", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_partitioned\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_partitioned\" ORDER BY id",
                "VALUES (1, DATE '2024-01-01', 100), (2, DATE '2024-01-01', 200), (3, DATE '2024-01-02', 300)");

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

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_order_details\"", "SELECT 3");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_order_details", 4);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_order_details\"", "SELECT 4");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_order_details\" ORDER BY order_id",
                "VALUES (1, 'Alice', 50), (2, 'Bob', 75), (3, 'Alice', 25), (4, 'Bob', 100)");

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

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_part_join\"", "SELECT 0");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_part_join", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_part_join\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_part_join\" ORDER BY order_id",
                "VALUES (1, 'Alice', DATE '2024-01-01', 50), " +
                        "(2, 'Bob', DATE '2024-01-01', 75), " +
                        "(3, 'Alice', DATE '2024-01-02', 25)");

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

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_mixed_stale\"", "SELECT 0");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_mixed_stale", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_mixed_stale\"", "SELECT 2");
        assertQuery("SELECT id, category, sale_date, amount FROM \"__mv_storage__test_mv_mixed_stale\" ORDER BY id",
                "VALUES (1, 'Electronics', DATE '2024-01-01', 500), (2, 'Books', DATE '2024-01-02', 300)");

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
        assertUpdate("CREATE TABLE test_pa_missing_base (" +
                "id BIGINT, " +
                "event_date DATE, " +
                "amount BIGINT) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_pa_missing_base VALUES " +
                "(1, DATE '2024-01-01', 100), " +
                "(2, DATE '2024-01-02', 200), " +
                "(3, DATE '2024-01-03', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_pa_missing_mv AS " +
                "SELECT id, amount FROM test_pa_missing_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_pa_missing_mv", 3);

        assertUpdate("INSERT INTO test_pa_missing_base VALUES (4, DATE '2024-01-04', 400)", 1);

        assertQuery("SELECT COUNT(*) FROM test_pa_missing_mv", "SELECT 4");
        assertQuery("SELECT id, amount FROM test_pa_missing_mv ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300), (4, 400)");

        assertUpdate("DROP MATERIALIZED VIEW test_pa_missing_mv");
        assertUpdate("DROP TABLE test_pa_missing_base");
    }

    @Test
    public void testPartitionAlignment_OverSpecifiedStorage()
    {
        assertUpdate("CREATE TABLE test_pa_over_table_a (" +
                "id BIGINT, " +
                "event_date DATE, " +
                "amount BIGINT) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("CREATE TABLE test_pa_over_table_b (" +
                "customer_id BIGINT, " +
                "region VARCHAR, " +
                "name VARCHAR) " +
                "WITH (partitioning = ARRAY['region'])");

        assertUpdate("INSERT INTO test_pa_over_table_a VALUES " +
                "(1, DATE '2024-01-01', 100), " +
                "(2, DATE '2024-01-02', 200), " +
                "(3, DATE '2024-01-03', 300)", 3);

        assertUpdate("INSERT INTO test_pa_over_table_b VALUES " +
                "(1, 'US', 'Alice'), " +
                "(2, 'US', 'Bob'), " +
                "(3, 'UK', 'Charlie')", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_pa_over_mv AS " +
                "SELECT a.id, a.event_date, a.amount, b.region, b.name " +
                "FROM test_pa_over_table_a a " +
                "JOIN test_pa_over_table_b b ON a.id = b.customer_id");

        assertUpdate("REFRESH MATERIALIZED VIEW test_pa_over_mv", 3);

        assertUpdate("INSERT INTO test_pa_over_table_a VALUES (1, DATE '2024-01-04', 150)", 1);

        assertQuery("SELECT COUNT(*) FROM test_pa_over_mv", "SELECT 4");
        assertQuery("SELECT id, event_date, amount, region, name FROM test_pa_over_mv ORDER BY id, event_date",
                "VALUES " +
                        "(1, DATE '2024-01-01', 100, 'US', 'Alice'), " +
                        "(1, DATE '2024-01-04', 150, 'US', 'Alice'), " +
                        "(2, DATE '2024-01-02', 200, 'US', 'Bob'), " +
                        "(3, DATE '2024-01-03', 300, 'UK', 'Charlie')");

        assertUpdate("DROP MATERIALIZED VIEW test_pa_over_mv");
        assertUpdate("DROP TABLE test_pa_over_table_b");
        assertUpdate("DROP TABLE test_pa_over_table_a");
    }

    @Test
    public void testAggregationMV_MisalignedPartitioning()
    {
        // Bug: When GROUP BY column differs from partition column and multiple partitions
        // are stale, the current implementation creates partial aggregates per partition
        // and GROUP BY treats them as distinct rows instead of re-aggregating.
        assertUpdate("CREATE TABLE test_agg_misaligned (" +
                "id BIGINT, " +
                "partition_col VARCHAR, " +
                "region VARCHAR, " +
                "sales BIGINT) " +
                "WITH (partitioning = ARRAY['partition_col'])");

        assertUpdate("INSERT INTO test_agg_misaligned VALUES " +
                "(1, 'A', 'US', 100), " +
                "(2, 'A', 'EU', 50), " +
                "(3, 'B', 'US', 200), " +
                "(4, 'B', 'EU', 75)", 4);

        assertUpdate("CREATE MATERIALIZED VIEW test_agg_mv AS " +
                "SELECT region, SUM(sales) as total_sales " +
                "FROM test_agg_misaligned " +
                "GROUP BY region");

        assertUpdate("REFRESH MATERIALIZED VIEW test_agg_mv", 2);

        assertQuery("SELECT * FROM test_agg_mv ORDER BY region",
                "VALUES ('EU', 125), ('US', 300)");

        assertUpdate("INSERT INTO test_agg_misaligned VALUES " +
                "(5, 'A', 'US', 10), " +
                "(6, 'B', 'US', 20)", 2);

        assertQuery("SELECT * FROM test_agg_mv ORDER BY region",
                "VALUES ('EU', 125), ('US', 330)");

        assertUpdate("DROP MATERIALIZED VIEW test_agg_mv");
        assertUpdate("DROP TABLE test_agg_misaligned");
    }

    @Test
    public void testAggregationMV_MultiTableJoin_BothStale()
    {
        // Bug: When both tables are stale, creates partial aggregates for each branch
        // which are treated as distinct rows instead of being re-aggregated.
        assertUpdate("CREATE TABLE test_multi_orders (" +
                "order_id BIGINT, " +
                "product_id BIGINT, " +
                "order_date DATE, " +
                "quantity BIGINT) " +
                "WITH (partitioning = ARRAY['order_date'])");

        assertUpdate("CREATE TABLE test_multi_products (" +
                "product_id BIGINT, " +
                "product_category VARCHAR, " +
                "price BIGINT) " +
                "WITH (partitioning = ARRAY['product_category'])");

        assertUpdate("INSERT INTO test_multi_orders VALUES " +
                "(1, 100, DATE '2024-01-01', 5), " +
                "(2, 200, DATE '2024-01-01', 3)", 2);
        assertUpdate("INSERT INTO test_multi_products VALUES " +
                "(100, 'Electronics', 50), " +
                "(200, 'Books', 20)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_multi_agg_mv AS " +
                "SELECT p.product_category, SUM(o.quantity * p.price) as total_revenue " +
                "FROM test_multi_orders o " +
                "JOIN test_multi_products p ON o.product_id = p.product_id " +
                "GROUP BY p.product_category");

        assertUpdate("REFRESH MATERIALIZED VIEW test_multi_agg_mv", 2);

        assertQuery("SELECT * FROM test_multi_agg_mv ORDER BY product_category",
                "VALUES ('Books', 60), ('Electronics', 250)");

        assertUpdate("INSERT INTO test_multi_orders VALUES " +
                "(3, 100, DATE '2024-01-02', 2), " +
                "(4, 200, DATE '2024-01-02', 4)", 2);

        assertUpdate("INSERT INTO test_multi_products VALUES " +
                "(300, 'Toys', 30)", 1);

        assertUpdate("INSERT INTO test_multi_orders VALUES " +
                "(5, 300, DATE '2024-01-02', 1)", 1);

        String explainResult = (String) computeScalar("EXPLAIN SELECT * FROM test_multi_agg_mv ORDER BY product_category");
        System.out.println("=== EXPLAIN PLAN ===");
        System.out.println(explainResult);
        System.out.println("===================");

        assertQuery("SELECT * FROM test_multi_agg_mv ORDER BY product_category",
                "VALUES ('Books', 140), ('Electronics', 350), ('Toys', 30)");

        assertUpdate("DROP MATERIALIZED VIEW test_multi_agg_mv");
        assertUpdate("DROP TABLE test_multi_products");
        assertUpdate("DROP TABLE test_multi_orders");
    }

    @Test
    public void testMaterializedViewWithCustomStorageTableName()
    {
        assertUpdate("CREATE TABLE test_custom_storage_base (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO test_custom_storage_base VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_custom_storage_mv " +
                "WITH (storage_table = 'my_custom_storage_table') " +
                "AS SELECT id, name, value FROM test_custom_storage_base");

        assertQuery("SELECT COUNT(*) FROM my_custom_storage_table", "SELECT 0");

        assertQueryFails("SELECT * FROM \"__mv_storage__test_custom_storage_mv\"", ".*does not exist.*");

        assertUpdate("REFRESH MATERIALIZED VIEW test_custom_storage_mv", 2);

        assertQuery("SELECT COUNT(*) FROM my_custom_storage_table", "SELECT 2");
        assertQuery("SELECT * FROM my_custom_storage_table ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertQuery("SELECT * FROM test_custom_storage_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertUpdate("INSERT INTO test_custom_storage_base VALUES (3, 'Charlie', 300)", 1);
        assertUpdate("REFRESH MATERIALIZED VIEW test_custom_storage_mv", 3);

        assertQuery("SELECT COUNT(*) FROM my_custom_storage_table", "SELECT 3");
        assertQuery("SELECT * FROM my_custom_storage_table ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_custom_storage_mv");

        assertQueryFails("SELECT * FROM my_custom_storage_table", ".*does not exist.*");

        assertUpdate("DROP TABLE test_custom_storage_base");
    }

    @Test
    public void testMaterializedViewWithCustomStorageSchema()
    {
        assertUpdate("CREATE SCHEMA IF NOT EXISTS test_storage_schema");

        assertUpdate("CREATE TABLE test_custom_schema_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_custom_schema_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_custom_schema_mv " +
                "WITH (storage_schema = 'test_storage_schema', " +
                "storage_table = 'storage_table') " +
                "AS SELECT id, value FROM test_schema.test_custom_schema_base");

        assertQuery("SELECT COUNT(*) FROM test_storage_schema.storage_table", "SELECT 0");

        assertQueryFails("SELECT * FROM test_schema.storage_table", ".*does not exist.*");

        assertUpdate("REFRESH MATERIALIZED VIEW test_schema.test_custom_schema_mv", 2);

        assertQuery("SELECT COUNT(*) FROM test_storage_schema.storage_table", "SELECT 2");
        assertQuery("SELECT * FROM test_storage_schema.storage_table ORDER BY id",
                "VALUES (1, 100), (2, 200)");

        assertQuery("SELECT * FROM test_custom_schema_mv ORDER BY id",
                "VALUES (1, 100), (2, 200)");

        assertUpdate("DROP MATERIALIZED VIEW test_schema.test_custom_schema_mv");
        assertQueryFails("SELECT * FROM test_storage_schema.storage_table", ".*does not exist.*");

        assertUpdate("DROP TABLE test_custom_schema_base");
        assertUpdate("DROP SCHEMA test_storage_schema");
    }

    @Test
    public void testMaterializedViewWithCustomPrefix()
    {
        assertUpdate("CREATE TABLE test_custom_prefix_base (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_custom_prefix_base VALUES (1, 'test')", 1);

        Session sessionWithCustomPrefix = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "materialized_view_storage_prefix", "custom_prefix_")
                .build();

        assertUpdate(sessionWithCustomPrefix, "CREATE MATERIALIZED VIEW test_custom_prefix_mv " +
                "AS SELECT id, name FROM test_custom_prefix_base");

        assertQuery("SELECT COUNT(*) FROM custom_prefix_test_custom_prefix_mv", "SELECT 0");

        assertQueryFails("SELECT * FROM \"__mv_storage__test_custom_prefix_mv\"", ".*does not exist.*");

        assertUpdate("REFRESH MATERIALIZED VIEW test_custom_prefix_mv", 1);

        assertQuery("SELECT COUNT(*) FROM custom_prefix_test_custom_prefix_mv", "SELECT 1");
        assertQuery("SELECT * FROM custom_prefix_test_custom_prefix_mv", "VALUES (1, 'test')");

        assertQuery("SELECT * FROM test_custom_prefix_mv", "VALUES (1, 'test')");

        assertUpdate("DROP MATERIALIZED VIEW test_custom_prefix_mv");
        assertQueryFails("SELECT * FROM custom_prefix_test_custom_prefix_mv", ".*does not exist.*");

        assertUpdate("DROP TABLE test_custom_prefix_base");
    }

    @Test
    public void testMaterializedViewWithValuesOnly()
    {
        assertUpdate("CREATE MATERIALIZED VIEW test_values_mv AS SELECT * FROM (VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)) AS t(id, name, value)");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_values_mv\"", "SELECT 0");

        assertQuery("SELECT COUNT(*) FROM test_values_mv", "SELECT 3");
        assertQuery("SELECT * FROM test_values_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_values_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_values_mv\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_values_mv\" ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertQuery("SELECT * FROM test_values_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_values_mv");
        assertQueryFails("SELECT * FROM \"__mv_storage__test_values_mv\"", ".*does not exist.*");
    }

    @Test
    public void testMaterializedViewWithBaseTableButNoColumnsSelected()
    {
        assertUpdate("CREATE TABLE test_no_cols_base (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO test_no_cols_base VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_no_cols_mv AS " +
                "SELECT 'constant' as label, 42 as fixed_value FROM test_no_cols_base");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_no_cols_mv\"", "SELECT 0");

        assertQuery("SELECT COUNT(*) FROM test_no_cols_mv", "SELECT 3");
        assertQuery("SELECT * FROM test_no_cols_mv",
                "VALUES ('constant', 42), ('constant', 42), ('constant', 42)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_no_cols_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_no_cols_mv\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_no_cols_mv\"",
                "VALUES ('constant', 42), ('constant', 42), ('constant', 42)");

        assertUpdate("INSERT INTO test_no_cols_base VALUES (4, 'Dave', 400)", 1);

        assertQuery("SELECT COUNT(*) FROM test_no_cols_mv", "SELECT 4");

        assertUpdate("REFRESH MATERIALIZED VIEW test_no_cols_mv", 4);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_no_cols_mv\"", "SELECT 4");

        assertUpdate("DROP MATERIALIZED VIEW test_no_cols_mv");
        assertQueryFails("SELECT * FROM \"__mv_storage__test_no_cols_mv\"", ".*does not exist.*");

        assertUpdate("DROP TABLE test_no_cols_base");
    }

    @Test
    public void testMaterializedViewOnEmptyBaseTable()
    {
        assertUpdate("CREATE TABLE test_empty_base (id BIGINT, name VARCHAR, value BIGINT)");

        assertUpdate("CREATE MATERIALIZED VIEW test_empty_mv AS SELECT id, name, value FROM test_empty_base");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_empty_mv\"", "SELECT 0");

        assertQuery("SELECT COUNT(*) FROM test_empty_mv", "SELECT 0");

        assertUpdate("REFRESH MATERIALIZED VIEW test_empty_mv", 0);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_empty_mv\"", "SELECT 0");

        assertUpdate("INSERT INTO test_empty_base VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);

        assertQuery("SELECT COUNT(*) FROM test_empty_mv", "SELECT 2");
        assertQuery("SELECT * FROM test_empty_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_empty_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_empty_mv\"", "SELECT 2");
        assertQuery("SELECT * FROM \"__mv_storage__test_empty_mv\" ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertUpdate("DROP MATERIALIZED VIEW test_empty_mv");
        assertQueryFails("SELECT * FROM \"__mv_storage__test_empty_mv\"", ".*does not exist.*");

        assertUpdate("DROP TABLE test_empty_base");
    }

    @Test
    public void testRefreshFailurePreservesOldData()
    {
        assertUpdate("CREATE TABLE test_refresh_failure_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_refresh_failure_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_refresh_failure_mv AS " +
                "SELECT id, value FROM test_refresh_failure_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_refresh_failure_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_refresh_failure_mv\"", "SELECT 2");
        assertQuery("SELECT * FROM \"__mv_storage__test_refresh_failure_mv\" ORDER BY id",
                "VALUES (1, 100), (2, 200)");

        assertUpdate("DROP TABLE test_refresh_failure_base");

        try {
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW test_refresh_failure_mv");
            throw new AssertionError("Expected REFRESH to fail when base table doesn't exist");
        }
        catch (Exception e) {
            if (!e.getMessage().contains("does not exist") && !e.getMessage().contains("not found")) {
                throw new AssertionError("Expected 'does not exist' error, got: " + e.getMessage());
            }
        }

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_refresh_failure_mv\"", "SELECT 2");
        assertQuery("SELECT * FROM \"__mv_storage__test_refresh_failure_mv\" ORDER BY id",
                "VALUES (1, 100), (2, 200)");

        assertUpdate("DROP MATERIALIZED VIEW test_refresh_failure_mv");
    }

    @Test
    public void testBaseTableDroppedAndRecreated()
    {
        assertUpdate("CREATE TABLE test_recreate_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_recreate_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_recreate_mv AS SELECT id, value FROM test_recreate_base");
        assertUpdate("REFRESH MATERIALIZED VIEW test_recreate_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_recreate_mv\"", "SELECT 2");
        assertQuery("SELECT * FROM \"__mv_storage__test_recreate_mv\" ORDER BY id",
                "VALUES (1, 100), (2, 200)");

        assertUpdate("DROP TABLE test_recreate_base");

        assertUpdate("CREATE TABLE test_recreate_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_recreate_base VALUES (3, 300), (4, 400), (5, 500)", 3);

        assertQuery("SELECT COUNT(*) FROM test_recreate_mv", "SELECT 3");
        assertQuery("SELECT * FROM test_recreate_mv ORDER BY id",
                "VALUES (3, 300), (4, 400), (5, 500)");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_recreate_mv\"", "SELECT 2");

        assertUpdate("REFRESH MATERIALIZED VIEW test_recreate_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_recreate_mv\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_recreate_mv\" ORDER BY id",
                "VALUES (3, 300), (4, 400), (5, 500)");

        assertUpdate("DROP MATERIALIZED VIEW test_recreate_mv");
        assertUpdate("DROP TABLE test_recreate_base");
    }

    @Test
    public void testStorageTableDroppedDirectly()
    {
        assertUpdate("CREATE TABLE test_storage_drop_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_storage_drop_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_storage_drop_mv AS SELECT id, value FROM test_storage_drop_base");
        assertUpdate("REFRESH MATERIALIZED VIEW test_storage_drop_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_storage_drop_mv\"", "SELECT 2");

        assertUpdate("DROP TABLE \"__mv_storage__test_storage_drop_mv\"");

        assertQueryFails("SELECT * FROM \"__mv_storage__test_storage_drop_mv\"", ".*does not exist.*");

        assertQueryFails("SELECT * FROM test_storage_drop_mv", ".*does not exist.*");

        assertUpdate("DROP MATERIALIZED VIEW test_storage_drop_mv");
        assertUpdate("DROP TABLE test_storage_drop_base");
    }

    @Test
    public void testMaterializedViewWithRenamedColumns()
    {
        assertUpdate("CREATE TABLE test_renamed_base (id BIGINT, original_name VARCHAR, original_value BIGINT)");
        assertUpdate("INSERT INTO test_renamed_base VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_renamed_mv AS " +
                "SELECT id AS person_id, original_name AS full_name, original_value AS amount " +
                "FROM test_renamed_base");

        assertQuery("SELECT COUNT(*) FROM test_renamed_mv", "SELECT 3");
        assertQuery("SELECT * FROM test_renamed_mv ORDER BY person_id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_renamed_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_renamed_mv\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_renamed_mv\" ORDER BY person_id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertQuery("SELECT * FROM test_renamed_mv ORDER BY person_id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertQuery("SELECT person_id, full_name FROM test_renamed_mv WHERE amount > 150 ORDER BY person_id",
                "VALUES (2, 'Bob'), (3, 'Charlie')");

        assertUpdate("INSERT INTO test_renamed_base VALUES (4, 'Dave', 400)", 1);

        assertQuery("SELECT COUNT(*) FROM test_renamed_mv", "SELECT 4");

        assertUpdate("REFRESH MATERIALIZED VIEW test_renamed_mv", 4);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_renamed_mv\"", "SELECT 4");
        assertQuery("SELECT * FROM \"__mv_storage__test_renamed_mv\" ORDER BY person_id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300), (4, 'Dave', 400)");

        assertUpdate("DROP MATERIALIZED VIEW test_renamed_mv");
        assertUpdate("DROP TABLE test_renamed_base");
    }

    @Test
    public void testMaterializedViewWithComputedColumns()
    {
        assertUpdate("CREATE TABLE test_computed_base (id BIGINT, quantity BIGINT, unit_price BIGINT)");
        assertUpdate("INSERT INTO test_computed_base VALUES (1, 5, 100), (2, 10, 50), (3, 3, 200)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_computed_mv AS " +
                "SELECT id, " +
                "quantity, " +
                "unit_price, " +
                "quantity * unit_price AS total_price, " +
                "quantity * 2 AS double_quantity, " +
                "'Order_' || CAST(id AS VARCHAR) AS order_label " +
                "FROM test_computed_base");

        assertQuery("SELECT COUNT(*) FROM test_computed_mv", "SELECT 3");
        assertQuery("SELECT id, quantity, unit_price, total_price, double_quantity, order_label FROM test_computed_mv ORDER BY id",
                "VALUES (1, 5, 100, 500, 10, 'Order_1'), " +
                        "(2, 10, 50, 500, 20, 'Order_2'), " +
                        "(3, 3, 200, 600, 6, 'Order_3')");

        assertUpdate("REFRESH MATERIALIZED VIEW test_computed_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_computed_mv\"", "SELECT 3");
        assertQuery("SELECT id, quantity, unit_price, total_price, double_quantity, order_label FROM \"__mv_storage__test_computed_mv\" ORDER BY id",
                "VALUES (1, 5, 100, 500, 10, 'Order_1'), " +
                        "(2, 10, 50, 500, 20, 'Order_2'), " +
                        "(3, 3, 200, 600, 6, 'Order_3')");

        assertQuery("SELECT * FROM test_computed_mv WHERE total_price > 550 ORDER BY id",
                "VALUES (3, 3, 200, 600, 6, 'Order_3')");

        assertQuery("SELECT id, order_label FROM test_computed_mv WHERE double_quantity >= 10 ORDER BY id",
                "VALUES (1, 'Order_1'), (2, 'Order_2')");

        assertUpdate("INSERT INTO test_computed_base VALUES (4, 8, 75)", 1);

        assertQuery("SELECT COUNT(*) FROM test_computed_mv", "SELECT 4");
        assertQuery("SELECT id, total_price, order_label FROM test_computed_mv WHERE id = 4",
                "VALUES (4, 600, 'Order_4')");

        assertUpdate("REFRESH MATERIALIZED VIEW test_computed_mv", 4);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_computed_mv\"", "SELECT 4");
        assertQuery("SELECT id, quantity, unit_price, total_price, order_label FROM \"__mv_storage__test_computed_mv\" WHERE id = 4",
                "VALUES (4, 8, 75, 600, 'Order_4')");

        assertUpdate("DROP MATERIALIZED VIEW test_computed_mv");
        assertUpdate("DROP TABLE test_computed_base");
    }

    @Test
    public void testMaterializedViewWithCustomTableProperties()
    {
        assertUpdate("CREATE TABLE test_custom_props_base (id BIGINT, name VARCHAR, region VARCHAR)");
        assertUpdate("INSERT INTO test_custom_props_base VALUES (1, 'Alice', 'US'), (2, 'Bob', 'EU'), (3, 'Charlie', 'APAC')", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_custom_props_mv " +
                "WITH (" +
                "    partitioning = ARRAY['region'], " +
                "    sorted_by = ARRAY['id'], " +
                "    \"write.format.default\" = 'ORC'" +
                ") AS " +
                "SELECT id, name, region FROM test_custom_props_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_custom_props_mv", 3);

        assertQuery("SELECT COUNT(*) FROM test_custom_props_mv", "SELECT 3");
        assertQuery("SELECT name FROM test_custom_props_mv WHERE region = 'US'", "VALUES ('Alice')");
        assertQuery("SELECT name FROM test_custom_props_mv WHERE region = 'EU'", "VALUES ('Bob')");

        String storageTableName = "__mv_storage__test_custom_props_mv";
        assertQuery("SELECT COUNT(*) FROM \"" + storageTableName + "\"", "SELECT 3");

        assertQuery("SELECT COUNT(*) FROM \"" + storageTableName + "\" WHERE region = 'APAC'", "SELECT 1");

        assertUpdate("INSERT INTO test_custom_props_base VALUES (4, 'David', 'US')", 1);
        assertUpdate("REFRESH MATERIALIZED VIEW test_custom_props_mv", 4);

        assertQuery("SELECT COUNT(*) FROM test_custom_props_mv WHERE region = 'US'", "SELECT 2");
        assertQuery("SELECT name FROM test_custom_props_mv WHERE region = 'US' ORDER BY id",
                "VALUES ('Alice'), ('David')");

        assertUpdate("DROP MATERIALIZED VIEW test_custom_props_mv");
        assertUpdate("DROP TABLE test_custom_props_base");
    }

    @Test
    public void testMaterializedViewWithNestedTypes()
    {
        assertUpdate("CREATE TABLE test_nested_base (" +
                "id BIGINT, " +
                "tags ARRAY(VARCHAR), " +
                "properties MAP(VARCHAR, VARCHAR), " +
                "address ROW(street VARCHAR, city VARCHAR, zipcode VARCHAR))");

        assertUpdate("INSERT INTO test_nested_base VALUES " +
                "(1, ARRAY['tag1', 'tag2'], MAP(ARRAY['key1', 'key2'], ARRAY['value1', 'value2']), ROW('123 Main St', 'NYC', '10001')), " +
                "(2, ARRAY['tag3'], MAP(ARRAY['key3'], ARRAY['value3']), ROW('456 Oak Ave', 'LA', '90001'))", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_nested_mv AS " +
                "SELECT id, tags, properties, address FROM test_nested_base");

        assertQuery("SELECT COUNT(*) FROM test_nested_mv", "SELECT 2");
        assertQuery("SELECT id, cardinality(tags) FROM test_nested_mv ORDER BY id",
                "VALUES (1, 2), (2, 1)");

        assertUpdate("REFRESH MATERIALIZED VIEW test_nested_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_nested_mv\"", "SELECT 2");

        assertQuery("SELECT id, cardinality(tags), address.city FROM test_nested_mv ORDER BY id",
                "VALUES (1, 2, 'NYC'), (2, 1, 'LA')");

        assertQuery("SELECT id FROM test_nested_mv WHERE element_at(properties, 'key1') = 'value1'",
                "VALUES (1)");

        assertUpdate("INSERT INTO test_nested_base VALUES " +
                "(3, ARRAY['tag4', 'tag5', 'tag6'], MAP(ARRAY['key4'], ARRAY['value4']), ROW('789 Elm St', 'Chicago', '60601'))", 1);

        assertQuery("SELECT COUNT(*) FROM test_nested_mv", "SELECT 3");

        assertUpdate("REFRESH MATERIALIZED VIEW test_nested_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_nested_mv\"", "SELECT 3");
        assertQuery("SELECT id, address.zipcode FROM test_nested_mv WHERE id = 3",
                "VALUES (3, '60601')");

        assertUpdate("DROP MATERIALIZED VIEW test_nested_mv");
        assertUpdate("DROP TABLE test_nested_base");
    }

    @Test
    public void testMaterializedViewAfterColumnAdded()
    {
        assertUpdate("CREATE TABLE test_evolve_add_base (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO test_evolve_add_base VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_evolve_add_mv AS " +
                "SELECT id, name, value FROM test_evolve_add_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_evolve_add_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_evolve_add_mv\"", "SELECT 2");
        assertQuery("SELECT * FROM test_evolve_add_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertUpdate("ALTER TABLE test_evolve_add_base ADD COLUMN region VARCHAR");

        assertUpdate("INSERT INTO test_evolve_add_base VALUES (3, 'Charlie', 300, 'US')", 1);

        assertQuery("SELECT COUNT(*) FROM test_evolve_add_mv", "SELECT 3");
        assertQuery("SELECT * FROM test_evolve_add_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_evolve_add_mv\"", "SELECT 2");

        assertUpdate("REFRESH MATERIALIZED VIEW test_evolve_add_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_evolve_add_mv\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_evolve_add_mv\" ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertUpdate("CREATE MATERIALIZED VIEW test_evolve_add_mv2 AS " +
                "SELECT id, name, value, region FROM test_evolve_add_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_evolve_add_mv2", 3);

        assertQuery("SELECT * FROM test_evolve_add_mv2 WHERE id = 3",
                "VALUES (3, 'Charlie', 300, 'US')");
        assertQuery("SELECT id, region FROM test_evolve_add_mv2 WHERE id IN (1, 2) ORDER BY id",
                "VALUES (1, NULL), (2, NULL)");

        assertUpdate("DROP MATERIALIZED VIEW test_evolve_add_mv");
        assertUpdate("DROP MATERIALIZED VIEW test_evolve_add_mv2");
        assertUpdate("DROP TABLE test_evolve_add_base");
    }

    @Test
    public void testMaterializedViewAfterColumnDropped()
    {
        assertUpdate("CREATE TABLE test_evolve_drop_base (id BIGINT, name VARCHAR, value BIGINT, status VARCHAR)");
        assertUpdate("INSERT INTO test_evolve_drop_base VALUES (1, 'Alice', 100, 'active'), (2, 'Bob', 200, 'inactive')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_evolve_drop_mv_all AS " +
                "SELECT id, name, value, status FROM test_evolve_drop_base");

        assertUpdate("CREATE MATERIALIZED VIEW test_evolve_drop_mv_subset AS " +
                "SELECT id, name, value FROM test_evolve_drop_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_evolve_drop_mv_all", 2);
        assertUpdate("REFRESH MATERIALIZED VIEW test_evolve_drop_mv_subset", 2);

        assertQuery("SELECT * FROM test_evolve_drop_mv_all ORDER BY id",
                "VALUES (1, 'Alice', 100, 'active'), (2, 'Bob', 200, 'inactive')");
        assertQuery("SELECT * FROM test_evolve_drop_mv_subset ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertUpdate("ALTER TABLE test_evolve_drop_base DROP COLUMN status");

        assertUpdate("INSERT INTO test_evolve_drop_base VALUES (3, 'Charlie', 300)", 1);

        assertQuery("SELECT COUNT(*) FROM test_evolve_drop_mv_subset", "SELECT 3");
        assertQuery("SELECT * FROM test_evolve_drop_mv_subset ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertQueryFails("SELECT * FROM test_evolve_drop_mv_all",
                ".*Column 'status' cannot be resolved.*");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_evolve_drop_mv_all\"", "SELECT 2");
        assertQuery("SELECT * FROM \"__mv_storage__test_evolve_drop_mv_all\" ORDER BY id",
                "VALUES (1, 'Alice', 100, 'active'), (2, 'Bob', 200, 'inactive')");

        assertUpdate("REFRESH MATERIALIZED VIEW test_evolve_drop_mv_subset", 3);
        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_evolve_drop_mv_subset\"", "SELECT 3");

        assertUpdate("DROP MATERIALIZED VIEW test_evolve_drop_mv_all");
        assertUpdate("DROP MATERIALIZED VIEW test_evolve_drop_mv_subset");
        assertUpdate("DROP TABLE test_evolve_drop_base");
    }

    @Test
    public void testDropNonExistentMaterializedView()
    {
        assertQueryFails("DROP MATERIALIZED VIEW non_existent_mv",
                ".*does not exist.*");
    }

    @Test
    public void testCreateMaterializedViewWithSameNameAsExistingTable()
    {
        assertUpdate("CREATE TABLE existing_table_name (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO existing_table_name VALUES (1, 'test')", 1);

        assertQueryFails("CREATE MATERIALIZED VIEW existing_table_name AS SELECT id, value FROM existing_table_name",
                ".*already exists.*");

        assertQuery("SELECT COUNT(*) FROM existing_table_name", "SELECT 1");
        assertQuery("SELECT * FROM existing_table_name", "VALUES (1, 'test')");

        assertUpdate("CREATE TABLE test_mv_base (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO test_mv_base VALUES (2, 'foo')", 1);

        assertQueryFails("CREATE MATERIALIZED VIEW existing_table_name AS SELECT id, name FROM test_mv_base",
                ".*already exists.*");

        assertUpdate("DROP TABLE existing_table_name");
        assertUpdate("DROP TABLE test_mv_base");
    }

    @Test
    public void testInformationSchemaMaterializedViews()
    {
        assertUpdate("CREATE TABLE test_is_mv_base1 (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("CREATE TABLE test_is_mv_base2 (category VARCHAR, amount BIGINT)");

        assertUpdate("INSERT INTO test_is_mv_base1 VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);
        assertUpdate("INSERT INTO test_is_mv_base2 VALUES ('A', 50), ('B', 75)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_is_mv1 AS SELECT id, name, value FROM test_is_mv_base1 WHERE id > 0");
        assertUpdate("CREATE MATERIALIZED VIEW test_is_mv2 AS SELECT category, SUM(amount) as total FROM test_is_mv_base2 GROUP BY category");

        assertQuery(
                "SELECT table_name FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name IN ('test_is_mv1', 'test_is_mv2') " +
                "ORDER BY table_name",
                "VALUES ('test_is_mv1'), ('test_is_mv2')");

        assertQuery(
                "SELECT table_catalog, table_schema, table_name, storage_schema, storage_table_name, base_tables " +
                "FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv1'",
                "SELECT 'iceberg', 'test_schema', 'test_is_mv1', 'test_schema', '__mv_storage__test_is_mv1', 'iceberg.test_schema.test_is_mv_base1'");

        assertQuery(
                "SELECT COUNT(*) FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv1' " +
                "AND view_definition IS NOT NULL AND length(view_definition) > 0",
                "SELECT 1");

        assertQuery(
                "SELECT table_name FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv2'",
                "VALUES ('test_is_mv2')");

        assertQuery(
                "SELECT COUNT(*) FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv1' " +
                "AND view_owner IS NOT NULL",
                "SELECT 1");

        assertQuery(
                "SELECT COUNT(*) FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv1' " +
                "AND view_security IS NOT NULL",
                "SELECT 1");

        assertQuery(
                "SELECT base_tables FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv2'",
                "VALUES ('iceberg.test_schema.test_is_mv_base2')");

        assertUpdate("DROP MATERIALIZED VIEW test_is_mv1");
        assertUpdate("DROP MATERIALIZED VIEW test_is_mv2");
        assertUpdate("DROP TABLE test_is_mv_base1");
        assertUpdate("DROP TABLE test_is_mv_base2");

        assertQuery(
                "SELECT COUNT(*) FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name IN ('test_is_mv1', 'test_is_mv2')",
                "VALUES 0");
    }

    @Test
    public void testInformationSchemaTablesWithMaterializedViews()
    {
        assertUpdate("CREATE TABLE test_is_tables_base (id BIGINT, name VARCHAR)");
        assertUpdate("CREATE VIEW test_is_tables_view AS SELECT id, name FROM test_is_tables_base");
        assertUpdate("CREATE MATERIALIZED VIEW test_is_tables_mv AS SELECT id, name FROM test_is_tables_base");

        assertQuery(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = 'test_schema' AND table_name IN ('test_is_tables_base', 'test_is_tables_view', 'test_is_tables_mv') " +
                        "ORDER BY table_name",
                "VALUES ('test_is_tables_base', 'BASE TABLE'), ('test_is_tables_mv', 'MATERIALIZED VIEW'), ('test_is_tables_view', 'VIEW')");

        assertQuery(
                "SELECT table_name FROM information_schema.views " +
                        "WHERE table_schema = 'test_schema' AND table_name IN ('test_is_tables_view', 'test_is_tables_mv') " +
                        "ORDER BY table_name",
                "VALUES ('test_is_tables_view')");

        assertUpdate("DROP MATERIALIZED VIEW test_is_tables_mv");
        assertUpdate("DROP VIEW test_is_tables_view");
        assertUpdate("DROP TABLE test_is_tables_base");
    }

    @Test
    public void testInformationSchemaMaterializedViewsAfterRefresh()
    {
        assertUpdate("CREATE TABLE test_is_mv_refresh_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_is_mv_refresh_base VALUES (1, 100), (2, 200)", 2);
        assertUpdate("CREATE MATERIALIZED VIEW test_is_mv_refresh AS SELECT id, value FROM test_is_mv_refresh_base");

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'NOT_MATERIALIZED'");

        assertUpdate("REFRESH MATERIALIZED VIEW test_is_mv_refresh", 2);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'FULLY_MATERIALIZED'");

        assertUpdate("INSERT INTO test_is_mv_refresh_base VALUES (3, 300)", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertUpdate("UPDATE test_is_mv_refresh_base SET value = 250 WHERE id = 2", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertUpdate("DELETE FROM test_is_mv_refresh_base WHERE id = 1", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertUpdate("REFRESH MATERIALIZED VIEW test_is_mv_refresh", 2);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'FULLY_MATERIALIZED'");

        assertUpdate("DROP MATERIALIZED VIEW test_is_mv_refresh");
        assertUpdate("DROP TABLE test_is_mv_refresh_base");

        assertQuery(
                "SELECT COUNT(*) FROM information_schema.materialized_views " +
                "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "VALUES 0");
    }

    @Test
    public void testStaleReadBehaviorFail()
    {
        assertUpdate("CREATE TABLE test_stale_fail_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_stale_fail_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_stale_fail " +
                "WITH (stale_read_behavior = 'FAIL', staleness_window = '0s') " +
                "AS SELECT id, value FROM test_stale_fail_base");

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_stale_fail'",
                "SELECT 'NOT_MATERIALIZED'");

        assertUpdate("REFRESH MATERIALIZED VIEW test_stale_fail", 2);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_stale_fail'",
                "SELECT 'FULLY_MATERIALIZED'");

        assertQuery("SELECT COUNT(*) FROM test_stale_fail", "SELECT 2");
        assertQuery("SELECT * FROM test_stale_fail ORDER BY id", "VALUES (1, 100), (2, 200)");

        assertUpdate("INSERT INTO test_stale_fail_base VALUES (3, 300)", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_stale_fail'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertQueryFails("SELECT * FROM test_stale_fail",
                ".*Materialized view .* is stale.*");

        assertUpdate("REFRESH MATERIALIZED VIEW test_stale_fail", 3);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_stale_fail'",
                "SELECT 'FULLY_MATERIALIZED'");

        assertQuery("SELECT COUNT(*) FROM test_stale_fail", "SELECT 3");

        assertUpdate("DROP MATERIALIZED VIEW test_stale_fail");
        assertUpdate("DROP TABLE test_stale_fail_base");
    }

    @Test
    public void testStaleReadBehaviorUseViewQuery()
    {
        assertUpdate("CREATE TABLE test_stale_use_query_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_stale_use_query_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_stale_use_query " +
                "WITH (stale_read_behavior = 'USE_VIEW_QUERY', staleness_window = '0s') " +
                "AS SELECT id, value FROM test_stale_use_query_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_stale_use_query", 2);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_stale_use_query'",
                "SELECT 'FULLY_MATERIALIZED'");

        assertQuery("SELECT COUNT(*) FROM test_stale_use_query", "SELECT 2");
        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_stale_use_query\"", "SELECT 2");

        assertUpdate("INSERT INTO test_stale_use_query_base VALUES (3, 300)", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_stale_use_query'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertQuery("SELECT COUNT(*) FROM test_stale_use_query", "SELECT 3");
        assertQuery("SELECT * FROM test_stale_use_query ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300)");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_stale_use_query\"", "SELECT 2");

        assertUpdate("DROP MATERIALIZED VIEW test_stale_use_query");
        assertUpdate("DROP TABLE test_stale_use_query_base");
    }

    @Test
    public void testMaterializedViewWithNoStaleReadBehavior()
    {
        assertUpdate("CREATE TABLE test_no_stale_config_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_no_stale_config_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_no_stale_config AS SELECT id, value FROM test_no_stale_config_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_no_stale_config", 2);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_no_stale_config'",
                "SELECT 'FULLY_MATERIALIZED'");

        assertQuery("SELECT COUNT(*) FROM test_no_stale_config", "SELECT 2");

        assertUpdate("INSERT INTO test_no_stale_config_base VALUES (3, 300)", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_no_stale_config'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertQuery("SELECT COUNT(*) FROM test_no_stale_config", "SELECT 3");

        assertUpdate("DROP MATERIALIZED VIEW test_no_stale_config");
        assertUpdate("DROP TABLE test_no_stale_config_base");
    }

    @Test
    public void testStalenessWindowAllowsStaleReads()
    {
        assertUpdate("CREATE TABLE test_staleness_window_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO test_staleness_window_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_staleness_window_mv " +
                "WITH (stale_read_behavior = 'FAIL', staleness_window = '1h') " +
                "AS SELECT id, value FROM test_staleness_window_base");

        assertUpdate("REFRESH MATERIALIZED VIEW test_staleness_window_mv", 2);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_staleness_window_mv'",
                "SELECT 'FULLY_MATERIALIZED'");

        assertQuery("SELECT COUNT(*) FROM test_staleness_window_mv", "SELECT 2");
        assertQuery("SELECT * FROM test_staleness_window_mv ORDER BY id", "VALUES (1, 100), (2, 200)");

        assertUpdate("INSERT INTO test_staleness_window_base VALUES (3, 300)", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_staleness_window_mv'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertQuery("SELECT COUNT(*) FROM test_staleness_window_mv", "SELECT 2");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_staleness_window_mv\"", "SELECT 2");

        assertUpdate("DROP MATERIALIZED VIEW test_staleness_window_mv");
        assertUpdate("DROP TABLE test_staleness_window_base");
    }
}
