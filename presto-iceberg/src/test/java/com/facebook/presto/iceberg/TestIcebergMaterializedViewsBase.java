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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.spi.StandardWarningCode.MATERIALIZED_VIEW_STALE_DATA;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public abstract class TestIcebergMaterializedViewsBase
        extends AbstractTestQueryFramework
{
    protected File warehouseLocation;

    @Test
    public void testCreateMaterializedView()
    {
        assertUpdate("CREATE TABLE test_mv_base (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO test_mv_base VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_simple AS SELECT id, name, value FROM test_mv_base");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_simple\"", "SELECT 0");

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_simple", "SELECT 3");
        assertMaterializedViewQuery("SELECT * FROM test_mv_simple ORDER BY id",
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

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_filtered", "SELECT 2");
        assertMaterializedViewQuery("SELECT * FROM test_mv_filtered ORDER BY id",
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

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_category_sales", "SELECT 2");
        assertMaterializedViewQuery("SELECT * FROM test_mv_category_sales ORDER BY category",
                "VALUES ('Books', 2, 800), ('Electronics', 2, 2500)");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_category_sales");
        assertUpdate("DROP TABLE test_mv_sales");
    }

    @Test
    public void testMaterializedViewStaleness()
    {
        assertUpdate("CREATE TABLE test_mv_stale_base (id BIGINT, value BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("INSERT INTO test_mv_stale_base VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_stale " +
                "AS SELECT id, value, dt FROM test_mv_stale_base");

        assertQuery("SELECT COUNT(*) FROM test_mv_stale", "SELECT 2");
        assertMaterializedViewQuery("SELECT * FROM test_mv_stale ORDER BY id",
                "VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')");

        assertUpdate("INSERT INTO test_mv_stale_base VALUES (3, 300, '2024-01-02')", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_stale", "SELECT 3");
        assertMaterializedViewQuery("SELECT * FROM test_mv_stale ORDER BY id",
                "VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01'), (3, 300, '2024-01-02')");

        assertUpdate("REFRESH MATERIALIZED VIEW test_mv_stale", 3);
        assertRefreshAndFullyMaterialized("test_mv_stale", 3);
        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_stale\"", "SELECT 3");

        assertUpdate("TRUNCATE TABLE test_mv_stale_base");
        assertQuery("SELECT COUNT(*) FROM test_mv_stale_base", "SELECT 0");
        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_stale", "SELECT 0");
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

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_drop", "SELECT 1");

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
        assertUpdate("CREATE TABLE test_mv_refresh_base (id BIGINT, value BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("INSERT INTO test_mv_refresh_base VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_refresh " +
                "AS SELECT id, value, dt FROM test_mv_refresh_base");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 0");

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_refresh", "SELECT 2");
        assertMaterializedViewQuery("SELECT * FROM test_mv_refresh ORDER BY id", "VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')");

        assertRefreshAndFullyMaterialized("test_mv_refresh", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 2");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_refresh\" ORDER BY id",
                "VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')");

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_refresh", "SELECT 2");
        assertMaterializedViewQuery("SELECT * FROM test_mv_refresh ORDER BY id", "VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')");

        assertUpdate("INSERT INTO test_mv_refresh_base VALUES (3, 300, '2024-01-02')", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_refresh", "SELECT 3");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 2");

        assertRefreshAndFullyMaterialized("test_mv_refresh", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_refresh\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_refresh\" ORDER BY id",
                "VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01'), (3, 300, '2024-01-02')");
        assertMaterializedViewResultsMatch("SELECT * FROM test_mv_refresh ORDER BY id");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_refresh");
        assertUpdate("DROP TABLE test_mv_refresh_base");
    }

    @Test
    public void testRefreshMaterializedViewWithAggregation()
    {
        assertUpdate("CREATE TABLE test_mv_agg_refresh_base (category VARCHAR, value BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("INSERT INTO test_mv_agg_refresh_base VALUES ('A', 10, '2024-01-01'), ('B', 20, '2024-01-01'), ('A', 15, '2024-01-01')", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_agg_refresh AS " +
                "SELECT category, SUM(value) as total, dt FROM test_mv_agg_refresh_base GROUP BY category, dt");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_agg_refresh\"", "SELECT 0");

        assertQuery("SELECT * FROM test_mv_agg_refresh ORDER BY category",
                "VALUES ('A', 25, '2024-01-01'), ('B', 20, '2024-01-01')");

        assertRefreshAndFullyMaterialized("test_mv_agg_refresh", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_agg_refresh\"", "SELECT 2");
        assertMaterializedViewResultsMatch("SELECT * FROM test_mv_agg_refresh ORDER BY category");

        assertUpdate("INSERT INTO test_mv_agg_refresh_base VALUES ('A', 5, '2024-01-02'), ('C', 30, '2024-01-02')", 2);

        assertMaterializedViewQuery("SELECT * FROM test_mv_agg_refresh ORDER BY category, dt",
                "VALUES ('A', 25, '2024-01-01'), ('A', 5, '2024-01-02'), ('B', 20, '2024-01-01'), ('C', 30, '2024-01-02')");

        assertRefreshAndFullyMaterialized("test_mv_agg_refresh", 4);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_agg_refresh\"", "SELECT 4");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_agg_refresh\" ORDER BY category, dt",
                "VALUES ('A', 25, '2024-01-01'), ('A', 5, '2024-01-02'), ('B', 20, '2024-01-01'), ('C', 30, '2024-01-02')");
        assertMaterializedViewResultsMatch("SELECT * FROM test_mv_agg_refresh ORDER BY category, dt");

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

        assertRefreshAndFullyMaterialized("test_mv_partitioned", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_partitioned\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_partitioned\" ORDER BY id",
                "VALUES (1, DATE '2024-01-01', 100), (2, DATE '2024-01-01', 200), (3, DATE '2024-01-02', 300)");

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_partitioned", "SELECT 3");
        assertMaterializedViewQuery("SELECT * FROM test_mv_partitioned ORDER BY id",
                "VALUES (1, DATE '2024-01-01', 100), (2, DATE '2024-01-01', 200), (3, DATE '2024-01-02', 300)");

        assertUpdate("INSERT INTO test_mv_partitioned_base VALUES " +
                "(4, DATE '2024-01-03', 400), " +
                "(5, DATE '2024-01-03', 500)", 2);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_partitioned", "SELECT 5");
        assertMaterializedViewQuery("SELECT * FROM test_mv_partitioned ORDER BY id",
                "VALUES (1, DATE '2024-01-01', 100), " +
                        "(2, DATE '2024-01-01', 200), " +
                        "(3, DATE '2024-01-02', 300), " +
                        "(4, DATE '2024-01-03', 400), " +
                        "(5, DATE '2024-01-03', 500)");

        assertUpdate("INSERT INTO test_mv_partitioned_base VALUES " +
                "(6, DATE '2024-01-04', 600)", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_partitioned", "SELECT 6");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_partitioned");
        assertUpdate("DROP TABLE test_mv_partitioned_base");
    }

    @Test
    public void testMinimalRefresh()
    {
        assertUpdate("CREATE TABLE minimal_table (id BIGINT, dt VARCHAR) WITH (partitioning = ARRAY['dt'])");
        assertUpdate("INSERT INTO minimal_table VALUES (1, '2024-01-01')", 1);
        assertUpdate("CREATE MATERIALIZED VIEW minimal_mv AS SELECT id, dt FROM minimal_table");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__minimal_mv\"", "SELECT 0");

        assertRefreshAndFullyMaterialized("minimal_mv", 1);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__minimal_mv\"", "SELECT 1");
        assertQuery("SELECT * FROM \"__mv_storage__minimal_mv\"", "VALUES (1, '2024-01-01')");

        assertUpdate("DROP MATERIALIZED VIEW minimal_mv");
        assertUpdate("DROP TABLE minimal_table");
    }

    @Test
    public void testJoinMaterializedViewLifecycle()
    {
        assertUpdate("CREATE TABLE test_mv_orders (order_id BIGINT, customer_id BIGINT, amount BIGINT, order_date VARCHAR) " +
                "WITH (partitioning = ARRAY['order_date'])");
        assertUpdate("CREATE TABLE test_mv_customers (customer_id BIGINT, customer_name VARCHAR)");

        assertUpdate("INSERT INTO test_mv_orders VALUES (1, 100, 50, '2024-01-01'), (2, 200, 75, '2024-01-01'), (3, 100, 25, '2024-01-01')", 3);
        assertUpdate("INSERT INTO test_mv_customers VALUES (100, 'Alice'), (200, 'Bob')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_order_details AS " +
                "SELECT o.order_id, c.customer_name, o.amount, o.order_date " +
                "FROM test_mv_orders o JOIN test_mv_customers c ON o.customer_id = c.customer_id");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_order_details\"", "SELECT 0");

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_order_details", "SELECT 3");
        assertMaterializedViewQuery("SELECT * FROM test_mv_order_details ORDER BY order_id",
                "VALUES (1, 'Alice', 50, '2024-01-01'), (2, 'Bob', 75, '2024-01-01'), (3, 'Alice', 25, '2024-01-01')");

        assertRefreshAndFullyMaterialized("test_mv_order_details", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_order_details\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_order_details\" ORDER BY order_id",
                "VALUES (1, 'Alice', 50, '2024-01-01'), (2, 'Bob', 75, '2024-01-01'), (3, 'Alice', 25, '2024-01-01')");

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_order_details", "SELECT 3");

        assertUpdate("INSERT INTO test_mv_orders VALUES (4, 200, 100, '2024-01-02')", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_order_details", "SELECT 4");
        assertMaterializedViewQuery("SELECT * FROM test_mv_order_details ORDER BY order_id",
                "VALUES (1, 'Alice', 50, '2024-01-01'), (2, 'Bob', 75, '2024-01-01'), (3, 'Alice', 25, '2024-01-01'), (4, 'Bob', 100, '2024-01-02')");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_order_details\"", "SELECT 3");

        assertRefreshAndFullyMaterialized("test_mv_order_details", 4);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_order_details\"", "SELECT 4");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_order_details\" ORDER BY order_id",
                "VALUES (1, 'Alice', 50, '2024-01-01'), (2, 'Bob', 75, '2024-01-01'), (3, 'Alice', 25, '2024-01-01'), (4, 'Bob', 100, '2024-01-02')");

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

        assertRefreshAndFullyMaterialized("test_mv_part_join", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_part_join\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_mv_part_join\" ORDER BY order_id",
                "VALUES (1, 'Alice', DATE '2024-01-01', 50), " +
                        "(2, 'Bob', DATE '2024-01-01', 75), " +
                        "(3, 'Alice', DATE '2024-01-02', 25)");

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_part_join", "SELECT 3");
        assertMaterializedViewQuery("SELECT * FROM test_mv_part_join ORDER BY order_id",
                "VALUES (1, 'Alice', DATE '2024-01-01', 50), " +
                        "(2, 'Bob', DATE '2024-01-01', 75), " +
                        "(3, 'Alice', DATE '2024-01-02', 25)");

        assertUpdate("INSERT INTO test_mv_part_orders VALUES (4, 200, DATE '2024-01-03', 100)", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_part_join", "SELECT 4");
        assertMaterializedViewQuery("SELECT * FROM test_mv_part_join ORDER BY order_id",
                "VALUES (1, 'Alice', DATE '2024-01-01', 50), " +
                        "(2, 'Bob', DATE '2024-01-01', 75), " +
                        "(3, 'Alice', DATE '2024-01-02', 25), " +
                        "(4, 'Bob', DATE '2024-01-03', 100)");

        assertRefreshAndFullyMaterialized("test_mv_part_join", 4);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_part_join\"", "SELECT 4");
        assertMaterializedViewResultsMatch("SELECT * FROM test_mv_part_join ORDER BY order_id");

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

        assertRefreshAndFullyMaterialized("test_mv_multi_stale", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_multi_stale\"", "SELECT 2");

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_multi_stale", "SELECT 2");

        assertUpdate("INSERT INTO test_mv_orders VALUES (3, DATE '2024-01-03', 300)", 1);
        assertUpdate("INSERT INTO test_mv_customers VALUES (3, DATE '2024-01-03', 'Charlie')", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_multi_stale", "SELECT 3");
        assertMaterializedViewQuery("SELECT order_id, name, order_date, reg_date, amount FROM test_mv_multi_stale ORDER BY order_id",
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

        assertRefreshAndFullyMaterialized("test_mv_three_tables", 1);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_three_tables\"", "SELECT 1");

        assertUpdate("INSERT INTO test_mv_t1 VALUES (2, DATE '2024-01-02', 150)", 1);
        assertUpdate("INSERT INTO test_mv_t2 VALUES (2, DATE '2024-01-01', 250)", 1);
        assertUpdate("INSERT INTO test_mv_t3 VALUES (2, DATE '2024-01-02', 350)", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_three_tables", "SELECT 2");

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

        assertRefreshAndFullyMaterialized("test_mv_diff_partitions", 2);

        assertUpdate("INSERT INTO test_mv_table_a VALUES " +
                "(3, DATE '2024-01-03', 300), " +
                "(4, DATE '2024-01-04', 400), " +
                "(5, DATE '2024-01-05', 500)", 3);

        assertUpdate("INSERT INTO test_mv_table_b VALUES " +
                "(3, DATE '2024-01-03', 'active'), " +
                "(4, DATE '2024-01-04', 'active'), " +
                "(5, DATE '2024-01-05', 'pending')", 3);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_diff_partitions", "SELECT 5");

        assertUpdate("DROP MATERIALIZED VIEW test_mv_diff_partitions");
        assertUpdate("DROP TABLE test_mv_table_b");
        assertUpdate("DROP TABLE test_mv_table_a");
    }

    @Test
    public void testMultiTableStaleness_NonPartitionedAndPartitionedBothStale()
    {
        assertUpdate("CREATE TABLE test_mv_non_part (id BIGINT, category VARCHAR, created_date DATE) " +
                "WITH (partitioning = ARRAY['created_date'])");

        assertUpdate("CREATE TABLE test_mv_part_sales (" +
                "id BIGINT, " +
                "sale_date DATE, " +
                "amount BIGINT) " +
                "WITH (partitioning = ARRAY['sale_date'])");

        assertUpdate("INSERT INTO test_mv_non_part VALUES (1, 'Electronics', DATE '2024-01-01'), (2, 'Books', DATE '2024-01-02')", 2);
        assertUpdate("INSERT INTO test_mv_part_sales VALUES " +
                "(1, DATE '2024-01-01', 500), " +
                "(2, DATE '2024-01-02', 300)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_mixed_stale AS " +
                "SELECT c.id, c.category, s.sale_date, s.amount " +
                "FROM test_mv_non_part c JOIN test_mv_part_sales s ON c.id = s.id");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_mixed_stale\"", "SELECT 0");

        assertRefreshAndFullyMaterialized("test_mv_mixed_stale", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_mv_mixed_stale\"", "SELECT 2");
        assertQuery("SELECT id, category, sale_date, amount FROM \"__mv_storage__test_mv_mixed_stale\" ORDER BY id",
                "VALUES (1, 'Electronics', DATE '2024-01-01', 500), (2, 'Books', DATE '2024-01-02', 300)");

        assertUpdate("INSERT INTO test_mv_non_part VALUES (3, 'Toys', DATE '2024-01-03')", 1);
        assertUpdate("INSERT INTO test_mv_part_sales VALUES (3, DATE '2024-01-03', 700)", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_mv_mixed_stale", "SELECT 3");
        assertMaterializedViewQuery("SELECT id, category, sale_date, amount FROM test_mv_mixed_stale ORDER BY id",
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

        assertRefreshAndFullyMaterialized("test_pa_matching_mv", 3);

        assertUpdate("INSERT INTO test_pa_matching_base VALUES (4, DATE '2024-01-04', 400)", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_pa_matching_mv", "SELECT 4");
        assertMaterializedViewQuery("SELECT id, event_date, amount FROM test_pa_matching_mv ORDER BY id",
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

        assertRefreshAndFullyMaterialized("test_pa_missing_mv", 3);

        assertUpdate("INSERT INTO test_pa_missing_base VALUES (4, DATE '2024-01-04', 400)", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_pa_missing_mv", "SELECT 4");
        assertMaterializedViewQuery("SELECT id, amount FROM test_pa_missing_mv ORDER BY id",
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

        assertRefreshAndFullyMaterialized("test_pa_over_mv", 3);

        assertUpdate("INSERT INTO test_pa_over_table_a VALUES (1, DATE '2024-01-04', 150)", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_pa_over_mv", "SELECT 4");
        assertMaterializedViewQuery("SELECT id, event_date, amount, region, name FROM test_pa_over_mv ORDER BY id, event_date",
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

        assertRefreshAndFullyMaterialized("test_agg_mv", 2);

        assertMaterializedViewQuery("SELECT * FROM test_agg_mv ORDER BY region",
                "VALUES ('EU', 125), ('US', 300)");

        assertUpdate("INSERT INTO test_agg_misaligned VALUES " +
                "(5, 'A', 'US', 10), " +
                "(6, 'B', 'US', 20)", 2);

        assertMaterializedViewQuery("SELECT * FROM test_agg_mv ORDER BY region",
                "VALUES ('EU', 125), ('US', 330)");

        assertUpdate("DROP MATERIALIZED VIEW test_agg_mv");
        assertUpdate("DROP TABLE test_agg_misaligned");
    }

    @Test
    public void testAggregationMV_MultiTableJoin_BothStale()
    {
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

        assertRefreshAndFullyMaterialized("test_multi_agg_mv", 2);

        assertMaterializedViewQuery("SELECT * FROM test_multi_agg_mv ORDER BY product_category",
                "VALUES ('Books', 60), ('Electronics', 250)");

        assertUpdate("INSERT INTO test_multi_orders VALUES " +
                "(3, 100, DATE '2024-01-02', 2), " +
                "(4, 200, DATE '2024-01-02', 4)", 2);

        assertUpdate("INSERT INTO test_multi_products VALUES " +
                "(300, 'Toys', 30)", 1);

        assertUpdate("INSERT INTO test_multi_orders VALUES " +
                "(5, 300, DATE '2024-01-02', 1)", 1);

        assertMaterializedViewQuery("SELECT * FROM test_multi_agg_mv ORDER BY product_category",
                "VALUES ('Books', 140), ('Electronics', 350), ('Toys', 30)");

        assertUpdate("DROP MATERIALIZED VIEW test_multi_agg_mv");
        assertUpdate("DROP TABLE test_multi_products");
        assertUpdate("DROP TABLE test_multi_orders");
    }

    @Test
    public void testMaterializedViewJoinAggregationWithMultipleStalePartitions()
    {
        assertUpdate("CREATE TABLE orders_bug (" +
                "order_id BIGINT, " +
                "product_id BIGINT, " +
                "quantity BIGINT, " +
                "order_date DATE) " +
                "WITH (partitioning = ARRAY['order_date'])");

        assertUpdate("CREATE TABLE products_bug (" +
                "product_id BIGINT, " +
                "category VARCHAR, " +
                "price BIGINT) " +
                "WITH (partitioning = ARRAY['category'])");

        assertUpdate("INSERT INTO orders_bug VALUES (1, 200, 5, DATE '2024-01-01')", 1);
        assertUpdate("INSERT INTO products_bug VALUES (200, 'Books', 50)", 1);

        // Create aggregation MV
        assertUpdate("CREATE MATERIALIZED VIEW mv_revenue AS " +
                "SELECT p.category, SUM(o.quantity * p.price) as total_revenue " +
                "FROM orders_bug o " +
                "JOIN products_bug p ON o.product_id = p.product_id " +
                "GROUP BY p.category");

        // Refresh: storage has Books = 250 (5 * 50)
        assertRefreshAndFullyMaterialized("mv_revenue", 1);
        assertMaterializedViewQuery("SELECT * FROM mv_revenue ORDER BY category",
                "VALUES ('Books', 250)");

        // Update product price in EXISTING partition - makes Books partition stale
        // Note: Iceberg doesn't support UPDATE, so we delete and reinsert
        assertUpdate("DELETE FROM products_bug WHERE product_id = 200", 1);
        assertUpdate("INSERT INTO products_bug VALUES (200, 'Books', 20)", 1);

        // Insert new order in NEW partition - makes 2024-01-02 partition stale
        assertUpdate("INSERT INTO orders_bug VALUES (2, 200, 3, DATE '2024-01-02')", 1);
        assertMaterializedViewQuery("SELECT * FROM mv_revenue ORDER BY category",
                "VALUES ('Books', 160)");

        // Additional check: exactly one row for Books
        assertMaterializedViewQuery("SELECT COUNT(*) FROM mv_revenue WHERE category = 'Books'", "SELECT 1");

        assertUpdate("DROP MATERIALIZED VIEW mv_revenue");
        assertUpdate("DROP TABLE products_bug");
        assertUpdate("DROP TABLE orders_bug");
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

        assertRefreshAndFullyMaterialized("test_custom_storage_mv", 2);

        assertQuery("SELECT COUNT(*) FROM my_custom_storage_table", "SELECT 2");
        assertQuery("SELECT * FROM my_custom_storage_table ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertMaterializedViewQuery("SELECT * FROM test_custom_storage_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertUpdate("INSERT INTO test_custom_storage_base VALUES (3, 'Charlie', 300)", 1);
        assertRefreshAndFullyMaterialized("test_custom_storage_mv", 3);

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
        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_custom_schema_mv'",
                "SELECT 'FULLY_MATERIALIZED'");

        assertQuery("SELECT COUNT(*) FROM test_storage_schema.storage_table", "SELECT 2");
        assertQuery("SELECT * FROM test_storage_schema.storage_table ORDER BY id",
                "VALUES (1, 100), (2, 200)");

        assertMaterializedViewQuery("SELECT * FROM test_custom_schema_mv ORDER BY id",
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

        assertRefreshAndFullyMaterialized("test_custom_prefix_mv", 1);

        assertQuery("SELECT COUNT(*) FROM custom_prefix_test_custom_prefix_mv", "SELECT 1");
        assertQuery("SELECT * FROM custom_prefix_test_custom_prefix_mv", "VALUES (1, 'test')");

        assertMaterializedViewQuery("SELECT * FROM test_custom_prefix_mv", "VALUES (1, 'test')");

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

        assertRefreshAndFullyMaterialized("test_values_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_values_mv\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_values_mv\" ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertMaterializedViewQuery("SELECT * FROM test_values_mv ORDER BY id",
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

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_no_cols_mv", "SELECT 3");
        assertMaterializedViewQuery("SELECT * FROM test_no_cols_mv",
                "VALUES ('constant', 42), ('constant', 42), ('constant', 42)");

        assertRefreshAndFullyMaterialized("test_no_cols_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_no_cols_mv\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_no_cols_mv\"",
                "VALUES ('constant', 42), ('constant', 42), ('constant', 42)");

        assertUpdate("INSERT INTO test_no_cols_base VALUES (4, 'Dave', 400)", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_no_cols_mv", "SELECT 4");

        assertRefreshAndFullyMaterialized("test_no_cols_mv", 4);

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

        assertRefreshAndFullyMaterialized("test_empty_mv", 0);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_empty_mv\"", "SELECT 0");

        assertUpdate("INSERT INTO test_empty_base VALUES (1, 'Alice', 100), (2, 'Bob', 200)", 2);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_empty_mv", "SELECT 2");
        assertMaterializedViewQuery("SELECT * FROM test_empty_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertRefreshAndFullyMaterialized("test_empty_mv", 2);

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

        assertRefreshAndFullyMaterialized("test_refresh_failure_mv", 2);

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
        assertRefreshAndFullyMaterialized("test_recreate_mv", 2);

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

        assertRefreshAndFullyMaterialized("test_recreate_mv", 3);

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
        assertRefreshAndFullyMaterialized("test_storage_drop_mv", 2);

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

        assertRefreshAndFullyMaterialized("test_renamed_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_renamed_mv\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_renamed_mv\" ORDER BY person_id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertQuery("SELECT * FROM test_renamed_mv ORDER BY person_id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertQuery("SELECT person_id, full_name FROM test_renamed_mv WHERE amount > 150 ORDER BY person_id",
                "VALUES (2, 'Bob'), (3, 'Charlie')");

        assertUpdate("INSERT INTO test_renamed_base VALUES (4, 'Dave', 400)", 1);

        assertQuery("SELECT COUNT(*) FROM test_renamed_mv", "SELECT 4");

        assertRefreshAndFullyMaterialized("test_renamed_mv", 4);

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

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_computed_mv", "SELECT 3");
        assertMaterializedViewQuery("SELECT id, quantity, unit_price, total_price, double_quantity, order_label FROM test_computed_mv ORDER BY id",
                "VALUES (1, 5, 100, 500, 10, 'Order_1'), " +
                        "(2, 10, 50, 500, 20, 'Order_2'), " +
                        "(3, 3, 200, 600, 6, 'Order_3')");

        assertRefreshAndFullyMaterialized("test_computed_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_computed_mv\"", "SELECT 3");
        assertQuery("SELECT id, quantity, unit_price, total_price, double_quantity, order_label FROM \"__mv_storage__test_computed_mv\" ORDER BY id",
                "VALUES (1, 5, 100, 500, 10, 'Order_1'), " +
                        "(2, 10, 50, 500, 20, 'Order_2'), " +
                        "(3, 3, 200, 600, 6, 'Order_3')");

        assertMaterializedViewQuery("SELECT * FROM test_computed_mv WHERE total_price > 550 ORDER BY id",
                "VALUES (3, 3, 200, 600, 6, 'Order_3')");

        assertMaterializedViewQuery("SELECT id, order_label FROM test_computed_mv WHERE double_quantity >= 10 ORDER BY id",
                "VALUES (1, 'Order_1'), (2, 'Order_2')");

        assertUpdate("INSERT INTO test_computed_base VALUES (4, 8, 75)", 1);

        assertMaterializedViewQuery("SELECT COUNT(*) FROM test_computed_mv", "SELECT 4");
        assertMaterializedViewQuery("SELECT id, total_price, order_label FROM test_computed_mv WHERE id = 4",
                "VALUES (4, 600, 'Order_4')");

        assertRefreshAndFullyMaterialized("test_computed_mv", 4);

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
                "    \"write.format.default\" = 'PARQUET'" +
                ") AS " +
                "SELECT id, name, region FROM test_custom_props_base");

        assertRefreshAndFullyMaterialized("test_custom_props_mv", 3);

        assertQuery("SELECT COUNT(*) FROM test_custom_props_mv", "SELECT 3");
        assertQuery("SELECT name FROM test_custom_props_mv WHERE region = 'US'", "VALUES ('Alice')");
        assertQuery("SELECT name FROM test_custom_props_mv WHERE region = 'EU'", "VALUES ('Bob')");

        String storageTableName = "__mv_storage__test_custom_props_mv";
        assertQuery("SELECT COUNT(*) FROM \"" + storageTableName + "\"", "SELECT 3");

        assertQuery("SELECT COUNT(*) FROM \"" + storageTableName + "\" WHERE region = 'APAC'", "SELECT 1");

        assertUpdate("INSERT INTO test_custom_props_base VALUES (4, 'David', 'US')", 1);
        assertRefreshAndFullyMaterialized("test_custom_props_mv", 4);

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

        assertRefreshAndFullyMaterialized("test_nested_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_nested_mv\"", "SELECT 2");

        assertQuery("SELECT id, cardinality(tags) FROM test_nested_mv ORDER BY id",
                "VALUES (1, 2), (2, 1)");

        assertQuery("SELECT id FROM test_nested_mv WHERE element_at(properties, 'key1') = 'value1'",
                "VALUES (1)");

        assertUpdate("INSERT INTO test_nested_base VALUES " +
                "(3, ARRAY['tag4', 'tag5', 'tag6'], MAP(ARRAY['key4'], ARRAY['value4']), ROW('789 Elm St', 'Chicago', '60601'))", 1);

        assertQuery("SELECT COUNT(*) FROM test_nested_mv", "SELECT 3");

        assertRefreshAndFullyMaterialized("test_nested_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_nested_mv\"", "SELECT 3");
        assertQuery("SELECT id, cardinality(tags) FROM test_nested_mv WHERE id = 3",
                "VALUES (3, 3)");

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

        assertRefreshAndFullyMaterialized("test_evolve_add_mv", 2);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_evolve_add_mv\"", "SELECT 2");
        assertQuery("SELECT * FROM test_evolve_add_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

        assertUpdate("ALTER TABLE test_evolve_add_base ADD COLUMN region VARCHAR");

        assertUpdate("INSERT INTO test_evolve_add_base VALUES (3, 'Charlie', 300, 'US')", 1);

        assertQuery("SELECT COUNT(*) FROM test_evolve_add_mv", "SELECT 3");
        assertQuery("SELECT * FROM test_evolve_add_mv ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_evolve_add_mv\"", "SELECT 2");

        assertRefreshAndFullyMaterialized("test_evolve_add_mv", 3);

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_evolve_add_mv\"", "SELECT 3");
        assertQuery("SELECT * FROM \"__mv_storage__test_evolve_add_mv\" ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertUpdate("CREATE MATERIALIZED VIEW test_evolve_add_mv2 AS " +
                "SELECT id, name, value, region FROM test_evolve_add_base");

        assertRefreshAndFullyMaterialized("test_evolve_add_mv2", 3);

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

        assertRefreshAndFullyMaterialized("test_evolve_drop_mv_all", 2);
        assertRefreshAndFullyMaterialized("test_evolve_drop_mv_subset", 2);

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

        assertRefreshAndFullyMaterialized("test_evolve_drop_mv_subset", 3);
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

        assertRefreshAndFullyMaterialized("test_is_mv_refresh", 2);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'FULLY_MATERIALIZED'");

        assertUpdate("INSERT INTO test_is_mv_refresh_base VALUES (3, 300)", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertUpdate("INSERT INTO test_is_mv_refresh_base VALUES (4, 400)", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertUpdate("INSERT INTO test_is_mv_refresh_base VALUES (5, 500)", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'test_is_mv_refresh'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        assertRefreshAndFullyMaterialized("test_is_mv_refresh", 5);

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

        MaterializedResult staleResult = getQueryRunner().execute(getSession(), "SELECT COUNT(*) FROM test_staleness_window_mv");
        assertEquals((long) staleResult.getMaterializedRows().get(0).getField(0), 2L);
        assertTrue(staleResult.getWarnings().stream()
                .anyMatch(warning -> warning.getWarningCode().equals(MATERIALIZED_VIEW_STALE_DATA.toWarningCode())));

        assertQuery("SELECT COUNT(*) FROM \"__mv_storage__test_staleness_window_mv\"", "SELECT 2");

        assertUpdate("DROP MATERIALIZED VIEW test_staleness_window_mv");
        assertUpdate("DROP TABLE test_staleness_window_base");
    }

    @Test
    public void testMultipleAggregatesWithSingleStaleTable()
    {
        assertUpdate("CREATE TABLE test_agg_single_stale (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "value BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_agg_single_stale VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', 200, DATE '2024-01-01'), " +
                "(3, 'B', 150, DATE '2024-01-01'), " +
                "(4, 'B', 250, DATE '2024-01-01')", 4);

        assertUpdate("CREATE MATERIALIZED VIEW test_multi_agg_mv AS " +
                "SELECT category, " +
                "  COUNT(*) as cnt, " +
                "  SUM(value) as total, " +
                "  AVG(value) as average, " +
                "  MIN(value) as minimum, " +
                "  MAX(value) as maximum " +
                "FROM test_agg_single_stale " +
                "GROUP BY category");

        assertRefreshAndFullyMaterialized("test_multi_agg_mv", 2);

        // Verify initial state
        assertMaterializedViewQuery("SELECT * FROM test_multi_agg_mv ORDER BY category",
                "VALUES ('A', 2, 300, 150.0, 100, 200), " +
                "       ('B', 2, 400, 200.0, 150, 250)");

        // Insert new data in new partition - makes table stale
        assertUpdate("INSERT INTO test_agg_single_stale VALUES " +
                "(5, 'A', 50, DATE '2024-01-02'), " +
                "(6, 'B', 300, DATE '2024-01-02'), " +
                "(7, 'C', 175, DATE '2024-01-02')", 3);

        // Expected results after stitching:
        // A: cnt=3, total=350, avg=116.67, min=50, max=200
        // B: cnt=3, total=700, avg=233.33, min=150, max=300
        // C: cnt=1, total=175, avg=175, min=175, max=175
        assertMaterializedViewQuery("SELECT category, cnt, total, minimum, maximum FROM test_multi_agg_mv ORDER BY category",
                "VALUES ('A', 3, 350, 50, 200), " +
                "       ('B', 3, 700, 150, 300), " +
                "       ('C', 1, 175, 175, 175)");

        assertUpdate("DROP MATERIALIZED VIEW test_multi_agg_mv");
        assertUpdate("DROP TABLE test_agg_single_stale");
    }

    @Test
    public void testMultipleAggregatesWithMultipleStaleTables()
    {
        assertUpdate("CREATE TABLE test_agg_orders (" +
                "order_id BIGINT, " +
                "product_id BIGINT, " +
                "quantity BIGINT, " +
                "price BIGINT, " +
                "order_date DATE) " +
                "WITH (partitioning = ARRAY['order_date'])");

        assertUpdate("CREATE TABLE test_agg_products (" +
                "product_id BIGINT, " +
                "category VARCHAR, " +
                "region VARCHAR) " +
                "WITH (partitioning = ARRAY['region'])");

        assertUpdate("INSERT INTO test_agg_orders VALUES " +
                "(1, 100, 5, 10, DATE '2024-01-01'), " +
                "(2, 200, 3, 20, DATE '2024-01-01'), " +
                "(3, 100, 2, 10, DATE '2024-01-01')", 3);

        assertUpdate("INSERT INTO test_agg_products VALUES " +
                "(100, 'Electronics', 'US'), " +
                "(200, 'Books', 'US')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_multi_agg_join_mv AS " +
                "SELECT p.category, " +
                "  COUNT(*) as order_count, " +
                "  SUM(o.quantity) as total_quantity, " +
                "  AVG(o.quantity) as avg_quantity, " +
                "  MIN(o.price) as min_price, " +
                "  MAX(o.price) as max_price, " +
                "  SUM(o.quantity * o.price) as revenue " +
                "FROM test_agg_orders o " +
                "JOIN test_agg_products p ON o.product_id = p.product_id " +
                "GROUP BY p.category");

        assertRefreshAndFullyMaterialized("test_multi_agg_join_mv", 2);

        // Initial: Electronics: 2 orders, 7 qty, avg 3.5, min 10, max 10, revenue 70
        //          Books: 1 order, 3 qty, avg 3, min 20, max 20, revenue 60
        assertMaterializedViewQuery("SELECT category, order_count, total_quantity, min_price, max_price, revenue FROM test_multi_agg_join_mv ORDER BY category",
                "VALUES ('Books', 1, 3, 20, 20, 60), " +
                "       ('Electronics', 2, 7, 10, 10, 70)");

        // Make orders table stale - add order in new date partition
        assertUpdate("INSERT INTO test_agg_orders VALUES (4, 200, 10, 25, DATE '2024-01-02')", 1);

        // Books should now have: 2 orders, 13 qty, min 20, max 25, revenue 310
        assertMaterializedViewQuery("SELECT category, order_count, total_quantity, min_price, max_price, revenue FROM test_multi_agg_join_mv ORDER BY category",
                "VALUES ('Books', 2, 13, 20, 25, 310), " +
                "       ('Electronics', 2, 7, 10, 10, 70)");

        // Make products table also stale - add product in new region
        assertUpdate("INSERT INTO test_agg_products VALUES (300, 'Clothing', 'EU')", 1);
        assertUpdate("INSERT INTO test_agg_orders VALUES (5, 300, 8, 30, DATE '2024-01-02')", 1);

        // Clothing should now appear: 1 order, 8 qty, min 30, max 30, revenue 240
        assertMaterializedViewQuery("SELECT category, order_count, total_quantity, min_price, max_price, revenue FROM test_multi_agg_join_mv ORDER BY category",
                "VALUES ('Books', 2, 13, 20, 25, 310), " +
                "       ('Clothing', 1, 8, 30, 30, 240), " +
                "       ('Electronics', 2, 7, 10, 10, 70)");

        assertUpdate("DROP MATERIALIZED VIEW test_multi_agg_join_mv");
        assertUpdate("DROP TABLE test_agg_products");
        assertUpdate("DROP TABLE test_agg_orders");
    }

    @Test
    public void testAggregationMVWithMultipleStaleTables()
    {
        // This test verifies that aggregation MVs with multiple stale tables produce
        // correct results by comparing stitched results with full recomputation.
        // This is a regression test for the MarkDistinct optimization which skips
        // deduplication for aggregation queries that already produce unique rows.
        assertUpdate("CREATE TABLE test_agg_multi_stale_t1 (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "value BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("CREATE TABLE test_agg_multi_stale_t2 (" +
                "id BIGINT, " +
                "region VARCHAR, " +
                "multiplier BIGINT, " +
                "reg_date DATE) " +
                "WITH (partitioning = ARRAY['reg_date'])");

        assertUpdate("INSERT INTO test_agg_multi_stale_t1 VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'B', 200, DATE '2024-01-01')", 2);

        assertUpdate("INSERT INTO test_agg_multi_stale_t2 VALUES " +
                "(1, 'US', 2, DATE '2024-01-01'), " +
                "(2, 'EU', 3, DATE '2024-01-01')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_agg_multi_stale_mv AS " +
                "SELECT t1.category, " +
                "  SUM(t1.value * t2.multiplier) as total " +
                "FROM test_agg_multi_stale_t1 t1 " +
                "JOIN test_agg_multi_stale_t2 t2 ON t1.id = t2.id " +
                "GROUP BY t1.category");

        assertRefreshAndFullyMaterialized("test_agg_multi_stale_mv", 2);

        // Initial: A: 100*2=200, B: 200*3=600
        assertMaterializedViewQuery("SELECT * FROM test_agg_multi_stale_mv ORDER BY category",
                "VALUES ('A', 200), ('B', 600)");

        // Make t1 stale by inserting into new partition
        assertUpdate("INSERT INTO test_agg_multi_stale_t1 VALUES " +
                "(1, 'A', 150, DATE '2024-01-02'), " +
                "(3, 'C', 300, DATE '2024-01-02')", 2);

        // Make t2 stale by inserting into new partition
        assertUpdate("INSERT INTO test_agg_multi_stale_t2 VALUES " +
                "(3, 'APAC', 4, DATE '2024-01-02')", 1);

        // Both tables are now stale. Verify results match full recomputation.
        // Expected: A: (100*2)+(150*2)=500, B: 200*3=600, C: 300*4=1200
        assertMaterializedViewResultsMatch("SELECT * FROM test_agg_multi_stale_mv ORDER BY category");

        assertMaterializedViewQuery("SELECT * FROM test_agg_multi_stale_mv ORDER BY category",
                "VALUES ('A', 500), ('B', 600), ('C', 1200)");

        assertUpdate("DROP MATERIALIZED VIEW test_agg_multi_stale_mv");
        assertUpdate("DROP TABLE test_agg_multi_stale_t2");
        assertUpdate("DROP TABLE test_agg_multi_stale_t1");
    }

    @Test
    public void testSelectDistinctMVWithMultipleStaleTables()
    {
        // This test verifies SELECT DISTINCT MVs with multiple stale tables produce correct results.
        // SELECT DISTINCT creates an AggregationNode in the plan, so the MarkDistinct optimization
        // should skip deduplication. This tests that correctness is maintained.
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

        // Create MV with SELECT DISTINCT on a JOIN
        assertUpdate("CREATE MATERIALIZED VIEW test_distinct_mv " +
                "WITH (partitioning = ARRAY['event_date', 'reg_date']) AS " +
                "SELECT DISTINCT t1.category, t2.region, t1.event_date, t2.reg_date " +
                "FROM test_distinct_t1 t1 " +
                "JOIN test_distinct_t2 t2 ON t1.id = t2.id");

        assertRefreshAndFullyMaterialized("test_distinct_mv", 2);

        // Initial: (A, US), (B, EU)
        assertMaterializedViewQuery("SELECT * FROM test_distinct_mv ORDER BY category",
                "VALUES ('A', 'US', DATE '2024-01-01', DATE '2024-01-01'), " +
                "       ('B', 'EU', DATE '2024-01-01', DATE '2024-01-01')");

        // Make t1 stale by inserting into new partition
        assertUpdate("INSERT INTO test_distinct_t1 VALUES " +
                "(1, 'A', 150, DATE '2024-01-02'), " +  // Same category as before, should deduplicate
                "(3, 'C', 300, DATE '2024-01-02')", 2);

        // Make t2 stale by inserting into new partition
        assertUpdate("INSERT INTO test_distinct_t2 VALUES " +
                "(3, 'APAC', 'Z', DATE '2024-01-02')", 1);

        // Both tables are now stale. Verify results match full recomputation.
        // The DISTINCT should ensure no duplicates even with multiple stale branches.
        assertMaterializedViewResultsMatch("SELECT * FROM test_distinct_mv ORDER BY category, region");

        // Expected: (A, US, 01-01, 01-01) from old data
        //           (A, US, 01-02, 01-01) from new t1 row (id=1, event_date=01-02) joining old t2 row (id=1, reg_date=01-01)
        //           (B, EU, 01-01, 01-01) from old data
        //           (C, APAC, 01-02, 01-02) from new data on both sides
        assertMaterializedViewQuery("SELECT * FROM test_distinct_mv ORDER BY category, region, event_date",
                "VALUES ('A', 'US', DATE '2024-01-01', DATE '2024-01-01'), " +
                "       ('A', 'US', DATE '2024-01-02', DATE '2024-01-01'), " +
                "       ('B', 'EU', DATE '2024-01-01', DATE '2024-01-01'), " +
                "       ('C', 'APAC', DATE '2024-01-02', DATE '2024-01-02')");

        assertUpdate("DROP MATERIALIZED VIEW test_distinct_mv");
        assertUpdate("DROP TABLE test_distinct_t2");
        assertUpdate("DROP TABLE test_distinct_t1");
    }

    @Test
    public void testAggregatesWithNullValues()
    {
        assertUpdate("CREATE TABLE test_agg_nulls (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "value BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_agg_nulls VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', NULL, DATE '2024-01-01'), " +
                "(3, 'B', 200, DATE '2024-01-01'), " +
                "(4, 'B', NULL, DATE '2024-01-01')", 4);

        assertUpdate("CREATE MATERIALIZED VIEW test_agg_nulls_mv AS " +
                "SELECT category, " +
                "  COUNT(*) as total_rows, " +
                "  COUNT(value) as non_null_count, " +
                "  SUM(value) as total, " +
                "  AVG(value) as average, " +
                "  MIN(value) as minimum, " +
                "  MAX(value) as maximum " +
                "FROM test_agg_nulls " +
                "GROUP BY category");

        assertRefreshAndFullyMaterialized("test_agg_nulls_mv", 2);

        // Initial: A has 2 rows, 1 non-null, sum=100, avg=100, min=100, max=100
        //          B has 2 rows, 1 non-null, sum=200, avg=200, min=200, max=200
        assertMaterializedViewQuery("SELECT category, total_rows, non_null_count, total, minimum, maximum FROM test_agg_nulls_mv ORDER BY category",
                "VALUES ('A', 2, 1, 100, 100, 100), " +
                "       ('B', 2, 1, 200, 200, 200)");

        // Insert more data with NULLs in new partition
        assertUpdate("INSERT INTO test_agg_nulls VALUES " +
                "(5, 'A', 150, DATE '2024-01-02'), " +
                "(6, 'A', NULL, DATE '2024-01-02'), " +
                "(7, 'B', NULL, DATE '2024-01-02')", 3);

        // A: 4 total, 2 non-null, sum=250, avg=125, min=100, max=150
        // B: 3 total, 1 non-null, sum=200, avg=200, min=200, max=200
        assertMaterializedViewQuery("SELECT category, total_rows, non_null_count, total, minimum, maximum FROM test_agg_nulls_mv ORDER BY category",
                "VALUES ('A', 4, 2, 250, 100, 150), " +
                "       ('B', 3, 1, 200, 200, 200)");

        assertUpdate("DROP MATERIALIZED VIEW test_agg_nulls_mv");
        assertUpdate("DROP TABLE test_agg_nulls");
    }

    @Test
    public void testCountDistinctWithStaleTables()
    {
        assertUpdate("CREATE TABLE test_count_distinct (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "user_id BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_count_distinct VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', 100, DATE '2024-01-01'), " +
                "(3, 'A', 200, DATE '2024-01-01'), " +
                "(4, 'B', 300, DATE '2024-01-01'), " +
                "(5, 'B', 300, DATE '2024-01-01')", 5);

        assertUpdate("CREATE MATERIALIZED VIEW test_count_distinct_mv AS " +
                "SELECT category, " +
                "  COUNT(*) as total_events, " +
                "  COUNT(DISTINCT user_id) as unique_users " +
                "FROM test_count_distinct " +
                "GROUP BY category");

        assertRefreshAndFullyMaterialized("test_count_distinct_mv", 2);

        // Initial: A has 3 events, 2 unique users; B has 2 events, 1 unique user
        assertMaterializedViewQuery("SELECT * FROM test_count_distinct_mv ORDER BY category",
                "VALUES ('A', 3, 2), " +
                "       ('B', 2, 1)");

        // Insert new events with some duplicate users and some new users
        assertUpdate("INSERT INTO test_count_distinct VALUES " +
                "(6, 'A', 100, DATE '2024-01-02'), " +  // Duplicate user 100
                "(7, 'A', 300, DATE '2024-01-02'), " +  // New user 300
                "(8, 'B', 300, DATE '2024-01-02'), " +  // Duplicate user 300
                "(9, 'B', 400, DATE '2024-01-02')", 4);  // New user 400

        // Expected: A has 5 events, 3 unique users (100, 200, 300)
        //           B has 4 events, 2 unique users (300, 400)
        assertMaterializedViewQuery("SELECT * FROM test_count_distinct_mv ORDER BY category",
                "VALUES ('A', 5, 3), " +
                "       ('B', 4, 2)");

        assertUpdate("DROP MATERIALIZED VIEW test_count_distinct_mv");
        assertUpdate("DROP TABLE test_count_distinct");
    }

    @Test
    public void testWindowFunctionSumPartitionNoStaleTables()
    {
        assertUpdate("CREATE TABLE test_window_base (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "value BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_window_base VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', 200, DATE '2024-01-01'), " +
                "(3, 'B', 150, DATE '2024-01-01'), " +
                "(4, 'B', 250, DATE '2024-01-01')", 4);

        assertUpdate("CREATE MATERIALIZED VIEW test_window_sum_mv AS " +
                "SELECT id, category, value, " +
                "  SUM(value) OVER (PARTITION BY category) as category_total, " +
                "  COUNT(*) OVER (PARTITION BY category) as category_count " +
                "FROM test_window_base");

        assertRefreshAndFullyMaterialized("test_window_sum_mv", 4);

        // With no staleness, should return data from storage
        // A: total=300, count=2
        // B: total=400, count=2
        assertMaterializedViewQuery("SELECT id, category, value, category_total, category_count FROM test_window_sum_mv ORDER BY id",
                "VALUES (1, 'A', 100, 300, 2), " +
                "       (2, 'A', 200, 300, 2), " +
                "       (3, 'B', 150, 400, 2), " +
                "       (4, 'B', 250, 400, 2)");

        assertUpdate("DROP MATERIALIZED VIEW test_window_sum_mv");
        assertUpdate("DROP TABLE test_window_base");
    }

    @Test
    public void testWindowFunctionSumPartitionWithSingleStaleTable()
    {
        assertUpdate("CREATE TABLE test_window_stale1 (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "value BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_window_stale1 VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', 200, DATE '2024-01-01'), " +
                "(3, 'B', 150, DATE '2024-01-01')", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_window_stale1_mv AS " +
                "SELECT id, category, value, " +
                "  SUM(value) OVER (PARTITION BY category) as category_total, " +
                "  COUNT(*) OVER (PARTITION BY category) as category_count " +
                "FROM test_window_stale1");

        assertRefreshAndFullyMaterialized("test_window_stale1_mv", 3);

        // Initial state: A total=300, count=2; B total=150, count=1
        assertMaterializedViewQuery("SELECT id, category, category_total, category_count FROM test_window_stale1_mv ORDER BY id",
                "VALUES (1, 'A', 300, 2), (2, 'A', 300, 2), (3, 'B', 150, 1)");

        // Insert into new partition to make table stale
        assertUpdate("INSERT INTO test_window_stale1 VALUES " +
                "(4, 'B', 75, DATE '2024-01-02'), " +
                "(5, 'A', 300, DATE '2024-01-02')", 2);

        // Expected: A total=600, count=3; B total=225, count=2
        assertMaterializedViewQuery("SELECT id, category, category_total, category_count FROM test_window_stale1_mv ORDER BY id",
                "VALUES (1, 'A', 600, 3), (2, 'A', 600, 3), (3, 'B', 225, 2), (4, 'B', 225, 2), (5, 'A', 600, 3)");

        assertUpdate("DROP MATERIALIZED VIEW test_window_stale1_mv");
        assertUpdate("DROP TABLE test_window_stale1");
    }

    @Test
    public void testWindowFunctionSumPartitionWithMultipleStaleTables()
    {
        assertUpdate("CREATE TABLE test_window_orders (" +
                "order_id BIGINT, " +
                "product_id BIGINT, " +
                "amount BIGINT, " +
                "order_date DATE) " +
                "WITH (partitioning = ARRAY['order_date'])");

        assertUpdate("CREATE TABLE test_window_products (" +
                "product_id BIGINT, " +
                "category VARCHAR, " +
                "region VARCHAR) " +
                "WITH (partitioning = ARRAY['region'])");

        assertUpdate("INSERT INTO test_window_orders VALUES " +
                "(1, 100, 500, DATE '2024-01-01'), " +
                "(2, 200, 300, DATE '2024-01-01'), " +
                "(3, 100, 700, DATE '2024-01-01')", 3);

        assertUpdate("INSERT INTO test_window_products VALUES " +
                "(100, 'Electronics', 'US'), " +
                "(200, 'Books', 'US')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_window_join_mv AS " +
                "SELECT o.order_id, p.category, o.amount, " +
                "  SUM(o.amount) OVER (PARTITION BY p.category) as category_total, " +
                "  AVG(o.amount) OVER (PARTITION BY p.category) as category_avg " +
                "FROM test_window_orders o " +
                "JOIN test_window_products p ON o.product_id = p.product_id");

        assertRefreshAndFullyMaterialized("test_window_join_mv", 3);

        // Initial: Electronics total=1200, avg=600; Books total=300, avg=300
        assertMaterializedViewQuery("SELECT order_id, category, amount, category_total, category_avg FROM test_window_join_mv ORDER BY order_id",
                "VALUES (1, 'Electronics', 500, 1200, 600.0), " +
                "       (2, 'Books', 300, 300, 300.0), " +
                "       (3, 'Electronics', 700, 1200, 600.0)");

        // Make orders table stale
        assertUpdate("INSERT INTO test_window_orders VALUES (4, 200, 250, DATE '2024-01-02')", 1);

        // Expected: Electronics total=1200, avg=600; Books total=550, avg=275
        assertMaterializedViewQuery("SELECT order_id, category, amount, category_total, category_avg FROM test_window_join_mv ORDER BY order_id",
                "VALUES (1, 'Electronics', 500, 1200, 600.0), " +
                "       (2, 'Books', 300, 550, 275.0), " +
                "       (3, 'Electronics', 700, 1200, 600.0), " +
                "       (4, 'Books', 250, 550, 275.0)");

        // Make products table also stale
        assertUpdate("INSERT INTO test_window_products VALUES (300, 'Clothing', 'EU')", 1);
        assertUpdate("INSERT INTO test_window_orders VALUES (5, 300, 400, DATE '2024-01-03')", 1);

        // Expected: Electronics total=1200, avg=600; Books total=550, avg=275; Clothing total=400, avg=400
        assertMaterializedViewQuery("SELECT order_id, category, amount, category_total, category_avg FROM test_window_join_mv ORDER BY order_id",
                "VALUES (1, 'Electronics', 500, 1200, 600.0), " +
                "       (2, 'Books', 300, 550, 275.0), " +
                "       (3, 'Electronics', 700, 1200, 600.0), " +
                "       (4, 'Books', 250, 550, 275.0), " +
                "       (5, 'Clothing', 400, 400, 400.0)");

        assertUpdate("DROP MATERIALIZED VIEW test_window_join_mv");
        assertUpdate("DROP TABLE test_window_products");
        assertUpdate("DROP TABLE test_window_orders");
    }

    @Test
    public void testLeftJoinWithNoStaleTables()
    {
        assertUpdate("CREATE TABLE test_left_employees (" +
                "emp_id BIGINT, " +
                "emp_name VARCHAR, " +
                "dept_id BIGINT, " +
                "hire_date DATE) " +
                "WITH (partitioning = ARRAY['hire_date'])");

        assertUpdate("CREATE TABLE test_left_departments (" +
                "dept_id BIGINT, " +
                "dept_name VARCHAR, " +
                "location VARCHAR) " +
                "WITH (partitioning = ARRAY['location'])");

        assertUpdate("INSERT INTO test_left_employees VALUES " +
                "(1, 'Alice', 100, DATE '2024-01-01'), " +
                "(2, 'Bob', 200, DATE '2024-01-01'), " +
                "(3, 'Charlie', 300, DATE '2024-01-01'), " +
                "(4, 'Dave', NULL, DATE '2024-01-01')", 4);

        assertUpdate("INSERT INTO test_left_departments VALUES " +
                "(100, 'Engineering', 'US'), " +
                "(200, 'Sales', 'US')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_left_join_mv AS " +
                "SELECT e.emp_id, e.emp_name, e.dept_id, d.dept_name, d.location " +
                "FROM test_left_employees e " +
                "LEFT JOIN test_left_departments d ON e.dept_id = d.dept_id");

        assertRefreshAndFullyMaterialized("test_left_join_mv", 4);

        // Verify results: Alice->Engineering, Bob->Sales, Charlie->NULL (dept 300 doesn't exist), Dave->NULL (NULL dept_id)
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_id, dept_name, location FROM test_left_join_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 100, 'Engineering', 'US'), " +
                "       (2, 'Bob', 200, 'Sales', 'US'), " +
                "       (3, 'Charlie', 300, NULL, NULL), " +
                "       (4, 'Dave', NULL, NULL, NULL)");

        assertUpdate("DROP MATERIALIZED VIEW test_left_join_mv");
        assertUpdate("DROP TABLE test_left_departments");
        assertUpdate("DROP TABLE test_left_employees");
    }

    @Test
    public void testLeftJoinWithLeftTableStale()
    {
        assertUpdate("CREATE TABLE test_left_stale_employees (" +
                "emp_id BIGINT, " +
                "emp_name VARCHAR, " +
                "dept_id BIGINT, " +
                "hire_date DATE) " +
                "WITH (partitioning = ARRAY['hire_date'])");

        assertUpdate("CREATE TABLE test_left_stale_departments (" +
                "dept_id BIGINT, " +
                "dept_name VARCHAR, " +
                "location VARCHAR) " +
                "WITH (partitioning = ARRAY['location'])");

        assertUpdate("INSERT INTO test_left_stale_employees VALUES " +
                "(1, 'Alice', 100, DATE '2024-01-01'), " +
                "(2, 'Bob', 200, DATE '2024-01-01')", 2);

        assertUpdate("INSERT INTO test_left_stale_departments VALUES " +
                "(100, 'Engineering', 'US'), " +
                "(200, 'Sales', 'US'), " +
                "(300, 'Marketing', 'EU')", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_left_stale_left_mv AS " +
                "SELECT e.emp_id, e.emp_name, e.dept_id, e.hire_date, d.dept_name, d.location " +
                "FROM test_left_stale_employees e " +
                "LEFT JOIN test_left_stale_departments d ON e.dept_id = d.dept_id");

        assertRefreshAndFullyMaterialized("test_left_stale_left_mv", 2);

        // Initial state: Alice->Engineering, Bob->Sales
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_name, location FROM test_left_stale_left_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 'Engineering', 'US'), " +
                "       (2, 'Bob', 'Sales', 'US')");

        // Make left table (employees) stale by adding new employee in new partition
        assertUpdate("INSERT INTO test_left_stale_employees VALUES " +
                "(3, 'Charlie', 300, DATE '2024-01-02'), " +
                "(4, 'Dave', NULL, DATE '2024-01-02'), " +
                "(5, 'Eve', 400, DATE '2024-01-02')", 3);

        // Expected: Charlie joins with Marketing, Dave has NULL dept (no match), Eve has NULL (dept 400 doesn't exist)
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_id, dept_name, location FROM test_left_stale_left_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 100, 'Engineering', 'US'), " +
                "       (2, 'Bob', 200, 'Sales', 'US'), " +
                "       (3, 'Charlie', 300, 'Marketing', 'EU'), " +
                "       (4, 'Dave', NULL, NULL, NULL), " +
                "       (5, 'Eve', 400, NULL, NULL)");

        assertUpdate("DROP MATERIALIZED VIEW test_left_stale_left_mv");
        assertUpdate("DROP TABLE test_left_stale_departments");
        assertUpdate("DROP TABLE test_left_stale_employees");
    }

    @Test
    public void testLeftJoinWithRightTableStale()
    {
        assertUpdate("CREATE TABLE test_right_stale_employees (" +
                "emp_id BIGINT, " +
                "emp_name VARCHAR, " +
                "dept_id BIGINT, " +
                "hire_date DATE) " +
                "WITH (partitioning = ARRAY['hire_date'])");

        assertUpdate("CREATE TABLE test_right_stale_departments (" +
                "dept_id BIGINT, " +
                "dept_name VARCHAR, " +
                "location VARCHAR) " +
                "WITH (partitioning = ARRAY['location'])");

        assertUpdate("INSERT INTO test_right_stale_employees VALUES " +
                "(1, 'Alice', 100, DATE '2024-01-01'), " +
                "(2, 'Bob', 200, DATE '2024-01-01'), " +
                "(3, 'Charlie', 300, DATE '2024-01-01')", 3);

        assertUpdate("INSERT INTO test_right_stale_departments VALUES " +
                "(100, 'Engineering', 'US'), " +
                "(200, 'Sales', 'US')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_right_stale_mv AS " +
                "SELECT e.emp_id, e.emp_name, e.dept_id, e.hire_date, d.dept_name, d.location " +
                "FROM test_right_stale_employees e " +
                "LEFT JOIN test_right_stale_departments d ON e.dept_id = d.dept_id");

        assertRefreshAndFullyMaterialized("test_right_stale_mv", 3);

        // Initial state: Alice->Engineering, Bob->Sales, Charlie->NULL (dept 300 doesn't exist)
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_name, location FROM test_right_stale_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 'Engineering', 'US'), " +
                "       (2, 'Bob', 'Sales', 'US'), " +
                "       (3, 'Charlie', NULL, NULL)");

        // Make right table (departments) stale by adding new department in new partition
        assertUpdate("INSERT INTO test_right_stale_departments VALUES " +
                "(300, 'Marketing', 'EU'), " +
                "(400, 'HR', 'EU')", 2);

        // Expected: Charlie now joins with Marketing; HR dept has no matching employees (LEFT JOIN preserves left table)
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_id, dept_name, location FROM test_right_stale_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 100, 'Engineering', 'US'), " +
                "       (2, 'Bob', 200, 'Sales', 'US'), " +
                "       (3, 'Charlie', 300, 'Marketing', 'EU')");

        assertUpdate("DROP MATERIALIZED VIEW test_right_stale_mv");
        assertUpdate("DROP TABLE test_right_stale_departments");
        assertUpdate("DROP TABLE test_right_stale_employees");
    }

    @Test
    public void testLeftJoinWithBothTablesStale()
    {
        assertUpdate("CREATE TABLE test_both_stale_employees (" +
                "emp_id BIGINT, " +
                "emp_name VARCHAR, " +
                "dept_id BIGINT, " +
                "hire_date DATE) " +
                "WITH (partitioning = ARRAY['hire_date'])");

        assertUpdate("CREATE TABLE test_both_stale_departments (" +
                "dept_id BIGINT, " +
                "dept_name VARCHAR, " +
                "location VARCHAR) " +
                "WITH (partitioning = ARRAY['location'])");

        assertUpdate("INSERT INTO test_both_stale_employees VALUES " +
                "(1, 'Alice', 100, DATE '2024-01-01'), " +
                "(2, 'Bob', 200, DATE '2024-01-01')", 2);

        assertUpdate("INSERT INTO test_both_stale_departments VALUES " +
                "(100, 'Engineering', 'US'), " +
                "(200, 'Sales', 'US')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_both_stale_mv AS " +
                "SELECT e.emp_id, e.emp_name, e.dept_id, e.hire_date, d.dept_name, d.location " +
                "FROM test_both_stale_employees e " +
                "LEFT JOIN test_both_stale_departments d ON e.dept_id = d.dept_id");

        assertRefreshAndFullyMaterialized("test_both_stale_mv", 2);

        // Initial state: Alice->Engineering, Bob->Sales
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_name, location FROM test_both_stale_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 'Engineering', 'US'), " +
                "       (2, 'Bob', 'Sales', 'US')");

        // Make both tables stale
        assertUpdate("INSERT INTO test_both_stale_employees VALUES " +
                "(3, 'Charlie', 300, DATE '2024-01-02'), " +
                "(4, 'Dave', NULL, DATE '2024-01-02')", 2);

        assertUpdate("INSERT INTO test_both_stale_departments VALUES " +
                "(300, 'Marketing', 'EU'), " +
                "(400, 'HR', 'EU')", 2);

        // Expected: Charlie joins with Marketing, Dave has NULL (no match), HR dept has no employees
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_id, dept_name, location FROM test_both_stale_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 100, 'Engineering', 'US'), " +
                "       (2, 'Bob', 200, 'Sales', 'US'), " +
                "       (3, 'Charlie', 300, 'Marketing', 'EU'), " +
                "       (4, 'Dave', NULL, NULL, NULL)");

        assertUpdate("DROP MATERIALIZED VIEW test_both_stale_mv");
        assertUpdate("DROP TABLE test_both_stale_departments");
        assertUpdate("DROP TABLE test_both_stale_employees");
    }

    @Test
    public void testLeftJoinWithAggregationAndMultipleStaleTables()
    {
        assertUpdate("CREATE TABLE test_agg_orders (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "amount BIGINT, " +
                "order_date DATE) " +
                "WITH (partitioning = ARRAY['order_date'])");

        assertUpdate("CREATE TABLE test_agg_customers (" +
                "customer_id BIGINT, " +
                "customer_name VARCHAR, " +
                "region VARCHAR) " +
                "WITH (partitioning = ARRAY['region'])");

        assertUpdate("INSERT INTO test_agg_orders VALUES " +
                "(1, 100, 500, DATE '2024-01-01'), " +
                "(2, 100, 300, DATE '2024-01-01'), " +
                "(3, 200, 700, DATE '2024-01-01'), " +
                "(4, 300, 150, DATE '2024-01-01')", 4);

        assertUpdate("INSERT INTO test_agg_customers VALUES " +
                "(100, 'Alice', 'US'), " +
                "(200, 'Bob', 'US')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_left_agg_mv AS " +
                "SELECT c.customer_id, c.customer_name, c.region, " +
                "  COUNT(o.order_id) as order_count, " +
                "  SUM(o.amount) as total_amount, " +
                "  AVG(o.amount) as avg_amount " +
                "FROM test_agg_customers c " +
                "LEFT JOIN test_agg_orders o ON c.customer_id = o.customer_id " +
                "GROUP BY c.customer_id, c.customer_name, c.region");

        assertRefreshAndFullyMaterialized("test_left_agg_mv", 2);

        // Initial: Alice has 2 orders (800 total, 400 avg), Bob has 1 order (700 total, 700 avg)
        // Customer 300 doesn't exist in customers table, so order 4 doesn't appear in LEFT JOIN result
        assertMaterializedViewQuery("SELECT customer_id, customer_name, order_count, total_amount, avg_amount FROM test_left_agg_mv ORDER BY customer_id",
                "VALUES (100, 'Alice', 2, 800, 400.0), " +
                "       (200, 'Bob', 1, 700, 700.0)");

        // Make orders table stale by adding new orders
        assertUpdate("INSERT INTO test_agg_orders VALUES " +
                "(5, 100, 200, DATE '2024-01-02'), " +
                "(6, 300, 150, DATE '2024-01-02')", 2);

        // Alice now has 3 orders (1000 total), Bob still has 1 order
        // Customer 300's order is still excluded (customer 300 doesn't exist yet)
        assertMaterializedViewQuery("SELECT customer_id, customer_name, order_count, total_amount FROM test_left_agg_mv ORDER BY customer_id",
                "VALUES (100, 'Alice', 3, 1000), " +
                "       (200, 'Bob', 1, 700)");

        // Make customers table also stale - add customers 300 and 400
        assertUpdate("INSERT INTO test_agg_customers VALUES " +
                "(300, 'Charlie', 'EU'), " +
                "(400, 'Dave', 'EU')", 2);

        // Charlie now appears with 2 orders (150 from order 4 + 150 from order 6 = 300 total)
        // Dave appears with 0 orders (no matching orders)
        assertMaterializedViewQuery("SELECT customer_id, customer_name, order_count, total_amount FROM test_left_agg_mv ORDER BY customer_id",
                "VALUES (100, 'Alice', 3, 1000), " +
                "       (200, 'Bob', 1, 700), " +
                "       (300, 'Charlie', 2, 300), " +
                "       (400, 'Dave', 0, NULL)");

        assertUpdate("DROP MATERIALIZED VIEW test_left_agg_mv");
        assertUpdate("DROP TABLE test_agg_customers");
        assertUpdate("DROP TABLE test_agg_orders");
    }

    @Test
    public void testLeftJoinWithWhereClauseFilteringNulls()
    {
        assertUpdate("CREATE TABLE test_filter_employees (" +
                "emp_id BIGINT, " +
                "emp_name VARCHAR, " +
                "dept_id BIGINT, " +
                "hire_date DATE) " +
                "WITH (partitioning = ARRAY['hire_date'])");

        assertUpdate("CREATE TABLE test_filter_departments (" +
                "dept_id BIGINT, " +
                "dept_name VARCHAR, " +
                "location VARCHAR) " +
                "WITH (partitioning = ARRAY['location'])");

        assertUpdate("INSERT INTO test_filter_employees VALUES " +
                "(1, 'Alice', 100, DATE '2024-01-01'), " +
                "(2, 'Bob', 200, DATE '2024-01-01'), " +
                "(3, 'Charlie', 300, DATE '2024-01-01'), " +
                "(4, 'Dave', NULL, DATE '2024-01-01')", 4);

        assertUpdate("INSERT INTO test_filter_departments VALUES " +
                "(100, 'Engineering', 'US'), " +
                "(200, 'Sales', 'US')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_filter_nulls_mv AS " +
                "SELECT e.emp_id, e.emp_name, e.dept_id, e.hire_date, d.dept_name, d.location " +
                "FROM test_filter_employees e " +
                "LEFT JOIN test_filter_departments d ON e.dept_id = d.dept_id " +
                "WHERE d.dept_name IS NOT NULL");

        assertRefreshAndFullyMaterialized("test_filter_nulls_mv", 2);

        // Initial: Only Alice and Bob (Charlie and Dave filtered out by WHERE clause)
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_name FROM test_filter_nulls_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 'Engineering'), " +
                "       (2, 'Bob', 'Sales')");

        // Make employees table stale
        assertUpdate("INSERT INTO test_filter_employees VALUES " +
                "(5, 'Eve', 100, DATE '2024-01-02'), " +
                "(6, 'Frank', 400, DATE '2024-01-02')", 2);

        // Eve joins with Engineering, Frank is filtered out (dept 400 doesn't exist)
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_name FROM test_filter_nulls_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 'Engineering'), " +
                "       (2, 'Bob', 'Sales'), " +
                "       (5, 'Eve', 'Engineering')");

        // Make departments table stale
        assertUpdate("INSERT INTO test_filter_departments VALUES " +
                "(300, 'Marketing', 'EU')", 1);

        // Charlie now joins with Marketing and appears in results
        assertMaterializedViewQuery("SELECT emp_id, emp_name, dept_name FROM test_filter_nulls_mv ORDER BY emp_id",
                "VALUES (1, 'Alice', 'Engineering'), " +
                "       (2, 'Bob', 'Sales'), " +
                "       (3, 'Charlie', 'Marketing'), " +
                "       (5, 'Eve', 'Engineering')");

        assertUpdate("DROP MATERIALIZED VIEW test_filter_nulls_mv");
        assertUpdate("DROP TABLE test_filter_departments");
        assertUpdate("DROP TABLE test_filter_employees");
    }

    @Test
    public void testGroupByHavingWithNoStaleTables()
    {
        assertUpdate("CREATE TABLE test_having_sales (" +
                "product_id BIGINT, " +
                "category VARCHAR, " +
                "revenue BIGINT, " +
                "sale_date DATE) " +
                "WITH (partitioning = ARRAY['sale_date'])");

        assertUpdate("INSERT INTO test_having_sales VALUES " +
                "(1, 'Electronics', 1000, DATE '2024-01-01'), " +
                "(2, 'Electronics', 1500, DATE '2024-01-01'), " +
                "(3, 'Books', 300, DATE '2024-01-01'), " +
                "(4, 'Books', 200, DATE '2024-01-01'), " +
                "(5, 'Clothing', 800, DATE '2024-01-01')", 5);

        assertUpdate("CREATE MATERIALIZED VIEW test_having_mv AS " +
                "SELECT category, SUM(revenue) as total_revenue, COUNT(*) as sale_count " +
                "FROM test_having_sales " +
                "GROUP BY category " +
                "HAVING SUM(revenue) > 500");

        assertRefreshAndFullyMaterialized("test_having_mv", 2);

        // Only Electronics (2500) and Clothing (800) pass the HAVING filter
        // Books (500) is filtered out
        assertMaterializedViewQuery("SELECT category, total_revenue, sale_count FROM test_having_mv ORDER BY category",
                "VALUES ('Clothing', 800, 1), " +
                "       ('Electronics', 2500, 2)");

        assertUpdate("DROP MATERIALIZED VIEW test_having_mv");
        assertUpdate("DROP TABLE test_having_sales");
    }

    @Test
    public void testGroupByHavingWithSingleStaleTable()
    {
        assertUpdate("CREATE TABLE test_having_stale1 (" +
                "product_id BIGINT, " +
                "category VARCHAR, " +
                "revenue BIGINT, " +
                "sale_date DATE) " +
                "WITH (partitioning = ARRAY['sale_date'])");

        assertUpdate("INSERT INTO test_having_stale1 VALUES " +
                "(1, 'Electronics', 600, DATE '2024-01-01'), " +
                "(2, 'Books', 400, DATE '2024-01-01'), " +
                "(3, 'Clothing', 300, DATE '2024-01-01')", 3);

        assertUpdate("CREATE MATERIALIZED VIEW test_having_stale1_mv AS " +
                "SELECT category, SUM(revenue) as total_revenue, COUNT(*) as sale_count " +
                "FROM test_having_stale1 " +
                "GROUP BY category " +
                "HAVING SUM(revenue) >= 500");

        assertRefreshAndFullyMaterialized("test_having_stale1_mv", 1);

        // Initial: Only Electronics (600) passes HAVING filter
        assertMaterializedViewQuery("SELECT category, total_revenue, sale_count FROM test_having_stale1_mv ORDER BY category",
                "VALUES ('Electronics', 600, 1)");

        // Make table stale - add sales that change HAVING results
        assertUpdate("INSERT INTO test_having_stale1 VALUES " +
                "(4, 'Books', 200, DATE '2024-01-02'), " +
                "(5, 'Clothing', 400, DATE '2024-01-02'), " +
                "(6, 'Toys', 800, DATE '2024-01-02')", 3);

        // Expected: Books now 600 (passes), Clothing now 700 (passes), Toys 800 (passes), Electronics still 600
        assertMaterializedViewQuery("SELECT category, total_revenue, sale_count FROM test_having_stale1_mv ORDER BY category",
                "VALUES ('Books', 600, 2), " +
                "       ('Clothing', 700, 2), " +
                "       ('Electronics', 600, 1), " +
                "       ('Toys', 800, 1)");

        assertUpdate("DROP MATERIALIZED VIEW test_having_stale1_mv");
        assertUpdate("DROP TABLE test_having_stale1");
    }

    @Test
    public void testGroupByHavingWithMultipleStaleTables()
    {
        assertUpdate("CREATE TABLE test_having_orders (" +
                "order_id BIGINT, " +
                "product_id BIGINT, " +
                "quantity BIGINT, " +
                "order_date DATE) " +
                "WITH (partitioning = ARRAY['order_date'])");

        assertUpdate("CREATE TABLE test_having_products (" +
                "product_id BIGINT, " +
                "category VARCHAR, " +
                "price BIGINT, " +
                "region VARCHAR) " +
                "WITH (partitioning = ARRAY['region'])");

        assertUpdate("INSERT INTO test_having_orders VALUES " +
                "(1, 100, 10, DATE '2024-01-01'), " +
                "(2, 200, 5, DATE '2024-01-01'), " +
                "(3, 100, 8, DATE '2024-01-01')", 3);

        assertUpdate("INSERT INTO test_having_products VALUES " +
                "(100, 'Electronics', 50, 'US'), " +
                "(200, 'Books', 20, 'US')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW test_having_join_mv AS " +
                "SELECT p.category, SUM(o.quantity * p.price) as total_revenue, COUNT(*) as order_count " +
                "FROM test_having_orders o " +
                "JOIN test_having_products p ON o.product_id = p.product_id " +
                "GROUP BY p.category " +
                "HAVING SUM(o.quantity * p.price) > 200");

        assertRefreshAndFullyMaterialized("test_having_join_mv", 1);

        // Initial: Electronics has revenue 900 (10*50 + 8*50), Books has 100 (5*20)
        // Only Electronics passes HAVING > 200
        assertMaterializedViewQuery("SELECT category, total_revenue, order_count FROM test_having_join_mv ORDER BY category",
                "VALUES ('Electronics', 900, 2)");

        // Make orders table stale
        assertUpdate("INSERT INTO test_having_orders VALUES " +
                "(4, 200, 15, DATE '2024-01-02')", 1);

        // Books now has revenue 400 (5*20 + 15*20), now passes HAVING filter
        assertMaterializedViewQuery("SELECT category, total_revenue, order_count FROM test_having_join_mv ORDER BY category",
                "VALUES ('Books', 400, 2), " +
                "       ('Electronics', 900, 2)");

        // Make products table also stale
        assertUpdate("INSERT INTO test_having_products VALUES " +
                "(300, 'Toys', 30, 'EU')", 1);
        assertUpdate("INSERT INTO test_having_orders VALUES " +
                "(5, 300, 10, DATE '2024-01-03')", 1);

        // Toys has revenue 300 (10*30), passes HAVING filter
        assertMaterializedViewQuery("SELECT category, total_revenue, order_count FROM test_having_join_mv ORDER BY category",
                "VALUES ('Books', 400, 2), " +
                "       ('Electronics', 900, 2), " +
                "       ('Toys', 300, 1)");

        assertUpdate("DROP MATERIALIZED VIEW test_having_join_mv");
        assertUpdate("DROP TABLE test_having_products");
        assertUpdate("DROP TABLE test_having_orders");
    }

    @Test
    public void testGroupByHavingWithSumCount()
    {
        assertUpdate("CREATE TABLE test_having_sumcount (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "amount BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_having_sumcount VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', 200, DATE '2024-01-01'), " +
                "(3, 'A', 300, DATE '2024-01-01'), " +
                "(4, 'B', 500, DATE '2024-01-01'), " +
                "(5, 'B', 600, DATE '2024-01-01'), " +
                "(6, 'C', 1000, DATE '2024-01-01')", 6);

        assertUpdate("CREATE MATERIALIZED VIEW test_having_sumcount_mv AS " +
                "SELECT category, SUM(amount) as total, COUNT(*) as cnt " +
                "FROM test_having_sumcount " +
                "GROUP BY category " +
                "HAVING SUM(amount) > 500 AND COUNT(*) >= 2");

        assertRefreshAndFullyMaterialized("test_having_sumcount_mv", 2);

        // A: sum=600, count=3 (passes both conditions)
        // B: sum=1100, count=2 (passes both conditions)
        // C: sum=1000, count=1 (fails count condition)
        assertMaterializedViewQuery("SELECT category, total, cnt FROM test_having_sumcount_mv ORDER BY category",
                "VALUES ('A', 600, 3), " +
                "       ('B', 1100, 2)");

        // Make table stale
        assertUpdate("INSERT INTO test_having_sumcount VALUES " +
                "(7, 'C', 100, DATE '2024-01-02'), " +
                "(8, 'D', 300, DATE '2024-01-02'), " +
                "(9, 'D', 400, DATE '2024-01-02')", 3);

        // C: sum=1100, count=2 (now passes both conditions)
        // D: sum=700, count=2 (passes both conditions)
        assertMaterializedViewQuery("SELECT category, total, cnt FROM test_having_sumcount_mv ORDER BY category",
                "VALUES ('A', 600, 3), " +
                "       ('B', 1100, 2), " +
                "       ('C', 1100, 2), " +
                "       ('D', 700, 2)");

        assertUpdate("DROP MATERIALIZED VIEW test_having_sumcount_mv");
        assertUpdate("DROP TABLE test_having_sumcount");
    }

    @Test
    public void testGroupByHavingWithAvg()
    {
        assertUpdate("CREATE TABLE test_having_avg (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "value BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_having_avg VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', 200, DATE '2024-01-01'), " +
                "(3, 'A', 300, DATE '2024-01-01'), " +
                "(4, 'B', 50, DATE '2024-01-01'), " +
                "(5, 'B', 100, DATE '2024-01-01'), " +
                "(6, 'C', 500, DATE '2024-01-01')", 6);

        assertUpdate("CREATE MATERIALIZED VIEW test_having_avg_mv AS " +
                "SELECT category, AVG(value) as avg_value, COUNT(*) as cnt " +
                "FROM test_having_avg " +
                "GROUP BY category " +
                "HAVING AVG(value) > 100");

        assertRefreshAndFullyMaterialized("test_having_avg_mv", 2);

        // A: avg=200 (passes)
        // B: avg=75 (fails)
        // C: avg=500 (passes)
        assertMaterializedViewQuery("SELECT category, avg_value, cnt FROM test_having_avg_mv ORDER BY category",
                "VALUES ('A', 200.0, 3), " +
                "       ('C', 500.0, 1)");

        // Make table stale - add values that change averages
        assertUpdate("INSERT INTO test_having_avg VALUES " +
                "(7, 'B', 150, DATE '2024-01-02'), " +
                "(8, 'B', 200, DATE '2024-01-02'), " +
                "(9, 'D', 250, DATE '2024-01-02')", 3);

        // B: avg=(50+100+150+200)/4=125 (now passes)
        // D: avg=250 (passes)
        assertMaterializedViewQuery("SELECT category, avg_value, cnt FROM test_having_avg_mv ORDER BY category",
                "VALUES ('A', 200.0, 3), " +
                "       ('B', 125.0, 4), " +
                "       ('C', 500.0, 1), " +
                "       ('D', 250.0, 1)");

        assertUpdate("DROP MATERIALIZED VIEW test_having_avg_mv");
        assertUpdate("DROP TABLE test_having_avg");
    }

    @Test
    public void testGroupByHavingWithCountDistinct()
    {
        assertUpdate("CREATE TABLE test_having_distinct (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "user_id BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_having_distinct VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', 100, DATE '2024-01-01'), " +
                "(3, 'A', 200, DATE '2024-01-01'), " +
                "(4, 'A', 200, DATE '2024-01-01'), " +
                "(5, 'B', 300, DATE '2024-01-01'), " +
                "(6, 'B', 300, DATE '2024-01-01'), " +
                "(7, 'C', 400, DATE '2024-01-01'), " +
                "(8, 'C', 500, DATE '2024-01-01'), " +
                "(9, 'C', 600, DATE '2024-01-01')", 9);

        assertUpdate("CREATE MATERIALIZED VIEW test_having_distinct_mv AS " +
                "SELECT category, COUNT(DISTINCT user_id) as unique_users, COUNT(*) as total_events " +
                "FROM test_having_distinct " +
                "GROUP BY category " +
                "HAVING COUNT(DISTINCT user_id) >= 2");

        assertRefreshAndFullyMaterialized("test_having_distinct_mv", 2);

        // A: 2 unique users (passes)
        // B: 1 unique user (fails)
        // C: 3 unique users (passes)
        assertMaterializedViewQuery("SELECT category, unique_users, total_events FROM test_having_distinct_mv ORDER BY category",
                "VALUES ('A', 2, 4), " +
                "       ('C', 3, 3)");

        // Make table stale
        assertUpdate("INSERT INTO test_having_distinct VALUES " +
                "(10, 'B', 400, DATE '2024-01-02'), " +
                "(11, 'D', 700, DATE '2024-01-02'), " +
                "(12, 'D', 800, DATE '2024-01-02')", 3);

        // B: 2 unique users (now passes)
        // D: 2 unique users (passes)
        assertMaterializedViewQuery("SELECT category, unique_users, total_events FROM test_having_distinct_mv ORDER BY category",
                "VALUES ('A', 2, 4), " +
                "       ('B', 2, 3), " +
                "       ('C', 3, 3), " +
                "       ('D', 2, 2)");

        assertUpdate("DROP MATERIALIZED VIEW test_having_distinct_mv");
        assertUpdate("DROP TABLE test_having_distinct");
    }

    @Test
    public void testGroupByHavingWithMultipleConditions()
    {
        assertUpdate("CREATE TABLE test_having_multi (" +
                "id BIGINT, " +
                "category VARCHAR, " +
                "amount BIGINT, " +
                "event_date DATE) " +
                "WITH (partitioning = ARRAY['event_date'])");

        assertUpdate("INSERT INTO test_having_multi VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', 200, DATE '2024-01-01'), " +
                "(3, 'A', 300, DATE '2024-01-01'), " +
                "(4, 'A', 400, DATE '2024-01-01'), " +
                "(5, 'B', 800, DATE '2024-01-01'), " +
                "(6, 'B', 900, DATE '2024-01-01'), " +
                "(7, 'C', 150, DATE '2024-01-01'), " +
                "(8, 'C', 250, DATE '2024-01-01'), " +
                "(9, 'D', 2000, DATE '2024-01-01')", 9);

        assertUpdate("CREATE MATERIALIZED VIEW test_having_multi_mv AS " +
                "SELECT category, " +
                "  SUM(amount) as total, " +
                "  AVG(amount) as average, " +
                "  COUNT(*) as cnt, " +
                "  MIN(amount) as minimum, " +
                "  MAX(amount) as maximum " +
                "FROM test_having_multi " +
                "GROUP BY category " +
                "HAVING SUM(amount) > 500 AND AVG(amount) >= 200 AND COUNT(*) >= 2");

        assertRefreshAndFullyMaterialized("test_having_multi_mv", 2);

        // A: sum=1000, avg=250, cnt=4, min=100, max=400 (passes all)
        // B: sum=1700, avg=850, cnt=2, min=800, max=900 (passes all)
        // C: sum=400, avg=200, cnt=2 (fails sum condition)
        // D: sum=2000, avg=2000, cnt=1 (fails cnt condition)
        assertMaterializedViewQuery("SELECT category, total, average, cnt, minimum, maximum FROM test_having_multi_mv ORDER BY category",
                "VALUES ('A', 1000, 250.0, 4, 100, 400), " +
                "       ('B', 1700, 850.0, 2, 800, 900)");

        // Make table stale
        assertUpdate("INSERT INTO test_having_multi VALUES " +
                "(10, 'C', 350, DATE '2024-01-02'), " +
                "(11, 'D', 500, DATE '2024-01-02'), " +
                "(12, 'E', 300, DATE '2024-01-02'), " +
                "(13, 'E', 400, DATE '2024-01-02')", 4);

        // C: sum=750, avg=250, cnt=3 (now passes all)
        // D: sum=2500, avg=1250, cnt=2 (now passes all)
        // E: sum=700, avg=350, cnt=2 (passes all)
        assertMaterializedViewQuery("SELECT category, total, average, cnt, minimum, maximum FROM test_having_multi_mv ORDER BY category",
                "VALUES ('A', 1000, 250.0, 4, 100, 400), " +
                "       ('B', 1700, 850.0, 2, 800, 900), " +
                "       ('C', 750, 250.0, 3, 150, 350), " +
                "       ('D', 2500, 1250.0, 2, 500, 2000), " +
                "       ('E', 700, 350.0, 2, 300, 400)");

        assertUpdate("DROP MATERIALIZED VIEW test_having_multi_mv");
        assertUpdate("DROP TABLE test_having_multi");
    }

    /**
     * Data provider for non-deterministic function tests.
     * Returns: [function expression, description, needs base tables]
     */
    @DataProvider(name = "nonDeterministicFunctions")
    public Object[][] nonDeterministicFunctions()
    {
        return new Object[][] {
                {"RAND()", "RAND() in SELECT", true},
                {"NOW()", "NOW() in SELECT", true},
                {"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP in SELECT", true},
        };
    }

    /**
     * Test that non-deterministic functions in SELECT cause fallback to full recompute.
     * Materialized views with non-deterministic functions should never use stitching because:
     * - Fresh branch would compute RAND()/NOW()/UUID() at read time
     * - Stale branch would compute different values
     * - Results would be inconsistent
     */
    @Test(dataProvider = "nonDeterministicFunctions")
    public void testNonDeterministicFunctionInSelect(String functionExpr, String description, boolean needsBaseTables)
    {
        // Create base table
        assertUpdate("CREATE TABLE test_nondeterministic_base (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("INSERT INTO test_nondeterministic_base VALUES (1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01')", 2);

        // Create MV with non-deterministic function
        assertUpdate("CREATE MATERIALIZED VIEW test_nondeterministic_mv AS " +
                "SELECT id, value, " + functionExpr + " as nondeterministic_value, dt FROM test_nondeterministic_base");

        // Initial refresh
        assertRefreshAndFullyMaterialized("test_nondeterministic_mv", 2);

        // Introduce staleness
        assertUpdate("INSERT INTO test_nondeterministic_base VALUES (3, 300, DATE '2024-01-02')", 1);

        // Query should fall back to full recompute (not use stitching)
        // We verify this by checking that the query succeeds and returns correct row count
        // The optimizer will automatically use full recompute instead of stitching
        assertQuery("SELECT id, value FROM test_nondeterministic_mv ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_nondeterministic_mv");
        assertUpdate("DROP TABLE test_nondeterministic_base");
    }

    /**
     * Test that non-deterministic functions in WHERE clause cause fallback to full recompute.
     */
    @Test(dataProvider = "nonDeterministicFunctions")
    public void testNonDeterministicFunctionInWhere(String functionExpr, String description, boolean needsBaseTables)
    {
        // Skip functions that can't be used in WHERE clause (UUID returns VARCHAR, can't compare with numbers easily)
        if (functionExpr.equals("UUID()")) {
            return;
        }

        // Create base table
        assertUpdate("CREATE TABLE test_nondeterministic_where (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("INSERT INTO test_nondeterministic_where VALUES (1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01')", 2);

        // Create MV with non-deterministic function in WHERE clause
        // Use a condition that's always true but contains the non-deterministic function
        String whereClause = functionExpr.contains("RAND") ? "RAND() >= 0" : functionExpr + " IS NOT NULL";
        assertUpdate("CREATE MATERIALIZED VIEW test_nondeterministic_where_mv AS " +
                "SELECT id, value, dt FROM test_nondeterministic_where WHERE " + whereClause);

        // Initial refresh
        assertRefreshAndFullyMaterialized("test_nondeterministic_where_mv", 2);

        // Introduce staleness
        assertUpdate("INSERT INTO test_nondeterministic_where VALUES (3, 300, DATE '2024-01-02')", 1);

        // Query should fall back to full recompute
        assertQuery("SELECT id, value FROM test_nondeterministic_where_mv ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300)");

        assertUpdate("DROP MATERIALIZED VIEW test_nondeterministic_where_mv");
        assertUpdate("DROP TABLE test_nondeterministic_where");
    }

    /**
     * Test that non-deterministic functions in aggregation cause fallback to full recompute.
     */
    @Test
    public void testNonDeterministicFunctionInAggregation()
    {
        // Create base table
        assertUpdate("CREATE TABLE test_nondeterministic_agg (id INTEGER, category VARCHAR, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("INSERT INTO test_nondeterministic_agg VALUES " +
                "(1, 'A', 100, DATE '2024-01-01'), " +
                "(2, 'A', 200, DATE '2024-01-01'), " +
                "(3, 'B', 300, DATE '2024-01-01')", 3);

        // Create MV with non-deterministic function in aggregation
        // Use RAND() to create a non-deterministic computed value before aggregation
        assertUpdate("CREATE MATERIALIZED VIEW test_nondeterministic_agg_mv AS " +
                "SELECT category, SUM(value * (1 + RAND())) as total FROM test_nondeterministic_agg GROUP BY category");

        // Initial refresh
        assertRefreshAndFullyMaterialized("test_nondeterministic_agg_mv", 2);

        // Introduce staleness
        assertUpdate("INSERT INTO test_nondeterministic_agg VALUES (4, 'A', 400, DATE '2024-01-02')", 1);

        // Query should fall back to full recompute
        // We just verify it returns the expected categories (values will vary due to RAND())
        assertQuery("SELECT category FROM test_nondeterministic_agg_mv ORDER BY category",
                "VALUES ('A'), ('B')");

        assertUpdate("DROP MATERIALIZED VIEW test_nondeterministic_agg_mv");
        assertUpdate("DROP TABLE test_nondeterministic_agg");
    }

    @Test
    public void testUnionInMVDefinitionWithNoStaleTables()
    {
        assertUpdate("CREATE TABLE test_union_base1 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE test_union_base2 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        assertUpdate("INSERT INTO test_union_base1 VALUES (1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO test_union_base2 VALUES (3, 300, DATE '2024-01-01'), (4, 400, DATE '2024-01-01')", 2);

        // Create MV with UNION
        assertUpdate("CREATE MATERIALIZED VIEW test_union_mv AS " +
                "SELECT id, value, dt FROM test_union_base1 " +
                "UNION " +
                "SELECT id, value, dt FROM test_union_base2");

        assertRefreshAndFullyMaterialized("test_union_mv", 4);

        // Query MV - all data is fresh, no stitching needed
        assertQuery("SELECT id, value FROM test_union_mv ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300), (4, 400)");

        assertUpdate("DROP MATERIALIZED VIEW test_union_mv");
        assertUpdate("DROP TABLE test_union_base1");
        assertUpdate("DROP TABLE test_union_base2");
    }

    @Test
    public void testUnionInMVDefinitionWithSingleStaleTable()
    {
        assertUpdate("CREATE TABLE test_union_stale1 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE test_union_stale2 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        assertUpdate("INSERT INTO test_union_stale1 VALUES (1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO test_union_stale2 VALUES (3, 300, DATE '2024-01-01'), (4, 400, DATE '2024-01-01')", 2);

        // Create MV with UNION
        assertUpdate("CREATE MATERIALIZED VIEW test_union_stale_mv AS " +
                "SELECT id, value, dt FROM test_union_stale1 " +
                "UNION " +
                "SELECT id, value, dt FROM test_union_stale2");

        assertRefreshAndFullyMaterialized("test_union_stale_mv", 4);

        // Insert into one table to make it stale
        assertUpdate("INSERT INTO test_union_stale1 VALUES (5, 500, DATE '2024-01-02')", 1);

        // Query MV - should stitch fresh from MV storage with stale from base table
        assertQuery("SELECT id, value FROM test_union_stale_mv ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)");

        // Verify stitching produces same result as full recompute
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM test_union_stale_mv ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW test_union_stale_mv");
        assertUpdate("DROP TABLE test_union_stale1");
        assertUpdate("DROP TABLE test_union_stale2");
    }

    @Test
    public void testUnionInMVDefinitionWithMultipleStaleTables()
    {
        assertUpdate("CREATE TABLE test_union_multi1 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE test_union_multi2 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        assertUpdate("INSERT INTO test_union_multi1 VALUES (1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO test_union_multi2 VALUES (3, 300, DATE '2024-01-01'), (4, 400, DATE '2024-01-01')", 2);

        // Create MV with UNION
        assertUpdate("CREATE MATERIALIZED VIEW test_union_multi_mv AS " +
                "SELECT id, value, dt FROM test_union_multi1 " +
                "UNION " +
                "SELECT id, value, dt FROM test_union_multi2");

        assertRefreshAndFullyMaterialized("test_union_multi_mv", 4);

        // Insert into both tables to make both stale
        assertUpdate("INSERT INTO test_union_multi1 VALUES (5, 500, DATE '2024-01-02')", 1);
        assertUpdate("INSERT INTO test_union_multi2 VALUES (6, 600, DATE '2024-01-02')", 1);

        // Query MV - should stitch fresh from MV storage with stale from both base tables
        assertQuery("SELECT id, value FROM test_union_multi_mv ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600)");

        // Verify stitching produces same result as full recompute
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM test_union_multi_mv ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW test_union_multi_mv");
        assertUpdate("DROP TABLE test_union_multi1");
        assertUpdate("DROP TABLE test_union_multi2");
    }

    @Test
    public void testUnionAllVsUnionDistinct()
    {
        assertUpdate("CREATE TABLE test_union_type1 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE test_union_type2 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Insert data with potential duplicates
        assertUpdate("INSERT INTO test_union_type1 VALUES (1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO test_union_type2 VALUES (1, 100, DATE '2024-01-01'), (3, 300, DATE '2024-01-01')", 2);

        // Test UNION (implicit DISTINCT)
        assertUpdate("CREATE MATERIALIZED VIEW test_union_distinct_mv AS " +
                "SELECT id, value, dt FROM test_union_type1 " +
                "UNION " +
                "SELECT id, value, dt FROM test_union_type2");

        assertRefreshAndFullyMaterialized("test_union_distinct_mv", 3);

        // Test UNION ALL (keeps duplicates)
        assertUpdate("CREATE MATERIALIZED VIEW test_union_all_mv AS " +
                "SELECT id, value, dt FROM test_union_type1 " +
                "UNION ALL " +
                "SELECT id, value, dt FROM test_union_type2");

        assertRefreshAndFullyMaterialized("test_union_all_mv", 4);

        // UNION should deduplicate (1, 100) appears only once
        assertQuery("SELECT id, value FROM test_union_distinct_mv ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300)");

        // UNION ALL should keep duplicates (1, 100) appears twice
        assertQuery("SELECT id, value FROM test_union_all_mv ORDER BY id",
                "VALUES (1, 100), (1, 100), (2, 200), (3, 300)");

        // Insert into one table to make it stale
        assertUpdate("INSERT INTO test_union_type1 VALUES (4, 400, DATE '2024-01-02')", 1);

        // Verify stitching preserves UNION semantics (deduplication)
        assertQuery("SELECT id, value FROM test_union_distinct_mv ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300), (4, 400)");

        // Verify stitching preserves UNION ALL semantics (no deduplication)
        assertQuery("SELECT id, value FROM test_union_all_mv ORDER BY id",
                "VALUES (1, 100), (1, 100), (2, 200), (3, 300), (4, 400)");

        // Verify correctness
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM test_union_distinct_mv ORDER BY id",
                true);
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM test_union_all_mv ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW test_union_distinct_mv");
        assertUpdate("DROP MATERIALIZED VIEW test_union_all_mv");
        assertUpdate("DROP TABLE test_union_type1");
        assertUpdate("DROP TABLE test_union_type2");
    }

    @Test
    public void testIntersectInMVDefinition()
    {
        assertUpdate("CREATE TABLE test_intersect_base1 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE test_intersect_base2 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        assertUpdate("INSERT INTO test_intersect_base1 VALUES (1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01'), (3, 300, DATE '2024-01-01')", 3);
        assertUpdate("INSERT INTO test_intersect_base2 VALUES (2, 200, DATE '2024-01-01'), (3, 300, DATE '2024-01-01'), (4, 400, DATE '2024-01-01')", 3);

        // Create MV with INTERSECT - should return only common rows
        assertUpdate("CREATE MATERIALIZED VIEW test_intersect_mv AS " +
                "SELECT id, value, dt FROM test_intersect_base1 " +
                "INTERSECT " +
                "SELECT id, value, dt FROM test_intersect_base2");

        assertRefreshAndFullyMaterialized("test_intersect_mv", 2);

        // Only (2, 200) and (3, 300) should appear
        assertQuery("SELECT id, value FROM test_intersect_mv ORDER BY id",
                "VALUES (2, 200), (3, 300)");

        // Insert into BOTH tables with same dt to create a true intersection
        assertUpdate("INSERT INTO test_intersect_base1 VALUES (4, 400, DATE '2024-01-02')", 1);
        assertUpdate("INSERT INTO test_intersect_base2 VALUES (4, 400, DATE '2024-01-02')", 1);

        // Now (4, 400, '2024-01-02') exists in both tables, creating a true intersection
        assertQuery("SELECT id, value FROM test_intersect_mv ORDER BY id",
                "VALUES (2, 200), (3, 300), (4, 400)");

        // Verify stitching produces same result as full recompute
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM test_intersect_mv ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW test_intersect_mv");
        assertUpdate("DROP TABLE test_intersect_base1");
        assertUpdate("DROP TABLE test_intersect_base2");
    }

    @Test
    public void testIntersectInMVDefinitionOneSideStale()
    {
        assertUpdate("CREATE TABLE test_intersect_one_stale_base1 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE test_intersect_one_stale_base2 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        assertUpdate("INSERT INTO test_intersect_one_stale_base1 VALUES (1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01'), (3, 300, DATE '2024-01-01')", 3);
        assertUpdate("INSERT INTO test_intersect_one_stale_base2 VALUES (2, 200, DATE '2024-01-01'), (3, 300, DATE '2024-01-01'), (4, 400, DATE '2024-01-01')", 3);

        // Create MV with INTERSECT - should return only common rows
        assertUpdate("CREATE MATERIALIZED VIEW test_intersect_one_stale_mv AS " +
                "SELECT id, value, dt FROM test_intersect_one_stale_base1 " +
                "INTERSECT " +
                "SELECT id, value, dt FROM test_intersect_one_stale_base2");

        assertRefreshAndFullyMaterialized("test_intersect_one_stale_mv", 2);

        // Only (2, 200) and (3, 300) should appear
        assertQuery("SELECT id, value FROM test_intersect_one_stale_mv ORDER BY id",
                "VALUES (2, 200), (3, 300)");

        // Insert into ONLY base1 (left side becomes stale, right side stays fresh)
        assertUpdate("INSERT INTO test_intersect_one_stale_base1 VALUES (5, 500, DATE '2024-01-02')", 1);

        // MV should not change since there's no matching row in base2
        assertQuery("SELECT id, value FROM test_intersect_one_stale_mv ORDER BY id",
                "VALUES (2, 200), (3, 300)");

        // Verify stitching produces same result as full recompute
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM test_intersect_one_stale_mv ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW test_intersect_one_stale_mv");
        assertUpdate("DROP TABLE test_intersect_one_stale_base1");
        assertUpdate("DROP TABLE test_intersect_one_stale_base2");
    }

    @Test
    public void testExceptInMVDefinition()
    {
        assertUpdate("CREATE TABLE test_except_base1 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE test_except_base2 (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        assertUpdate("INSERT INTO test_except_base1 VALUES (1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01'), (3, 300, DATE '2024-01-01')", 3);
        assertUpdate("INSERT INTO test_except_base2 VALUES (2, 200, DATE '2024-01-01'), (4, 400, DATE '2024-01-01')", 2);

        // Create MV with EXCEPT - returns rows in base1 but not in base2
        assertUpdate("CREATE MATERIALIZED VIEW test_except_mv AS " +
                "SELECT id, value, dt FROM test_except_base1 " +
                "EXCEPT " +
                "SELECT id, value, dt FROM test_except_base2");

        assertRefreshAndFullyMaterialized("test_except_mv", 2);

        // Only (1, 100) and (3, 300) should appear (2, 200) is excluded
        assertQuery("SELECT id, value FROM test_except_mv ORDER BY id",
                "VALUES (1, 100), (3, 300)");

        // Insert into first table to make it stale
        assertUpdate("INSERT INTO test_except_base1 VALUES (5, 500, DATE '2024-01-02')", 1);

        // (5, 500) is in base1 but not base2, so it should appear
        assertQuery("SELECT id, value FROM test_except_mv ORDER BY id",
                "VALUES (1, 100), (3, 300), (5, 500)");

        // Verify stitching produces same result as full recompute
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM test_except_mv ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW test_except_mv");
        assertUpdate("DROP TABLE test_except_base1");
        assertUpdate("DROP TABLE test_except_base2");
    }

    /**
     * Test UNION of two tables, then those unioned results are joined with a third table.
     * This tests that fresh tables in UNION get FALSE predicate, and that predicate
     * doesn't interfere with the JOIN above it.
     *
     * Pattern: (T1 UNION T2) JOIN T3
     * Multi-column partitioning ensures proper constraint handling.
     */
    @Test
    public void testUnionThenJoinWithMultiPartitioning()
    {
        assertUpdate("CREATE TABLE union_join_t1 (id BIGINT, key BIGINT, date VARCHAR, region VARCHAR) " +
                "WITH (partitioning = ARRAY['date', 'region'])");
        assertUpdate("CREATE TABLE union_join_t2 (id BIGINT, key BIGINT, date VARCHAR, region VARCHAR) " +
                "WITH (partitioning = ARRAY['date', 'region'])");
        assertUpdate("CREATE TABLE union_join_t3 (key BIGINT, name VARCHAR, date VARCHAR, region VARCHAR) " +
                "WITH (partitioning = ARRAY['date', 'region'])");

        // Insert initial data across multiple partitions
        assertUpdate("INSERT INTO union_join_t1 VALUES (1, 100, '2024-01-01', 'US')", 1);
        assertUpdate("INSERT INTO union_join_t1 VALUES (2, 101, '2024-01-01', 'EU')", 1);
        assertUpdate("INSERT INTO union_join_t2 VALUES (3, 100, '2024-01-01', 'US')", 1);
        assertUpdate("INSERT INTO union_join_t2 VALUES (4, 101, '2024-01-01', 'EU')", 1);
        assertUpdate("INSERT INTO union_join_t3 VALUES (100, 'Alice', '2024-01-01', 'US')", 1);
        assertUpdate("INSERT INTO union_join_t3 VALUES (101, 'Bob', '2024-01-01', 'EU')", 1);

        // Create MV: (T1 UNION T2) JOIN T3 with multi-column partitioning
        assertUpdate("CREATE MATERIALIZED VIEW mv_union_join " +
                "WITH (partitioning = ARRAY['date', 'region']) AS " +
                "SELECT u.id, t3.name, u.date, u.region " +
                "FROM (SELECT id, key, date, region FROM union_join_t1 " +
                "      UNION ALL " +
                "      SELECT id, key, date, region FROM union_join_t2) u " +
                "JOIN union_join_t3 t3 ON u.key = t3.key AND u.date = t3.date AND u.region = t3.region");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_union_join");

        // Make T1 stale in ONLY ONE partition: (date='2024-01-02', region='US')
        // This leaves other partitions fresh, ensuring data table is used for stitching
        assertUpdate("INSERT INTO union_join_t1 VALUES (5, 200, '2024-01-02', 'US')", 1);
        assertUpdate("INSERT INTO union_join_t3 VALUES (200, 'Charlie', '2024-01-02', 'US')", 1);

        // Query the MV - should use UNION stitching
        // Expected behavior:
        // - Storage scan with filter: NOT (date='2024-01-02' AND region='US')
        // - Stale branch recomputes only (date='2024-01-02', region='US'):
        //   - T1 with date='2024-01-02' AND region='US' (stale table)
        //   - T2 gets FALSE predicate (fresh table in UNION with stale branch)
        //   - T3 sees all data (not in UNION, participating in JOIN)
        assertQuery("SELECT * FROM mv_union_join ORDER BY id",
                "VALUES (1, 'Alice', '2024-01-01', 'US'), " +
                        "(2, 'Bob', '2024-01-01', 'EU'), " +
                        "(3, 'Alice', '2024-01-01', 'US'), " +
                        "(4, 'Bob', '2024-01-01', 'EU'), " +
                        "(5, 'Charlie', '2024-01-02', 'US')");
        assertMaterializedViewResultsMatch(getSession(), "SELECT * FROM mv_union_join ORDER BY id");

        assertUpdate("DROP MATERIALIZED VIEW mv_union_join");
        assertUpdate("DROP TABLE union_join_t3");
        assertUpdate("DROP TABLE union_join_t2");
        assertUpdate("DROP TABLE union_join_t1");
    }

    /**
     * Test JOIN of two tables, then those joined results are unioned with a third table.
     * This tests that the fresh table in UNION gets FALSE predicate correctly,
     * and that tables in the JOIN (not in UNION) see all data.
     *
     * Pattern: (T1 JOIN T2) UNION T3
     * Multi-column partitioning with partial staleness ensures data table usage.
     */
    @Test
    public void testJoinThenUnionWithMultiPartitioning()
    {
        assertUpdate("CREATE TABLE join_union_t1 (id BIGINT, key BIGINT, date VARCHAR, region VARCHAR) " +
                "WITH (partitioning = ARRAY['date', 'region'])");
        assertUpdate("CREATE TABLE join_union_t2 (key BIGINT, name VARCHAR, date VARCHAR, region VARCHAR) " +
                "WITH (partitioning = ARRAY['date', 'region'])");
        assertUpdate("CREATE TABLE join_union_t3 (id BIGINT, name VARCHAR, date VARCHAR, region VARCHAR) " +
                "WITH (partitioning = ARRAY['date', 'region'])");

        // Insert initial data across multiple partitions
        assertUpdate("INSERT INTO join_union_t1 VALUES (1, 100, '2024-01-01', 'US')", 1);
        assertUpdate("INSERT INTO join_union_t1 VALUES (2, 101, '2024-01-01', 'EU')", 1);
        assertUpdate("INSERT INTO join_union_t2 VALUES (100, 'Alice', '2024-01-01', 'US')", 1);
        assertUpdate("INSERT INTO join_union_t2 VALUES (101, 'Bob', '2024-01-01', 'EU')", 1);
        assertUpdate("INSERT INTO join_union_t3 VALUES (3, 'Charlie', '2024-01-01', 'US')", 1);
        assertUpdate("INSERT INTO join_union_t3 VALUES (4, 'David', '2024-01-01', 'EU')", 1);

        // Create MV: (T1 JOIN T2) UNION T3 with multi-column partitioning
        assertUpdate("CREATE MATERIALIZED VIEW mv_join_union " +
                "WITH (partitioning = ARRAY['date', 'region']) AS " +
                "SELECT j.id, j.name, j.date, j.region FROM " +
                "  (SELECT t1.id, t2.name, t1.date, t1.region " +
                "   FROM join_union_t1 t1 " +
                "   JOIN join_union_t2 t2 ON t1.key = t2.key AND t1.date = t2.date AND t1.region = t2.region) j " +
                "UNION ALL " +
                "SELECT id, name, date, region FROM join_union_t3");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_join_union");

        // Make T1 stale in ONLY ONE partition: (date='2024-01-02', region='EU')
        // This leaves (date='2024-01-01', region='US') and (date='2024-01-01', region='EU') fresh
        assertUpdate("INSERT INTO join_union_t1 VALUES (5, 200, '2024-01-02', 'EU')", 1);
        assertUpdate("INSERT INTO join_union_t2 VALUES (200, 'Eve', '2024-01-02', 'EU')", 1);

        // Query the MV - should use UNION stitching
        // Expected behavior:
        // - Storage scan with filter: NOT (date='2024-01-02' AND region='EU')
        // - Stale branch recomputes only (date='2024-01-02', region='EU'):
        //   - T1 with date='2024-01-02' AND region='EU' (stale table)
        //   - T2 sees all data (not in UNION, participating in JOIN with stale table)
        //   - T3 gets FALSE predicate (fresh table in UNION with stale branch)
        assertQuery("SELECT * FROM mv_join_union ORDER BY id",
                "VALUES (1, 'Alice', '2024-01-01', 'US'), " +
                        "(2, 'Bob', '2024-01-01', 'EU'), " +
                        "(3, 'Charlie', '2024-01-01', 'US'), " +
                        "(4, 'David', '2024-01-01', 'EU'), " +
                        "(5, 'Eve', '2024-01-02', 'EU')");

        assertUpdate("DROP MATERIALIZED VIEW mv_join_union");
        assertUpdate("DROP TABLE join_union_t3");
        assertUpdate("DROP TABLE join_union_t2");
        assertUpdate("DROP TABLE join_union_t1");
    }

    /**
     * Edge case: Nested UNIONs with JOIN in between.
     * Pattern: (T1 UNION T2) JOIN (T3 UNION T4)
     * All tables inside the JOINs should see all data, even though they're in inner UNIONs.
     */
    @Test
    public void testUnionJoinUnionNesting()
    {
        assertUpdate("CREATE TABLE unju_t1 (id BIGINT, key BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE unju_t2 (id BIGINT, key BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE unju_t3 (key BIGINT, name VARCHAR, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE unju_t4 (key BIGINT, name VARCHAR, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");

        // Initial data
        assertUpdate("INSERT INTO unju_t1 VALUES (1, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO unju_t2 VALUES (2, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO unju_t3 VALUES (100, 'Alice', '2024-01-01')", 1);
        assertUpdate("INSERT INTO unju_t4 VALUES (100, 'Bob', '2024-01-01')", 1);

        // MV: (T1 UNION T2) JOIN (T3 UNION T4)
        assertUpdate("CREATE MATERIALIZED VIEW mv_unju AS " +
                "SELECT u1.id, u2.name, u1.date " +
                "FROM (SELECT id, key, date FROM unju_t1 UNION ALL SELECT id, key, date FROM unju_t2) u1 " +
                "JOIN (SELECT key, name, date FROM unju_t3 UNION ALL SELECT key, name, date FROM unju_t4) u2 " +
                "ON u1.key = u2.key AND u1.date = u2.date");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_unju");

        // Make T1 stale
        assertUpdate("INSERT INTO unju_t1 VALUES (3, 200, '2024-01-02')", 1);
        assertUpdate("INSERT INTO unju_t3 VALUES (200, 'Charlie', '2024-01-02')", 1);

        // Expected: T1 sees stale predicate, T2/T3/T4 all see ALL data (inside JOINs)
        // Result should include new row from recompute
        assertQuery("SELECT * FROM mv_unju ORDER BY id",
                "VALUES (1, 'Alice', '2024-01-01'), " +
                        "(1, 'Bob', '2024-01-01'), " +
                        "(2, 'Alice', '2024-01-01'), " +
                        "(2, 'Bob', '2024-01-01'), " +
                        "(3, 'Charlie', '2024-01-02')");

        assertUpdate("DROP MATERIALIZED VIEW mv_unju");
        assertUpdate("DROP TABLE unju_t4");
        assertUpdate("DROP TABLE unju_t3");
        assertUpdate("DROP TABLE unju_t2");
        assertUpdate("DROP TABLE unju_t1");
    }

    /**
     * Test INTERSECT inside JOIN.
     * Pattern: (T1 INTERSECT T2) JOIN (T3 INTERSECT T4)
     * When T1 becomes stale:
     * - T1 sees stale predicate (dt='2024-01-02')
     * - T2, T3, T4 all see ALL data (because INTERSECT needs complete data from both sides)
     * This verifies that INTERSECT is treated like JOIN, not like UNION.
     */
    @Test
    public void testIntersectJoinIntersectNesting()
    {
        assertUpdate("CREATE TABLE inji_t1 (id BIGINT, key BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE inji_t2 (id BIGINT, key BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE inji_t3 (key BIGINT, name VARCHAR, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE inji_t4 (key BIGINT, name VARCHAR, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");

        // Initial data: Create intersections in both T1/T2 and T3/T4
        assertUpdate("INSERT INTO inji_t1 VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO inji_t2 VALUES (1, 100, '2024-01-01'), (3, 300, '2024-01-01')", 2); // (1, 100) is common
        assertUpdate("INSERT INTO inji_t3 VALUES (100, 'Alice', '2024-01-01'), (200, 'Bob', '2024-01-01')", 2);
        assertUpdate("INSERT INTO inji_t4 VALUES (100, 'Alice', '2024-01-01'), (300, 'Charlie', '2024-01-01')", 2); // (100, 'Alice') is common

        // MV: (T1 INTERSECT T2) JOIN (T3 INTERSECT T4)
        // After initial data, LEFT side produces (1, 100, '2024-01-01'), RIGHT side produces (100, 'Alice', '2024-01-01')
        // JOIN produces (1, 'Alice', '2024-01-01')
        assertUpdate("CREATE MATERIALIZED VIEW mv_inji AS " +
                "SELECT u1.id, u2.name, u1.date " +
                "FROM (SELECT id, key, date FROM inji_t1 INTERSECT SELECT id, key, date FROM inji_t2) u1 " +
                "JOIN (SELECT key, name, date FROM inji_t3 INTERSECT SELECT key, name, date FROM inji_t4) u2 " +
                "ON u1.key = u2.key AND u1.date = u2.date");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_inji");

        // Verify initial state
        assertQuery("SELECT * FROM mv_inji ORDER BY id",
                "VALUES (1, 'Alice', '2024-01-01')");

        // Make T1 and T3 stale by inserting into new partition
        // New intersection on LEFT: (4, 400) appears in both T1 and T2
        // New intersection on RIGHT: (400, 'David') appears in both T3 and T4
        assertUpdate("INSERT INTO inji_t1 VALUES (4, 400, '2024-01-02')", 1);
        assertUpdate("INSERT INTO inji_t2 VALUES (4, 400, '2024-01-02')", 1);
        assertUpdate("INSERT INTO inji_t3 VALUES (400, 'David', '2024-01-02')", 1);
        assertUpdate("INSERT INTO inji_t4 VALUES (400, 'David', '2024-01-02')", 1);

        // Expected: Both old and new rows
        // With partition stitching:
        // - T1 sees stale predicate (dt='2024-01-02')
        // - T2, T3, T4 must see ALL data for INTERSECT to work correctly
        // Result should include both (1, 'Alice', '2024-01-01') from storage and (4, 'David', '2024-01-02') from recompute
        assertQuery("SELECT * FROM mv_inji ORDER BY id",
                "VALUES (1, 'Alice', '2024-01-01'), (4, 'David', '2024-01-02')");

        // Verify stitching produces same result as full recompute
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT * FROM mv_inji ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW mv_inji");
        assertUpdate("DROP TABLE inji_t4");
        assertUpdate("DROP TABLE inji_t3");
        assertUpdate("DROP TABLE inji_t2");
        assertUpdate("DROP TABLE inji_t1");
    }

    /**
     * Edge case: UNION inside JOIN inside UNION.
     * Pattern: ((T1 UNION T2) JOIN T3) UNION T4
     * - T1, T2 inside inner UNION but also inside JOIN → should see all data (JOIN barrier)
     * - T3 inside JOIN → should see all data
     * - T4 in outer UNION → should get FALSE predicate
     */
    @Test
    public void testUnionJoinUnionTripleNesting()
    {
        assertUpdate("CREATE TABLE ujut_t1 (id BIGINT, key BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE ujut_t2 (id BIGINT, key BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE ujut_t3 (key BIGINT, name VARCHAR, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE ujut_t4 (id BIGINT, name VARCHAR, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");

        // Initial data
        assertUpdate("INSERT INTO ujut_t1 VALUES (1, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO ujut_t2 VALUES (2, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO ujut_t3 VALUES (100, 'Alice', '2024-01-01')", 1);
        assertUpdate("INSERT INTO ujut_t4 VALUES (4, 'Bob', '2024-01-01')", 1);

        // MV: ((T1 UNION T2) JOIN T3) UNION T4
        assertUpdate("CREATE MATERIALIZED VIEW mv_ujut AS " +
                "SELECT j.id, j.name, j.date FROM " +
                "  (SELECT u.id, t3.name, u.date FROM " +
                "     (SELECT id, key, date FROM ujut_t1 UNION ALL SELECT id, key, date FROM ujut_t2) u " +
                "     JOIN ujut_t3 t3 ON u.key = t3.key AND u.date = t3.date) j " +
                "UNION ALL " +
                "SELECT id, name, date FROM ujut_t4");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_ujut");

        // Make T1 stale
        assertUpdate("INSERT INTO ujut_t1 VALUES (5, 200, '2024-01-02')", 1);
        assertUpdate("INSERT INTO ujut_t3 VALUES (200, 'Charlie', '2024-01-02')", 1);

        // Expected behavior:
        // - T1 gets stale predicate (date='2024-01-02')
        // - T2 sees all data (inside JOIN, even though also in inner UNION)
        // - T3 sees all data (inside JOIN with stale T1)
        // - T4 gets FALSE predicate (fresh table in outer UNION)
        assertQuery("SELECT * FROM mv_ujut ORDER BY id",
                "VALUES (1, 'Alice', '2024-01-01'), " +
                        "(2, 'Alice', '2024-01-01'), " +
                        "(4, 'Bob', '2024-01-01'), " +
                        "(5, 'Charlie', '2024-01-02')");

        assertUpdate("DROP MATERIALIZED VIEW mv_ujut");
        assertUpdate("DROP TABLE ujut_t4");
        assertUpdate("DROP TABLE ujut_t3");
        assertUpdate("DROP TABLE ujut_t2");
        assertUpdate("DROP TABLE ujut_t1");
    }

    /**
     * Edge case: Multiple JOIN layers.
     * Pattern: ((T1 JOIN T2) JOIN T3) UNION T4
     * - T1, T2, T3 all inside nested JOINs → should see all data
     * - T4 in UNION → should get FALSE predicate
     */
    @Test
    public void testNestedJoinsWithUnion()
    {
        assertUpdate("CREATE TABLE njwu_t1 (id BIGINT, key1 BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE njwu_t2 (key1 BIGINT, key2 BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE njwu_t3 (key2 BIGINT, name VARCHAR, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE njwu_t4 (id BIGINT, name VARCHAR, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");

        // Initial data
        assertUpdate("INSERT INTO njwu_t1 VALUES (1, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO njwu_t2 VALUES (100, 200, '2024-01-01')", 1);
        assertUpdate("INSERT INTO njwu_t3 VALUES (200, 'Alice', '2024-01-01')", 1);
        assertUpdate("INSERT INTO njwu_t4 VALUES (4, 'Bob', '2024-01-01')", 1);

        // MV: ((T1 JOIN T2) JOIN T3) UNION T4
        assertUpdate("CREATE MATERIALIZED VIEW mv_njwu AS " +
                "SELECT j.id, j.name, j.date FROM " +
                "  (SELECT t1.id, t3.name, t1.date FROM njwu_t1 t1 " +
                "     JOIN njwu_t2 t2 ON t1.key1 = t2.key1 AND t1.date = t2.date " +
                "     JOIN njwu_t3 t3 ON t2.key2 = t3.key2 AND t2.date = t3.date) j " +
                "UNION ALL " +
                "SELECT id, name, date FROM njwu_t4");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_njwu");

        // Make T1 stale
        assertUpdate("INSERT INTO njwu_t1 VALUES (5, 300, '2024-01-02')", 1);
        assertUpdate("INSERT INTO njwu_t2 VALUES (300, 400, '2024-01-02')", 1);
        assertUpdate("INSERT INTO njwu_t3 VALUES (400, 'Charlie', '2024-01-02')", 1);

        // Expected:
        // - T1 gets stale predicate
        // - T2, T3 see all data (in JOINs with stale table)
        // - T4 gets FALSE (fresh table in UNION)
        assertQuery("SELECT * FROM mv_njwu ORDER BY id",
                "VALUES (1, 'Alice', '2024-01-01'), " +
                        "(4, 'Bob', '2024-01-01'), " +
                        "(5, 'Charlie', '2024-01-02')");

        assertUpdate("DROP MATERIALIZED VIEW mv_njwu");
        assertUpdate("DROP TABLE njwu_t4");
        assertUpdate("DROP TABLE njwu_t3");
        assertUpdate("DROP TABLE njwu_t2");
        assertUpdate("DROP TABLE njwu_t1");
    }

    /**
     * Test deeply nested joins: (A JOIN B) JOIN (C JOIN D)
     * This pattern creates a balanced binary tree of joins where:
     * - Each leaf (A, B, C, D) is a table
     * - Two inner joins produce intermediate results
     * - A final join combines the intermediate results
     *
     * When A becomes stale:
     * - A sees stale predicate
     * - B sees all data (must join with stale data from A)
     * - C sees all data (joined in a subexpression that joins with stale side)
     * - D sees all data (joined in a subexpression that joins with stale side)
     *
     * This tests that the UNION stitching logic correctly handles nested join structures.
     */
    @Test
    public void testDeeplyNestedJoins()
    {
        // Create 4 tables: orders (A), customers (B), products (C), categories (D)
        assertUpdate("CREATE TABLE dnj_orders (order_id BIGINT, customer_id BIGINT, product_id BIGINT, order_date DATE) " +
                "WITH (partitioning = ARRAY['order_date'])");
        assertUpdate("CREATE TABLE dnj_customers (customer_id BIGINT, customer_name VARCHAR, region VARCHAR, reg_date DATE) " +
                "WITH (partitioning = ARRAY['reg_date'])");
        assertUpdate("CREATE TABLE dnj_products (product_id BIGINT, product_name VARCHAR, category_id BIGINT, product_date DATE) " +
                "WITH (partitioning = ARRAY['product_date'])");
        assertUpdate("CREATE TABLE dnj_categories (category_id BIGINT, category_name VARCHAR, cat_date DATE) " +
                "WITH (partitioning = ARRAY['cat_date'])");

        // Initial data - all tables have data for '2024-01-01'
        assertUpdate("INSERT INTO dnj_orders VALUES " +
                "(1, 100, 1000, DATE '2024-01-01'), " +
                "(2, 200, 2000, DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO dnj_customers VALUES " +
                "(100, 'Alice', 'US', DATE '2024-01-01'), " +
                "(200, 'Bob', 'EU', DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO dnj_products VALUES " +
                "(1000, 'Laptop', 10, DATE '2024-01-01'), " +
                "(2000, 'Phone', 20, DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO dnj_categories VALUES " +
                "(10, 'Electronics', DATE '2024-01-01'), " +
                "(20, 'Mobile', DATE '2024-01-01')", 2);

        // MV: (orders JOIN customers) JOIN (products JOIN categories)
        // This creates a balanced tree structure:
        //                 JOIN
        //                /    \
        //           JOIN       JOIN
        //          /    \     /    \
        //     orders  cust  prod  cats
        assertUpdate("CREATE MATERIALIZED VIEW mv_dnj AS " +
                "SELECT oc.order_id, oc.customer_name, pc.product_name, pc.category_name, oc.order_date " +
                "FROM " +
                "  (SELECT o.order_id, c.customer_name, o.product_id, o.order_date FROM dnj_orders o " +
                "   JOIN dnj_customers c ON o.customer_id = c.customer_id AND o.order_date = c.reg_date) oc " +
                "  JOIN " +
                "  (SELECT p.product_id, p.product_name, cat.category_name, p.product_date FROM dnj_products p " +
                "   JOIN dnj_categories cat ON p.category_id = cat.category_id AND p.product_date = cat.cat_date) pc " +
                "  ON oc.product_id = pc.product_id AND oc.order_date = pc.product_date");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_dnj");

        // Verify initial state - 2 rows
        assertMaterializedViewQuery("SELECT * FROM mv_dnj ORDER BY order_id",
                "VALUES (1, 'Alice', 'Laptop', 'Electronics', DATE '2024-01-01'), " +
                        "(2, 'Bob', 'Phone', 'Mobile', DATE '2024-01-01')");

        // ============================================================
        // Scenario 1: Make only 1 table stale (orders)
        // ============================================================
        // Pre-populate customers, products, categories for partition '2024-01-02' BEFORE refresh
        // so that when we insert into orders AFTER refresh, only orders is stale
        assertUpdate("INSERT INTO dnj_customers VALUES (300, 'Charlie', 'US', DATE '2024-01-02')", 1);
        assertUpdate("INSERT INTO dnj_products VALUES (3000, 'Tablet', 10, DATE '2024-01-02')", 1);
        assertUpdate("INSERT INTO dnj_categories VALUES (10, 'Electronics', DATE '2024-01-02')", 1);
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_dnj");

        // Now insert into orders only - making only orders stale for '2024-01-02'
        assertUpdate("INSERT INTO dnj_orders VALUES (3, 300, 3000, DATE '2024-01-02')", 1);

        // Verify MV is now partially materialized (stale)
        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'mv_dnj'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        // Only orders (A) is stale for partition '2024-01-02'
        // The join will use fresh data from customers, products, categories
        assertMaterializedViewQuery("SELECT * FROM mv_dnj ORDER BY order_id",
                "VALUES (1, 'Alice', 'Laptop', 'Electronics', DATE '2024-01-01'), " +
                        "(2, 'Bob', 'Phone', 'Mobile', DATE '2024-01-01'), " +
                        "(3, 'Charlie', 'Tablet', 'Electronics', DATE '2024-01-02')");

        assertMaterializedViewResultsMatch(getSession(),
                "SELECT * FROM mv_dnj ORDER BY order_id",
                true);

        // ============================================================
        // Scenario 2: Make 2 tables stale (orders and products)
        // ============================================================
        // Pre-populate customers and categories for partition '2024-01-03' BEFORE refresh
        assertUpdate("INSERT INTO dnj_customers VALUES (400, 'Diana', 'APAC', DATE '2024-01-03')", 1);
        assertUpdate("INSERT INTO dnj_categories VALUES (20, 'Wearables', DATE '2024-01-03')", 1);
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_dnj");

        // Now insert into orders and products only - making 2 tables stale
        assertUpdate("INSERT INTO dnj_orders VALUES (4, 400, 4000, DATE '2024-01-03')", 1);
        assertUpdate("INSERT INTO dnj_products VALUES (4000, 'Watch', 20, DATE '2024-01-03')", 1);

        // Verify MV is now partially materialized (stale)
        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'mv_dnj'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        // orders (A) and products (C) are stale for partition '2024-01-03'
        // customers (B) and categories (D) are fresh
        assertMaterializedViewQuery("SELECT * FROM mv_dnj ORDER BY order_id",
                "VALUES (1, 'Alice', 'Laptop', 'Electronics', DATE '2024-01-01'), " +
                        "(2, 'Bob', 'Phone', 'Mobile', DATE '2024-01-01'), " +
                        "(3, 'Charlie', 'Tablet', 'Electronics', DATE '2024-01-02'), " +
                        "(4, 'Diana', 'Watch', 'Wearables', DATE '2024-01-03')");

        assertMaterializedViewResultsMatch(getSession(),
                "SELECT * FROM mv_dnj ORDER BY order_id",
                true);

        // ============================================================
        // Scenario 3: Make 3 tables stale (orders, customers, products)
        // ============================================================
        // Pre-populate only categories for partition '2024-01-04' BEFORE refresh
        assertUpdate("INSERT INTO dnj_categories VALUES (10, 'Electronics', DATE '2024-01-04')", 1);
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_dnj");

        // Now insert into orders, customers, and products - making 3 tables stale
        assertUpdate("INSERT INTO dnj_orders VALUES (5, 500, 5000, DATE '2024-01-04')", 1);
        assertUpdate("INSERT INTO dnj_customers VALUES (500, 'Eve', 'EMEA', DATE '2024-01-04')", 1);
        assertUpdate("INSERT INTO dnj_products VALUES (5000, 'Headphones', 10, DATE '2024-01-04')", 1);

        // Verify MV is now partially materialized (stale)
        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'mv_dnj'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        // orders (A), customers (B), and products (C) are stale for partition '2024-01-04'
        // Only categories (D) is fresh
        assertMaterializedViewQuery("SELECT * FROM mv_dnj ORDER BY order_id",
                "VALUES (1, 'Alice', 'Laptop', 'Electronics', DATE '2024-01-01'), " +
                        "(2, 'Bob', 'Phone', 'Mobile', DATE '2024-01-01'), " +
                        "(3, 'Charlie', 'Tablet', 'Electronics', DATE '2024-01-02'), " +
                        "(4, 'Diana', 'Watch', 'Wearables', DATE '2024-01-03'), " +
                        "(5, 'Eve', 'Headphones', 'Electronics', DATE '2024-01-04')");

        assertMaterializedViewResultsMatch(getSession(),
                "SELECT * FROM mv_dnj ORDER BY order_id",
                true);

        // ============================================================
        // Scenario 4: Make all 4 tables stale
        // ============================================================
        // Refresh first, then insert into all 4 tables for a new partition
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_dnj");

        assertUpdate("INSERT INTO dnj_orders VALUES (6, 600, 6000, DATE '2024-01-05')", 1);
        assertUpdate("INSERT INTO dnj_customers VALUES (600, 'Frank', 'LATAM', DATE '2024-01-05')", 1);
        assertUpdate("INSERT INTO dnj_products VALUES (6000, 'Speaker', 30, DATE '2024-01-05')", 1);
        assertUpdate("INSERT INTO dnj_categories VALUES (30, 'Audio', DATE '2024-01-05')", 1);

        // Verify MV is now partially materialized (stale)
        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'mv_dnj'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        // All 4 tables are stale for partition '2024-01-05'
        assertMaterializedViewQuery("SELECT * FROM mv_dnj ORDER BY order_id",
                "VALUES (1, 'Alice', 'Laptop', 'Electronics', DATE '2024-01-01'), " +
                        "(2, 'Bob', 'Phone', 'Mobile', DATE '2024-01-01'), " +
                        "(3, 'Charlie', 'Tablet', 'Electronics', DATE '2024-01-02'), " +
                        "(4, 'Diana', 'Watch', 'Wearables', DATE '2024-01-03'), " +
                        "(5, 'Eve', 'Headphones', 'Electronics', DATE '2024-01-04'), " +
                        "(6, 'Frank', 'Speaker', 'Audio', DATE '2024-01-05')");

        assertMaterializedViewResultsMatch(getSession(),
                "SELECT * FROM mv_dnj ORDER BY order_id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW mv_dnj");
        assertUpdate("DROP TABLE dnj_categories");
        assertUpdate("DROP TABLE dnj_products");
        assertUpdate("DROP TABLE dnj_customers");
        assertUpdate("DROP TABLE dnj_orders");
    }

    /**
     * Edge case: Aggregation should also act as a barrier.
     * Pattern: (SELECT ... FROM T1 GROUP BY ...) UNION T2
     * - T1 inside aggregation → should see all data
     * - T2 in UNION → should get FALSE predicate
     */
    @Test
    public void testAggregationWithUnion()
    {
        assertUpdate("CREATE TABLE awu_t1 (id BIGINT, value BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");
        assertUpdate("CREATE TABLE awu_t2 (id BIGINT, value BIGINT, date VARCHAR) " +
                "WITH (partitioning = ARRAY['date'])");

        // Initial data
        assertUpdate("INSERT INTO awu_t1 VALUES (1, 10, '2024-01-01'), (1, 20, '2024-01-01')", 2);
        assertUpdate("INSERT INTO awu_t2 VALUES (2, 100, '2024-01-01')", 1);

        // MV: (aggregated T1) UNION T2
        assertUpdate("CREATE MATERIALIZED VIEW mv_awu AS " +
                "SELECT id, SUM(value) as total, date FROM awu_t1 GROUP BY id, date " +
                "UNION ALL " +
                "SELECT id, value as total, date FROM awu_t2");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_awu");

        // Make T1 stale by adding more rows to aggregate
        assertUpdate("INSERT INTO awu_t1 VALUES (1, 30, '2024-01-02')", 1);

        // Expected:
        // - T1 sees all data for date='2024-01-02' to compute SUM correctly
        // - T2 gets FALSE (fresh table in UNION)
        assertQuery("SELECT * FROM mv_awu ORDER BY id, date",
                "VALUES (1, 30, '2024-01-01'), " +
                        "(1, 30, '2024-01-02'), " +
                        "(2, 100, '2024-01-01')");

        assertUpdate("DROP MATERIALIZED VIEW mv_awu");
        assertUpdate("DROP TABLE awu_t2");
        assertUpdate("DROP TABLE awu_t1");
    }

    /**
     * Test (A JOIN B) EXCEPT (C JOIN D) pattern with various staleness combinations.
     * This tests that JOINs inside EXCEPT are handled correctly:
     * - When a table on the left side of EXCEPT becomes stale, all tables on the left side
     *   must get complete data for the JOIN to work, and the right side needs all data too.
     * - When a table on the right side becomes stale, similar logic applies.
     */
    @Test
    public void testJoinExceptJoinWithLeftSideStale()
    {
        // Pattern: (A JOIN B) EXCEPT (C JOIN D) where A becomes stale
        assertUpdate("CREATE TABLE jexj_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexj_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexj_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexj_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data: create rows that will be in left but NOT in right (for EXCEPT to return)
        // Left side (A JOIN B): produces (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01')
        assertUpdate("INSERT INTO jexj_a VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jexj_b VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);
        // Right side (C JOIN D): produces (1, 'x', '2024-01-01') - will be excluded
        assertUpdate("INSERT INTO jexj_c VALUES (1, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO jexj_d VALUES (100, 'x', '2024-01-01')", 1);

        // MV: (A JOIN B) EXCEPT (C JOIN D) -> should produce (2, 'y', '2024-01-01')
        assertUpdate("CREATE MATERIALIZED VIEW mv_jexj AS " +
                "SELECT a.id, b.value, a.dt " +
                "FROM jexj_a a JOIN jexj_b b ON a.key = b.key AND a.dt = b.dt " +
                "EXCEPT " +
                "SELECT c.id, d.value, c.dt " +
                "FROM jexj_c c JOIN jexj_d d ON c.key = d.key AND c.dt = d.dt");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jexj");

        // Verify initial state
        assertQuery("SELECT * FROM mv_jexj ORDER BY id", "VALUES (2, 'y', '2024-01-01')");

        // Make A stale by adding new partition
        // New left side row: (3, 'z', '2024-01-02')
        assertUpdate("INSERT INTO jexj_a VALUES (3, 300, '2024-01-02')", 1);
        assertUpdate("INSERT INTO jexj_b VALUES (300, 'z', '2024-01-02')", 1);

        // Expected:
        // - A gets stale predicate (dt='2024-01-02')
        // - B, C, D must see ALL data (inside EXCEPT, predicate propagation disabled)
        // - Result: (2, 'y', '2024-01-01') from storage + (3, 'z', '2024-01-02') from recompute
        assertQuery("SELECT * FROM mv_jexj ORDER BY id",
                "VALUES (2, 'y', '2024-01-01'), (3, 'z', '2024-01-02')");

        // Verify stitching produces same result as full recompute
        assertMaterializedViewResultsMatch(getSession(), "SELECT * FROM mv_jexj ORDER BY id", true);

        assertUpdate("DROP MATERIALIZED VIEW mv_jexj");
        assertUpdate("DROP TABLE jexj_d");
        assertUpdate("DROP TABLE jexj_c");
        assertUpdate("DROP TABLE jexj_b");
        assertUpdate("DROP TABLE jexj_a");
    }

    /**
     * Test (A JOIN B) EXCEPT (C JOIN D) where C (right side) becomes stale.
     */
    @Test
    public void testJoinExceptJoinWithRightSideStale()
    {
        assertUpdate("CREATE TABLE jexjr_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexjr_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexjr_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexjr_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data
        // Left side: produces (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01')
        assertUpdate("INSERT INTO jexjr_a VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jexjr_b VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);
        // Right side: produces (1, 'x', '2024-01-01')
        assertUpdate("INSERT INTO jexjr_c VALUES (1, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO jexjr_d VALUES (100, 'x', '2024-01-01')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW mv_jexjr AS " +
                "SELECT a.id, b.value, a.dt " +
                "FROM jexjr_a a JOIN jexjr_b b ON a.key = b.key AND a.dt = b.dt " +
                "EXCEPT " +
                "SELECT c.id, d.value, c.dt " +
                "FROM jexjr_c c JOIN jexjr_d d ON c.key = d.key AND c.dt = d.dt");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jexjr");

        // Verify initial state: (2, 'y') is not in right side, so it's in EXCEPT result
        assertQuery("SELECT * FROM mv_jexjr ORDER BY id", "VALUES (2, 'y', '2024-01-01')");

        // Make C stale by adding new partition
        // This adds (3, 'z', '2024-01-02') to right side, which should be excluded from left
        assertUpdate("INSERT INTO jexjr_c VALUES (3, 300, '2024-01-02')", 1);
        assertUpdate("INSERT INTO jexjr_d VALUES (300, 'z', '2024-01-02')", 1);
        // Also add matching data to left side
        assertUpdate("INSERT INTO jexjr_a VALUES (3, 300, '2024-01-02'), (4, 400, '2024-01-02')", 2);
        assertUpdate("INSERT INTO jexjr_b VALUES (300, 'z', '2024-01-02'), (400, 'w', '2024-01-02')", 2);

        // Expected:
        // - C gets stale predicate
        // - A, B, D must see ALL data
        // - For partition '2024-01-02':
        //   Left has: (3, 'z'), (4, 'w')
        //   Right has: (3, 'z')
        //   EXCEPT gives: (4, 'w')
        // - Final: (2, 'y', '2024-01-01') from storage + (4, 'w', '2024-01-02') from recompute
        assertQuery("SELECT * FROM mv_jexjr ORDER BY id",
                "VALUES (2, 'y', '2024-01-01'), (4, 'w', '2024-01-02')");

        assertMaterializedViewResultsMatch(getSession(), "SELECT * FROM mv_jexjr ORDER BY id", true);

        assertUpdate("DROP MATERIALIZED VIEW mv_jexjr");
        assertUpdate("DROP TABLE jexjr_d");
        assertUpdate("DROP TABLE jexjr_c");
        assertUpdate("DROP TABLE jexjr_b");
        assertUpdate("DROP TABLE jexjr_a");
    }

    /**
     * Test (A JOIN B) EXCEPT (C JOIN D) where both A and C become stale.
     * This should create two union branches (one for each stale table).
     */
    @Test
    public void testJoinExceptJoinWithBothSidesStale()
    {
        assertUpdate("CREATE TABLE jexjb_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexjb_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexjb_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexjb_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data
        assertUpdate("INSERT INTO jexjb_a VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jexjb_b VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);
        assertUpdate("INSERT INTO jexjb_c VALUES (1, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO jexjb_d VALUES (100, 'x', '2024-01-01')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW mv_jexjb AS " +
                "SELECT a.id, b.value, a.dt " +
                "FROM jexjb_a a JOIN jexjb_b b ON a.key = b.key AND a.dt = b.dt " +
                "EXCEPT " +
                "SELECT c.id, d.value, c.dt " +
                "FROM jexjb_c c JOIN jexjb_d d ON c.key = d.key AND c.dt = d.dt");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jexjb");

        assertQuery("SELECT * FROM mv_jexjb ORDER BY id", "VALUES (2, 'y', '2024-01-01')");

        // Make BOTH A and C stale by adding to new partition
        assertUpdate("INSERT INTO jexjb_a VALUES (3, 300, '2024-01-02'), (4, 400, '2024-01-02')", 2);
        assertUpdate("INSERT INTO jexjb_b VALUES (300, 'z', '2024-01-02'), (400, 'w', '2024-01-02')", 2);
        assertUpdate("INSERT INTO jexjb_c VALUES (3, 300, '2024-01-02')", 1);
        assertUpdate("INSERT INTO jexjb_d VALUES (300, 'z', '2024-01-02')", 1);

        // Expected:
        // - Creates two union branches (one for A stale, one for C stale)
        // - For partition '2024-01-02':
        //   Left: (3, 'z'), (4, 'w')
        //   Right: (3, 'z')
        //   EXCEPT: (4, 'w')
        // - Final: (2, 'y', '2024-01-01') from storage + (4, 'w', '2024-01-02')
        assertQuery("SELECT * FROM mv_jexjb ORDER BY id",
                "VALUES (2, 'y', '2024-01-01'), (4, 'w', '2024-01-02')");

        assertMaterializedViewResultsMatch(getSession(), "SELECT * FROM mv_jexjb ORDER BY id", true);

        assertUpdate("DROP MATERIALIZED VIEW mv_jexjb");
        assertUpdate("DROP TABLE jexjb_d");
        assertUpdate("DROP TABLE jexjb_c");
        assertUpdate("DROP TABLE jexjb_b");
        assertUpdate("DROP TABLE jexjb_a");
    }

    /**
     * Test (A JOIN B) EXCEPT (C JOIN D) where A and B both become stale (same side of EXCEPT).
     * This tests that tables on the same side of a JOIN within EXCEPT are handled correctly.
     */
    @Test
    public void testJoinExceptJoinWithSameSideJoinTablesStale()
    {
        assertUpdate("CREATE TABLE jexjs_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexjs_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexjs_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jexjs_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data
        assertUpdate("INSERT INTO jexjs_a VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jexjs_b VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);
        assertUpdate("INSERT INTO jexjs_c VALUES (1, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO jexjs_d VALUES (100, 'x', '2024-01-01')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW mv_jexjs AS " +
                "SELECT a.id, b.value, a.dt " +
                "FROM jexjs_a a JOIN jexjs_b b ON a.key = b.key AND a.dt = b.dt " +
                "EXCEPT " +
                "SELECT c.id, d.value, c.dt " +
                "FROM jexjs_c c JOIN jexjs_d d ON c.key = d.key AND c.dt = d.dt");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jexjs");

        assertQuery("SELECT * FROM mv_jexjs ORDER BY id", "VALUES (2, 'y', '2024-01-01')");

        // Make BOTH A and B stale (same side of EXCEPT, joined together)
        assertUpdate("INSERT INTO jexjs_a VALUES (3, 300, '2024-01-02')", 1);
        assertUpdate("INSERT INTO jexjs_b VALUES (300, 'z', '2024-01-02')", 1);

        // Expected:
        // - Creates two union branches (one for A, one for B)
        // - Both need complete data from C and D for EXCEPT
        // - For partition '2024-01-02':
        //   Left: (3, 'z')
        //   Right: nothing (C and D have no data for this partition)
        //   EXCEPT: (3, 'z')
        // - Final: (2, 'y', '2024-01-01') from storage + (3, 'z', '2024-01-02')
        assertQuery("SELECT * FROM mv_jexjs ORDER BY id",
                "VALUES (2, 'y', '2024-01-01'), (3, 'z', '2024-01-02')");

        assertMaterializedViewResultsMatch(getSession(), "SELECT * FROM mv_jexjs ORDER BY id", true);

        assertUpdate("DROP MATERIALIZED VIEW mv_jexjs");
        assertUpdate("DROP TABLE jexjs_d");
        assertUpdate("DROP TABLE jexjs_c");
        assertUpdate("DROP TABLE jexjs_b");
        assertUpdate("DROP TABLE jexjs_a");
    }

    /**
     * Test (A JOIN B) INTERSECT (C JOIN D) where A (left side of INTERSECT) becomes stale.
     * INTERSECT returns rows that exist in BOTH sides, so data must match.
     */
    @Test
    public void testJoinIntersectJoinWithLeftSideStale()
    {
        // Pattern: (A JOIN B) INTERSECT (C JOIN D) where A becomes stale
        assertUpdate("CREATE TABLE jintl_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jintl_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jintl_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jintl_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data: create matching rows on both sides for INTERSECT to return
        // Left side (A JOIN B): produces (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01')
        assertUpdate("INSERT INTO jintl_a VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jintl_b VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);
        // Right side (C JOIN D): produces (1, 'x', '2024-01-01') - will be in INTERSECT result
        assertUpdate("INSERT INTO jintl_c VALUES (1, 100, '2024-01-01')", 1);
        assertUpdate("INSERT INTO jintl_d VALUES (100, 'x', '2024-01-01')", 1);

        // MV: (A JOIN B) INTERSECT (C JOIN D) -> should produce (1, 'x', '2024-01-01')
        assertUpdate("CREATE MATERIALIZED VIEW mv_jintl AS " +
                "SELECT a.id, b.value, a.dt " +
                "FROM jintl_a a JOIN jintl_b b ON a.key = b.key AND a.dt = b.dt " +
                "INTERSECT " +
                "SELECT c.id, d.value, c.dt " +
                "FROM jintl_c c JOIN jintl_d d ON c.key = d.key AND c.dt = d.dt");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jintl");

        // Verify initial state
        assertQuery("SELECT * FROM mv_jintl ORDER BY id", "VALUES (1, 'x', '2024-01-01')");

        // Make A stale by adding new partition with matching data on both sides
        assertUpdate("INSERT INTO jintl_a VALUES (3, 300, '2024-01-02'), (4, 400, '2024-01-02')", 2);
        assertUpdate("INSERT INTO jintl_b VALUES (300, 'z', '2024-01-02'), (400, 'w', '2024-01-02')", 2);
        // Add matching row to right side - only (3, 'z') will be in INTERSECT
        assertUpdate("INSERT INTO jintl_c VALUES (3, 300, '2024-01-02')", 1);
        assertUpdate("INSERT INTO jintl_d VALUES (300, 'z', '2024-01-02')", 1);

        // Expected:
        // - A gets stale predicate (dt='2024-01-02')
        // - Inside INTERSECT, predicate propagation is disabled between set operation branches
        // - Result: (1, 'x', '2024-01-01') from storage + (3, 'z', '2024-01-02') from recompute
        assertQuery("SELECT * FROM mv_jintl ORDER BY id",
                "VALUES (1, 'x', '2024-01-01'), (3, 'z', '2024-01-02')");

        // Verify stitching produces same result as full recompute
        assertMaterializedViewResultsMatch(getSession(), "SELECT * FROM mv_jintl ORDER BY id", true);

        assertUpdate("DROP MATERIALIZED VIEW mv_jintl");
        assertUpdate("DROP TABLE jintl_d");
        assertUpdate("DROP TABLE jintl_c");
        assertUpdate("DROP TABLE jintl_b");
        assertUpdate("DROP TABLE jintl_a");
    }

    /**
     * Test (A JOIN B) INTERSECT (C JOIN D) where C (right side) becomes stale.
     */
    @Test
    public void testJoinIntersectJoinWithRightSideStale()
    {
        assertUpdate("CREATE TABLE jintr_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jintr_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jintr_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jintr_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data
        // Left side: produces (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01')
        assertUpdate("INSERT INTO jintr_a VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jintr_b VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);
        // Right side: produces (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01')
        assertUpdate("INSERT INTO jintr_c VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jintr_d VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_jintr AS " +
                "SELECT a.id, b.value, a.dt " +
                "FROM jintr_a a JOIN jintr_b b ON a.key = b.key AND a.dt = b.dt " +
                "INTERSECT " +
                "SELECT c.id, d.value, c.dt " +
                "FROM jintr_c c JOIN jintr_d d ON c.key = d.key AND c.dt = d.dt");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jintr");

        // Verify initial state: both rows match on both sides
        assertQuery("SELECT * FROM mv_jintr ORDER BY id", "VALUES (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01')");

        // Make C stale (right side of INTERSECT)
        assertUpdate("INSERT INTO jintr_c VALUES (3, 300, '2024-01-02'), (4, 400, '2024-01-02')", 2);
        assertUpdate("INSERT INTO jintr_d VALUES (300, 'z', '2024-01-02'), (400, 'w', '2024-01-02')", 2);
        // Add matching data to left side - only (3, 'z') will be in INTERSECT
        assertUpdate("INSERT INTO jintr_a VALUES (3, 300, '2024-01-02')", 1);
        assertUpdate("INSERT INTO jintr_b VALUES (300, 'z', '2024-01-02')", 1);

        // Expected:
        // - C gets stale predicate
        // - For partition '2024-01-02':
        //   Left has: (3, 'z')
        //   Right has: (3, 'z'), (4, 'w')
        //   INTERSECT gives: (3, 'z')
        // - Final: (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01') from storage + (3, 'z', '2024-01-02')
        assertQuery("SELECT * FROM mv_jintr ORDER BY id",
                "VALUES (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01'), (3, 'z', '2024-01-02')");

        assertMaterializedViewResultsMatch(getSession(), "SELECT * FROM mv_jintr ORDER BY id", true);

        assertUpdate("DROP MATERIALIZED VIEW mv_jintr");
        assertUpdate("DROP TABLE jintr_d");
        assertUpdate("DROP TABLE jintr_c");
        assertUpdate("DROP TABLE jintr_b");
        assertUpdate("DROP TABLE jintr_a");
    }

    /**
     * Test (A JOIN B) INTERSECT (C JOIN D) where both A and C become stale.
     * This should create two union branches (one for each stale table).
     */
    @Test
    public void testJoinIntersectJoinWithBothSidesStale()
    {
        assertUpdate("CREATE TABLE jintb_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jintb_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jintb_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jintb_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data with full match on both sides
        assertUpdate("INSERT INTO jintb_a VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jintb_b VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);
        assertUpdate("INSERT INTO jintb_c VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jintb_d VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_jintb AS " +
                "SELECT a.id, b.value, a.dt " +
                "FROM jintb_a a JOIN jintb_b b ON a.key = b.key AND a.dt = b.dt " +
                "INTERSECT " +
                "SELECT c.id, d.value, c.dt " +
                "FROM jintb_c c JOIN jintb_d d ON c.key = d.key AND c.dt = d.dt");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jintb");

        assertQuery("SELECT * FROM mv_jintb ORDER BY id", "VALUES (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01')");

        // Make BOTH A and C stale by adding to new partition with matching data
        assertUpdate("INSERT INTO jintb_a VALUES (3, 300, '2024-01-02'), (4, 400, '2024-01-02')", 2);
        assertUpdate("INSERT INTO jintb_b VALUES (300, 'z', '2024-01-02'), (400, 'w', '2024-01-02')", 2);
        assertUpdate("INSERT INTO jintb_c VALUES (3, 300, '2024-01-02'), (5, 500, '2024-01-02')", 2);
        assertUpdate("INSERT INTO jintb_d VALUES (300, 'z', '2024-01-02'), (500, 'v', '2024-01-02')", 2);

        // Expected:
        // - Creates two union branches (one for A stale, one for C stale)
        // - For partition '2024-01-02':
        //   Left: (3, 'z'), (4, 'w')
        //   Right: (3, 'z'), (5, 'v')
        //   INTERSECT: (3, 'z')
        // - Final: (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01') from storage + (3, 'z', '2024-01-02')
        assertQuery("SELECT * FROM mv_jintb ORDER BY id",
                "VALUES (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01'), (3, 'z', '2024-01-02')");

        assertMaterializedViewResultsMatch(getSession(), "SELECT * FROM mv_jintb ORDER BY id", true);

        assertUpdate("DROP MATERIALIZED VIEW mv_jintb");
        assertUpdate("DROP TABLE jintb_d");
        assertUpdate("DROP TABLE jintb_c");
        assertUpdate("DROP TABLE jintb_b");
        assertUpdate("DROP TABLE jintb_a");
    }

    /**
     * Test (A JOIN B) INTERSECT (C JOIN D) where A and B both become stale (same side of INTERSECT).
     * This tests that tables on the same side of a JOIN within INTERSECT are handled correctly.
     */
    @Test
    public void testJoinIntersectJoinWithSameSideJoinTablesStale()
    {
        assertUpdate("CREATE TABLE jints_a (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jints_b (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jints_c (id BIGINT, key BIGINT, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE jints_d (key BIGINT, value VARCHAR, dt VARCHAR) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data with full match
        assertUpdate("INSERT INTO jints_a VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jints_b VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);
        assertUpdate("INSERT INTO jints_c VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO jints_d VALUES (100, 'x', '2024-01-01'), (200, 'y', '2024-01-01')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_jints AS " +
                "SELECT a.id, b.value, a.dt " +
                "FROM jints_a a JOIN jints_b b ON a.key = b.key AND a.dt = b.dt " +
                "INTERSECT " +
                "SELECT c.id, d.value, c.dt " +
                "FROM jints_c c JOIN jints_d d ON c.key = d.key AND c.dt = d.dt");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_jints");

        assertQuery("SELECT * FROM mv_jints ORDER BY id", "VALUES (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01')");

        // Make BOTH A and B stale (same side of INTERSECT, joined together)
        // Also add matching data to C and D for INTERSECT to work
        assertUpdate("INSERT INTO jints_a VALUES (3, 300, '2024-01-02')", 1);
        assertUpdate("INSERT INTO jints_b VALUES (300, 'z', '2024-01-02')", 1);
        assertUpdate("INSERT INTO jints_c VALUES (3, 300, '2024-01-02')", 1);
        assertUpdate("INSERT INTO jints_d VALUES (300, 'z', '2024-01-02')", 1);

        // Expected:
        // - Creates two union branches (one for A, one for B)
        // - For partition '2024-01-02':
        //   Left: (3, 'z')
        //   Right: (3, 'z')
        //   INTERSECT: (3, 'z')
        // - Final: (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01') from storage + (3, 'z', '2024-01-02')
        assertQuery("SELECT * FROM mv_jints ORDER BY id",
                "VALUES (1, 'x', '2024-01-01'), (2, 'y', '2024-01-01'), (3, 'z', '2024-01-02')");

        assertMaterializedViewResultsMatch(getSession(), "SELECT * FROM mv_jints ORDER BY id", true);

        assertUpdate("DROP MATERIALIZED VIEW mv_jints");
        assertUpdate("DROP TABLE jints_d");
        assertUpdate("DROP TABLE jints_c");
        assertUpdate("DROP TABLE jints_b");
        assertUpdate("DROP TABLE jints_a");
    }

    @Test
    public void testExceptWithoutJoinRightSideStale()
    {
        // Create two tables with the same schema - partition column 'dt' maps through to MV
        assertUpdate("CREATE TABLE except_nojoin_left (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE except_nojoin_right (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data:
        // Left:  (1, 100), (2, 200), (3, 300) for 2024-01-01
        // Right: (2, 200) for 2024-01-01
        // EXCEPT result: (1, 100), (3, 300) - rows in left but not in right
        assertUpdate("INSERT INTO except_nojoin_left VALUES " +
                "(1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01'), (3, 300, DATE '2024-01-01')", 3);
        assertUpdate("INSERT INTO except_nojoin_right VALUES (2, 200, DATE '2024-01-01')", 1);

        // Create MV with pure EXCEPT (no JOINs)
        assertUpdate("CREATE MATERIALIZED VIEW mv_except_nojoin " +
                "WITH (partitioning = ARRAY['dt']) AS " +
                "SELECT id, value, dt FROM except_nojoin_left " +
                "EXCEPT " +
                "SELECT id, value, dt FROM except_nojoin_right");

        assertRefreshAndFullyMaterialized("mv_except_nojoin", 2);

        assertQuery("SELECT id, value FROM mv_except_nojoin ORDER BY id",
                "VALUES (1, 100), (3, 300)");

        // Make RIGHT side stale by inserting a new partition
        // This tests that predicate propagation works from right to left via PassthroughColumnEquivalences
        // Right side: add (5, 500) for 2024-01-02
        assertUpdate("INSERT INTO except_nojoin_right VALUES (5, 500, DATE '2024-01-02')", 1);

        // Also add data to left side for the same partition
        // Left: add (5, 500), (6, 600) for 2024-01-02
        // EXCEPT result for 2024-01-02: (6, 600) - since (5, 500) is in both
        assertUpdate("INSERT INTO except_nojoin_left VALUES " +
                "(5, 500, DATE '2024-01-02'), (6, 600, DATE '2024-01-02')", 2);

        // Query should produce correct results via stitching:
        // - Fresh data from storage: (1, 100), (3, 300) for 2024-01-01
        // - Recomputed stale data: (6, 600) for 2024-01-02
        // The key test is that the right-side stale predicate (dt='2024-01-02')
        // propagates to the left side via PassthroughColumnEquivalences, not EqualityInference
        assertQuery("SELECT id, value FROM mv_except_nojoin ORDER BY id",
                "VALUES (1, 100), (3, 300), (6, 600)");

        // Verify stitching produces same result as full recompute
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM mv_except_nojoin ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW mv_except_nojoin");
        assertUpdate("DROP TABLE except_nojoin_right");
        assertUpdate("DROP TABLE except_nojoin_left");
    }

    @Test
    public void testExceptWithoutJoinBothSidesStale()
    {
        assertUpdate("CREATE TABLE except_both_left (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE except_both_right (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data for 2024-01-01
        assertUpdate("INSERT INTO except_both_left VALUES " +
                "(1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO except_both_right VALUES (2, 200, DATE '2024-01-01')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW mv_except_both " +
                "WITH (partitioning = ARRAY['dt']) AS " +
                "SELECT id, value, dt FROM except_both_left " +
                "EXCEPT " +
                "SELECT id, value, dt FROM except_both_right");

        assertRefreshAndFullyMaterialized("mv_except_both", 1);
        assertQuery("SELECT id, value FROM mv_except_both ORDER BY id", "VALUES (1, 100)");

        // Make LEFT side stale: add partition 2024-01-02
        assertUpdate("INSERT INTO except_both_left VALUES (3, 300, DATE '2024-01-02')", 1);

        // Make RIGHT side stale: add partition 2024-01-03
        // Also add corresponding left data
        assertUpdate("INSERT INTO except_both_right VALUES (4, 400, DATE '2024-01-03')", 1);
        assertUpdate("INSERT INTO except_both_left VALUES " +
                "(4, 400, DATE '2024-01-03'), (5, 500, DATE '2024-01-03')", 2);

        // Expected results:
        // - 2024-01-01: (1, 100) from storage (fresh)
        // - 2024-01-02: (3, 300) from left delta (left stale, right has nothing)
        // - 2024-01-03: (5, 500) from right stale propagation
        //               (4, 400) is in both sides so excluded by EXCEPT
        assertQuery("SELECT id, value FROM mv_except_both ORDER BY id",
                "VALUES (1, 100), (3, 300), (5, 500)");

        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM mv_except_both ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW mv_except_both");
        assertUpdate("DROP TABLE except_both_right");
        assertUpdate("DROP TABLE except_both_left");
    }

    @Test
    public void testExceptBothSidesStaleSamePartition()
    {
        assertUpdate("CREATE TABLE except_same_left (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");
        assertUpdate("CREATE TABLE except_same_right (id INTEGER, value INTEGER, dt DATE) " +
                "WITH (partitioning = ARRAY['dt'])");

        // Initial data for dt='2024-01-01'
        // Left: (1, 100), (2, 200)
        // Right: (3, 300)  -- doesn't overlap with Left
        // EXCEPT result: (1, 100), (2, 200)
        assertUpdate("INSERT INTO except_same_left VALUES " +
                "(1, 100, DATE '2024-01-01'), (2, 200, DATE '2024-01-01')", 2);
        assertUpdate("INSERT INTO except_same_right VALUES (3, 300, DATE '2024-01-01')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW mv_except_same " +
                "WITH (partitioning = ARRAY['dt']) AS " +
                "SELECT id, value, dt FROM except_same_left " +
                "EXCEPT " +
                "SELECT id, value, dt FROM except_same_right");

        assertRefreshAndFullyMaterialized("mv_except_same", 2);
        assertQuery("SELECT id, value FROM mv_except_same ORDER BY id", "VALUES (1, 100), (2, 200)");

        // Now add data to BOTH L and R in the SAME partition dt='2024-01-01':
        // - L gets new row (10, 1000)
        // - R gets (2, 200) which should SUBTRACT the existing L row (2, 200)
        //
        // After inserts:
        // Left: (1, 100), (2, 200), (10, 1000)
        // Right: (3, 300), (2, 200)
        // EXCEPT result: (1, 100), (10, 1000)  -- (2, 200) is now in R
        assertUpdate("INSERT INTO except_same_left VALUES (10, 1000, DATE '2024-01-01')", 1);
        assertUpdate("INSERT INTO except_same_right VALUES (2, 200, DATE '2024-01-01')", 1);

        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'mv_except_same'",
                "SELECT 'PARTIALLY_MATERIALIZED'");

        // Expected: (1, 100), (10, 1000)
        //
        // How the differential rewrite computes this correctly:
        // - L's stale partition: dt='2024-01-01', R's stale partition: dt='2024-01-01' (same!)
        // - deltaLeft: ∆L - R' = {(1,100),(2,200),(10,1000)} - {(3,300),(2,200)} = {(1,100),(10,1000)}
        //     Note: ∆L includes ALL rows from stale partitions (not just new rows)
        // - deltaRight: L[unchanged] filtered to R's stale - R'
        //     L[unchanged] excludes dt='2024-01-01' (since L is stale there) = {}
        //     This is CORRECT - using unchanged prevents duplicates when L and R share stale partitions
        // - deltaResult = {(1,100),(10,1000)} ∪ {} = {(1,100),(10,1000)}
        // - freshPlan = {} (MV storage has no rows outside stale partitions)
        // - Final: {} ∪ {(1,100),(10,1000)} = {(1,100),(10,1000)} ✓
        Session useStitchingSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                .build();
        assertQuery(useStitchingSession,
                "SELECT id, value FROM mv_except_same ORDER BY id",
                "VALUES (1, 100), (10, 1000)");

        assertMaterializedViewResultsMatch(getSession(),
                "SELECT id, value FROM mv_except_same ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW mv_except_same");
        assertUpdate("DROP TABLE except_same_right");
        assertUpdate("DROP TABLE except_same_left");
    }

    @Test
    public void testExceptJoinTableScan()
    {
        // Pattern: (A EXCEPT B) JOIN C
        // Tests that EXCEPT's unchanged variant correctly accounts for anti-monotonicity
        // in the right input when used as the left child of a JOIN.
        assertUpdate("CREATE TABLE exjt_a (id BIGINT, key BIGINT) " +
                "WITH (partitioning = ARRAY['id', 'key'])");
        assertUpdate("CREATE TABLE exjt_b (id BIGINT, key BIGINT) " +
                "WITH (partitioning = ARRAY['id', 'key'])");
        assertUpdate("CREATE TABLE exjt_c (key BIGINT, value VARCHAR) " +
                "WITH (partitioning = ARRAY['key', 'value'])");

        // Left side (A EXCEPT B): produces (1, 100), (3, 300)
        assertUpdate("INSERT INTO exjt_a VALUES (1, 100), (2, 200), (3, 300), (4, 400)", 4);
        assertUpdate("INSERT INTO exjt_b VALUES (2, 200), (4, 400), (5, 500)", 3);
        // Right side C: (300, 'z')
        assertUpdate("INSERT INTO exjt_c VALUES (300, 'z')", 1);

        // MV: (A EXCEPT B) JOIN C -> should produce (3, 'z')
        assertUpdate("CREATE MATERIALIZED VIEW mv_exjt AS " +
                "WITH nt(id, key) AS (SELECT * FROM exjt_a EXCEPT SELECT * FROM exjt_b) " +
                "SELECT a.id, b.value " +
                "FROM nt a JOIN exjt_c b ON a.key = b.key");
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW mv_exjt");

        // Verify initial state
        assertQuery("SELECT * FROM mv_exjt ORDER BY id", "VALUES (3, 'z')");

        // Now make changes:
        // A gains (5, 500), (6, 600), (7, 700)
        // B gains (1, 100), (6, 600) -- note: (1, 100) was in A EXCEPT B before, now removed
        // C gains (100, 'x'), (700, 'h')
        assertUpdate("INSERT INTO exjt_a VALUES (5, 500), (6, 600), (7, 700)", 3);
        assertUpdate("INSERT INTO exjt_b VALUES (1, 100), (6, 600)", 2);
        assertUpdate("INSERT INTO exjt_c VALUES (100, 'x'), (700, 'h')", 2);

        // After updates:
        // A EXCEPT B = {(3, 300), (7, 700)}
        //   - (1, 100) removed because B now has it
        //   - (5, 500) in both A and B
        //   - (6, 600) in both A and B
        //   - (7, 700) only in A
        // (A EXCEPT B) JOIN C = {(3, 'z'), (7, 'h')}
        //   - (3, 300) joins C's (300, 'z')
        //   - (7, 700) joins C's (700, 'h')
        //   - (1, 100) should NOT appear since it's no longer in A EXCEPT B
        assertQuery("SELECT * FROM mv_exjt ORDER BY id", "VALUES (3, 'z'), (7, 'h')");
        assertMaterializedViewResultsMatch(getSession(),
                "SELECT * FROM mv_exjt ORDER BY id",
                true);

        assertUpdate("DROP MATERIALIZED VIEW mv_exjt");
        assertUpdate("DROP TABLE exjt_c");
        assertUpdate("DROP TABLE exjt_b");
        assertUpdate("DROP TABLE exjt_a");
    }

    private void assertMaterializedViewQuery(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertQuery(actual, expected);
        assertMaterializedViewResultsMatch(actual);
    }

    private void assertRefreshAndFullyMaterialized(String viewName, long expectedRows)
    {
        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, expectedRows);
        assertQuery(
                "SELECT freshness_state FROM information_schema.materialized_views " +
                        "WHERE table_schema = 'test_schema' AND table_name = '" + viewName + "'",
                "SELECT 'FULLY_MATERIALIZED'");
    }

    private void assertMaterializedViewResultsMatch(String query)
    {
        assertMaterializedViewResultsMatch(query, true);
    }

    private void assertMaterializedViewResultsMatch(Session session, String query)
    {
        assertMaterializedViewResultsMatch(session, query, true);
    }

    private void assertMaterializedViewResultsMatch(String query, boolean assertOrdered)
    {
        // Query with USE_STITCHING mode (default - uses storage + stitching)
        Session withStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                .build();

        // Query with USE_VIEW_QUERY mode (forces recompute from base tables)
        Session skipStorageSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("materialized_view_force_stale", "true")
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                .build();

        // Verify that both approaches produce identical results
        if (assertOrdered) {
            assertQueryWithSameQueryRunner(skipStorageSession, query, withStorageSession, query);
        }
        else {
            // For unordered comparison, materialize both results and compare as sets
            MaterializedResult withStorageResult = computeActual(withStorageSession, query);
            MaterializedResult skipStorageResult = computeActual(skipStorageSession, query);
            assertEqualsIgnoreOrder(skipStorageResult.getMaterializedRows(), withStorageResult.getMaterializedRows());
        }
    }

    private void assertMaterializedViewResultsMatch(Session session, String query, boolean assertOrdered)
    {
        // Query with USE_STITCHING mode (default - uses storage + stitching)
        Session withStorageSession = Session.builder(session)
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                .build();

        // Query with USE_VIEW_QUERY mode (forces recompute from base tables)
        Session skipStorageSession = Session.builder(session)
                .setSystemProperty("materialized_view_force_stale", "true")
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_VIEW_QUERY")
                .build();

        // Verify that both approaches produce identical results
        if (assertOrdered) {
            assertQueryWithSameQueryRunner(skipStorageSession, query, withStorageSession, query);
        }
        else {
            // For unordered comparison, materialize both results and compare as sets
            MaterializedResult withStorageResult = computeActual(withStorageSession, query);
            MaterializedResult skipStorageResult = computeActual(skipStorageSession, query);
            assertEqualsIgnoreOrder(skipStorageResult.getMaterializedRows(), withStorageResult.getMaterializedRows());
        }
    }

    @Test
    public void testSecurityInvokerWithRowFilterBlocksStitching()
    {
        assertUpdate("CREATE TABLE mv_security_base (id BIGINT, value BIGINT, ds VARCHAR) WITH (partitioning = ARRAY['ds'])");
        assertUpdate("INSERT INTO mv_security_base VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-01')", 2);
        assertUpdate("INSERT INTO mv_security_base VALUES (3, 300, '2024-01-02'), (4, 400, '2024-01-02')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_security_test SECURITY INVOKER " +
                "WITH (partitioning = ARRAY['ds']) AS SELECT id, value, ds FROM mv_security_base");
        assertUpdate("REFRESH MATERIALIZED VIEW mv_security_test", 4);

        // Insert new data to make MV partially stale
        assertUpdate("INSERT INTO mv_security_base VALUES (5, 500, '2024-01-03'), (6, 600, '2024-01-03')", 2);

        try {
            // Add row filter: restricted_user cannot see ds='2024-01-01'
            getQueryRunner().getAccessControl().rowFilter(
                    new QualifiedObjectName("iceberg", "test_schema", "mv_security_base"),
                    "restricted_user",
                    new ViewExpression("restricted_user", Optional.of("iceberg"), Optional.of("test_schema"), "ds <> '2024-01-01'"));

            Session restrictedStitchingSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setIdentity(new Identity("restricted_user", Optional.empty()))
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                    .build();

            // With the security fix:
            // - Stitching is blocked because row filter exists on base table with INVOKER security
            // - Falls back to view query which applies the row filter
            // - restricted_user should NOT see ds='2024-01-01' rows
            // Without the fix, stitching would bypass row filters for fresh partitions
            assertQuery(restrictedStitchingSession,
                    "SELECT id, value, ds FROM mv_security_test ORDER BY id",
                    "VALUES (3, 300, '2024-01-02'), (4, 400, '2024-01-02'), (5, 500, '2024-01-03'), (6, 600, '2024-01-03')");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
            assertUpdate("DROP MATERIALIZED VIEW mv_security_test");
            assertUpdate("DROP TABLE mv_security_base");
        }
    }
}
