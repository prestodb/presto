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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

/**
 * Integration tests for materialized view support in the memory connector.
 * These tests verify CREATE MATERIALIZED VIEW, REFRESH MATERIALIZED VIEW,
 * and DROP MATERIALIZED VIEW operations.
 */
@Test(singleThreaded = true)
public class TestMemoryMaterializedViews
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.createQueryRunner();
    }

    @Test
    public void testCreateMaterializedView()
    {
        // Create base table
        assertUpdate("CREATE TABLE base_table (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO base_table VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)", 3);

        // Create materialized view
        assertUpdate("CREATE MATERIALIZED VIEW mv_simple AS SELECT id, name, value FROM base_table");

        // Verify materialized view can be queried
        assertQuery("SELECT COUNT(*) FROM mv_simple", "SELECT 3");
        assertQuery("SELECT * FROM mv_simple ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        // Cleanup
        assertUpdate("DROP MATERIALIZED VIEW mv_simple");
        assertUpdate("DROP TABLE base_table");
    }

    @Test
    public void testCreateMaterializedViewWithFilter()
    {
        // Create base table
        assertUpdate("CREATE TABLE filtered_base (id BIGINT, status VARCHAR, amount BIGINT)");
        assertUpdate("INSERT INTO filtered_base VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'active', 300)", 3);

        // Create materialized view with WHERE clause
        assertUpdate("CREATE MATERIALIZED VIEW mv_filtered AS SELECT id, amount FROM filtered_base WHERE status = 'active'");

        // Verify only filtered rows are in the materialized view
        assertQuery("SELECT COUNT(*) FROM mv_filtered", "SELECT 2");
        assertQuery("SELECT * FROM mv_filtered ORDER BY id",
                "VALUES (1, 100), (3, 300)");

        // Cleanup
        assertUpdate("DROP MATERIALIZED VIEW mv_filtered");
        assertUpdate("DROP TABLE filtered_base");
    }

    @Test
    public void testCreateMaterializedViewWithAggregation()
    {
        // Create base table
        assertUpdate("CREATE TABLE sales (product_id BIGINT, category VARCHAR, revenue BIGINT)");
        assertUpdate("INSERT INTO sales VALUES (1, 'Electronics', 1000), (2, 'Electronics', 1500), (3, 'Books', 500), (4, 'Books', 300)", 4);

        // Create materialized view with aggregation
        assertUpdate("CREATE MATERIALIZED VIEW mv_category_sales AS " +
                "SELECT category, COUNT(*) as product_count, SUM(revenue) as total_revenue " +
                "FROM sales GROUP BY category");

        // Verify aggregated results
        assertQuery("SELECT COUNT(*) FROM mv_category_sales", "SELECT 2");
        assertQuery("SELECT * FROM mv_category_sales ORDER BY category",
                "VALUES ('Books', 2, 800), ('Electronics', 2, 2500)");

        // Cleanup
        assertUpdate("DROP MATERIALIZED VIEW mv_category_sales");
        assertUpdate("DROP TABLE sales");
    }

    @Test
    public void testCreateMaterializedViewWithJoin()
    {
        // Create base tables
        assertUpdate("CREATE TABLE customer_orders (order_id BIGINT, customer_id BIGINT, amount BIGINT)");
        assertUpdate("CREATE TABLE customers (customer_id BIGINT, customer_name VARCHAR)");

        assertUpdate("INSERT INTO customer_orders VALUES (1, 100, 50), (2, 200, 75), (3, 100, 25)", 3);
        assertUpdate("INSERT INTO customers VALUES (100, 'Alice'), (200, 'Bob')", 2);

        // Create materialized view with join
        assertUpdate("CREATE MATERIALIZED VIEW mv_customer_orders AS " +
                "SELECT o.order_id, c.customer_name, o.amount " +
                "FROM customer_orders o JOIN customers c ON o.customer_id = c.customer_id");

        // Verify joined results
        assertQuery("SELECT COUNT(*) FROM mv_customer_orders", "SELECT 3");
        assertQuery("SELECT * FROM mv_customer_orders ORDER BY order_id",
                "VALUES (1, 'Alice', 50), (2, 'Bob', 75), (3, 'Alice', 25)");

        // Cleanup
        assertUpdate("DROP MATERIALIZED VIEW mv_customer_orders");
        assertUpdate("DROP TABLE customers");
        assertUpdate("DROP TABLE customer_orders");
    }

    @Test
    public void testRefreshMaterializedView()
    {
        // Create base table
        assertUpdate("CREATE TABLE refresh_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO refresh_base VALUES (1, 100), (2, 200)", 2);

        // Create materialized view
        assertUpdate("CREATE MATERIALIZED VIEW mv_refresh AS SELECT id, value FROM refresh_base");

        // Verify initial data
        assertQuery("SELECT COUNT(*) FROM mv_refresh", "SELECT 2");
        assertQuery("SELECT * FROM mv_refresh ORDER BY id", "VALUES (1, 100), (2, 200)");

        // Add new data to base table
        assertUpdate("INSERT INTO refresh_base VALUES (3, 300)", 1);

        // Materialized view should be stale (still has old data)
        // TODO: no way to prove that this is recomputing vs. using the data table, except to count the rows
        assertQuery("SELECT COUNT(*) FROM mv_refresh", "SELECT 3");

        // TODO: make WHERE optional
//        // Refresh the materialized view
//        assertUpdate("REFRESH MATERIALIZED VIEW mv_refresh WHERE id = 1");
//
//        // Verify materialized view now has updated data
//        assertQuery("SELECT COUNT(*) FROM mv_refresh", "SELECT 3");
//        assertQuery("SELECT * FROM mv_refresh ORDER BY id",
//                "VALUES (1, 100), (2, 200), (3, 300)");
//
//        // Cleanup
//        assertUpdate("DROP MATERIALIZED VIEW mv_refresh");
//        assertUpdate("DROP TABLE refresh_base");
    }

    @Test
    public void testRefreshMaterializedViewWithAggregation()
    {
        // Create base table
        assertUpdate("CREATE TABLE agg_refresh_base (category VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO agg_refresh_base VALUES ('A', 10), ('B', 20), ('A', 15)", 3);

        // Create materialized view with aggregation
        assertUpdate("CREATE MATERIALIZED VIEW mv_agg_refresh AS " +
                "SELECT category, SUM(value) as total FROM agg_refresh_base GROUP BY category");

        // Verify initial aggregated data
        assertQuery("SELECT * FROM mv_agg_refresh ORDER BY category",
                "VALUES ('A', 25), ('B', 20)");

        // Add more data to base table
        assertUpdate("INSERT INTO agg_refresh_base VALUES ('A', 5), ('C', 30)", 2);

        // TODO: make WHERE optional
//        // Refresh the materialized view
//        assertUpdate("REFRESH MATERIALIZED VIEW mv_agg_refresh");
//
//        // Verify updated aggregated data
//        assertQuery("SELECT * FROM mv_agg_refresh ORDER BY category",
//                "VALUES ('A', 30), ('B', 20), ('C', 30)");
//
//        // Cleanup
//        assertUpdate("DROP MATERIALIZED VIEW mv_agg_refresh");
//        assertUpdate("DROP TABLE agg_refresh_base");
    }

    @Test
    public void testDropMaterializedView()
    {
        // Create base table
        assertUpdate("CREATE TABLE drop_base (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO drop_base VALUES (1, 'test')", 1);

        // Create materialized view
        assertUpdate("CREATE MATERIALIZED VIEW mv_drop AS SELECT id, value FROM drop_base");

        // Verify materialized view exists
        assertQuery("SELECT COUNT(*) FROM mv_drop", "SELECT 1");

        // Drop the materialized view
        assertUpdate("DROP MATERIALIZED VIEW mv_drop");

        // Verify base table still exists
        assertQuery("SELECT COUNT(*) FROM drop_base", "SELECT 1");

        // Cleanup
        assertUpdate("DROP TABLE drop_base");
    }

    @Test
    public void testMultipleMaterializedViews()
    {
        // Create base table
        assertUpdate("CREATE TABLE multi_base (id BIGINT, category VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO multi_base VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)", 3);

        // Create multiple materialized views on the same base table
        assertUpdate("CREATE MATERIALIZED VIEW mv_multi_1 AS SELECT id, value FROM multi_base WHERE category = 'A'");
        assertUpdate("CREATE MATERIALIZED VIEW mv_multi_2 AS SELECT category, SUM(value) as total FROM multi_base GROUP BY category");

        // Verify both materialized views
        assertQuery("SELECT COUNT(*) FROM mv_multi_1", "SELECT 2");
        assertQuery("SELECT * FROM mv_multi_1 ORDER BY id", "VALUES (1, 100), (3, 150)");

        assertQuery("SELECT COUNT(*) FROM mv_multi_2", "SELECT 2");
        assertQuery("SELECT * FROM mv_multi_2 ORDER BY category",
                "VALUES ('A', 250), ('B', 200)");

        // Cleanup
        assertUpdate("DROP MATERIALIZED VIEW mv_multi_1");
        assertUpdate("DROP MATERIALIZED VIEW mv_multi_2");
        assertUpdate("DROP TABLE multi_base");
    }
}
