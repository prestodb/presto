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

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.LEGACY_MATERIALIZED_VIEWS;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

@Test(singleThreaded = true)
public class TestMemoryMaterializedViews
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .setSystemProperty(LEGACY_MATERIALIZED_VIEWS, "false")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(4)
                .build();

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", ImmutableMap.of());

        return queryRunner;
    }

    @Test
    public void testCreateMaterializedView()
    {
        assertUpdate("CREATE TABLE base_table (id BIGINT, name VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO base_table VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW mv_simple AS SELECT id, name, value FROM base_table");

        assertQuery("SELECT COUNT(*) FROM mv_simple", "SELECT 3");
        assertQuery("SELECT * FROM mv_simple ORDER BY id",
                "VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)");

        assertUpdate("DROP MATERIALIZED VIEW mv_simple");
        assertUpdate("DROP TABLE base_table");
    }

    @Test
    public void testCreateMaterializedViewDuplicateName()
    {
        assertUpdate("CREATE TABLE dup_base (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO dup_base VALUES (1, 'test')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW mv_dup AS SELECT id, value FROM dup_base");

        assertQueryFails("CREATE MATERIALIZED VIEW mv_dup AS SELECT id FROM dup_base",
                ".*Materialized view .* already exists.*");

        assertUpdate("DROP MATERIALIZED VIEW mv_dup");
        assertUpdate("DROP TABLE dup_base");
    }

    @Test
    public void testCreateMaterializedViewWithFilter()
    {
        assertUpdate("CREATE TABLE filtered_base (id BIGINT, status VARCHAR, amount BIGINT)");
        assertUpdate("INSERT INTO filtered_base VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'active', 300)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW mv_filtered AS SELECT id, amount FROM filtered_base WHERE status = 'active'");

        assertQuery("SELECT COUNT(*) FROM mv_filtered", "SELECT 2");
        assertQuery("SELECT * FROM mv_filtered ORDER BY id",
                "VALUES (1, 100), (3, 300)");

        assertUpdate("DROP MATERIALIZED VIEW mv_filtered");
        assertUpdate("DROP TABLE filtered_base");
    }

    @Test
    public void testCreateMaterializedViewWithComplexFilter()
    {
        assertUpdate("CREATE TABLE complex_filter_base (id BIGINT, status VARCHAR, amount BIGINT, priority INTEGER)");
        assertUpdate("INSERT INTO complex_filter_base VALUES (1, 'active', 100, 1), (2, 'inactive', 200, 2), (3, 'active', 50, 3), (4, 'active', 150, 1)", 4);

        assertUpdate("CREATE MATERIALIZED VIEW mv_complex_filter AS " +
                "SELECT id, amount, priority FROM complex_filter_base " +
                "WHERE status = 'active' AND amount > 75 AND priority = 1");

        assertQuery("SELECT COUNT(*) FROM mv_complex_filter", "SELECT 2");
        assertQuery("SELECT * FROM mv_complex_filter ORDER BY id",
                "VALUES (1, 100, 1), (4, 150, 1)");

        assertUpdate("DROP MATERIALIZED VIEW mv_complex_filter");
        assertUpdate("DROP TABLE complex_filter_base");
    }

    @Test
    public void testCreateMaterializedViewWithAggregation()
    {
        assertUpdate("CREATE TABLE sales (product_id BIGINT, category VARCHAR, revenue BIGINT)");
        assertUpdate("INSERT INTO sales VALUES (1, 'Electronics', 1000), (2, 'Electronics', 1500), (3, 'Books', 500), (4, 'Books', 300)", 4);

        assertUpdate("CREATE MATERIALIZED VIEW mv_category_sales AS " +
                "SELECT category, COUNT(*) as product_count, SUM(revenue) as total_revenue " +
                "FROM sales GROUP BY category");

        assertQuery("SELECT COUNT(*) FROM mv_category_sales", "SELECT 2");
        assertQuery("SELECT * FROM mv_category_sales ORDER BY category",
                "VALUES ('Books', 2, 800), ('Electronics', 2, 2500)");

        assertUpdate("DROP MATERIALIZED VIEW mv_category_sales");
        assertUpdate("DROP TABLE sales");
    }

    @Test
    public void testCreateMaterializedViewWithComputedColumns()
    {
        assertUpdate("CREATE TABLE transactions (trans_id BIGINT, amount BIGINT, tax_rate DOUBLE)");
        assertUpdate("INSERT INTO transactions VALUES (1, 100, 0.08), (2, 200, 0.08), (3, 150, 0.10)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW mv_computed AS " +
                "SELECT trans_id, amount, tax_rate, " +
                "CAST(amount * tax_rate AS BIGINT) as tax_amount, " +
                "CAST(amount * (1 + tax_rate) AS BIGINT) as total_amount " +
                "FROM transactions");

        assertQuery("SELECT COUNT(*) FROM mv_computed", "SELECT 3");
        assertQuery("SELECT trans_id, amount, tax_amount, total_amount FROM mv_computed ORDER BY trans_id",
                "VALUES (1, 100, 8, 108), (2, 200, 16, 216), (3, 150, 15, 165)");

        assertUpdate("DROP MATERIALIZED VIEW mv_computed");
        assertUpdate("DROP TABLE transactions");
    }

    @Test
    public void testCreateMaterializedViewWithJoin()
    {
        assertUpdate("CREATE TABLE customer_orders (order_id BIGINT, customer_id BIGINT, amount BIGINT)");
        assertUpdate("CREATE TABLE customers (customer_id BIGINT, customer_name VARCHAR)");

        assertUpdate("INSERT INTO customer_orders VALUES (1, 100, 50), (2, 200, 75), (3, 100, 25)", 3);
        assertUpdate("INSERT INTO customers VALUES (100, 'Alice'), (200, 'Bob')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_customer_orders AS " +
                "SELECT o.order_id, c.customer_name, o.amount " +
                "FROM customer_orders o JOIN customers c ON o.customer_id = c.customer_id");

        assertQuery("SELECT COUNT(*) FROM mv_customer_orders", "SELECT 3");
        assertQuery("SELECT * FROM mv_customer_orders ORDER BY order_id",
                "VALUES (1, 'Alice', 50), (2, 'Bob', 75), (3, 'Alice', 25)");

        assertUpdate("DROP MATERIALIZED VIEW mv_customer_orders");
        assertUpdate("DROP TABLE customers");
        assertUpdate("DROP TABLE customer_orders");
    }

    @Test
    public void testRefreshMaterializedView()
    {
        assertUpdate("CREATE TABLE refresh_base (id BIGINT, value BIGINT)");
        assertUpdate("INSERT INTO refresh_base VALUES (1, 100), (2, 200)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_refresh AS SELECT id, value FROM refresh_base");

        assertQuery("SELECT COUNT(*) FROM mv_refresh", "SELECT 2");
        assertQuery("SELECT * FROM mv_refresh ORDER BY id", "VALUES (1, 100), (2, 200)");

        assertUpdate("INSERT INTO refresh_base VALUES (3, 300)", 1);

        assertQuery("SELECT COUNT(*) FROM mv_refresh", "SELECT 3");

        assertUpdate("REFRESH MATERIALIZED VIEW mv_refresh", 3);

        assertQuery("SELECT COUNT(*) FROM mv_refresh", "SELECT 3");
        assertQuery("SELECT * FROM mv_refresh ORDER BY id",
                "VALUES (1, 100), (2, 200), (3, 300)");

        assertUpdate("DROP MATERIALIZED VIEW mv_refresh");
        assertUpdate("DROP TABLE refresh_base");
    }

    @Test
    public void testRefreshMaterializedViewWithAggregation()
    {
        assertUpdate("CREATE TABLE agg_refresh_base (category VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO agg_refresh_base VALUES ('A', 10), ('B', 20), ('A', 15)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW mv_agg_refresh AS " +
                "SELECT category, SUM(value) as total FROM agg_refresh_base GROUP BY category");

        assertQuery("SELECT * FROM mv_agg_refresh ORDER BY category",
                "VALUES ('A', 25), ('B', 20)");

        assertUpdate("INSERT INTO agg_refresh_base VALUES ('A', 5), ('C', 30)", 2);

        assertUpdate("REFRESH MATERIALIZED VIEW mv_agg_refresh", 3);

        assertQuery("SELECT * FROM mv_agg_refresh ORDER BY category",
                "VALUES ('A', 30), ('B', 20), ('C', 30)");

        assertUpdate("DROP MATERIALIZED VIEW mv_agg_refresh");
        assertUpdate("DROP TABLE agg_refresh_base");
    }

    @Test
    public void testRefreshNonExistentMaterializedView()
    {
        assertQueryFails("REFRESH MATERIALIZED VIEW mv_nonexistent",
                ".*Materialized view .* does not exist.*");
    }

    @Test
    public void testDropMaterializedView()
    {
        assertUpdate("CREATE TABLE drop_base (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO drop_base VALUES (1, 'test')", 1);

        assertUpdate("CREATE MATERIALIZED VIEW mv_drop AS SELECT id, value FROM drop_base");

        assertQuery("SELECT COUNT(*) FROM mv_drop", "SELECT 1");

        assertUpdate("DROP MATERIALIZED VIEW mv_drop");

        assertQuery("SELECT COUNT(*) FROM drop_base", "SELECT 1");

        assertUpdate("DROP TABLE drop_base");
    }

    @Test
    public void testDropNonExistentMaterializedView()
    {
        assertQueryFails("DROP MATERIALIZED VIEW mv_nonexistent",
                ".*Materialized view .* does not exist.*");
    }

    @Test
    public void testCreateMaterializedViewWithEmptyBaseTable()
    {
        assertUpdate("CREATE TABLE empty_base (id BIGINT, value VARCHAR)");

        assertUpdate("CREATE MATERIALIZED VIEW mv_empty AS SELECT id, value FROM empty_base");

        assertQuery("SELECT COUNT(*) FROM mv_empty", "SELECT 0");

        assertUpdate("DROP MATERIALIZED VIEW mv_empty");
        assertUpdate("DROP TABLE empty_base");
    }

    @Test
    public void testMultipleMaterializedViews()
    {
        assertUpdate("CREATE TABLE multi_base (id BIGINT, category VARCHAR, value BIGINT)");
        assertUpdate("INSERT INTO multi_base VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)", 3);

        assertUpdate("CREATE MATERIALIZED VIEW mv_multi_1 AS SELECT id, value FROM multi_base WHERE category = 'A'");
        assertUpdate("CREATE MATERIALIZED VIEW mv_multi_2 AS SELECT category, SUM(value) as total FROM multi_base GROUP BY category");

        assertQuery("SELECT COUNT(*) FROM mv_multi_1", "SELECT 2");
        assertQuery("SELECT * FROM mv_multi_1 ORDER BY id", "VALUES (1, 100), (3, 150)");

        assertQuery("SELECT COUNT(*) FROM mv_multi_2", "SELECT 2");
        assertQuery("SELECT * FROM mv_multi_2 ORDER BY category",
                "VALUES ('A', 250), ('B', 200)");

        assertUpdate("DROP MATERIALIZED VIEW mv_multi_1");
        assertUpdate("DROP MATERIALIZED VIEW mv_multi_2");
        assertUpdate("DROP TABLE multi_base");
    }

    @Test
    public void testCreateMaterializedViewWithMultiTableJoin()
    {
        assertUpdate("CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, product_id BIGINT, quantity BIGINT)");
        assertUpdate("CREATE TABLE customers (customer_id BIGINT, customer_name VARCHAR, region VARCHAR)");
        assertUpdate("CREATE TABLE products (product_id BIGINT, product_name VARCHAR, unit_price BIGINT)");

        assertUpdate("INSERT INTO orders VALUES (1, 100, 1, 2), (2, 200, 2, 1), (3, 100, 2, 3)", 3);
        assertUpdate("INSERT INTO customers VALUES (100, 'Alice', 'East'), (200, 'Bob', 'West')", 2);
        assertUpdate("INSERT INTO products VALUES (1, 'Widget', 50), (2, 'Gadget', 75)", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_order_details AS " +
                "SELECT o.order_id, c.customer_name, c.region, p.product_name, o.quantity, " +
                "CAST(p.unit_price * o.quantity AS BIGINT) as total_price " +
                "FROM orders o " +
                "JOIN customers c ON o.customer_id = c.customer_id " +
                "JOIN products p ON o.product_id = p.product_id");

        assertQuery("SELECT COUNT(*) FROM mv_order_details", "SELECT 3");
        assertQuery("SELECT order_id, customer_name, product_name, total_price FROM mv_order_details ORDER BY order_id",
                "VALUES (1, 'Alice', 'Widget', 100), (2, 'Bob', 'Gadget', 75), (3, 'Alice', 'Gadget', 225)");

        assertUpdate("DROP MATERIALIZED VIEW mv_order_details");
        assertUpdate("DROP TABLE products");
        assertUpdate("DROP TABLE customers");
        assertUpdate("DROP TABLE orders");
    }

    @Test
    public void testRefreshMaterializedViewAfterBaseTableDropped()
    {
        assertUpdate("CREATE TABLE temp_base (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO temp_base VALUES (1, 'test'), (2, 'data')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_temp AS SELECT id, value FROM temp_base");

        assertQuery("SELECT COUNT(*) FROM mv_temp", "SELECT 2");

        assertUpdate("DROP TABLE temp_base");

        assertQueryFails("REFRESH MATERIALIZED VIEW mv_temp",
                ".*Table .* does not exist.*");

        assertUpdate("DROP MATERIALIZED VIEW mv_temp");
    }

    @Test
    public void testMaterializedViewBecomesUnqueryableAfterBaseTableDropped()
    {
        assertUpdate("CREATE TABLE persist_base (id BIGINT, value VARCHAR)");
        assertUpdate("INSERT INTO persist_base VALUES (1, 'test'), (2, 'data')", 2);

        assertUpdate("CREATE MATERIALIZED VIEW mv_persist AS SELECT id, value FROM persist_base");

        assertQuery("SELECT COUNT(*) FROM mv_persist", "SELECT 2");
        assertQuery("SELECT * FROM mv_persist ORDER BY id", "VALUES (1, 'test'), (2, 'data')");

        assertUpdate("INSERT INTO persist_base VALUES (3, 'more')", 1);
        assertUpdate("REFRESH MATERIALIZED VIEW mv_persist", 3);

        assertQuery("SELECT COUNT(*) FROM mv_persist", "SELECT 3");
        assertQuery("SELECT * FROM mv_persist ORDER BY id", "VALUES (1, 'test'), (2, 'data'), (3, 'more')");

        assertUpdate("DROP TABLE persist_base");

        assertQueryFails("SELECT COUNT(*) FROM mv_persist",
                ".*Table .* does not exist.*");

        assertQueryFails("REFRESH MATERIALIZED VIEW mv_persist",
                ".*Table .* does not exist.*");

        assertUpdate("DROP MATERIALIZED VIEW mv_persist");
    }
}
