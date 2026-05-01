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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.common.ColumnLineageEntry;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.SourceColumn;
import com.facebook.presto.common.TransformationSubtype;
import com.facebook.presto.common.TransformationType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertEquals;

/**
 * Tests both direct and indirect column lineage collection through the SQL analyzer.
 *
 * Direct lineage: which source columns' data flows into each output column (per-output-column).
 * Indirect lineage: which source columns influence the result set without providing data (query-level).
 *
 * Tables used:
 *   t1(a BIGINT, b BIGINT, c BIGINT, d BIGINT)              -- from AbstractAnalyzerTest
 *   t2(a BIGINT, b BIGINT)                                    -- from AbstractAnalyzerTest
 *   t6(a BIGINT, b VARCHAR, c BIGINT, d BIGINT)              -- from AbstractAnalyzerTest
 *   lineage_orders(order_id, customer_id, amount, region, status)   -- created below
 *   lineage_customers(customer_id, name, country, tier)              -- created below
 *   lineage_products(product_id, order_id, price, category)         -- created below
 */
public class TestIndirectColumnLineage
        extends AbstractAnalyzerTest
{
    private static final QualifiedObjectName T1 = QualifiedObjectName.valueOf("tpch.s1.t1");
    private static final QualifiedObjectName T2 = QualifiedObjectName.valueOf("tpch.s1.t2");
    private static final QualifiedObjectName T6 = QualifiedObjectName.valueOf("tpch.s1.t6");
    private static final QualifiedObjectName ORDERS = QualifiedObjectName.valueOf("tpch.s1.lineage_orders");
    private static final QualifiedObjectName CUSTOMERS = QualifiedObjectName.valueOf("tpch.s1.lineage_customers");
    private static final QualifiedObjectName PRODUCTS = QualifiedObjectName.valueOf("tpch.s1.lineage_products");

    @BeforeClass
    public void setupLineageTables()
    {
        SchemaTableName orders = new SchemaTableName("s1", "lineage_orders");
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, (Consumer<Session>) session -> metadata.createTable(session, TPCH_CATALOG,
                        new ConnectorTableMetadata(orders, ImmutableList.of(
                                ColumnMetadata.builder().setName("order_id").setType(BIGINT).build(),
                                ColumnMetadata.builder().setName("customer_id").setType(BIGINT).build(),
                                ColumnMetadata.builder().setName("amount").setType(BIGINT).build(),
                                ColumnMetadata.builder().setName("region").setType(VARCHAR).build(),
                                ColumnMetadata.builder().setName("status").setType(VARCHAR).build())),
                        false));

        SchemaTableName customers = new SchemaTableName("s1", "lineage_customers");
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, (Consumer<Session>) session -> metadata.createTable(session, TPCH_CATALOG,
                        new ConnectorTableMetadata(customers, ImmutableList.of(
                                ColumnMetadata.builder().setName("customer_id").setType(BIGINT).build(),
                                ColumnMetadata.builder().setName("name").setType(VARCHAR).build(),
                                ColumnMetadata.builder().setName("country").setType(VARCHAR).build(),
                                ColumnMetadata.builder().setName("tier").setType(VARCHAR).build())),
                        false));

        SchemaTableName products = new SchemaTableName("s1", "lineage_products");
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, (Consumer<Session>) session -> metadata.createTable(session, TPCH_CATALOG,
                        new ConnectorTableMetadata(products, ImmutableList.of(
                                ColumnMetadata.builder().setName("product_id").setType(BIGINT).build(),
                                ColumnMetadata.builder().setName("order_id").setType(BIGINT).build(),
                                ColumnMetadata.builder().setName("price").setType(BIGINT).build(),
                                ColumnMetadata.builder().setName("category").setType(VARCHAR).build())),
                        false));
    }

    // =========================================================================
    // 1. Identity projection (SELECT col) -> DIRECT only, no indirect
    // =========================================================================

    @Test
    public void testIdentityProjection()
    {
        assertLineage("SELECT a FROM t1",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of());
    }

    @Test
    public void testMultipleIdentityProjections()
    {
        assertLineage("SELECT a, b, c FROM t1",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "b", ImmutableSet.of(direct(T1, "b")),
                        "c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of());
    }

    // =========================================================================
    // 2. Derived expressions -> DIRECT/TRANSFORMATION
    // =========================================================================

    @Test
    public void testDerivedExpression()
    {
        // SELECT a + b AS c FROM t1
        // Output c has direct sources: t1.a and t1.b
        assertLineage("SELECT a + b AS result FROM t1",
                ImmutableMap.of("result", ImmutableSet.of(direct(T1, "a"), direct(T1, "b"))),
                ImmutableSet.of());
    }

    @Test
    public void testConstantExpression()
    {
        // SELECT 1 AS flag, a FROM t1
        // flag has no direct sources; a has direct source t1.a
        assertLineage("SELECT 1 AS flag, a FROM t1",
                ImmutableMap.of(
                        "flag", ImmutableSet.of(),
                        "a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of());
    }

    // =========================================================================
    // 3. WHERE clause -> INDIRECT/FILTER
    // =========================================================================

    @Test
    public void testWhereFilter()
    {
        assertLineage("SELECT b FROM t1 WHERE a > 10",
                ImmutableMap.of("b", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.FILTER)));
    }

    @Test
    public void testWhereMultipleColumns()
    {
        assertLineage("SELECT c FROM t1 WHERE a > 10 AND b < 5",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testWhereOnProjectedColumn()
    {
        // a is both direct (projected) and indirect (filter)
        assertLineage("SELECT a FROM t1 WHERE a > 10",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.FILTER)));
    }

    @Test
    public void testWhereWithExpression()
    {
        assertLineage("SELECT c FROM t1 WHERE a + b > 10",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 4. JOIN -> INDIRECT/JOIN
    // =========================================================================

    @Test
    public void testJoinOn()
    {
        assertLineage("SELECT t1.c, t2.b FROM t1 JOIN t2 ON t1.a = t2.a",
                ImmutableMap.of(
                        "c", ImmutableSet.of(direct(T1, "c")),
                        "b", ImmutableSet.of(direct(T2, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    @Test
    public void testJoinUsing()
    {
        assertLineage("SELECT t1.c FROM t1 JOIN t2 USING (a)",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    @Test
    public void testJoinWithWhere()
    {
        assertLineage("SELECT t1.c FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b > 5",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCompositeJoinCondition()
    {
        assertLineage("SELECT t1.c FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN),
                        indirect(T1, "b", TransformationSubtype.JOIN),
                        indirect(T2, "b", TransformationSubtype.JOIN)));
    }

    @Test
    public void testLeftJoin()
    {
        assertLineage("SELECT t1.c FROM t1 LEFT JOIN t2 ON t1.a = t2.a",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    @Test
    public void testCrossJoinNoIndirect()
    {
        assertLineage("SELECT t1.a FROM t1 CROSS JOIN t2",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of());
    }

    @Test
    public void testCrossJoinWithWhere()
    {
        assertLineage("SELECT t1.a FROM t1 CROSS JOIN t2 WHERE t2.b > 5",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testSelfJoin()
    {
        assertLineage("SELECT x.c FROM t1 x JOIN t1 y ON x.a = y.a WHERE y.b > 5",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 5. GROUP BY -> INDIRECT/GROUP_BY
    // =========================================================================

    @Test
    public void testGroupBy()
    {
        assertLineage("SELECT a, SUM(b) AS total FROM t1 GROUP BY a",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testGroupByMultipleColumns()
    {
        assertLineage("SELECT a, b, SUM(c) AS total FROM t1 GROUP BY a, b",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "b", ImmutableSet.of(direct(T1, "b")),
                        "total", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testGroupByOrdinal()
    {
        assertLineage("SELECT a, SUM(b) AS total FROM t1 GROUP BY 1",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testGroupByExpression()
    {
        assertIndirectSources("SELECT a + b, SUM(c) FROM t1 GROUP BY a + b",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.GROUP_BY)));
    }

    // =========================================================================
    // 6. HAVING -> INDIRECT/FILTER
    // =========================================================================

    @Test
    public void testHaving()
    {
        assertLineage("SELECT a, SUM(b) AS s FROM t1 GROUP BY a HAVING SUM(b) > 100",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "s", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 7. ORDER BY -> INDIRECT/SORT (outermost only)
    // =========================================================================

    @Test
    public void testOrderBy()
    {
        assertLineage("SELECT a FROM t1 ORDER BY b",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T1, "b", TransformationSubtype.SORT)));
    }

    @Test
    public void testOrderByProjectedColumn()
    {
        assertLineage("SELECT a, b FROM t1 ORDER BY a",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "b", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.SORT)));
    }

    // =========================================================================
    // 8. Subquery propagation
    // =========================================================================

    @Test
    public void testSubqueryWherePropagates()
    {
        assertLineage("SELECT a FROM (SELECT a, b FROM t1 WHERE c > 10) sub",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T1, "c", TransformationSubtype.FILTER)));
    }

    @Test
    public void testSubqueryWithOuterWhere()
    {
        assertLineage("SELECT a FROM (SELECT a, b FROM t1 WHERE c > 10) sub WHERE b > 5",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testInnerOrderByDoesNotPropagate()
    {
        assertLineage("SELECT a FROM (SELECT a, b FROM t1 ORDER BY b) sub",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of());
    }

    @Test
    public void testNestedSubqueryPropagation()
    {
        // Three levels of nesting, each with WHERE
        assertLineage(
                "SELECT a FROM (SELECT a, b FROM (SELECT a, b, c FROM t1 WHERE d > 1) s1 WHERE c > 2) s2 WHERE b > 3",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "d", TransformationSubtype.FILTER),
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 9. CTE propagation
    // =========================================================================

    @Test
    public void testCteWherePropagates()
    {
        assertLineage("WITH cte AS (SELECT a, b FROM t1 WHERE c > 10) SELECT a FROM cte",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T1, "c", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCteWithOuterWhere()
    {
        assertLineage("WITH cte AS (SELECT a, b FROM t1 WHERE c > 10) SELECT a FROM cte WHERE b > 5",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCteGroupByPropagates()
    {
        assertLineage("WITH cte AS (SELECT a, SUM(b) AS total FROM t1 GROUP BY a) SELECT total FROM cte",
                ImmutableMap.of("total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testMultipleCtes()
    {
        assertIndirectSources(
                "WITH cte1 AS (SELECT a, b FROM t1 WHERE c > 1), " +
                        "cte2 AS (SELECT a, b FROM t2 WHERE b > 2) " +
                        "SELECT cte1.a FROM cte1 JOIN cte2 ON cte1.a = cte2.a",
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    // =========================================================================
    // 10. UNION / set operations
    // =========================================================================

    @Test
    public void testUnionAllDirectSources()
    {
        // UNION ALL: output comes from both branches
        assertLineage("SELECT a FROM t1 UNION ALL SELECT a FROM t2",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"), direct(T2, "a"))),
                ImmutableSet.of());
    }

    @Test
    public void testUnionWithWhere()
    {
        assertIndirectSources(
                "SELECT a FROM t1 WHERE b > 5 UNION ALL SELECT a FROM t2 WHERE b > 10",
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 11. IN subquery
    // =========================================================================

    @Test
    public void testInSubquery()
    {
        assertLineage("SELECT b FROM t1 WHERE a IN (SELECT a FROM t2)",
                ImmutableMap.of("b", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 12. Combined / full scenarios from design doc
    // =========================================================================

    @Test
    public void testAggregationWithFilterAndGroupBy()
    {
        assertLineage("SELECT a, SUM(b) AS total FROM t1 WHERE c > 10 GROUP BY a",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testJoinWithGroupByAndFilter()
    {
        assertIndirectSources(
                "SELECT t1.a, SUM(t2.b) AS total FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.c > 5 GROUP BY t1.a",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN),
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testFullQueryAllClauses()
    {
        // WHERE + GROUP BY + HAVING + ORDER BY
        assertLineage(
                "SELECT a, SUM(b) AS total FROM t1 WHERE c > 10 GROUP BY a HAVING SUM(b) > 100 ORDER BY a",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.SORT)));
    }

    // =========================================================================
    // 13. Realistic business query scenarios
    // =========================================================================

    @Test
    public void testRealisticReportQuery()
    {
        // Typical business report:
        // SELECT region, SUM(amount) AS total_amount
        // FROM lineage_orders
        // WHERE status = 'completed'
        // GROUP BY region
        // HAVING SUM(amount) > 1000
        // ORDER BY total_amount
        assertLineage(
                "SELECT region, SUM(amount) AS total_amount FROM lineage_orders " +
                        "WHERE status = 'completed' GROUP BY region HAVING SUM(amount) > 1000 ORDER BY total_amount",
                ImmutableMap.of(
                        "region", ImmutableSet.of(direct(ORDERS, "region")),
                        "total_amount", ImmutableSet.of(direct(ORDERS, "amount"))),
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.FILTER),
                        indirect(ORDERS, "region", TransformationSubtype.GROUP_BY),
                        indirect(ORDERS, "amount", TransformationSubtype.FILTER),
                        indirect(ORDERS, "amount", TransformationSubtype.SORT)));
    }

    @Test
    public void testRealisticJoinQuery()
    {
        // Customer report with join:
        // SELECT c.name, SUM(o.amount) AS total
        // FROM lineage_orders o JOIN lineage_customers c ON o.customer_id = c.customer_id
        // WHERE c.country = 'US'
        // GROUP BY c.name
        assertLineage(
                "SELECT c.name, SUM(o.amount) AS total " +
                        "FROM lineage_orders o JOIN lineage_customers c ON o.customer_id = c.customer_id " +
                        "WHERE c.country = 'US' GROUP BY c.name",
                ImmutableMap.of(
                        "name", ImmutableSet.of(direct(CUSTOMERS, "name")),
                        "total", ImmutableSet.of(direct(ORDERS, "amount"))),
                ImmutableSet.of(
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "country", TransformationSubtype.FILTER),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testRealisticThreeWayJoin()
    {
        // Three-way join: orders -> customers -> products
        assertIndirectSources(
                "SELECT c.name, SUM(p.price) AS total_price " +
                        "FROM lineage_orders o " +
                        "JOIN lineage_customers c ON o.customer_id = c.customer_id " +
                        "JOIN lineage_products p ON o.order_id = p.order_id " +
                        "WHERE c.tier = 'premium' AND p.category = 'electronics' " +
                        "GROUP BY c.name " +
                        "ORDER BY total_price",
                ImmutableSet.of(
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(ORDERS, "order_id", TransformationSubtype.JOIN),
                        indirect(PRODUCTS, "order_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "tier", TransformationSubtype.FILTER),
                        indirect(PRODUCTS, "category", TransformationSubtype.FILTER),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY),
                        indirect(PRODUCTS, "price", TransformationSubtype.SORT)));
    }

    @Test
    public void testRealisticCteWithJoin()
    {
        // CTE filtering + outer join
        assertIndirectSources(
                "WITH active_orders AS (" +
                        "  SELECT order_id, customer_id, amount FROM lineage_orders WHERE status = 'active'" +
                        ") " +
                        "SELECT c.name, SUM(ao.amount) AS total " +
                        "FROM active_orders ao JOIN lineage_customers c ON ao.customer_id = c.customer_id " +
                        "GROUP BY c.name",
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testRealisticInSubquery()
    {
        // Filter using IN subquery
        assertLineage(
                "SELECT name, country FROM lineage_customers " +
                        "WHERE customer_id IN (SELECT customer_id FROM lineage_orders WHERE amount > 1000)",
                ImmutableMap.of(
                        "name", ImmutableSet.of(direct(CUSTOMERS, "name")),
                        "country", ImmutableSet.of(direct(CUSTOMERS, "country"))),
                ImmutableSet.of(
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.FILTER),
                        indirect(ORDERS, "amount", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 14. Edge cases
    // =========================================================================

    @Test
    public void testNoIndirectSources()
    {
        assertLineage("SELECT a, b FROM t1",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "b", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of());
    }

    @Test
    public void testSelectStar()
    {
        assertLineage("SELECT * FROM t2",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T2, "a")),
                        "b", ImmutableSet.of(direct(T2, "b"))),
                ImmutableSet.of());
    }

    @Test
    public void testSelectStarWithWhere()
    {
        assertLineage("SELECT * FROM t2 WHERE a > 5",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T2, "a")),
                        "b", ImmutableSet.of(direct(T2, "b"))),
                ImmutableSet.of(indirect(T2, "a", TransformationSubtype.FILTER)));
    }

    @Test
    public void testColumnAliasDoesNotAffectLineage()
    {
        assertLineage("SELECT a AS x, b AS y FROM t1 WHERE c > 5",
                ImmutableMap.of(
                        "x", ImmutableSet.of(direct(T1, "a")),
                        "y", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "c", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCountStar()
    {
        // COUNT(*) has no direct source columns
        assertLineage("SELECT COUNT(*) AS cnt FROM t1 WHERE a > 5 GROUP BY b",
                ImmutableMap.of("cnt", ImmutableSet.of()),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testMultipleLevelsOfAliasing()
    {
        // Direct lineage should trace through multiple alias layers
        assertLineage(
                "SELECT z FROM (SELECT y AS z FROM (SELECT a AS y FROM t1 WHERE b > 1) s1) s2",
                ImmutableMap.of("z", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testExpressionCombiningColumns()
    {
        // Expression combining columns from different tables
        assertLineage(
                "SELECT t1.a + t2.b AS combined FROM t1 JOIN t2 ON t1.a = t2.a",
                ImmutableMap.of("combined", ImmutableSet.of(direct(T1, "a"), direct(T2, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    @Test
    public void testUnionDirectSourcesMerged()
    {
        // UNION merges direct sources from both branches
        assertLineage("SELECT a FROM t1 WHERE b > 5 UNION ALL SELECT a FROM t2 WHERE b > 10",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"), direct(T2, "a"))),
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testSubqueryDirectLineagePreservation()
    {
        // Direct lineage traces through subquery
        assertLineage(
                "SELECT x FROM (SELECT a + b AS x, c FROM t1) sub WHERE c > 10",
                ImmutableMap.of("x", ImmutableSet.of(direct(T1, "a"), direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "c", TransformationSubtype.FILTER)));
    }

    @Test
    public void testJoinDirectSourcesFromBothSides()
    {
        // Output columns pull direct from different sides of join
        assertLineage(
                "SELECT t1.a, t2.b, t1.c + t2.a AS combined FROM t1 JOIN t2 ON t1.b = t2.b",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "b", ImmutableSet.of(direct(T2, "b")),
                        "combined", ImmutableSet.of(direct(T1, "c"), direct(T2, "a"))),
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.JOIN),
                        indirect(T2, "b", TransformationSubtype.JOIN)));
    }

    @Test
    public void testWhereOrCondition()
    {
        // OR condition references multiple columns
        assertLineage("SELECT c FROM t1 WHERE a > 10 OR b < 5",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testMultipleAggregates()
    {
        // Multiple aggregates over different columns
        assertLineage("SELECT a, SUM(b) AS sum_b, MAX(c) AS max_c, COUNT(d) AS cnt_d FROM t1 GROUP BY a",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "sum_b", ImmutableSet.of(direct(T1, "b")),
                        "max_c", ImmutableSet.of(direct(T1, "c")),
                        "cnt_d", ImmutableSet.of(direct(T1, "d"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    // =========================================================================
    // 15. Subquery in FROM: single level
    // =========================================================================

    @Test
    public void testSubqueryWithGroupBy()
    {
        // GROUP BY inside subquery propagates
        assertLineage(
                "SELECT total FROM (SELECT a, SUM(b) AS total FROM t1 GROUP BY a) sub",
                ImmutableMap.of("total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testSubqueryWithJoin()
    {
        // JOIN inside subquery propagates
        assertLineage(
                "SELECT c FROM (SELECT t1.c, t2.b FROM t1 JOIN t2 ON t1.a = t2.a) sub",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    @Test
    public void testSubqueryWithHaving()
    {
        // HAVING inside subquery propagates
        assertLineage(
                "SELECT total FROM (SELECT a, SUM(b) AS total FROM t1 GROUP BY a HAVING SUM(b) > 100) sub",
                ImmutableMap.of("total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testSubqueryWithAllClauses()
    {
        // Subquery with WHERE + GROUP BY + HAVING, outer has WHERE
        assertLineage(
                "SELECT total FROM (" +
                        "  SELECT a, SUM(b) AS total FROM t1 WHERE c > 1 GROUP BY a HAVING SUM(b) > 10" +
                        ") sub WHERE total > 50",
                ImmutableMap.of("total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 16. Subquery in FROM: multi-level nesting
    // =========================================================================

    @Test
    public void testSubqueryGroupByThenOuterWhere()
    {
        // Inner GROUP BY + outer WHERE
        assertLineage(
                "SELECT a FROM (SELECT a, SUM(b) AS total FROM t1 GROUP BY a) sub WHERE total > 100",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testSubqueryJoinThenOuterGroupBy()
    {
        // Inner JOIN + outer GROUP BY
        assertIndirectSources(
                "SELECT a, SUM(b) AS total FROM (" +
                        "  SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.a = t2.a" +
                        ") sub GROUP BY a",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testTwoSubqueriesJoined()
    {
        // Two subqueries each with filters, joined in outer query
        assertIndirectSources(
                "SELECT s1.a FROM " +
                        "(SELECT a, b FROM t1 WHERE c > 1) s1 " +
                        "JOIN (SELECT a, b FROM t2 WHERE b > 2) s2 " +
                        "ON s1.a = s2.a",
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    @Test
    public void testFourLevelNesting()
    {
        // 4 levels, each adding a different clause type
        assertLineage(
                "SELECT a FROM (" +                          // level 4: outer WHERE
                        "  SELECT a, b FROM (" +             // level 3: GROUP BY
                        "    SELECT a, b, SUM(c) AS sc FROM (" +  // level 2: JOIN
                        "      SELECT t1.a, t1.b, t1.c FROM t1 JOIN t2 ON t1.a = t2.a" + // level 1: WHERE
                        "      WHERE t1.d > 1" +
                        "    ) s1 GROUP BY a, b, c" +
                        "  ) s2" +
                        ") s3 WHERE b > 5",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN),
                        indirect(T1, "d", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.GROUP_BY),
                        indirect(T1, "c", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 17. CTE: advanced scenarios
    // =========================================================================

    @Test
    public void testCteWithJoinInside()
    {
        // CTE contains a JOIN; outer query uses it simply
        assertLineage(
                "WITH joined AS (" +
                        "  SELECT t1.c, t2.b FROM t1 JOIN t2 ON t1.a = t2.a" +
                        ") SELECT c FROM joined",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    @Test
    public void testCteWithGroupByAndHaving()
    {
        // CTE with GROUP BY + HAVING
        assertLineage(
                "WITH agg AS (" +
                        "  SELECT a, SUM(b) AS total FROM t1 GROUP BY a HAVING SUM(b) > 100" +
                        ") SELECT a, total FROM agg",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCteUsedMultipleTimes()
    {
        // Same CTE referenced twice via self-join; indirect sources should deduplicate
        assertIndirectSources(
                "WITH filtered AS (SELECT a, b FROM t1 WHERE c > 1) " +
                        "SELECT x.a FROM filtered x JOIN filtered y ON x.a = y.b",
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T1, "b", TransformationSubtype.JOIN)));
    }

    @Test
    public void testChainedCtes()
    {
        // CTE2 references CTE1; each adds its own filter
        assertLineage(
                "WITH cte1 AS (SELECT a, b, c FROM t1 WHERE d > 1), " +
                        "cte2 AS (SELECT a, b FROM cte1 WHERE c > 2) " +
                        "SELECT a FROM cte2 WHERE b > 3",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "d", TransformationSubtype.FILTER),
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testChainedCtesWithGroupBy()
    {
        // CTE1 filters, CTE2 groups
        assertLineage(
                "WITH filtered AS (SELECT a, b FROM t1 WHERE c > 1), " +
                        "grouped AS (SELECT a, SUM(b) AS total FROM filtered GROUP BY a) " +
                        "SELECT a, total FROM grouped",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testCteWithJoinToBaseTable()
    {
        // CTE joined with a base table
        assertIndirectSources(
                "WITH cte AS (SELECT a, b FROM t1 WHERE c > 1) " +
                        "SELECT cte.a, t2.b FROM cte JOIN t2 ON cte.a = t2.a WHERE t2.b > 5",
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN),
                        indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testMultipleCtesBothJoined()
    {
        // Two independent CTEs from different tables, joined in outer
        assertLineage(
                "WITH orders_cte AS (" +
                        "  SELECT order_id, customer_id, amount FROM lineage_orders WHERE status = 'active'" +
                        "), " +
                        "customers_cte AS (" +
                        "  SELECT customer_id, name FROM lineage_customers WHERE country = 'US'" +
                        ") " +
                        "SELECT c.name, SUM(o.amount) AS total " +
                        "FROM orders_cte o JOIN customers_cte c ON o.customer_id = c.customer_id " +
                        "GROUP BY c.name",
                ImmutableMap.of(
                        "name", ImmutableSet.of(direct(CUSTOMERS, "name")),
                        "total", ImmutableSet.of(direct(ORDERS, "amount"))),
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.FILTER),
                        indirect(CUSTOMERS, "country", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testCteInnerOrderByPropagates()
    {
        // ORDER BY inside CTE propagates (no special CTE handling)
        assertLineage(
                "WITH cte AS (SELECT a, b FROM t1 ORDER BY b) SELECT a FROM cte",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T1, "b", TransformationSubtype.SORT)));
    }

    @Test
    public void testCteInnerOrderByOuterOrderByDoes()
    {
        // Both inner and outer ORDER BY tracked
        assertLineage(
                "WITH cte AS (SELECT a, b FROM t1 ORDER BY c) SELECT a FROM cte ORDER BY b",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.SORT),
                        indirect(T1, "b", TransformationSubtype.SORT)));
    }

    // =========================================================================
    // 18. Subquery in WHERE (IN, EXISTS, scalar)
    // =========================================================================

    @Test
    public void testInSubqueryWithFilter()
    {
        // IN subquery itself has a WHERE clause
        assertLineage(
                "SELECT a FROM t1 WHERE b IN (SELECT a FROM t2 WHERE b > 10)",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testInSubqueryWithCte()
    {
        // CTE used in an IN subquery
        assertIndirectSources(
                "WITH active AS (SELECT customer_id FROM lineage_orders WHERE status = 'active') " +
                        "SELECT name FROM lineage_customers WHERE customer_id IN (SELECT customer_id FROM active)",
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.FILTER),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.FILTER)));
    }

    @Test
    public void testInSubqueryWithGroupBy()
    {
        // IN subquery with GROUP BY inside
        assertIndirectSources(
                "SELECT a FROM t1 WHERE b IN (SELECT SUM(a) FROM t2 GROUP BY b)",
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.GROUP_BY)));
    }

    // =========================================================================
    // 19. Mixed CTE + subquery nesting
    // =========================================================================

    @Test
    public void testCteContainingSubquery()
    {
        // CTE body has a subquery in FROM
        assertLineage(
                "WITH cte AS (" +
                        "  SELECT a FROM (SELECT a, b FROM t1 WHERE c > 1) sub WHERE b > 2" +
                        ") SELECT a FROM cte",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testSubqueryReferencingCte()
    {
        // Outer query has subquery in FROM that references a CTE
        assertLineage(
                "WITH cte AS (SELECT a, b FROM t1 WHERE c > 1) " +
                        "SELECT a FROM (SELECT a, b FROM cte WHERE b > 2) sub",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCteWithSubqueryJoin()
    {
        // CTE joined with a subquery
        assertIndirectSources(
                "WITH cte AS (SELECT a, b FROM t1 WHERE c > 1) " +
                        "SELECT cte.a FROM cte " +
                        "JOIN (SELECT a, b FROM t2 WHERE b > 2) sub ON cte.a = sub.a",
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    // =========================================================================
    // 20. Realistic multi-level CTE scenarios
    // =========================================================================

    @Test
    public void testRealisticPipelineCtes()
    {
        // Data pipeline: filter -> join -> aggregate
        assertLineage(
                "WITH active_orders AS (" +
                        "  SELECT order_id, customer_id, amount FROM lineage_orders WHERE status = 'completed'" +
                        "), " +
                        "enriched AS (" +
                        "  SELECT c.name, o.amount " +
                        "  FROM active_orders o JOIN lineage_customers c ON o.customer_id = c.customer_id " +
                        "  WHERE c.tier = 'premium'" +
                        "), " +
                        "summary AS (" +
                        "  SELECT name, SUM(amount) AS total FROM enriched GROUP BY name" +
                        ") " +
                        "SELECT name, total FROM summary WHERE total > 1000 ORDER BY total",
                ImmutableMap.of(
                        "name", ImmutableSet.of(direct(CUSTOMERS, "name")),
                        "total", ImmutableSet.of(direct(ORDERS, "amount"))),
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "tier", TransformationSubtype.FILTER),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY),
                        indirect(ORDERS, "amount", TransformationSubtype.FILTER),
                        indirect(ORDERS, "amount", TransformationSubtype.SORT)));
    }

    @Test
    public void testRealisticUnionCtes()
    {
        // Two CTEs with different filters, unioned
        assertLineage(
                "WITH us_orders AS (" +
                        "  SELECT order_id, amount FROM lineage_orders WHERE region = 'US'" +
                        "), " +
                        "eu_orders AS (" +
                        "  SELECT order_id, amount FROM lineage_orders WHERE region = 'EU'" +
                        ") " +
                        "SELECT order_id, amount FROM us_orders " +
                        "UNION ALL " +
                        "SELECT order_id, amount FROM eu_orders",
                ImmutableMap.of(
                        "order_id", ImmutableSet.of(direct(ORDERS, "order_id")),
                        "amount", ImmutableSet.of(direct(ORDERS, "amount"))),
                ImmutableSet.of(
                        indirect(ORDERS, "region", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCteWithInSubqueryFromAnotherCte()
    {
        // CTE1 provides filter list, CTE2 filters using IN with CTE1
        assertIndirectSources(
                "WITH premium_customers AS (" +
                        "  SELECT customer_id FROM lineage_customers WHERE tier = 'premium'" +
                        "), " +
                        "premium_orders AS (" +
                        "  SELECT order_id, amount FROM lineage_orders " +
                        "  WHERE customer_id IN (SELECT customer_id FROM premium_customers)" +
                        ") " +
                        "SELECT order_id, amount FROM premium_orders",
                ImmutableSet.of(
                        indirect(CUSTOMERS, "tier", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.FILTER),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 20b. GROUPING SETS — used in 100% of production queries
    // =========================================================================

    @Test
    public void testGroupingSetsSimple()
    {
        // GROUP BY GROUPING SETS(a) is equivalent to GROUP BY a
        assertLineage(
                "SELECT a, SUM(b) AS total FROM t1 GROUP BY GROUPING SETS(a)",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testGroupingSetsMultipleColumns()
    {
        // GROUP BY GROUPING SETS((a, b)) — both columns are GROUP_BY
        assertLineage(
                "SELECT a, b, SUM(c) AS total FROM t1 GROUP BY GROUPING SETS((a, b))",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "b", ImmutableSet.of(direct(T1, "b")),
                        "total", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testGroupingSetsMultipleSets()
    {
        // GROUP BY GROUPING SETS(a, (a, b)) — a and b are both GROUP_BY
        assertIndirectSources(
                "SELECT a, b, SUM(c) AS total FROM t1 GROUP BY GROUPING SETS(a, (a, b))",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testGroupingSetsWithWhere()
    {
        // GROUPING SETS + WHERE — both filter and group tracked
        assertLineage(
                "SELECT a, SUM(b) AS total FROM t1 WHERE c > 10 GROUP BY GROUPING SETS(a)",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testGroupingSetsWithJoin()
    {
        // GROUPING SETS with JOIN — production pattern
        assertIndirectSources(
                "SELECT t1.a, SUM(t2.b) AS total FROM t1 JOIN t2 ON t1.a = t2.a " +
                        "GROUP BY GROUPING SETS(t1.a)",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testGroupingSetsInCte()
    {
        // GROUPING SETS inside CTE — should propagate
        assertLineage(
                "WITH agg AS (" +
                        "  SELECT a, SUM(b) AS total FROM t1 GROUP BY GROUPING SETS(a)" +
                        ") SELECT a, total FROM agg WHERE total > 100",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testGroupingSetsRealisticPattern()
    {
        // Realistic production pattern: CASE in SELECT, GROUPING SETS, WHERE, JOIN
        assertIndirectSources(
                "SELECT " +
                        "  COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id ELSE NULL END) AS completed_orders, " +
                        "  c.customer_id " +
                        "FROM lineage_orders o " +
                        "JOIN lineage_customers c ON o.customer_id = c.customer_id " +
                        "WHERE o.amount > 0 " +
                        "GROUP BY GROUPING SETS(c.customer_id)",
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.CONDITIONAL),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(ORDERS, "amount", TransformationSubtype.FILTER),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.GROUP_BY)));
    }

    // =========================================================================
    // 20c. FULL OUTER JOIN — 19% of production queries
    // =========================================================================

    @Test
    public void testFullOuterJoin()
    {
        assertLineage(
                "SELECT t1.c, t2.b FROM t1 FULL OUTER JOIN t2 ON t1.a = t2.a",
                ImmutableMap.of(
                        "c", ImmutableSet.of(direct(T1, "c")),
                        "b", ImmutableSet.of(direct(T2, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    @Test
    public void testFullOuterJoinWithCoalesce()
    {
        // Production pattern: FULL JOIN with COALESCE on join keys
        assertIndirectSources(
                "SELECT COALESCE(t1.a, t2.a) AS id, t1.b, t2.b AS b2 " +
                        "FROM t1 FULL OUTER JOIN t2 ON t1.a = t2.a",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    // =========================================================================
    // 20d. WHERE expression patterns from production (BETWEEN, LIKE, IS NULL)
    // =========================================================================

    @Test
    public void testBetweenInWhere()
    {
        // t6 has: a BIGINT, b VARCHAR, c BIGINT, d BIGINT
        assertLineage(
                "SELECT b FROM t6 WHERE a BETWEEN 1 AND 10",
                ImmutableMap.of("b", ImmutableSet.of(direct(T6, "b"))),
                ImmutableSet.of(indirect(T6, "a", TransformationSubtype.FILTER)));
    }

    @Test
    public void testLikeInWhere()
    {
        assertLineage(
                "SELECT a FROM t6 WHERE b LIKE 'pattern%'",
                ImmutableMap.of("a", ImmutableSet.of(direct(T6, "a"))),
                ImmutableSet.of(indirect(T6, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testIsNotNullInWhere()
    {
        assertLineage(
                "SELECT b FROM t1 WHERE a IS NOT NULL",
                ImmutableMap.of("b", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.FILTER)));
    }

    @Test
    public void testIsNullInWhere()
    {
        assertLineage(
                "SELECT b FROM t1 WHERE a IS NULL",
                ImmutableMap.of("b", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCaseWhenWithInList()
    {
        // CASE WHEN b IN ('a', 'b') THEN a ELSE c END — b is condition-only
        assertLineage(
                "SELECT CASE WHEN b IN ('a', 'b') THEN a ELSE c END AS x FROM t6",
                ImmutableMap.of("x", ImmutableSet.of(direct(T6, "a"), direct(T6, "c"))),
                ImmutableSet.of(indirect(T6, "b", TransformationSubtype.CONDITIONAL)));
    }

    @Test
    public void testMultipleWherePredicateTypes()
    {
        // Combined: BETWEEN + LIKE + IS NOT NULL + IN
        assertIndirectSources(
                "SELECT d FROM t6 WHERE a BETWEEN 1 AND 10 AND b LIKE 'x%' AND c IS NOT NULL AND a IN (1, 2, 3)",
                ImmutableSet.of(
                        indirect(T6, "a", TransformationSubtype.FILTER),
                        indirect(T6, "b", TransformationSubtype.FILTER),
                        indirect(T6, "c", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 20e. CASE inside DISTINCT COUNT — common production pattern
    // =========================================================================

    @Test
    public void testCountDistinctCase()
    {
        // COUNT(DISTINCT CASE WHEN a > 0 THEN b ELSE NULL END)
        // a is condition-only -> removed from direct
        assertLineage(
                "SELECT COUNT(DISTINCT CASE WHEN a > 0 THEN b ELSE NULL END) AS cnt FROM t1 GROUP BY c",
                ImmutableMap.of("cnt", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL),
                        indirect(T1, "c", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testCountDistinctCaseWithJoinAndGroupingSets()
    {
        // Full production-like pattern: CASE in COUNT(DISTINCT), JOIN, WHERE, GROUPING SETS
        assertIndirectSources(
                "SELECT " +
                        "  COUNT(DISTINCT CASE WHEN o.status = 'active' THEN o.order_id ELSE NULL END) AS active_count, " +
                        "  c.customer_id " +
                        "FROM lineage_orders o " +
                        "JOIN lineage_customers c ON o.customer_id = c.customer_id " +
                        "WHERE o.amount IS NOT NULL " +
                        "GROUP BY GROUPING SETS(c.customer_id, (c.customer_id, c.country))",
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.CONDITIONAL),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(ORDERS, "amount", TransformationSubtype.FILTER),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.GROUP_BY),
                        indirect(CUSTOMERS, "country", TransformationSubtype.GROUP_BY)));
    }

    // =========================================================================
    // 21. EXISTS subquery
    // =========================================================================

    @Test
    public void testExistsSubquery()
    {
        // EXISTS with correlated predicate: t2.a = t1.b filters t1 rows
        assertLineage(
                "SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.b)",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER)));
    }

    @Test
    public void testNotExistsSubquery()
    {
        assertLineage(
                "SELECT a FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.a)",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 22. Scalar subquery in WHERE
    // =========================================================================

    @Test
    public void testScalarSubqueryInWhere()
    {
        // WHERE b > (SELECT AVG(b) FROM t2)
        // t1.b is FILTER (outer), t2.b is FILTER (part of filtering expression)
        assertLineage(
                "SELECT a FROM t1 WHERE b > (SELECT AVG(b) FROM t2)",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCorrelatedScalarSubqueryInWhere()
    {
        // WHERE a > (SELECT MAX(a) FROM t2 WHERE t2.b = t1.b)
        assertLineage(
                "SELECT c FROM t1 WHERE a > (SELECT MAX(a) FROM t2 WHERE t2.b = t1.b)",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 23. Same column in multiple roles
    // =========================================================================

    @Test
    public void testSameColumnMultipleRoles()
    {
        // t1.a appears as DIRECT + FILTER + GROUP_BY + SORT
        // The indirect set should have 3 distinct entries (different subtypes)
        assertLineage(
                "SELECT a, SUM(b) AS total FROM t1 WHERE a > 5 GROUP BY a ORDER BY a",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "a", TransformationSubtype.SORT)));
    }

    @Test
    public void testSameColumnDirectAndIndirect()
    {
        // t1.b is DIRECT for total (SUM(b)) and INDIRECT/FILTER (HAVING SUM(b) > 100)
        assertLineage(
                "SELECT a, SUM(b) AS total FROM t1 GROUP BY a HAVING SUM(b) > 100",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 23b. DISTINCT -> INDIRECT/GROUP_BY on all projected columns
    // =========================================================================

    @Test
    public void testDistinct()
    {
        assertLineage("SELECT DISTINCT a, b FROM t1",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "b", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testDistinctWithWhere()
    {
        assertLineage("SELECT DISTINCT a FROM t1 WHERE b > 5",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testDistinctWithExpression()
    {
        // DISTINCT on expression: both source columns become GROUP_BY
        assertIndirectSources("SELECT DISTINCT a + b AS x FROM t1",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.GROUP_BY)));
    }

    // =========================================================================
    // 24. Window functions -> INDIRECT/WINDOW
    // =========================================================================

    @Test
    public void testWindowFunctionPartitionBy()
    {
        // PARTITION BY b, ORDER BY c -> INDIRECT/WINDOW, removed from direct
        // ROW_NUMBER() has no column arguments so b,c are window-only
        assertLineage(
                "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c) AS rn FROM t1",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "rn", ImmutableSet.of()),
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.WINDOW),
                        indirect(T1, "c", TransformationSubtype.WINDOW)));
    }

    @Test
    public void testWindowFunctionWithWhere()
    {
        assertLineage(
                "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c) AS rn FROM t1 WHERE d > 5",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "rn", ImmutableSet.of()),
                ImmutableSet.of(
                        indirect(T1, "d", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.WINDOW),
                        indirect(T1, "c", TransformationSubtype.WINDOW)));
    }

    @Test
    public void testWindowFunctionSumOver()
    {
        // SUM(a) OVER (PARTITION BY b) — a is direct (aggregated), b is window-only
        assertLineage(
                "SELECT SUM(a) OVER (PARTITION BY b) AS running_total FROM t1",
                ImmutableMap.of("running_total", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T1, "b", TransformationSubtype.WINDOW)));
    }

    @Test
    public void testWindowFunctionPartitionByOnly()
    {
        // COUNT(*) OVER (PARTITION BY b) — COUNT(*) has no column args, b is window-only
        assertLineage(
                "SELECT a, COUNT(*) OVER (PARTITION BY b) AS cnt FROM t1",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "cnt", ImmutableSet.of()),
                ImmutableSet.of(indirect(T1, "b", TransformationSubtype.WINDOW)));
    }

    // =========================================================================
    // 25. Implicit join (comma syntax)
    // =========================================================================

    @Test
    public void testImplicitJoinComma()
    {
        // Comma join is CROSS JOIN + WHERE, so join predicate is FILTER not JOIN
        assertLineage(
                "SELECT t1.c FROM t1, t2 WHERE t1.a = t2.a",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER)));
    }

    @Test
    public void testImplicitJoinWithExtraFilter()
    {
        assertLineage(
                "SELECT t1.c FROM t1, t2 WHERE t1.a = t2.a AND t1.b > 5",
                ImmutableMap.of("c", ImmutableSet.of(direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 27. CASE / IF / COALESCE
    //
    // CASE/IF condition columns are INDIRECT/CONDITIONAL and excluded from direct sources.
    // Value branches (THEN/ELSE) remain DIRECT.
    // A column appearing in both condition and value stays DIRECT and gets CONDITIONAL too.
    // =========================================================================

    @Test
    public void testCaseWhen()
    {
        // CASE WHEN a > 0 THEN b ELSE c END AS x
        // a is condition-only -> INDIRECT/CONDITIONAL, removed from direct
        // b, c are value branches -> DIRECT
        assertLineage(
                "SELECT CASE WHEN a > 0 THEN b ELSE c END AS x FROM t1",
                ImmutableMap.of("x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.CONDITIONAL)));
    }

    @Test
    public void testCaseWhenWithWhere()
    {
        assertLineage(
                "SELECT CASE WHEN a > 0 THEN b ELSE c END AS x FROM t1 WHERE d > 10",
                ImmutableMap.of("x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "d", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL)));
    }

    @Test
    public void testCaseWhenMultipleBranches()
    {
        // CASE WHEN a > 0 THEN b WHEN c > 0 THEN d ELSE a END
        // a is in condition AND value (ELSE a) -> stays DIRECT + gets CONDITIONAL
        // c is condition-only -> INDIRECT/CONDITIONAL, removed from direct
        assertLineage(
                "SELECT CASE WHEN a > 0 THEN b WHEN c > 0 THEN d ELSE a END AS x FROM t1",
                ImmutableMap.of("x", ImmutableSet.of(
                        direct(T1, "a"), direct(T1, "b"), direct(T1, "d"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL),
                        indirect(T1, "c", TransformationSubtype.CONDITIONAL)));
    }

    @Test
    public void testIfExpression()
    {
        // IF(a > 0, b, c) — a is condition-only -> removed from direct
        assertLineage(
                "SELECT IF(a > 0, b, c) AS x FROM t1",
                ImmutableMap.of("x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.CONDITIONAL)));
    }

    @Test
    public void testCoalesce()
    {
        // COALESCE(a, b) — both provide values, no CONDITIONAL
        assertLineage(
                "SELECT COALESCE(a, b) AS x FROM t1",
                ImmutableMap.of("x", ImmutableSet.of(direct(T1, "a"), direct(T1, "b"))),
                ImmutableSet.of());
    }

    @Test
    public void testCaseInWhereClause()
    {
        // CASE inside WHERE — all columns become FILTER (no CONDITIONAL since it's in WHERE, not SELECT)
        assertLineage(
                "SELECT d FROM t1 WHERE CASE WHEN a > 0 THEN b ELSE c END > 10",
                ImmutableMap.of("d", ImmutableSet.of(direct(T1, "d"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T1, "c", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCaseInGroupBy()
    {
        // CASE in SELECT adds CONDITIONAL; same CASE in GROUP BY adds GROUP_BY for all columns
        assertIndirectSources(
                "SELECT CASE WHEN a > 0 THEN b ELSE c END AS grp, SUM(d) FROM t1 " +
                        "GROUP BY CASE WHEN a > 0 THEN b ELSE c END",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.GROUP_BY),
                        indirect(T1, "c", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testNestedCaseExpressions()
    {
        // CASE WHEN a > 0 THEN (CASE WHEN b > 0 THEN c ELSE d END) ELSE a END
        // a: condition + value (ELSE a) -> stays DIRECT + CONDITIONAL
        // b: inner condition-only -> removed from direct, CONDITIONAL
        // c, d: value-only -> DIRECT
        assertLineage(
                "SELECT CASE WHEN a > 0 THEN CASE WHEN b > 0 THEN c ELSE d END ELSE a END AS x FROM t1",
                ImmutableMap.of("x", ImmutableSet.of(
                        direct(T1, "a"), direct(T1, "c"), direct(T1, "d"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL),
                        indirect(T1, "b", TransformationSubtype.CONDITIONAL)));
    }

    @Test
    public void testCaseWithAggregation()
    {
        // SUM(CASE WHEN a > 0 THEN b ELSE 0 END) — a is condition-only -> removed from direct
        assertLineage(
                "SELECT SUM(CASE WHEN a > 0 THEN b ELSE 0 END) AS total FROM t1 GROUP BY c",
                ImmutableMap.of("total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL),
                        indirect(T1, "c", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testCaseInJoinCondition()
    {
        // CASE in JOIN ON — all columns become JOIN (no CONDITIONAL for non-SELECT clauses)
        assertIndirectSources(
                "SELECT t1.d FROM t1 JOIN t2 ON CASE WHEN t1.a > 0 THEN t1.b ELSE t1.c END = t2.a",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T1, "b", TransformationSubtype.JOIN),
                        indirect(T1, "c", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN)));
    }

    @Test
    public void testCaseInSubquery()
    {
        // CASE in subquery SELECT — a removed from direct, added as CONDITIONAL
        assertLineage(
                "SELECT x FROM (SELECT CASE WHEN a > 0 THEN b ELSE c END AS x, d FROM t1) sub WHERE d > 5",
                ImmutableMap.of("x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL),
                        indirect(T1, "d", TransformationSubtype.FILTER)));
    }

    @Test
    public void testConditionalIsPerColumn()
    {
        // CASE WHEN a > 0 THEN b ELSE c END AS x, d FROM t1
        // CONDITIONAL for 'a' should only affect column 'x', not 'd'
        assertPerColumnLineage(
                "SELECT CASE WHEN a > 0 THEN b ELSE c END AS x, d FROM t1",
                ImmutableMap.of(
                        "x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c")),
                        "d", ImmutableSet.of(direct(T1, "d"))),
                ImmutableMap.of(
                        "x", ImmutableSet.of(indirect(T1, "a", TransformationSubtype.CONDITIONAL)),
                        "d", ImmutableSet.of()));
    }

    @Test
    public void testConditionalPerColumnWithWhere()
    {
        // WHERE filter applies to all columns, CONDITIONAL only to the CASE column
        assertPerColumnLineage(
                "SELECT CASE WHEN a > 0 THEN b ELSE c END AS x, d FROM t1 WHERE d > 5",
                ImmutableMap.of(
                        "x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c")),
                        "d", ImmutableSet.of(direct(T1, "d"))),
                ImmutableMap.of(
                        "x", ImmutableSet.of(
                                indirect(T1, "d", TransformationSubtype.FILTER),
                                indirect(T1, "a", TransformationSubtype.CONDITIONAL)),
                        "d", ImmutableSet.of(
                                indirect(T1, "d", TransformationSubtype.FILTER))));
    }

    @Test
    public void testWindowIsPerColumn()
    {
        // WINDOW for b,c should only affect column 'rn', not 'a'
        // b,c are window-only so removed from direct sources of rn
        assertPerColumnLineage(
                "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c) AS rn FROM t1",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "rn", ImmutableSet.of()),
                ImmutableMap.of(
                        "a", ImmutableSet.of(),
                        "rn", ImmutableSet.of(
                                indirect(T1, "b", TransformationSubtype.WINDOW),
                                indirect(T1, "c", TransformationSubtype.WINDOW))));
    }

    @Test
    public void testWindowPerColumnWithWhere()
    {
        // WHERE applies to all, WINDOW only to rn
        assertPerColumnLineage(
                "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c) AS rn FROM t1 WHERE d > 5",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "rn", ImmutableSet.of()),
                ImmutableMap.of(
                        "a", ImmutableSet.of(indirect(T1, "d", TransformationSubtype.FILTER)),
                        "rn", ImmutableSet.of(
                                indirect(T1, "d", TransformationSubtype.FILTER),
                                indirect(T1, "b", TransformationSubtype.WINDOW),
                                indirect(T1, "c", TransformationSubtype.WINDOW))));
    }

    @Test
    public void testConditionalPerColumnThroughCte()
    {
        // CASE inside CTE: CONDITIONAL should stay on the correct column after CTE reference
        assertPerColumnLineage(
                "WITH cte AS (SELECT CASE WHEN a > 0 THEN b ELSE c END AS x, d FROM t1) " +
                        "SELECT x, d FROM cte",
                ImmutableMap.of(
                        "x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c")),
                        "d", ImmutableSet.of(direct(T1, "d"))),
                ImmutableMap.of(
                        "x", ImmutableSet.of(indirect(T1, "a", TransformationSubtype.CONDITIONAL)),
                        "d", ImmutableSet.of()));
    }

    @Test
    public void testConditionalPerColumnThroughSubquery()
    {
        // CASE inside aliased subquery
        assertPerColumnLineage(
                "SELECT x, d FROM (SELECT CASE WHEN a > 0 THEN b ELSE c END AS x, d FROM t1) sub",
                ImmutableMap.of(
                        "x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c")),
                        "d", ImmutableSet.of(direct(T1, "d"))),
                ImmutableMap.of(
                        "x", ImmutableSet.of(indirect(T1, "a", TransformationSubtype.CONDITIONAL)),
                        "d", ImmutableSet.of()));
    }

    @Test
    public void testWindowPerColumnThroughCte()
    {
        // WINDOW inside CTE: WINDOW should stay on rn, not leak to a
        assertPerColumnLineage(
                "WITH cte AS (SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c) AS rn FROM t1) " +
                        "SELECT a, rn FROM cte",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "rn", ImmutableSet.of()),
                ImmutableMap.of(
                        "a", ImmutableSet.of(),
                        "rn", ImmutableSet.of(
                                indirect(T1, "b", TransformationSubtype.WINDOW),
                                indirect(T1, "c", TransformationSubtype.WINDOW))));
    }

    @Test
    public void testConditionalPerColumnThroughUnion()
    {
        // CASE in one UNION branch: CONDITIONAL propagates to the output column position
        assertPerColumnLineage(
                "SELECT x, d FROM (" +
                        "  SELECT CASE WHEN a > 0 THEN b ELSE c END AS x, d FROM t1 " +
                        "  UNION ALL " +
                        "  SELECT a AS x, b AS d FROM t2" +
                        ") sub",
                ImmutableMap.of(
                        "x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c"), direct(T2, "a")),
                        "d", ImmutableSet.of(direct(T1, "d"), direct(T2, "b"))),
                ImmutableMap.of(
                        "x", ImmutableSet.of(indirect(T1, "a", TransformationSubtype.CONDITIONAL)),
                        "d", ImmutableSet.of()));
    }

    // =========================================================================
    // 27d. Production query patterns: GROUPING(), nested CASE+WINDOW, aggregate arithmetic
    // =========================================================================

    @Test
    public void testGroupingFunctionAsOutput()
    {
        // GROUPING(a) returns a bitmask — its arguments are the GROUP BY columns.
        // The arguments should be tracked as GROUP_BY (from the GROUP BY clause),
        // but GROUPING() itself references them via the expression, so they appear as direct too.
        assertLineage(
                "SELECT a, SUM(b) AS total, GROUPING(a) AS grp_flag FROM t1 GROUP BY GROUPING SETS(a)",
                ImmutableMap.of(
                        "a", ImmutableSet.of(direct(T1, "a")),
                        "total", ImmutableSet.of(direct(T1, "b")),
                        "grp_flag", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T1, "a", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testCaseInsidePartitionBy()
    {
        // ROW_NUMBER() OVER (PARTITION BY CASE WHEN a IS NOT NULL THEN a ELSE b END, c ORDER BY d)
        // CASE condition (a null-check) gets CONDITIONAL + WINDOW
        // CASE values (a, b) get WINDOW (they're in PARTITION BY)
        // c gets WINDOW, d gets WINDOW
        // Since a appears in both condition and value of the CASE, it stays in WINDOW
        assertIndirectSources(
                "SELECT ROW_NUMBER() OVER (" +
                        "  PARTITION BY CASE WHEN a IS NOT NULL THEN a ELSE b END, c " +
                        "  ORDER BY d" +
                        ") AS rn FROM t1",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL),
                        indirect(T1, "a", TransformationSubtype.WINDOW),
                        indirect(T1, "b", TransformationSubtype.WINDOW),
                        indirect(T1, "c", TransformationSubtype.WINDOW),
                        indirect(T1, "d", TransformationSubtype.WINDOW)));
    }

    @Test
    public void testAggregateArithmetic()
    {
        // COUNT(CASE WHEN a > 0 THEN b END) + COUNT(CASE WHEN c > 0 THEN d END) AS total
        // Both CASE conditions (a, c) are CONDITIONAL
        // Both value columns (b, d) are DIRECT
        // The + operator doesn't introduce additional indirect
        assertLineage(
                "SELECT COUNT(CASE WHEN a > 0 THEN b ELSE NULL END) + " +
                        "COUNT(CASE WHEN c > 0 THEN d ELSE NULL END) AS total FROM t1",
                ImmutableMap.of("total", ImmutableSet.of(direct(T1, "b"), direct(T1, "d"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL),
                        indirect(T1, "c", TransformationSubtype.CONDITIONAL)));
    }

    @Test
    public void testOuterCaseWrappingAggregate()
    {
        // CASE WHEN COUNT(DISTINCT CASE WHEN a > 0 THEN b END) > 0 THEN 1 ELSE 0 END
        // Outer CASE condition is COUNT(...) > 0 — the condition contains an inner CASE.
        // CASE-aware condition collector only picks up inner CASE condition (a), not value (b).
        // Inner CASE value (b) stays as DIRECT source.
        // Outer CASE value branches are constants (1, 0).
        assertLineage(
                "SELECT CASE WHEN COUNT(DISTINCT CASE WHEN a > 0 THEN b ELSE NULL END) > 0 " +
                        "THEN 1 ELSE 0 END AS flag FROM t1 GROUP BY c",
                ImmutableMap.of("flag", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.CONDITIONAL),
                        indirect(T1, "c", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testTwoIndependentCasePerColumn()
    {
        // Two output columns each with their own CASE — CONDITIONAL should be isolated per column.
        // c is DIRECT for x (value branch) AND CONDITIONAL for y (condition). Same column,
        // different roles in different output columns.
        assertPerColumnLineage(
                "SELECT CASE WHEN a > 0 THEN b ELSE c END AS x, " +
                        "CASE WHEN c > 0 THEN d ELSE a END AS y FROM t1",
                ImmutableMap.of(
                        "x", ImmutableSet.of(direct(T1, "b"), direct(T1, "c")),
                        "y", ImmutableSet.of(direct(T1, "d"), direct(T1, "a"))),
                ImmutableMap.of(
                        "x", ImmutableSet.of(indirect(T1, "a", TransformationSubtype.CONDITIONAL)),
                        "y", ImmutableSet.of(indirect(T1, "c", TransformationSubtype.CONDITIONAL))));
    }

    @Test
    public void testAggregateFilterClause()
    {
        // COUNT(b) FILTER (WHERE a > 0) — the FILTER clause column 'a' is included in the
        // function call's source fields by ExpressionAnalyzer, so it appears as DIRECT.
        // This documents current behavior: FILTER (WHERE ...) inside an aggregate is not
        // tracked as INDIRECT/FILTER — it's treated as part of the aggregate expression.
        assertLineage(
                "SELECT COUNT(b) FILTER (WHERE a > 0) AS cnt FROM t1",
                ImmutableMap.of("cnt", ImmutableSet.of(direct(T1, "b"), direct(T1, "a"))),
                ImmutableSet.of());
    }

    @Test
    public void testProductionPatternCtasFullStack()
    {
        // Full production pattern: CTAS + subquery nesting + INNER JOIN + WHERE +
        // COUNT(DISTINCT CASE ...) + GROUPING SETS + GROUPING()
        //
        // Modeled after real experiment analysis queries:
        //   CREATE TABLE AS (
        //     SELECT
        //       COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id ELSE NULL END) AS metric,
        //       c.customer_id AS unit_id,
        //       GROUPING(c.customer_id, c.country) AS grp_flag
        //     FROM (
        //       SELECT order_id, customer_id, status, amount
        //       FROM lineage_orders WHERE amount > 0
        //     ) o
        //     JOIN lineage_customers c ON o.customer_id = c.customer_id
        //     WHERE c.tier = 'premium'
        //     GROUP BY GROUPING SETS(c.customer_id, (c.customer_id, c.country))
        //)
        assertPerColumnLineage(
                "SELECT " +
                        "  COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id ELSE NULL END) AS metric, " +
                        "  c.customer_id AS unit_id, " +
                        "  GROUPING(c.customer_id, c.country) AS grp_flag " +
                        "FROM (" +
                        "  SELECT order_id, customer_id, status, amount " +
                        "  FROM lineage_orders WHERE amount > 0" +
                        ") o " +
                        "JOIN lineage_customers c ON o.customer_id = c.customer_id " +
                        "WHERE c.tier = 'premium' " +
                        "GROUP BY GROUPING SETS(c.customer_id, (c.customer_id, c.country))",
                // Direct sources per column
                ImmutableMap.of(
                        // metric: COUNT(DISTINCT CASE ... THEN order_id ...) — order_id is direct, status is condition-only
                        "metric", ImmutableSet.of(direct(ORDERS, "order_id")),
                        // unit_id: passthrough from customers.customer_id
                        "unit_id", ImmutableSet.of(direct(CUSTOMERS, "customer_id")),
                        // grp_flag: GROUPING() references customer_id and country
                        "grp_flag", ImmutableSet.of(direct(CUSTOMERS, "customer_id"), direct(CUSTOMERS, "country"))),
                // Indirect sources per column
                ImmutableMap.of(
                        "metric", ImmutableSet.of(
                                // CASE condition: status is CONDITIONAL (per-column)
                                indirect(ORDERS, "status", TransformationSubtype.CONDITIONAL),
                                // Subquery WHERE: amount is FILTER (query-level)
                                indirect(ORDERS, "amount", TransformationSubtype.FILTER),
                                // JOIN: both customer_id columns (query-level)
                                indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                                indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                                // Outer WHERE: tier is FILTER (query-level)
                                indirect(CUSTOMERS, "tier", TransformationSubtype.FILTER),
                                // GROUPING SETS: customer_id and country (query-level)
                                indirect(CUSTOMERS, "customer_id", TransformationSubtype.GROUP_BY),
                                indirect(CUSTOMERS, "country", TransformationSubtype.GROUP_BY)),
                        "unit_id", ImmutableSet.of(
                                // No CONDITIONAL or WINDOW (per-column) — only query-level indirect
                                indirect(ORDERS, "amount", TransformationSubtype.FILTER),
                                indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                                indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                                indirect(CUSTOMERS, "tier", TransformationSubtype.FILTER),
                                indirect(CUSTOMERS, "customer_id", TransformationSubtype.GROUP_BY),
                                indirect(CUSTOMERS, "country", TransformationSubtype.GROUP_BY)),
                        "grp_flag", ImmutableSet.of(
                                // Same query-level indirect as unit_id
                                indirect(ORDERS, "amount", TransformationSubtype.FILTER),
                                indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                                indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                                indirect(CUSTOMERS, "tier", TransformationSubtype.FILTER),
                                indirect(CUSTOMERS, "customer_id", TransformationSubtype.GROUP_BY),
                                indirect(CUSTOMERS, "country", TransformationSubtype.GROUP_BY))));
    }

    // =========================================================================
    // 28. UNION then outer clauses
    // =========================================================================

    @Test
    public void testUnionThenOuterWhere()
    {
        // Outer WHERE on union subquery resolves to source columns from both branches
        assertLineage(
                "SELECT a FROM (SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2) sub WHERE b > 5",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"), direct(T2, "a"))),
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testUnionThenOuterGroupByAndOrderBy()
    {
        assertIndirectSources(
                "SELECT a, SUM(b) AS total FROM (" +
                        "  SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2" +
                        ") sub GROUP BY a ORDER BY total",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T2, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.SORT),
                        indirect(T2, "b", TransformationSubtype.SORT)));
    }

    @Test
    public void testUnionWithFiltersThenOuterFilter()
    {
        // Each branch has its own filter, then outer adds another
        assertIndirectSources(
                "SELECT a FROM (" +
                        "  SELECT a, b FROM t1 WHERE c > 1 UNION ALL SELECT a, b FROM t2 WHERE b > 2" +
                        ") sub WHERE b > 3",
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER),
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 29. CTE referenced in multiple different contexts
    // =========================================================================

    @Test
    public void testCteReferencedWithDifferentRoles()
    {
        // Same CTE used as both sides of a join with different aliases, plus WHERE
        assertIndirectSources(
                "WITH cte AS (SELECT a, b FROM t1 WHERE c > 1) " +
                        "SELECT x.a FROM cte x JOIN cte y ON x.a = y.b WHERE y.a > 5",
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T1, "b", TransformationSubtype.JOIN),
                        indirect(T1, "a", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 30. NOT IN subquery
    // =========================================================================

    @Test
    public void testNotInSubquery()
    {
        assertLineage(
                "SELECT a FROM t1 WHERE b NOT IN (SELECT a FROM t2 WHERE b > 5)",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER),
                        indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 31. Subquery in HAVING
    // =========================================================================

    @Test
    public void testScalarSubqueryInHaving()
    {
        assertIndirectSources(
                "SELECT a, COUNT(*) AS cnt FROM t1 GROUP BY a HAVING COUNT(*) > (SELECT AVG(b) FROM t2)",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T2, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testCorrelatedSubqueryInHaving()
    {
        assertIndirectSources(
                "SELECT a, SUM(b) AS total FROM t1 GROUP BY a " +
                        "HAVING SUM(b) > (SELECT AVG(a) FROM t2 WHERE t2.a = t1.a)",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "b", TransformationSubtype.FILTER),
                        indirect(T2, "a", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 32. Multi-level CTE with aggregation at each level
    // =========================================================================

    @Test
    public void testMultiLevelCteAggregation()
    {
        // Level 1: filter, Level 2: group by + aggregate
        assertLineage(
                "WITH daily AS (" +
                        "  SELECT a, SUM(b) AS total FROM t1 WHERE c > 1 GROUP BY a" +
                        "), " +
                        "summary AS (" +
                        "  SELECT SUM(total) AS grand_total FROM daily WHERE a > 10" +
                        ") " +
                        "SELECT grand_total FROM summary",
                ImmutableMap.of("grand_total", ImmutableSet.of(direct(T1, "b"))),
                ImmutableSet.of(
                        indirect(T1, "c", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.GROUP_BY),
                        indirect(T1, "a", TransformationSubtype.FILTER)));
    }

    @Test
    public void testThreeLevelCteChain()
    {
        // Level 1: filter, Level 2: join, Level 3: group by
        assertIndirectSources(
                "WITH step1 AS (" +
                        "  SELECT order_id, customer_id, amount FROM lineage_orders WHERE status = 'active'" +
                        "), " +
                        "step2 AS (" +
                        "  SELECT s.order_id, c.name, s.amount " +
                        "  FROM step1 s JOIN lineage_customers c ON s.customer_id = c.customer_id" +
                        "), " +
                        "step3 AS (" +
                        "  SELECT name, SUM(amount) AS total FROM step2 GROUP BY name" +
                        ") " +
                        "SELECT name, total FROM step3",
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY)));
    }

    // =========================================================================
    // 33. Views
    // =========================================================================

    @Test
    public void testViewDirectLineage()
    {
        // view_invoker1 is defined as "SELECT a, b, c FROM t1"
        // Direct lineage traces through the view to the underlying table
        assertLineage("SELECT a FROM view_invoker1",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of());
    }

    @Test
    public void testViewWithOuterWhere()
    {
        // WHERE on view columns traces back to t1
        assertLineage("SELECT a FROM view_invoker1 WHERE b > 5",
                ImmutableMap.of("a", ImmutableSet.of(direct(T1, "a"))),
                ImmutableSet.of(indirect(T1, "b", TransformationSubtype.FILTER)));
    }

    @Test
    public void testViewJoinedWithTable()
    {
        // View joined with base table
        assertIndirectSources(
                "SELECT v.a, t2.b FROM view_invoker1 v JOIN t2 ON v.a = t2.a WHERE v.c > 5",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.JOIN),
                        indirect(T2, "a", TransformationSubtype.JOIN),
                        indirect(T1, "c", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 34. Multiple ORDER BY columns
    // =========================================================================

    @Test
    public void testMultipleOrderByColumns()
    {
        assertIndirectSources("SELECT c FROM t1 ORDER BY a, b",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.SORT),
                        indirect(T1, "b", TransformationSubtype.SORT)));
    }

    @Test
    public void testOrderByExpression()
    {
        assertIndirectSources("SELECT c FROM t1 ORDER BY a + b",
                ImmutableSet.of(
                        indirect(T1, "a", TransformationSubtype.SORT),
                        indirect(T1, "b", TransformationSubtype.SORT)));
    }

    // =========================================================================
    // 36. Scalar subquery in SELECT
    // =========================================================================

    @Test
    public void testScalarSubqueryInSelect()
    {
        // Scalar subquery: (SELECT MAX(b) FROM t2 WHERE t2.a = t1.a)
        // Direct: max_b comes from t2.b
        // The correlated WHERE is inside the scalar subquery, affecting that column's value
        assertIndirectSources(
                "SELECT a, (SELECT MAX(b) FROM t2 WHERE t2.a = t1.a) AS max_b FROM t1",
                ImmutableSet.of(
                        indirect(T2, "a", TransformationSubtype.FILTER),
                        indirect(T1, "a", TransformationSubtype.FILTER)));
    }

    // =========================================================================
    // 37. Production archetype coverage
    //
    // Tests matching the exact structural patterns found in 62 production queries.
    // Each test covers a gap where individual features were tested but not combined.
    // =========================================================================

    @Test
    public void testCteMultiJoinGroupingSetsFullJoin()
    {
        // Archetype: single-CTE + multi-JOIN + GROUPING_SETS + FULL_JOIN (8 production queries)
        assertIndirectSources(
                "WITH order_metrics AS (" +
                        "  SELECT customer_id, SUM(amount) AS total FROM lineage_orders WHERE status = 'completed' " +
                        "  GROUP BY GROUPING SETS(customer_id)" +
                        ") " +
                        "SELECT COALESCE(o.customer_id, c.customer_id) AS id, o.total, c.name " +
                        "FROM order_metrics o " +
                        "FULL OUTER JOIN lineage_customers c ON o.customer_id = c.customer_id",
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.GROUP_BY),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN)));
    }

    @Test
    public void testMultiCteMultiJoinGroupingSetsFullJoin()
    {
        // Archetype: multi-CTE + multi-JOIN + GROUPING_SETS + FULL_JOIN (4 production queries)
        assertIndirectSources(
                "WITH order_agg AS (" +
                        "  SELECT customer_id, " +
                        "    COUNT(DISTINCT CASE WHEN status = 'completed' THEN order_id ELSE NULL END) AS completed " +
                        "  FROM lineage_orders WHERE amount > 0 " +
                        "  GROUP BY GROUPING SETS(customer_id)" +
                        "), " +
                        "product_agg AS (" +
                        "  SELECT order_id, SUM(price) AS total_price FROM lineage_products " +
                        "  GROUP BY GROUPING SETS(order_id)" +
                        ") " +
                        "SELECT o.customer_id, o.completed, c.name " +
                        "FROM order_agg o " +
                        "FULL OUTER JOIN lineage_customers c ON o.customer_id = c.customer_id " +
                        "LEFT JOIN lineage_orders lo ON o.customer_id = lo.customer_id",
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.CONDITIONAL),
                        indirect(ORDERS, "amount", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.GROUP_BY),
                        indirect(PRODUCTS, "order_id", TransformationSubtype.GROUP_BY),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN)));
    }

    @Test
    public void testCteMultiJoinGroupingSetsUnion()
    {
        // Archetype: single-CTE + multi-JOIN + GROUPING_SETS + UNION (4 production queries)
        assertIndirectSources(
                "WITH metrics AS (" +
                        "  SELECT customer_id, amount FROM lineage_orders WHERE region = 'US' " +
                        "  UNION ALL " +
                        "  SELECT customer_id, amount FROM lineage_orders WHERE region = 'EU'" +
                        ") " +
                        "SELECT c.name, SUM(m.amount) AS total " +
                        "FROM metrics m " +
                        "JOIN lineage_customers c ON m.customer_id = c.customer_id " +
                        "GROUP BY GROUPING SETS(c.name)",
                ImmutableSet.of(
                        indirect(ORDERS, "region", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testMultiCteMultiJoinGroupingSets()
    {
        // Archetype: multi-CTE + multi-JOIN + GROUPING_SETS (4 production queries)
        assertIndirectSources(
                "WITH filtered_orders AS (" +
                        "  SELECT order_id, customer_id, amount FROM lineage_orders WHERE status = 'active'" +
                        "), " +
                        "enriched AS (" +
                        "  SELECT fo.order_id, fo.amount, p.category " +
                        "  FROM filtered_orders fo " +
                        "  JOIN lineage_products p ON fo.order_id = p.order_id" +
                        ") " +
                        "SELECT c.name, SUM(e.amount) AS total " +
                        "FROM enriched e " +
                        "JOIN lineage_customers c ON e.order_id = c.customer_id " +
                        "GROUP BY GROUPING SETS(c.name)",
                ImmutableSet.of(
                        indirect(ORDERS, "status", TransformationSubtype.FILTER),
                        indirect(ORDERS, "order_id", TransformationSubtype.JOIN),
                        indirect(PRODUCTS, "order_id", TransformationSubtype.JOIN),
                        indirect(ORDERS, "order_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testMultiCteMultiJoinGroupingSetsUnion()
    {
        // Archetype: multi-CTE + multi-JOIN + GROUPING_SETS + UNION (3 production queries)
        assertIndirectSources(
                "WITH us_data AS (" +
                        "  SELECT customer_id, amount FROM lineage_orders WHERE region = 'US'" +
                        "), " +
                        "eu_data AS (" +
                        "  SELECT customer_id, amount FROM lineage_orders WHERE region = 'EU'" +
                        ") " +
                        "SELECT c.name, SUM(combined.amount) AS total " +
                        "FROM (" +
                        "  SELECT customer_id, amount FROM us_data " +
                        "  UNION ALL " +
                        "  SELECT customer_id, amount FROM eu_data" +
                        ") combined " +
                        "JOIN lineage_customers c ON combined.customer_id = c.customer_id " +
                        "GROUP BY GROUPING SETS(c.name)",
                ImmutableSet.of(
                        indirect(ORDERS, "region", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY)));
    }

    @Test
    public void testCteMultiJoinGroupingSetsNoFullJoin()
    {
        // Archetype: single-CTE + multi-JOIN + GROUPING_SETS (3 production queries)
        assertIndirectSources(
                "WITH order_data AS (" +
                        "  SELECT order_id, customer_id, amount FROM lineage_orders WHERE amount IS NOT NULL" +
                        ") " +
                        "SELECT c.name, SUM(p.price) AS total_price " +
                        "FROM order_data o " +
                        "JOIN lineage_customers c ON o.customer_id = c.customer_id " +
                        "JOIN lineage_products p ON o.order_id = p.order_id " +
                        "WHERE c.tier = 'premium' " +
                        "GROUP BY GROUPING SETS(c.name)",
                ImmutableSet.of(
                        indirect(ORDERS, "amount", TransformationSubtype.FILTER),
                        indirect(ORDERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "customer_id", TransformationSubtype.JOIN),
                        indirect(ORDERS, "order_id", TransformationSubtype.JOIN),
                        indirect(PRODUCTS, "order_id", TransformationSubtype.JOIN),
                        indirect(CUSTOMERS, "tier", TransformationSubtype.FILTER),
                        indirect(CUSTOMERS, "name", TransformationSubtype.GROUP_BY)));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static SourceColumn direct(QualifiedObjectName table, String column)
    {
        return new SourceColumn(table, column);
    }

    private static ColumnLineageEntry indirect(QualifiedObjectName table, String column, TransformationSubtype subtype)
    {
        return new ColumnLineageEntry(table, column, TransformationType.INDIRECT, subtype);
    }

    private void assertLineage(
            @Language("SQL") String query,
            Map<String, Set<SourceColumn>> expectedDirectPerColumn,
            Set<ColumnLineageEntry> expectedIndirect)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(CLIENT_SESSION, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata, WarningCollector.NOOP, Optional.empty(), query);
                    Statement statement = SQL_PARSER.createStatement(query);
                    Analysis analysis = analyzer.analyzeSemantic(statement, false);

                    // Verify direct lineage per output column
                    Collection<Field> outputFields = analysis.getOutputDescriptor().getVisibleFields();
                    Map<String, Set<SourceColumn>> actualDirectPerColumn = new LinkedHashMap<>();
                    // Collect union of all per-column indirect sources
                    ImmutableSet.Builder<ColumnLineageEntry> allIndirectBuilder = ImmutableSet.builder();
                    for (Field field : outputFields) {
                        String name = field.getName().orElse("?");
                        actualDirectPerColumn.put(name, analysis.getSourceColumns(field));
                        allIndirectBuilder.addAll(analysis.getAllIndirectSourcesForField(field));
                    }
                    assertEquals(actualDirectPerColumn, expectedDirectPerColumn,
                            "Direct sources mismatch for: " + query);
                    assertEquals(allIndirectBuilder.build(), expectedIndirect,
                            "Indirect sources mismatch for: " + query);
                });
    }

    /**
     * Assert per-column indirect lineage (query-level + per-field merged per column).
     */
    private void assertPerColumnLineage(
            @Language("SQL") String query,
            Map<String, Set<SourceColumn>> expectedDirectPerColumn,
            Map<String, Set<ColumnLineageEntry>> expectedIndirectPerColumn)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(CLIENT_SESSION, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata, WarningCollector.NOOP, Optional.empty(), query);
                    Statement statement = SQL_PARSER.createStatement(query);
                    Analysis analysis = analyzer.analyzeSemantic(statement, false);

                    Collection<Field> outputFields = analysis.getOutputDescriptor().getVisibleFields();
                    Map<String, Set<SourceColumn>> actualDirectPerColumn = new LinkedHashMap<>();
                    Map<String, Set<ColumnLineageEntry>> actualIndirectPerColumn = new LinkedHashMap<>();
                    for (Field field : outputFields) {
                        String name = field.getName().orElse("?");
                        actualDirectPerColumn.put(name, analysis.getSourceColumns(field));
                        actualIndirectPerColumn.put(name, analysis.getAllIndirectSourcesForField(field));
                    }
                    assertEquals(actualDirectPerColumn, expectedDirectPerColumn,
                            "Direct sources mismatch for: " + query);
                    assertEquals(actualIndirectPerColumn, expectedIndirectPerColumn,
                            "Per-column indirect sources mismatch for: " + query);
                });
    }

    private void assertIndirectSources(@Language("SQL") String query, Set<ColumnLineageEntry> expected)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(CLIENT_SESSION, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata, WarningCollector.NOOP, Optional.empty(), query);
                    Statement statement = SQL_PARSER.createStatement(query);
                    Analysis analysis = analyzer.analyzeSemantic(statement, false);
                    // Check union of all per-column indirect sources
                    ImmutableSet.Builder<ColumnLineageEntry> allIndirect = ImmutableSet.builder();
                    for (Field field : analysis.getOutputDescriptor().getVisibleFields()) {
                        allIndirect.addAll(analysis.getAllIndirectSourcesForField(field));
                    }
                    assertEquals(allIndirect.build(), expected,
                            "Indirect sources mismatch for: " + query);
                });
    }
}
