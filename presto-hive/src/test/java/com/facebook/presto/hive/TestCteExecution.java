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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.QueryAssertions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS_ENABLED;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static io.airlift.tpch.TpchTable.REGION;
import static io.airlift.tpch.TpchTable.SUPPLIER;

@Test(singleThreaded = true)
public class TestCteExecution
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, CUSTOMER, LINE_ITEM, PART_SUPPLIER, NATION, REGION, PART, SUPPLIER),
                ImmutableMap.of(
                        "query.cte-partitioning-provider-catalog", "hive"),
                "sql-standard",
                ImmutableMap.of("hive.pushdown-filter-enabled", "true",
                        "hive.enable-parquet-dereference-pushdown", "true"),
                Optional.empty());
    }

    @Test
    public void testSimplePersistentCte()
    {
        QueryRunner queryRunner = getQueryRunner();
        compareResults(queryRunner.execute(getMaterializedSession(),
                        "WITH  temp as (SELECT orderkey FROM ORDERS) " +
                                "SELECT * FROM temp t1 "),
                queryRunner.execute(getSession(),
                        "WITH  temp as (SELECT orderkey FROM ORDERS) " +
                                "SELECT * FROM temp t1 "));
    }

    @Test
    public void testPersistentCteWithTimeStampWithTimeZoneType()
    {
        String testQuery = "WITH cte AS (" +
                "  SELECT ts FROM (VALUES " +
                "    (CAST('2023-01-01 00:00:00.000 UTC' AS TIMESTAMP WITH TIME ZONE)), " +
                "    (CAST('2023-06-01 12:00:00.000 UTC' AS TIMESTAMP WITH TIME ZONE)), " +
                "    (CAST('2023-12-31 23:59:59.999 UTC' AS TIMESTAMP WITH TIME ZONE))" +
                "  ) AS t(ts)" +
                ")" +
                "SELECT ts FROM cte";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(queryRunner.execute(getMaterializedSession(),
                        testQuery),
                queryRunner.execute(getSession(),
                        testQuery));
    }

    @Test
    public void testPersistentCteWithStructTypes()
    {
        String testQuery = "WITH temp AS (" +
                "  SELECT * FROM (VALUES " +
                "    (CAST(ROW('example_status', 100) AS ROW(status VARCHAR, amount INTEGER)), 1)," +
                "    (CAST(ROW('another_status', 200) AS ROW(status VARCHAR, amount INTEGER)), 2)" +
                "  ) AS t (order_details, orderkey)" +
                ") SELECT * FROM temp";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(queryRunner.execute(getMaterializedSession(),
                        testQuery),
                queryRunner.execute(getSession(),
                        testQuery));
    }

    @Test
    public void testCteWithZeroLengthVarchar()
    {
        String testQuery = "WITH temp AS (" +
                "  SELECT * FROM (VALUES " +
                "    (CAST('' AS VARCHAR(0)), 9)" +
                "  ) AS t (text_column, number_column)" +
                ") SELECT * FROM temp";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(queryRunner.execute(getMaterializedSession(),
                        testQuery),
                queryRunner.execute(getSession(),
                        testQuery));
    }

    @Test
    public void testDependentPersistentCtes()
    {
        String testQuery = "WITH  cte1 AS (SELECT orderkey FROM ORDERS WHERE orderkey < 100), " +
                "      cte2 AS (SELECT * FROM cte1 WHERE orderkey > 50) " +
                "SELECT * FROM cte2";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testMultipleIndependentPersistentCtes()
    {
        String testQuery = "WITH  cte1 AS (SELECT orderkey FROM ORDERS WHERE orderkey < 100), " +
                "      cte2 AS (SELECT custkey FROM CUSTOMER WHERE custkey < 50) " +
                "SELECT * FROM cte1, cte2 WHERE cte1.orderkey = cte2.custkey";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testNestedPersistentCtes()
    {
        String testQuery = "WITH  cte1 AS (" +
                "   SELECT orderkey FROM ORDERS WHERE orderkey IN " +
                "       (WITH  cte2 AS (SELECT orderkey FROM ORDERS WHERE orderkey < 100) " +
                "        SELECT orderkey FROM cte2 WHERE orderkey > 50)" +
                ") SELECT * FROM cte1";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testRefinedCtesOutsideScope()
    {
        String testQuery = "WITH  cte1 AS ( WITH cte2 as (SELECT orderkey FROM ORDERS WHERE orderkey < 100)" +
                "SELECT * FROM cte2), " +
                " cte2 AS (SELECT * FROM customer WHERE custkey < 50) " +
                "SELECT * FROM cte2  JOIN cte1 ON true";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testRedefinedCteWithSameDefinitionDifferentBase()
    {
        String testQuery = "SELECT (with test_base AS (SELECT colB FROM (VALUES (1)) AS TempTable(colB)), \n" +
                "test_cte as (  SELECT colB FROM test_base)\n" +
                "SELECT * FROM test_cte\n" +
                "),\n" +
                "(WITH test_base AS (\n" +
                "    SELECT text_column\n" +
                "    FROM (VALUES ('Some Text', 9)) AS t (text_column, number_column)\n" +
                "), \n" +
                "test_cte AS (\n" +
                "    SELECT * FROM test_base\n" +
                ")\n" +
                "SELECT  CONCAT(text_column , 'XYZ') FROM test_cte\n" +
                ")\n";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testComplexRefinedCtesOutsideScope()
    {
        String testQuery = "WITH " +
                "cte1 AS ( " +
                "   SELECT orderkey, totalprice FROM ORDERS WHERE orderkey < 100 " +
                "), " +
                "cte2 AS ( " +
                "   WITH cte3 AS ( WITH cte4 AS (SELECT orderkey, totalprice FROM cte1 WHERE totalprice > 1000) SELECT * FROM cte4) " +
                "   SELECT cte3.orderkey FROM cte3 " +
                "), " +
                "cte3 AS ( " +
                "   SELECT * FROM customer WHERE custkey < 50 " +
                ") " +
                "SELECT cte3.*, cte2.orderkey FROM cte3 JOIN cte2 ON cte3.custkey = cte2.orderkey";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testChainedPersistentCtes()
    {
        String testQuery = "WITH  cte1 AS (SELECT orderkey FROM ORDERS WHERE orderkey < 100), " +
                "      cte2 AS (SELECT orderkey FROM cte1 WHERE orderkey > 50), " +
                "     cte3 AS (SELECT orderkey FROM cte2 WHERE orderkey < 75) " +
                "SELECT * FROM cte3";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testSimplePersistentCteWithJoinInCteDef()
    {
        String testQuery = "WITH  temp as " +
                "(SELECT * FROM ORDERS o1 " +
                "JOIN ORDERS o2 ON o1.orderkey = o2.orderkey) " +
                "SELECT * FROM temp t1 ";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testSimplePersistentCteMultipleUses()
    {
        String testQuery = " WITH  temp as" +
                " (SELECT * FROM ORDERS) " +
                "SELECT * FROM temp t1 JOIN temp t2 on " +
                "t1.orderkey = t2.orderkey WHERE t1.orderkey < 10";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testPersistentCteMultipleColumns()
    {
        String testQuery = " WITH  temp as (SELECT * FROM ORDERS) " +
                "SELECT * FROM temp t1";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testJoinAndAggregationWithPersistentCtes()
    {
        String testQuery = "WITH  cte1 AS (" +
                "   SELECT orderkey, COUNT(*) as item_count FROM lineitem" +
                "   GROUP BY orderkey)," +
                "    cte2 AS (" +
                "   SELECT c.custkey, c.name FROM CUSTOMER c" +
                "   WHERE c.mktsegment = 'BUILDING')" +
                "   SELECT * FROM cte1" +
                "   JOIN cte2 ON cte1.orderkey = cte2.custkey";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testNestedPersistentCtes2()
    {
        String testQuery = "WITH  cte1 AS (" +
                "   WITH  cte2 AS (" +
                "       SELECT nationkey FROM NATION" +
                "       WHERE regionkey = 1)" +
                "   SELECT * FROM cte2" +
                "   WHERE nationkey < 5)" +
                "SELECT * FROM cte1";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testPersistentCteWithUnion()
    {
        String testQuery = "WITH  cte AS (" +
                "   SELECT orderkey FROM ORDERS WHERE orderkey < 100" +
                "   UNION" +
                "   SELECT orderkey FROM ORDERS WHERE orderkey > 500)" +
                "SELECT * FROM cte";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testPersistentCteWithSelfJoin()
    {
        String testQuery = "WITH  cte AS (" +
                "   SELECT * FROM ORDERS)" +
                "SELECT * FROM cte c1" +
                " JOIN cte c2 ON c1.orderkey = c2.orderkey WHERE c1.orderkey < 100";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testPersistentCteWithWindowFunction()
    {
        String testQuery = "WITH cte AS (" +
                "   SELECT *, ROW_NUMBER() OVER(PARTITION BY orderstatus ORDER BY orderkey) as row" +
                "   FROM ORDERS)" +
                "SELECT * FROM cte WHERE row <= 5";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testPersistentCteWithMultipleDependentSubCtes()
    {
        String testQuery = "WITH  cte1 AS (" +
                "   SELECT * FROM ORDERS)," +
                "     cte2 AS (SELECT * FROM cte1 WHERE orderkey < 100)," +
                "     cte3 AS (SELECT * FROM cte1 WHERE orderkey >= 100)" +
                "SELECT * FROM cte2 UNION ALL SELECT * FROM cte3";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testTopCustomersByOrderValue()
    {
        String testQuery = "WITH  cte AS (" +
                "   SELECT c.custkey, c.name, SUM(o.totalprice) as total_spent " +
                "   FROM CUSTOMER c JOIN ORDERS o ON c.custkey = o.custkey " +
                "   GROUP BY c.custkey, c.name)" +
                "SELECT * FROM cte " +
                "ORDER BY total_spent DESC " +
                "LIMIT 5";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery), true);
    }

    @Test
    public void testSupplierDataAnalysis()
    {
        String testQuery = "WITH cte AS (" +
                "   SELECT s.suppkey, s.name, n.name as nation, r.name as region, ROUND(SUM(ps.supplycost), 8)  as total_supply_cost " +
                "   FROM partsupp ps JOIN SUPPLIER s ON ps.suppkey = s.suppkey " +
                "                        JOIN NATION n ON s.nationkey = n.nationkey " +
                "                        JOIN REGION r ON n.regionkey = r.regionkey " +
                "   GROUP BY s.suppkey, s.name, n.name, r.name) " +
                "SELECT * FROM cte " +
                "WHERE total_supply_cost > 1000";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testCustomerOrderPatternAnalysis()
    {
        String testQuery = "WITH  cte AS (" +
                "   SELECT c.name as customer_name, r.name as region_name, EXTRACT(year FROM o.orderdate) as order_year, COUNT(*) as order_count " +
                "   FROM CUSTOMER c JOIN ORDERS o ON c.custkey = o.custkey " +
                "                  JOIN NATION n ON c.nationkey = n.nationkey " +
                "                  JOIN REGION r ON n.regionkey = r.regionkey " +
                "   GROUP BY c.name, r.name, EXTRACT(year FROM o.orderdate)) " +
                "SELECT * FROM cte " +
                "ORDER BY customer_name, order_year";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testLowStockAnalysis()
    {
        String testQuery = "WITH cte AS (" +
                "   SELECT p.partkey, p.name, p.type, SUM(ps.availqty) as total_qty " +
                "   FROM PART p JOIN partsupp ps ON p.partkey = ps.partkey " +
                "   GROUP BY p.partkey, p.name, p.type) " +
                "SELECT * FROM cte " +
                "WHERE total_qty < 100";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testComplexChainOfDependentAndNestedPersistentCtes()
    {
        String testQuery = "WITH  " +
                "    cte1 AS (" +
                "        SELECT * FROM ORDERS WHERE orderkey < 1000" +
                "    )," +
                "    cte2 AS (" +
                "        SELECT * FROM cte1 WHERE custkey < 500" +
                "    )," +
                "     cte3 AS (" +
                "        SELECT cte2.*, cte1.totalprice AS cte1_totalprice " +
                "        FROM cte2 " +
                "        JOIN cte1 ON cte2.orderkey = cte1.orderkey " +
                "        WHERE cte1.totalprice < 150000" +
                "    )," +
                "     cte4 AS (" +
                "        SELECT * FROM cte3 WHERE orderstatus = 'O'" +
                "    )," +
                "    cte5 AS (" +
                "        SELECT orderkey FROM cte4 WHERE cte1_totalprice < 100000" +
                "    )," +
                "    cte6 AS (" +
                "        SELECT * FROM cte5, LATERAL (" +
                "            SELECT * FROM cte2 WHERE cte2.orderkey = cte5.orderkey" +
                "        ) x" +
                "    )" +
                "SELECT * FROM cte6";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testComplexQuery1()
    {
        String testQuery = "WITH  customer_nation AS (" +
                "   SELECT c.custkey, c.name, n.name AS nation_name, r.name AS region_name " +
                "   FROM CUSTOMER c " +
                "   JOIN NATION n ON c.nationkey = n.nationkey " +
                "   JOIN REGION r ON n.regionkey = r.regionkey), " +
                " customer_orders AS (" +
                "   SELECT co.custkey, co.name, co.nation_name, co.region_name, o.orderkey, o.orderdate " +
                "   FROM customer_nation co " +
                "   JOIN ORDERS o ON co.custkey = o.custkey), " +
                "order_lineitems AS (" +
                "   SELECT co.*, l.partkey, l.quantity, l.extendedprice " +
                "   FROM customer_orders co " +
                "   JOIN lineitem l ON co.orderkey = l.orderkey), " +
                " customer_part_analysis AS (" +
                "   SELECT ol.*, p.name AS part_name, p.type AS part_type " +
                "   FROM order_lineitems ol " +
                "   JOIN PART p ON ol.partkey = p.partkey) " +
                "SELECT * FROM customer_part_analysis " +
                "WHERE region_name = 'AMERICA' " +
                "ORDER BY nation_name, custkey, orderdate";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testComplexQuery2()
    {
        String testQuery = "WITH  supplier_region AS (" +
                "   SELECT s.suppkey, s.name AS supplier_name, n.name AS nation_name, r.name AS region_name " +
                "   FROM SUPPLIER s " +
                "   JOIN NATION n ON s.nationkey = n.nationkey " +
                "   JOIN REGION r ON n.regionkey = r.regionkey), " +
                " supplier_parts AS (" +
                "   SELECT sr.*, ps.partkey, ps.availqty, ps.supplycost " +
                "   FROM supplier_region sr " +
                "   JOIN partsupp ps ON sr.suppkey = ps.suppkey), " +
                "parts_info AS (" +
                "   SELECT sp.*, p.name AS part_name, p.type AS part_type, p.size AS part_size " +
                "   FROM supplier_parts sp " +
                "   JOIN PART p ON sp.partkey = p.partkey), " +
                " full_supplier_part_info AS (" +
                "   SELECT pi.*, n.comment AS nation_comment, r.comment AS region_comment " +
                "   FROM parts_info pi " +
                "   JOIN NATION n ON pi.nation_name = n.name " +
                "   JOIN REGION r ON pi.region_name = r.name) " +
                "SELECT * FROM full_supplier_part_info " +
                "WHERE part_type LIKE '%BRASS' " +
                "ORDER BY region_name, supplier_name";
        QueryRunner queryRunner = getQueryRunner();
        compareResults(
                queryRunner.execute(getMaterializedSession(), testQuery),
                queryRunner.execute(getSession(), testQuery));
    }

    @Test
    public void testSimplePersistentCteForCtasQueries()
    {
        QueryRunner queryRunner = getQueryRunner();

        // Create tables with Ctas
        queryRunner.execute(getMaterializedSession(),
                "CREATE TABLE persistent_table as (WITH  temp as (SELECT orderkey FROM ORDERS) " +
                        "SELECT * FROM temp t1 )");
        queryRunner.execute(getSession(),
                "CREATE TABLE non_persistent_table as (WITH  temp as (SELECT orderkey FROM ORDERS) " +
                        "SELECT * FROM temp t1) ");

        // Compare contents with a select
        compareResults(queryRunner.execute(getSession(),
                        "SELECT * FROM persistent_table"),
                queryRunner.execute(getSession(),
                        "SELECT * FROM non_persistent_table"));

        // drop tables
        queryRunner.execute(getSession(),
                "DROP TABLE persistent_table");
        queryRunner.execute(getSession(),
                "DROP TABLE non_persistent_table");
    }

    @Test
    public void testComplexPersistentCteForCtasQueries()
    {
        QueryRunner queryRunner = getQueryRunner();
        // Create tables with Ctas
        queryRunner.execute(getMaterializedSession(),
                "CREATE TABLE persistent_table as ( " +
                        "WITH  supplier_region AS (" +
                        "   SELECT s.suppkey, s.name AS supplier_name, n.name AS nation_name, r.name AS region_name " +
                        "   FROM SUPPLIER s " +
                        "   JOIN NATION n ON s.nationkey = n.nationkey " +
                        "   JOIN REGION r ON n.regionkey = r.regionkey), " +
                        " supplier_parts AS (" +
                        "   SELECT sr.*, ps.partkey, ps.availqty, ps.supplycost " +
                        "   FROM supplier_region sr " +
                        "   JOIN partsupp ps ON sr.suppkey = ps.suppkey), " +
                        "parts_info AS (" +
                        "   SELECT sp.*, p.name AS part_name, p.type AS part_type, p.size AS part_size " +
                        "   FROM supplier_parts sp " +
                        "   JOIN PART p ON sp.partkey = p.partkey), " +
                        " full_supplier_part_info AS (" +
                        "   SELECT pi.*, n.comment AS nation_comment, r.comment AS region_comment " +
                        "   FROM parts_info pi " +
                        "   JOIN NATION n ON pi.nation_name = n.name " +
                        "   JOIN REGION r ON pi.region_name = r.name) " +
                        "SELECT * FROM full_supplier_part_info " +
                        "WHERE part_type LIKE '%BRASS' " +
                        "ORDER BY region_name, supplier_name)");
        queryRunner.execute(getSession(),
                "CREATE TABLE non_persistent_table as ( " +
                        "WITH  supplier_region AS (" +
                        "   SELECT s.suppkey, s.name AS supplier_name, n.name AS nation_name, r.name AS region_name " +
                        "   FROM SUPPLIER s " +
                        "   JOIN NATION n ON s.nationkey = n.nationkey " +
                        "   JOIN REGION r ON n.regionkey = r.regionkey), " +
                        " supplier_parts AS (" +
                        "   SELECT sr.*, ps.partkey, ps.availqty, ps.supplycost " +
                        "   FROM supplier_region sr " +
                        "   JOIN partsupp ps ON sr.suppkey = ps.suppkey), " +
                        "parts_info AS (" +
                        "   SELECT sp.*, p.name AS part_name, p.type AS part_type, p.size AS part_size " +
                        "   FROM supplier_parts sp " +
                        "   JOIN PART p ON sp.partkey = p.partkey), " +
                        " full_supplier_part_info AS (" +
                        "   SELECT pi.*, n.comment AS nation_comment, r.comment AS region_comment " +
                        "   FROM parts_info pi " +
                        "   JOIN NATION n ON pi.nation_name = n.name " +
                        "   JOIN REGION r ON pi.region_name = r.name) " +
                        "SELECT * FROM full_supplier_part_info " +
                        "WHERE part_type LIKE '%BRASS' " +
                        "ORDER BY region_name, supplier_name)");

        // Compare contents with a select
        compareResults(queryRunner.execute(getSession(),
                        "SELECT * FROM persistent_table"),
                queryRunner.execute(getSession(),
                        "SELECT * FROM non_persistent_table"));

        // drop tables
        queryRunner.execute(getSession(),
                "DROP TABLE persistent_table");
        queryRunner.execute(getSession(),
                "DROP TABLE non_persistent_table");
    }

    @Test
    public void testSimplePersistentCteForInsertQueries()
    {
        QueryRunner queryRunner = getQueryRunner();

        // Create tables without data
        queryRunner.execute(getSession(),
                "CREATE TABLE persistent_table (orderkey BIGINT)");
        queryRunner.execute(getSession(),
                "CREATE TABLE non_persistent_table (orderkey BIGINT)");

        // Insert data into tables using CTEs
        queryRunner.execute(getMaterializedSession(),
                "INSERT INTO persistent_table " +
                        "WITH  temp AS (SELECT orderkey FROM ORDERS) " +
                        "SELECT * FROM temp");
        queryRunner.execute(getSession(),
                "INSERT INTO non_persistent_table " +
                        "WITH temp AS (SELECT orderkey FROM ORDERS) " +
                        "SELECT * FROM temp");

        // Compare contents with a select
        compareResults(queryRunner.execute(getSession(),
                        "SELECT * FROM persistent_table"),
                queryRunner.execute(getSession(),
                        "SELECT * FROM non_persistent_table"));

        // drop tables
        queryRunner.execute(getSession(),
                "DROP TABLE persistent_table");
        queryRunner.execute(getSession(),
                "DROP TABLE non_persistent_table");
    }

    @Test
    public void testComplexPersistentCteForInsertQueries()
    {
        QueryRunner queryRunner = getQueryRunner();
        // Create tables without data
        // Create tables
        String createTableBase = " (suppkey BIGINT, supplier_name VARCHAR, nation_name VARCHAR, region_name VARCHAR, " +
                "partkey BIGINT, availqty BIGINT, supplycost DOUBLE, " +
                "part_name VARCHAR, part_type VARCHAR, part_size BIGINT, " +
                "nation_comment VARCHAR, region_comment VARCHAR)";

        queryRunner.execute(getSession(),
                "CREATE TABLE persistent_table" + createTableBase);

        queryRunner.execute(getSession(),
                "CREATE TABLE non_persistent_table" + createTableBase);

        queryRunner.execute(getMaterializedSession(),
                "INSERT INTO persistent_table  " +
                        "WITH  supplier_region AS (" +
                        "   SELECT s.suppkey, s.name AS supplier_name, n.name AS nation_name, r.name AS region_name " +
                        "   FROM SUPPLIER s " +
                        "   JOIN NATION n ON s.nationkey = n.nationkey " +
                        "   JOIN REGION r ON n.regionkey = r.regionkey), " +
                        " supplier_parts AS (" +
                        "   SELECT sr.*, ps.partkey, ps.availqty, ps.supplycost " +
                        "   FROM supplier_region sr " +
                        "   JOIN partsupp ps ON sr.suppkey = ps.suppkey), " +
                        "parts_info AS (" +
                        "   SELECT sp.*, p.name AS part_name, p.type AS part_type, p.size AS part_size " +
                        "   FROM supplier_parts sp " +
                        "   JOIN PART p ON sp.partkey = p.partkey), " +
                        " full_supplier_part_info AS (" +
                        "   SELECT pi.*, n.comment AS nation_comment, r.comment AS region_comment " +
                        "   FROM parts_info pi " +
                        "   JOIN NATION n ON pi.nation_name = n.name " +
                        "   JOIN REGION r ON pi.region_name = r.name) " +
                        "SELECT * FROM full_supplier_part_info " +
                        "WHERE part_type LIKE '%BRASS' " +
                        "ORDER BY region_name, supplier_name");
        queryRunner.execute(getSession(),
                "INSERT INTO non_persistent_table  " +
                        "WITH  supplier_region AS (" +
                        "   SELECT s.suppkey, s.name AS supplier_name, n.name AS nation_name, r.name AS region_name " +
                        "   FROM SUPPLIER s " +
                        "   JOIN NATION n ON s.nationkey = n.nationkey " +
                        "   JOIN REGION r ON n.regionkey = r.regionkey), " +
                        " supplier_parts AS (" +
                        "   SELECT sr.*, ps.partkey, ps.availqty, ps.supplycost " +
                        "   FROM supplier_region sr " +
                        "   JOIN partsupp ps ON sr.suppkey = ps.suppkey), " +
                        "parts_info AS (" +
                        "   SELECT sp.*, p.name AS part_name, p.type AS part_type, p.size AS part_size " +
                        "   FROM supplier_parts sp " +
                        "   JOIN PART p ON sp.partkey = p.partkey), " +
                        " full_supplier_part_info AS (" +
                        "   SELECT pi.*, n.comment AS nation_comment, r.comment AS region_comment " +
                        "   FROM parts_info pi " +
                        "   JOIN NATION n ON pi.nation_name = n.name " +
                        "   JOIN REGION r ON pi.region_name = r.name) " +
                        "SELECT * FROM full_supplier_part_info " +
                        "WHERE part_type LIKE '%BRASS' " +
                        "ORDER BY region_name, supplier_name");

        // Compare contents with a select
        compareResults(queryRunner.execute(getSession(),
                        "SELECT * FROM persistent_table"),
                queryRunner.execute(getSession(),
                        "SELECT * FROM non_persistent_table"));

        // drop tables
        queryRunner.execute(getSession(),
                "DROP TABLE persistent_table");
        queryRunner.execute(getSession(),
                "DROP TABLE non_persistent_table");
    }

    @Test
    public void testSimplePersistentCteForViewQueries()
    {
        QueryRunner queryRunner = getQueryRunner();

        // Create views
        queryRunner.execute(getMaterializedSession(),
                "CREATE VIEW persistent_view AS WITH  temp AS (SELECT orderkey FROM ORDERS) " +
                        "SELECT * FROM temp");
        queryRunner.execute(getSession(),
                "CREATE VIEW non_persistent_view AS WITH temp AS (SELECT orderkey FROM ORDERS) " +
                        "SELECT * FROM temp");
        // Compare contents of views with a select
        compareResults(queryRunner.execute(getMaterializedSession(), "SELECT * FROM persistent_view"),
                queryRunner.execute(getSession(), "SELECT * FROM non_persistent_view"));

        // Drop views
        queryRunner.execute(getSession(), "DROP VIEW persistent_view");
        queryRunner.execute(getSession(), "DROP VIEW non_persistent_view");
    }

    @Test
    public void testComplexPersistentCteForViewQueries()
    {
        QueryRunner queryRunner = getQueryRunner();
        // Create Views
        queryRunner.execute(getMaterializedSession(),
                "CREATE View persistent_view as " +
                        "WITH  supplier_region AS (" +
                        "   SELECT s.suppkey, s.name AS supplier_name, n.name AS nation_name, r.name AS region_name " +
                        "   FROM SUPPLIER s " +
                        "   JOIN NATION n ON s.nationkey = n.nationkey " +
                        "   JOIN REGION r ON n.regionkey = r.regionkey), " +
                        " supplier_parts AS (" +
                        "   SELECT sr.*, ps.partkey, ps.availqty, ps.supplycost " +
                        "   FROM supplier_region sr " +
                        "   JOIN partsupp ps ON sr.suppkey = ps.suppkey), " +
                        "parts_info AS (" +
                        "   SELECT sp.*, p.name AS part_name, p.type AS part_type, p.size AS part_size " +
                        "   FROM supplier_parts sp " +
                        "   JOIN PART p ON sp.partkey = p.partkey), " +
                        " full_supplier_part_info AS (" +
                        "   SELECT pi.*, n.comment AS nation_comment, r.comment AS region_comment " +
                        "   FROM parts_info pi " +
                        "   JOIN NATION n ON pi.nation_name = n.name " +
                        "   JOIN REGION r ON pi.region_name = r.name) " +
                        "SELECT * FROM full_supplier_part_info " +
                        "WHERE part_type LIKE '%BRASS' " +
                        "ORDER BY region_name, supplier_name");
        queryRunner.execute(getSession(),
                "CREATE View non_persistent_view as " +
                        "WITH  supplier_region AS (" +
                        "   SELECT s.suppkey, s.name AS supplier_name, n.name AS nation_name, r.name AS region_name " +
                        "   FROM SUPPLIER s " +
                        "   JOIN NATION n ON s.nationkey = n.nationkey " +
                        "   JOIN REGION r ON n.regionkey = r.regionkey), " +
                        " supplier_parts AS (" +
                        "   SELECT sr.*, ps.partkey, ps.availqty, ps.supplycost " +
                        "   FROM supplier_region sr " +
                        "   JOIN partsupp ps ON sr.suppkey = ps.suppkey), " +
                        "parts_info AS (" +
                        "   SELECT sp.*, p.name AS part_name, p.type AS part_type, p.size AS part_size " +
                        "   FROM supplier_parts sp " +
                        "   JOIN PART p ON sp.partkey = p.partkey), " +
                        " full_supplier_part_info AS (" +
                        "   SELECT pi.*, n.comment AS nation_comment, r.comment AS region_comment " +
                        "   FROM parts_info pi " +
                        "   JOIN NATION n ON pi.nation_name = n.name " +
                        "   JOIN REGION r ON pi.region_name = r.name) " +
                        "SELECT * FROM full_supplier_part_info " +
                        "WHERE part_type LIKE '%BRASS' " +
                        "ORDER BY region_name, supplier_name");

        // Compare contents with a select
        compareResults(queryRunner.execute(getMaterializedSession(),
                        "SELECT * FROM persistent_view"),
                queryRunner.execute(getSession(),
                        "SELECT * FROM non_persistent_view"));

        // drop views
        queryRunner.execute(getSession(),
                "DROP View persistent_view");
        queryRunner.execute(getSession(),
                "DROP View non_persistent_view");
    }

    private void compareResults(MaterializedResult actual, MaterializedResult expected)
    {
        compareResults(actual, expected, false);
    }

    private void compareResults(MaterializedResult actual, MaterializedResult expected, boolean checkOrdering)
    {
        // Verify result count
        assertEquals(actual.getRowCount(),
                expected.getRowCount(), String.format("Expected %d rows got %d rows", expected.getRowCount(), actual.getRowCount()));
        if (checkOrdering) {
            assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows(), "Correctness check failed! Rows are not equal");
            return;
        }
        QueryAssertions.assertEqualsIgnoreOrder(actual, expected, "Correctness check failed! Rows are not equal");
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "NONE")
                .build();
    }
    protected Session getMaterializedSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();
    }
}
