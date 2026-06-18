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
import com.facebook.presto.spi.eventlistener.CTEInformation;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.QueryAssertions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.SystemSessionProperties.CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED;
import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_WRITTEN_INTERMEDIATE_BYTES;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_OPTIMIZER_INFO_ENABLED;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static io.airlift.tpch.TpchTable.REGION;
import static io.airlift.tpch.TpchTable.SUPPLIER;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestCteExecution
        extends AbstractTestQueryFramework
{
    private static final Pattern CTE_INFO_MATCHER = Pattern.compile("CTEInfo.*");

    protected QueryRunner createQueryRunner(boolean singleNode)
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, CUSTOMER, LINE_ITEM, PART_SUPPLIER, NATION, REGION, PART, SUPPLIER),
                ImmutableMap.of(
                        "query.cte-partitioning-provider-catalog", "hive",
                        "single-node-execution-enabled", "" + singleNode),
                "sql-standard",
                ImmutableMap.of("hive.pushdown-filter-enabled", "true",
                        "hive.enable-parquet-dereference-pushdown", "true",
                        "hive.temporary-table-storage-format", "PAGEFILE"),
                Optional.empty());
    }

    @Test
    public void testCteExecutionWhereOneCteRemovedBySimplifyEmptyInputRule()
    {
        String sql = "WITH t as(select orderkey, count(*) as count from (select orderkey from orders where false) group by orderkey)," +
                "t1 as (SELECT * FROM orders)," +
                " b AS ((SELECT orderkey FROM t) UNION (SELECT orderkey FROM t1)) " +
                "SELECT * FROM b";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, sql, ImmutableList.of(generateMaterializedCTEInformation("t", 1, false, false),
                generateMaterializedCTEInformation("t1", 1, false, true), generateMaterializedCTEInformation("b", 1, false, true)));
    }

    @Test
    public void testCteExecutionWhereChildPlanRemovedBySimplifyEmptyInputRule()
    {
        String sql = "WITH t as(SELECT * FROM orders LEFT JOIN (select orderkey from orders where false) ON TRUE) " +
                "SELECT * FROM t";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, sql, ImmutableList.of(generateMaterializedCTEInformation("t", 1, false, true)));
    }

    @Test
    public void testSimplePersistentCte()
    {
        QueryRunner queryRunner = getQueryRunner();
        String sql = "WITH  temp as (SELECT orderkey FROM ORDERS) " +
                "SELECT * FROM temp t1 ";
        verifyResults(queryRunner, sql, ImmutableList.of(generateMaterializedCTEInformation("temp", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte", 1, false, true)));
    }

    @Test
    public void testComplexCommonFilterPushdown()
    {
        QueryRunner queryRunner = getQueryRunner();
        String testQuery = "WITH order_platform_data AS (\n" +
                "  SELECT\n" +
                "    o.orderkey AS order_key,\n" +
                "    o.orderdate AS datestr,\n" +
                "    o.orderpriority AS event_type\n" +
                "  FROM\n" +
                "    orders o\n" +
                "  WHERE\n" +
                "    o.orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-01-31'\n" +
                "    AND o.orderpriority IN ('1-URGENT', '3-MEDIUM')\n" +
                "  UNION ALL\n" +
                "  SELECT\n" +
                "    l.orderkey AS order_key,\n" +
                "    o.orderdate AS datestr,\n" +
                "    o.orderpriority AS event_type\n" +
                "  FROM\n" +
                "    lineitem l\n" +
                "    JOIN orders o ON l.orderkey = o.orderkey\n" +
                "  WHERE\n" +
                "    o.orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-01-31'\n" +
                "    AND o.orderpriority IN ('2-HIGH', '5-LOW')\n" +
                "),\n" +
                "urgent AS (\n" +
                "    SELECT order_key, datestr\n" +
                "    FROM order_platform_data\n" +
                "    WHERE event_type = '1-URGENT'\n" +
                "),\n" +
                "medium AS (\n" +
                "    SELECT order_key, datestr\n" +
                "    FROM order_platform_data\n" +
                "    WHERE event_type = '3-MEDIUM'\n" +
                "),\n" +
                "high AS (\n" +
                "    SELECT order_key, datestr\n" +
                "    FROM order_platform_data\n" +
                "    WHERE event_type = '2-HIGH'\n" +
                "),\n" +
                "low AS (\n" +
                "    SELECT order_key, datestr\n" +
                "    FROM order_platform_data\n" +
                "    WHERE event_type = '5-LOW'\n" +
                ")\n" +
                "SELECT\n" +
                "    ofin.order_key AS order_key,\n" +
                "    ofin.datestr AS order_date\n" +
                " FROM " +
                "    urgent ofin\n" +
                "    LEFT JOIN medium oproc ON ofin.datestr = oproc.datestr\n" +
                "   LEFT JOIN low on oproc.datestr = low.datestr" +
                "  LEFT JOIN high on low.datestr = high.datestr" +
                " ORDER BY\n" +
                "    ofin.order_key\n";
        Session materializedSession = Session.builder(super.getSession())
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC_COMPLEX_QUERIES_ONLY")
                .setSystemProperty(CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED, "true")
                .build();
        verifyResults(materializedSession, getSession(), queryRunner, testQuery,
                ImmutableList.of(generateMaterializedCTEInformation("order_platform_data", 4, false, true),
                        generateMaterializedCTEInformation("urgent", 1, false, false),
                        generateMaterializedCTEInformation("medium", 1, false, false),
                        generateMaterializedCTEInformation("high", 1, false, false),
                        generateMaterializedCTEInformation("low", 1, false, false)), false);
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("temp", 1, false, true)));
    }

    // ToDo prestodb/21791: write with 0 length varchar fails in hive
    // See reference - While Presto supports Varchar of length 0 (as discussed in trinodb/trino#1136
    @Test(enabled = false)
    public void testCteWithZeroLengthVarchar()
    {
        String testQuery = "WITH temp AS (" +
                "  SELECT * FROM (VALUES " +
                "    (CAST('' AS VARCHAR(0)), 9)" +
                "  ) AS t (text_column, number_column)" +
                ") SELECT * FROM temp";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("temp", 1, false, true)));
    }

    @Test
    public void testDependentPersistentCtes()
    {
        String testQuery = "WITH  cte1 AS (SELECT orderkey FROM ORDERS WHERE orderkey < 100), " +
                "      cte2 AS (SELECT * FROM cte1 WHERE orderkey > 50) " +
                "SELECT * FROM cte2";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 1, false, true)));
    }

    @Test
    public void testMultipleIndependentPersistentCtes()
    {
        String testQuery = "WITH  cte1 AS (SELECT orderkey FROM ORDERS WHERE orderkey < 100), " +
                "      cte2 AS (SELECT custkey FROM CUSTOMER WHERE custkey < 50) " +
                "SELECT * FROM cte1, cte2 WHERE cte1.orderkey = cte2.custkey";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 1, false, true)));
    }

    @Test
    public void testRefinedCtesOutsideScope()
    {
        String testQuery = "WITH  cte1 AS ( WITH cte2 as (SELECT orderkey FROM ORDERS WHERE orderkey < 100)" +
                "SELECT * FROM cte2), " +
                " cte2 AS (SELECT * FROM customer WHERE custkey < 50) " +
                "SELECT * FROM cte2  JOIN cte1 ON true";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 1, false, true),
                generateMaterializedCTEInformation("cte2", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("test_cte", 1, false, true)));
    }

    @Test
    public void testPersistentCteForVarbinaryType()
    {
        String testQuery = "WITH  dataset AS (\n" +
                "    SELECT data FROM (VALUES \n" +
                "        (1, ARRAY[ROW('John Doe', 30)], from_base64('Sm9obiBEb2U=')), " +
                "        (2, ARRAY[ROW('Jane Smith', 25)], from_base64('SmFuZSBTbWl0aA=='))," +
                "        (3, ARRAY[ROW('Bob Johnson', 40)], from_base64('Qm9iIEpvaG5zb24=')) -- 'Bob Johnson' in base64\n" +
                "    ) AS t (id, people, data)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithBigInt()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT id FROM (VALUES \n" +
                "        (1),\n" +
                "        (2),\n" +
                "        (3)\n" +
                "    ) AS t (id)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithInteger()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT id FROM (VALUES \n" +
                "        (123456789),\n" +
                "        (987654321),\n" +
                "        (-2147483648)\n" +
                "    ) AS t (id)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithSmallInt()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT id FROM (VALUES \n" +
                "        (CAST(32767 AS SMALLINT)),\n" +
                "        (CAST(-32768 AS SMALLINT)),\n" +
                "        (CAST(12345 AS SMALLINT))\n" +
                "    ) AS t (id)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithTinyInt()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT id FROM (VALUES \n" +
                "        (CAST(127 AS TINYINT)),\n" +
                "        (CAST(-128 AS TINYINT)),\n" +
                "        (CAST(0 AS TINYINT))\n" +
                "    ) AS t (id)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithReal()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT value FROM (VALUES \n" +
                "        (CAST(123.45 AS REAL)),\n" +
                "        (CAST(-123.45 AS REAL)),\n" +
                "        (CAST(0.0 AS REAL))\n" +
                "    ) AS t (value)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithBoolean()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT flag FROM (VALUES \n" +
                "        (true),\n" +
                "        (false),\n" +
                "        (true)\n" +
                "    ) AS t (flag)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithDecimal()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT value FROM (VALUES \n" +
                "        (DECIMAL '10.5'),\n" +
                "        (DECIMAL '20.75'),\n" +
                "        (DECIMAL '30.00')\n" +
                "    ) AS t (value)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithChar()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT charColumn FROM (VALUES \n" +
                "        (CAST('A' AS CHAR(1))),\n" + // Single character 'A'
                "        (CAST('B' AS CHAR(1))),\n" + // Single character 'B'
                "        (CAST('C' AS CHAR(1)))\n" +  // Single character 'C'
                "    ) AS t (charColumn)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithArrayWhereInnerTypeSupported()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT arr FROM (VALUES \n" +
                "        (ARRAY[1, 2, 3]),\n" +
                "        (ARRAY[4, 5, 6]),\n" +
                "        (ARRAY[7, 8, 9])\n" +
                "    ) AS t (arr)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithArrayWhereInnerTypeIsNotSupported()
    {
        String testQuery = "WITH  dataset AS (\n" +
                "    SELECT people FROM (VALUES \n" +
                "        (1, ARRAY[ROW('John Doe', 30)], from_base64('Sm9obiBEb2U=')), -- 'John Doe' in base64\n" +
                "        (2, ARRAY[ROW('Jane Smith', 25)], from_base64('SmFuZSBTbWl0aA==')), -- 'Jane Smith' in base64\n" +
                "        (3, ARRAY[ROW('Bob Johnson', 40)], from_base64('Qm9iIEpvaG5zb24=')) -- 'Bob Johnson' in base64\n" +
                "    ) AS t (id, people, data)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithMap()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT map FROM (VALUES \n" +
                "        (MAP(ARRAY['key1', 'key2'], ARRAY[1, 2])),\n" +
                "        (MAP(ARRAY['key3', 'key4'], ARRAY[3, 4]))\n" +
                "    ) AS t (map)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithVarbinary()
    {
        String testQuery = "WITH dataset AS (\n" +
                "    SELECT data FROM (VALUES \n" +
                "        (from_base64('YmluYXJ5RGF0YTE=')),\n" + // 'binaryData1' in base64
                "        (from_base64('YmluYXJ5RGF0YTJ='))\n" +  // 'binaryData2' in base64
                "    ) AS t (data)\n" +
                ")\n" +
                "SELECT * FROM dataset";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("dataset", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 1, false, true),
                generateMaterializedCTEInformation("cte2", 1, false, true),
                generateMaterializedCTEInformation("cte3", 1, false, true)));
    }

    @Test
    public void testChainedPersistentCtes()
    {
        String testQuery = "WITH  cte1 AS (SELECT orderkey FROM ORDERS WHERE orderkey < 100), " +
                "      cte2 AS (SELECT orderkey FROM cte1 WHERE orderkey > 50), " +
                "     cte3 AS (SELECT orderkey FROM cte2 WHERE orderkey < 75) " +
                "SELECT * FROM cte3";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 1, false, true),
                generateMaterializedCTEInformation("cte2", 1, false, true),
                generateMaterializedCTEInformation("cte3", 1, false, true)));
    }

    @Test
    public void testSimplePersistentCteWithJoinInCteDef()
    {
        String testQuery = "WITH  temp as " +
                "(SELECT * FROM ORDERS o1 " +
                "JOIN ORDERS o2 ON o1.orderkey = o2.orderkey) " +
                "SELECT * FROM temp t1 ";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("temp", 1, false, true)));
    }

    @Test
    public void testSimplePersistentCteMultipleUses()
    {
        String testQuery = " WITH  temp as" +
                " (SELECT * FROM ORDERS) " +
                "SELECT * FROM temp t1 JOIN temp t2 on " +
                "t1.orderkey = t2.orderkey WHERE t1.orderkey < 10";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("temp", 2, false, true)));
    }

    @Test
    public void testPersistentCteMultipleColumns()
    {
        String testQuery = " WITH  temp as (SELECT * FROM ORDERS) " +
                "SELECT * FROM temp t1";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("temp", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 1, false, true),
                generateMaterializedCTEInformation("cte2", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 1, false, true),
                generateMaterializedCTEInformation("cte2", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte", 1, false, true)));
    }

    @Test
    public void testPersistentCteWithSelfJoin()
    {
        String testQuery = "WITH  cte AS (" +
                "   SELECT * FROM ORDERS)" +
                "SELECT * FROM cte c1" +
                " JOIN cte c2 ON c1.orderkey = c2.orderkey WHERE c1.orderkey < 100";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte", 2, false, true)));
    }

    @Test
    public void testPersistentCteWithWindowFunction()
    {
        String testQuery = "WITH cte AS (" +
                "   SELECT *, ROW_NUMBER() OVER(PARTITION BY orderstatus ORDER BY orderkey) as row" +
                "   FROM ORDERS)" +
                "SELECT * FROM cte WHERE row <= 5";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 2, false, true),
                generateMaterializedCTEInformation("cte2", 1, false, true),
                generateMaterializedCTEInformation("cte3", 1, false, true)));
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
        Session materializedSession = getMaterializedSession();
        Session session = getSession();
        verifyResults(materializedSession, session, queryRunner, testQuery,
                ImmutableList.of(generateMaterializedCTEInformation("cte", 1, false, true)), true);
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("cte1", 3, false, true),
                generateMaterializedCTEInformation("cte2", 2, false, true),
                generateMaterializedCTEInformation("cte3", 1, false, true),
                generateMaterializedCTEInformation("cte4", 1, false, true),
                generateMaterializedCTEInformation("cte5", 1, false, true),
                generateMaterializedCTEInformation("cte6", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("customer_nation", 1, false, true),
                generateMaterializedCTEInformation("customer_orders", 1, false, true),
                generateMaterializedCTEInformation("order_lineitems", 1, false, true),
                generateMaterializedCTEInformation("customer_part_analysis", 1, false, true)));
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
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("supplier_region", 1, false, true),
                generateMaterializedCTEInformation("supplier_parts", 1, false, true),
                generateMaterializedCTEInformation("parts_info", 1, false, true),
                generateMaterializedCTEInformation("full_supplier_part_info", 1, false, true)));
    }

    @Test
    public void testComplexQuery3()
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
                "JOIN REGION r ON pi.region_name = r.name" +
                "   JOIN NATION n ON pi.nation_name = n.name) " +
                "SELECT * FROM full_supplier_part_info " +
                "WHERE part_type LIKE '%BRASS' " +
                "ORDER BY region_name, supplier_name";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("supplier_region", 1, false, true),
                generateMaterializedCTEInformation("supplier_parts", 1, false, true),
                generateMaterializedCTEInformation("parts_info", 1, false, true),
                generateMaterializedCTEInformation("full_supplier_part_info", 1, false, true)));
    }

    @Test
    public void testSimplePersistentCteForCtasQueries()
    {
        QueryRunner queryRunner = getQueryRunner();
        String persistentTableName = generateRandomTableName("persistent_table");
        String nonPersistentTableName = generateRandomTableName("non_persistent_table");
        try {
            // Create tables with Ctas
            Session materializedSession = getMaterializedSession();
            String testQuery = format("CREATE TABLE %s as (WITH  temp as (SELECT orderkey FROM ORDERS) " +
                    "SELECT * FROM temp t1 )", persistentTableName);
            verifyCTEExplainPlan(materializedSession, testQuery, ImmutableList.of(generateMaterializedCTEInformation("temp", 1, false, true)));
            queryRunner.execute(materializedSession,
                    testQuery);
            queryRunner.execute(getSession(),
                    format("CREATE TABLE %s as (WITH  temp as (SELECT orderkey FROM ORDERS) " +
                            "SELECT * FROM temp t1) ", nonPersistentTableName));

            // Compare contents with a select
            compareResults(queryRunner.execute(getSession(),
                            "SELECT * FROM " + persistentTableName),
                    queryRunner.execute(getSession(),
                            "SELECT * FROM " + nonPersistentTableName));
        }
        finally {
            // drop tables
            queryRunner.execute(getSession(),
                    "DROP TABLE " + persistentTableName);
            queryRunner.execute(getSession(),
                    "DROP TABLE " + nonPersistentTableName);
        }
    }

    @Test
    public void testComplexPersistentCteForCtasQueries()
    {
        String persistentTableName = generateRandomTableName("persistent_table");
        String nonPersistentTableName = generateRandomTableName("non_persistent_table");
        QueryRunner queryRunner = getQueryRunner();
        try {
            // Create tables with Ctas
            Session materializedSession = getMaterializedSession();
            String testQuery = format("CREATE TABLE %s as ( " +
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
                    "WHERE part_type LIKE '%%BRASS' " +
                    "ORDER BY region_name, supplier_name)", persistentTableName);
            verifyCTEExplainPlan(materializedSession, testQuery,
                    ImmutableList.of(generateMaterializedCTEInformation("supplier_region", 1, false, true),
                            generateMaterializedCTEInformation("supplier_parts", 1, false, true),
                            generateMaterializedCTEInformation("parts_info", 1, false, true),
                            generateMaterializedCTEInformation("full_supplier_part_info", 1, false, true)));
            queryRunner.execute(materializedSession,
                    testQuery);
            queryRunner.execute(getSession(),
                    format("CREATE TABLE %s as ( " +
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
                            "WHERE part_type LIKE '%%BRASS' " +
                            "ORDER BY region_name, supplier_name)", nonPersistentTableName));

            // Compare contents with a select
            compareResults(queryRunner.execute(getSession(),
                            "SELECT * FROM " + persistentTableName),
                    queryRunner.execute(getSession(),
                            "SELECT * FROM " + nonPersistentTableName));
        }
        finally {
            // drop tables
            queryRunner.execute(getSession(),
                    "DROP TABLE " + persistentTableName);
            queryRunner.execute(getSession(),
                    "DROP TABLE " + nonPersistentTableName);
        }
    }

    @Test
    public void testSimplePersistentCteForInsertQueries()
    {
        String persistentTableName = generateRandomTableName("persistent_table");
        String nonPersistentTableName = generateRandomTableName("non_persistent_table");

        QueryRunner queryRunner = getQueryRunner();

        try {
            // Create tables without data
            queryRunner.execute(getSession(),
                    format("CREATE TABLE %s (orderkey BIGINT)", persistentTableName));
            queryRunner.execute(getSession(),
                    format("CREATE TABLE %s (orderkey BIGINT)", nonPersistentTableName));

            // Insert data into tables using CTEs
            Session materializedSession = getMaterializedSession();
            String testQuery = format("INSERT INTO %s " +
                    "WITH  temp AS (SELECT orderkey FROM ORDERS) " +
                    "SELECT * FROM temp", persistentTableName);
            queryRunner.execute(materializedSession,
                    testQuery);
            queryRunner.execute(getSession(),
                    format("INSERT INTO %s " +
                            "WITH temp AS (SELECT orderkey FROM ORDERS) " +
                            "SELECT * FROM temp", nonPersistentTableName));

            // Compare contents with a select
            compareResults(queryRunner.execute(getSession(),
                            "SELECT * FROM " + persistentTableName),
                    queryRunner.execute(getSession(),
                            "SELECT * FROM " + nonPersistentTableName));
            verifyCTEExplainPlan(materializedSession, testQuery, ImmutableList.of(generateMaterializedCTEInformation("temp", 1, false, true)));
        }
        finally {
            // drop tables
            queryRunner.execute(getSession(),
                    "DROP TABLE " + persistentTableName);
            queryRunner.execute(getSession(),
                    "DROP TABLE " + nonPersistentTableName);
        }
    }

    @Test
    public void testComplexPersistentCteForInsertQueries()
    {
        String persistentTableName = generateRandomTableName("persistent_table");
        String nonPersistentTableName = generateRandomTableName("non_persistent_table");

        QueryRunner queryRunner = getQueryRunner();
        // Create tables without data
        // Create tables
        try {
            String createTableBase = " (suppkey BIGINT, supplier_name VARCHAR, nation_name VARCHAR, region_name VARCHAR, " +
                    "partkey BIGINT, availqty BIGINT, supplycost DOUBLE, " +
                    "part_name VARCHAR, part_type VARCHAR, part_size BIGINT, " +
                    "nation_comment VARCHAR, region_comment VARCHAR)";

            queryRunner.execute(getSession(),
                    "CREATE TABLE " + persistentTableName + " " + createTableBase);

            queryRunner.execute(getSession(),
                    "CREATE TABLE " + nonPersistentTableName + " " + createTableBase);

            Session materializedSession = getMaterializedSession();
            String testQuery = format("INSERT INTO %s  " +
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
                    "WHERE part_type LIKE '%%BRASS' " +
                    "ORDER BY region_name, supplier_name", persistentTableName);
            queryRunner.execute(materializedSession,
                    testQuery);
            queryRunner.execute(getSession(),
                    format("INSERT INTO %s  " +
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
                            "WHERE part_type LIKE '%%BRASS' " +
                            "ORDER BY region_name, supplier_name", nonPersistentTableName));

            // Compare contents with a select
            compareResults(queryRunner.execute(getSession(),
                            "SELECT * FROM " + persistentTableName),
                    queryRunner.execute(getSession(),
                            "SELECT * FROM " + nonPersistentTableName));
            verifyCTEExplainPlan(materializedSession, testQuery,
                    ImmutableList.of(generateMaterializedCTEInformation("supplier_region", 1, false, true),
                            generateMaterializedCTEInformation("supplier_parts", 1, false, true),
                            generateMaterializedCTEInformation("parts_info", 1, false, true),
                            generateMaterializedCTEInformation("full_supplier_part_info", 1, false, true)));
        }
        finally {
            // drop tables
            queryRunner.execute(getSession(),
                    "DROP TABLE " + persistentTableName);
            queryRunner.execute(getSession(),
                    "DROP TABLE " + nonPersistentTableName);
        }
    }

    @Test
    public void testSimplePersistentCteForViewQueries()
    {
        String persistentViewName = generateRandomTableName("persistent_view");
        String nonPersistentViewName = generateRandomTableName("non_persistent_view");

        QueryRunner queryRunner = getQueryRunner();

        try {
            // Create views
            Session materializedSession = getMaterializedSession();
            queryRunner.execute(materializedSession,
                    format("CREATE VIEW %s AS WITH  temp AS (SELECT orderkey FROM ORDERS) " +
                            "SELECT * FROM temp", persistentViewName));
            queryRunner.execute(getSession(),
                    format("CREATE VIEW %s AS WITH temp AS (SELECT orderkey FROM ORDERS) " +
                            "SELECT * FROM temp", nonPersistentViewName));
            // Compare contents of views with a select
            String testQuery = "SELECT * FROM " + persistentViewName;
            compareResults(queryRunner.execute(getMaterializedSession(), testQuery),
                    queryRunner.execute(getSession(), "SELECT * FROM " + nonPersistentViewName));
            verifyCTEExplainPlan(materializedSession, testQuery, ImmutableList.of(generateMaterializedCTEInformation("temp", 1, false, true)));
        }
        finally {
            // Drop views
            queryRunner.execute(getSession(), "DROP VIEW " + persistentViewName);
            queryRunner.execute(getSession(), "DROP VIEW " + nonPersistentViewName);
        }
    }

    @Test
    public void testComplexPersistentCteForViewQueries()
    {
        String persistentViewName = generateRandomTableName("persistent_view");
        String nonPersistentViewName = generateRandomTableName("non_persistent_view");

        QueryRunner queryRunner = getQueryRunner();
        try {
            // Create Views
            Session materializedSession = getMaterializedSession();
            queryRunner.execute(materializedSession,
                    format("CREATE View %s as " +
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
                            "WHERE part_type LIKE '%%BRASS' " +
                            "ORDER BY region_name, supplier_name", persistentViewName));
            queryRunner.execute(getSession(),
                    format("CREATE View %s as " +
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
                            "WHERE part_type LIKE '%%BRASS' " +
                            "ORDER BY region_name, supplier_name", nonPersistentViewName));

            // Compare contents with a select
            String testQuery = "SELECT * FROM " + persistentViewName;
            compareResults(queryRunner.execute(getMaterializedSession(),
                            testQuery),
                    queryRunner.execute(getSession(),
                            "SELECT * FROM " + nonPersistentViewName));
            verifyCTEExplainPlan(materializedSession, testQuery,
                    ImmutableList.of(generateMaterializedCTEInformation("supplier_region", 1, false, true),
                            generateMaterializedCTEInformation("supplier_parts", 1, false, true),
                            generateMaterializedCTEInformation("parts_info", 1, false, true),
                            generateMaterializedCTEInformation("full_supplier_part_info", 1, false, true)));
        }
        finally {
            // drop views
            queryRunner.execute(getSession(),
                    "DROP View " + persistentViewName);
            queryRunner.execute(getSession(),
                    "DROP View " + nonPersistentViewName);
        }
    }

    public void testCteProjectionPushDown()
    {
        QueryRunner queryRunner = getQueryRunner();
        String query = "WITH  temp as (SELECT * FROM ORDERS) " +
                "SELECT * FROM (select orderkey from temp) t JOIN (select custkey, orderkey as orderkey2 from temp) t2 ON t.orderkey=t2.orderkey2";
        verifyResults(queryRunner, query, ImmutableList.of(generateMaterializedCTEInformation("temp", 2, false, true)));
    }

    @Test
    public void testCteFilterPushDown()
    {
        QueryRunner queryRunner = getQueryRunner();
        String query = "WITH  temp as (SELECT * FROM ORDERS) " +
                "SELECT * FROM (select orderkey from temp where orderkey > 20) t JOIN (select custkey, orderkey as orderkey2 from temp where custkey < 1000) t2 ON t.orderkey=t2.orderkey2";
        verifyResults(queryRunner, query, ImmutableList.of(generateMaterializedCTEInformation("temp", 2, false, true)));
    }

    @Test
    public void testCteNoFilterPushDown()
    {
        QueryRunner queryRunner = getQueryRunner();
        // one CTE consumer used without a filter: no filter pushdown
        String query = "WITH  temp as (SELECT * FROM ORDERS) " +
                "SELECT * FROM (select orderkey from temp where orderkey > 20) t UNION ALL select orderkey from temp";
        verifyResults(queryRunner, query, ImmutableList.of(generateMaterializedCTEInformation("temp", 2, false, true)));
    }

    @Test
    public void testChainedCteProjectionAndFilterPushDown()
    {
        QueryRunner queryRunner = getQueryRunner();
        String query = "WITH cte1 AS (SELECT * FROM ORDERS WHERE orderkey < 1000), " +
                "cte5 AS (SELECT orderkey FROM cte1 WHERE totalprice < 100000) " +
                "SELECT * FROM cte5";
        verifyResults(queryRunner, query, ImmutableList.of(generateMaterializedCTEInformation("cte1", 1, false, true),
                generateMaterializedCTEInformation("cte5", 1, false, true)));
    }

    @Test
    public void testCTEMaterializationWithEnhancedScheduling()
    {
        QueryRunner queryRunner = getQueryRunner();
        String sql = "WITH  temp as (SELECT orderkey FROM ORDERS) " +
                "SELECT * FROM temp t1 JOIN (SELECT custkey FROM customer) c ON t1.orderkey=c.custkey";
        verifyResults(queryRunner, sql, ImmutableList.of(generateMaterializedCTEInformation("temp", 1, false, true)));
    }

    @Test
    public void testWrittenIntemediateByteLimit()
            throws Exception
    {
        String testQuery = "WITH  cte1 AS (SELECT * FROM ORDERS JOIN ORDERS ON TRUE) " +
                "SELECT * FROM cte1";
        Session session = Session.builder(getMaterializedSession())
                .setSystemProperty(QUERY_MAX_WRITTEN_INTERMEDIATE_BYTES, "0MB")
                .build();
        assertQueryFails(session, testQuery, "Query has exceeded WrittenIntermediate Limit of 0MB.*");
    }

    @Test
    public void testNestedCteWithSameName()
    {
        String testQuery = "with t1 as ( select orderkey k from orders where orderkey > 5), t2 as ( select orderkey k from orders where orderkey < 10 ), t3 as " +
                "( select t1.k, t2.k from t1 left join t2 on t1.k=t2.k ), t4 as ( with t2 as ( select orderkey k from orders where orderkey > 5 ), " +
                "t1 as ( select orderkey k from orders where orderkey < 10 ), t3 as ( select t1.k, t2.k from t1 left join t2 on t1.k=t2.k ) select * from t3 ) " +
                "select * from t3 except select * from t4";
        QueryRunner queryRunner = getQueryRunner();
        verifyResults(queryRunner, testQuery, ImmutableList.of(generateMaterializedCTEInformation("t1", 1, false, true),
                generateMaterializedCTEInformation("t2", 1, false, true),
                generateMaterializedCTEInformation("t3", 1, false, true),
                generateMaterializedCTEInformation("t4", 1, false, true)));
    }

    private void verifyResults(QueryRunner queryRunner, String query, List<CTEInformation> expectedCTEInfoValues)
    {
        Session materializedSession = getMaterializedSession();
        Session session = getSession();
        verifyResults(materializedSession, session, queryRunner, query, expectedCTEInfoValues, false);
    }

    private void verifyResults(Session materializedSession, Session session, QueryRunner queryRunner, String query, List<CTEInformation> expectedCTEInfoValues, boolean checkOrdering)
    {
        compareResults(queryRunner.execute(materializedSession,
                        query),
                queryRunner.execute(session,
                        query), checkOrdering);
        verifyCTEExplainPlan(materializedSession, query, expectedCTEInfoValues);
    }

    private void verifyCTEExplainPlan(Session materializedSession, String query, List<CTEInformation> expectedCTEInfoValues)
    {
        //Verify CTE Explain plan
        MaterializedResult materializedResult = computeActual(materializedSession, "explain " + query);
        String explain = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());
        checkCTEInfoMatch(explain, expectedCTEInfoValues);
    }

    private void compareResults(MaterializedResult actual, MaterializedResult expected)
    {
        compareResults(actual, expected, false);
    }

    private void compareResults(MaterializedResult actual, MaterializedResult expected, boolean checkOrdering)
    {
        // Verify result count
        assertEquals(actual.getRowCount(),
                expected.getRowCount(), format("Expected %d rows got %d rows", expected.getRowCount(), actual.getRowCount()));
        if (checkOrdering) {
            assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows(), "Correctness check failed! Rows are not equal");
            return;
        }
        QueryAssertions.assertEqualsIgnoreOrder(actual, expected, "Correctness check failed! Rows are not equal");
    }

    private void checkCTEInfoMatch(String explain, List<CTEInformation> expectedCTEInfoValues)
    {
        Matcher matcher = CTE_INFO_MATCHER.matcher(explain);
        assertTrue(matcher.find());

        String cteInfo = matcher.group();
        for (CTEInformation value : expectedCTEInfoValues) {
            assertTrue(cteInfo.contains(value.getCteName() + ": " + value.getNumberOfReferences() +
                            " (is_view: " + value.getIsView() +
                            ") (is_materialized: " + value.isMaterialized() + ")"),
                    format("Explain plan %s missing expected CTEInfo for: %s", explain, value.getCteName()));
        }
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
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .setSystemProperty(CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED, "true")
                .build();
    }

    private CTEInformation generateMaterializedCTEInformation(String name, int frequency, boolean isView, boolean isMaterialized)
    {
        return new CTEInformation(name, name, frequency, isView, isMaterialized);
    }

    private String generateRandomTableName(String prefix)
    {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }
}
