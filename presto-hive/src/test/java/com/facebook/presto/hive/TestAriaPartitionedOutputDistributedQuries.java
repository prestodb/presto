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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZED_PARTITIONED_OUTPUT;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.tpch.TpchTable.getTables;
import static io.airlift.units.Duration.nanosSince;
import static org.testng.Assert.assertEquals;

public class TestAriaPartitionedOutputDistributedQuries
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestAriaPartitionedOutputDistributedQuries.class);

    protected TestAriaPartitionedOutputDistributedQuries()
    {
        super(() -> createQueryRunner());
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner(ImmutableList.of());

        Session noAria = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(OPTIMIZED_PARTITIONED_OUTPUT, "false")
                .build();

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, noAria, getTables());

        createTable(queryRunner, noAria, "lineitem_nulls", "CREATE TABLE lineitem_nulls AS\n" +
                "SELECT *,\n" +
                "   IF(have_complex_nulls, null, map(array[1, 2, 3], array[orderkey, partkey, suppkey])) as order_part_supp_map,\n" +
                "   IF(have_complex_nulls, null, array[orderkey, partkey, suppkey]) as order_part_supp_array\n" +
                "FROM (\n" +
                "   SELECT \n" +
                "       orderkey,\n" +
                "       linenumber,\n" +
                "       have_simple_nulls and mod(orderkey + linenumber, 5) = 0 AS have_complex_nulls,\n" +
                "       IF(have_simple_nulls and mod(orderkey + linenumber, 11) = 0, null, partkey) as partkey,\n" +
                "       IF(have_simple_nulls and mod(orderkey + linenumber, 13) = 0, null, suppkey) as suppkey,\n" +
                "       IF(have_simple_nulls and mod (orderkey + linenumber, 17) = 0, null, quantity) as quantity,\n" +
                "       IF(have_simple_nulls and mod (orderkey + linenumber, 19) = 0, null, extendedprice) as extendedprice,\n" +
                "       IF(have_simple_nulls and mod (orderkey + linenumber, 23) = 0, null, shipmode) as shipmode,\n" +
                "       IF(have_simple_nulls and mod (orderkey + linenumber, 7) = 0, null, comment) as comment,\n" +
                "       IF(have_simple_nulls and mod(orderkey + linenumber, 31) = 0, null, returnflag = 'R') as is_returned,\n" +
                "       IF(have_simple_nulls and mod(orderkey + linenumber, 37) = 0, null, CAST(quantity + 1 as real)) as float_quantity\n" +
                "   FROM (SELECT mod(orderkey, 198000) > 99000 as have_simple_nulls, * from tpch.tiny.lineitem))\n");

        createTable(queryRunner, noAria, "lineitem_map_array", "CREATE TABLE lineitem_map_array AS\n" +
                "SELECT\n" +
                "    *,\n" +
                "    map(ARRAY[1, 2, 3], ARRAY[l_orderkey, l_partkey, l_suppkey]) AS l_map,\n" +
                "    ARRAY[l_orderkey,\n" +
                "    l_partkey,\n" +
                "    l_suppkey] AS l_array\n" +
                "FROM (\n" +
                "    SELECT\n" +
                "        orderkey AS l_orderkey,\n" +
                "        partkey AS l_partkey,\n" +
                "        suppkey AS l_suppkey,\n" +
                "        linenumber AS l_linenumber,\n" +
                "        quantity AS l_quantity,\n" +
                "        extendedprice AS l_extendedprice,\n" +
                "        shipmode AS l_shipmode,\n" +
                "        COMMENT AS l_comment,\n" +
                "        returnflag = 'R' AS is_returned,\n" +
                "        CAST(quantity + 1 AS REAL) AS l_floatQuantity\n" +
                "    FROM lineitem\n" +
                ")\n");

        createTable(queryRunner, noAria, "lineitem_array_of_array", "CREATE TABLE lineitem_array_of_array AS\n" +
                "SELECT\n" +
                "    l_partkey,\n" +
                "    l_suppkey,\n" +
                "    ARRAY_AGG(l_array) l_array_of_array\n" +
                "FROM (\n" +
                "    SELECT\n" +
                "        l_partkey,\n" +
                "        l_suppkey,\n" +
                "        l_array\n" +
                "    FROM lineitem_map_array\n" +
                "    )\n" +
                "GROUP BY \n" +
                "    l_partkey,\n" +
                "    l_suppkey");

        createTable(queryRunner, noAria, "lineitem_string_structs_with_nulls", "CREATE TABLE lineitem_string_structs_with_nulls AS\n" +
                "SELECT\n" +
                "    orderkey,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    linenumber,\n" +
                "    CAST(\n" +
                "        IF(mod(partkey, 5) = 0, NULL,\n" +
                "            ROW(\n" +
                "                comment,\n" +
                "                IF (mod(partkey, 13) = 0, NULL,\n" +
                "                    CONCAT(CAST(partkey AS VARCHAR), comment)\n" +
                "                )\n" +
                "            )\n" +
                "        ) AS ROW(\n" +
                "            comment VARCHAR,\n" +
                "            partkey_comment VARCHAR\n" +
                "        )\n" +
                "    ) AS partkey_struct\n" +
//                "    CAST(\n" +
//                "        IF(mod(suppkey, 7) = 0, NULL,\n" +
//                "            ROW(\n" +
//                "                IF (mod(suppkey, 17) = 0, NULL,\n" +
//                "                    CONCAT(CAST(suppkey AS VARCHAR), comment)\n" +
//                "                ),\n" +
//                "                CONCAT(CAST(quantity AS VARCHAR), comment)\n" +
//                "            )\n" +
//                "        ) AS ROW(\n" +
//                "            suppkey_comment VARCHAR,\n" +
//                "            quantity_comment varchar\n" +
//                "        )\n" +
//                "    ) AS suppkey_quantity_struct\n" +
                "FROM tpch.tiny.lineitem\n" +
                "WHERE orderkey < 100000");

        createTable(queryRunner, noAria, "lineitem_struct", "CREATE TABLE lineitem_struct AS\n" +
                "SELECT\n" +
                "    l.orderkey AS l_orderkey,\n" +
                "    linenumber AS l_linenumber,\n" +
                "    CAST(\n" +
                "        ROW (\n" +
                "            l.partkey,\n" +
                "            l.suppkey,\n" +
                "            extendedprice,\n" +
                "            discount,\n" +
                "            quantity,\n" +
                "            shipdate,\n" +
                "            receiptdate,\n" +
                "            commitdate,\n" +
                "            l.comment\n" +
                "        ) AS ROW(\n" +
                "            l_partkey BIGINT,\n" +
                "            l_suppkey BIGINT,\n" +
                "            l_extendedprice DOUBLE,\n" +
                "            l_discount DOUBLE,\n" +
                "            l_quantity DOUBLE,\n" +
                "            l_shipdate DATE,\n" +
                "            l_receiptdate DATE,\n" +
                "            l_commitdate DATE,\n" +
                "            l_comment VARCHAR(44)\n" +
                "        )\n" +
                "    ) AS l_shipment,\n" +
                "    CASE\n" +
                "        WHEN S.nationkey = C.nationkey THEN NULL\n" +
                "        ELSE CAST(\n" +
                "            ROW(\n" +
                "                S.NATIONKEY,\n" +
                "                C.NATIONKEY,\n" +
                "                CASE\n" +
                "                    WHEN (S.NATIONKEY IN (6, 7, 19) AND C.NATIONKEY IN (6, 7, 19)) THEN 1\n" +
                "                    ELSE 0\n" +
                "                END,\n" +
                "                CASE\n" +
                "                    WHEN s.nationkey = 24 AND c.nationkey = 10 THEN 1\n" +
                "                    ELSE 0\n" +
                "                END,\n" +
                "                CASE\n" +
                "                    WHEN p.comment LIKE '%fur%' OR p.comment LIKE '%care%' THEN ROW(\n" +
                "                        o.orderdate,\n" +
                "                        l.shipdate,\n" +
                "                        l.partkey + l.suppkey,\n" +
                "                        CONCAT(p.comment, l.comment)\n" +
                "                    )\n" +
                "                    ELSE NULL\n" +
                "                END\n" +
                "            ) AS ROW (\n" +
                "                s_nation BIGINT,\n" +
                "                c_nation BIGINT,\n" +
                "                is_inside_eu int,\n" +
                "                is_restricted int,\n" +
                "                license ROW (applydate DATE, grantdate DATE, filing_no BIGINT, COMMENT VARCHAR)\n" +
                "            )\n" +
                "        )\n" +
                "    END AS l_export\n" +
                "FROM lineitem l,\n" +
                "    orders o,\n" +
                "    customer c,\n" +
                "    supplier s,\n" +
                "    part p\n" +
                "WHERE\n" +
                "    l.orderkey = o.orderkey\n" +
                "    AND l.partkey = p.partkey\n" +
                "    AND l.suppkey = s.suppkey\n" +
                "    AND c.custkey = o.custkey");

        return queryRunner;
    }

    private static void createTable(QueryRunner queryRunner, Session session, String tableName, String sql)
    {
        log.info("Creating %s table", tableName);
        long start = System.nanoTime();
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Created %s rows for %s in %s", rows, tableName, nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private Session ariaSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZED_PARTITIONED_OUTPUT, "true")
                .build();
    }

    private Session noAriaSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
            .setSystemProperty(OPTIMIZED_PARTITIONED_OUTPUT, "false")
            .build();
    }

    @Test
    public void testInteger()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    l.linenumber\n" +
                "FROM lineitem l, partsupp p\n" +
                "WHERE\n" +
                "    l.partkey = p.partkey\n" +
                "    AND l.suppkey = p.suppkey\n" +
                "    AND p.availqty < 1000");
    }

    @Test
    public void testBigInt()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    l.partkey\n" +
                "FROM lineitem l, partsupp p\n" +
                "WHERE\n" +
                "    l.partkey = p.partkey\n" +
                "    AND l.suppkey = p.suppkey\n" +
                "    AND p.availqty < 1000");
    }

    @Test
    public void testIpAddress()
    {
        String sql = "WITH lineitem_ipaddress AS \n" +
                "(\n" +
                "SELECT\n" +
                "    partkey,\n" +
                "    CAST(\n" +
                "        CONCAT(\n" +
                "            CONCAT(\n" +
                "                CONCAT(\n" +
                "                    CONCAT(\n" +
                "                        CONCAT(\n" +
                "                            CONCAT(CAST((orderkey % 255) AS VARCHAR), '.'),\n" +
                "                            CAST((partkey % 255) AS VARCHAR)\n" +
                "                        ),\n" +
                "                        '.'\n" +
                "                    ),\n" +
                "                    CAST(suppkey AS VARCHAR)\n" +
                "                ),\n" +
                "                '.'\n" +
                "            ),\n" +
                "            CAST(linenumber AS VARCHAR)\n" +
                "        ) AS IPADDRESS\n" +
                "    ) AS ip\n" +
                "FROM lineitem\n" +
                "    )\n" +
                "SELECT\n" +
                "    CHECKSUM(l.ip) \n" +
                "FROM lineitem_ipaddress l,\n" +
                "    partsupp p\n" +
                "WHERE\n" +
                "    l.partkey = p.partkey";

        Object expected = computeActual(noAriaSession(), sql).getOnlyValue();
        Object result = computeActual(ariaSession(), sql).getOnlyValue();
        assertEquals(result, expected);
    }

    @Test
    public void testBigIntWithNulls()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    l.partkey\n" +
                "FROM lineitem_nulls l, partsupp p\n" +
                "WHERE\n" +
                "    l.partkey = p.partkey\n" +
                "    AND l.suppkey = p.suppkey\n" +
                "    AND p.availqty < 1000");
    }

    @Test
    public void testVarchar()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    l.comment\n" +
                "FROM lineitem l, partsupp p\n" +
                "WHERE\n" +
                "    l.partkey = p.partkey\n" +
                "    AND l.suppkey = p.suppkey\n" +
                "    AND p.availqty < 1000");
    }

    @Test
    public void testDictionaryOfVarcharWithNulls()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    l.shipmode\n" +
                "FROM lineitem l,\n" +
                "    partsupp p\n" +
                "WHERE\n" +
                "    l.partkey = p.partkey\n" +
                "    AND l.suppkey = p.suppkey\n" +
                "    AND p.availqty < 1000");
    }

    @Test
    public void testArrayOfBigInt()
    {
        String sql = "SELECT\n" +
                "    checksum(l.l_array)\n" +
                "FROM lineitem_map_array l,\n" +
                "    partsupp p\n" +
                "WHERE\n" +
                "    l.l_partkey = p.partkey\n" +
                "    AND l.l_suppkey = p.suppkey\n" +
                "    AND p.availqty < 1000";

        Object expected = computeActual(noAriaSession(), sql).getOnlyValue();
        Object result = computeActual(ariaSession(), sql).getOnlyValue();
        assertEquals(result, expected);
    }

    @Test
    public void testArrayOfArray()
    {
        String sql = "SELECT\n" +
                "    checksum(l.l_shipment)\n" +
                "FROM lineitem_struct l,\n" +
                "    orders o\n" +
                "WHERE\n" +
                "    l.l_orderkey = o.orderkey";

        Object expected = computeActual(noAriaSession(), sql).getOnlyValue();
        Object result = computeActual(ariaSession(), sql).getOnlyValue();
        assertEquals(result, expected);
    }

    @Test
    public void testMap()
    {
        String sql = "SELECT\n" +
                "    checksum(l.l_map)\n" +
                "FROM lineitem_map_array l,\n" +
                "    orders o\n" +
                "WHERE\n" +
                "    l.l_orderkey = o.orderkey";

        Object expected = computeActual(noAriaSession(), sql).getOnlyValue();
        Object result = computeActual(ariaSession(), sql).getOnlyValue();
        assertEquals(result, expected);
    }

    @Test
    public void testStruct()
    {
        String sql = "SELECT\n" +
                "    checksum(l.l_shipment)\n" +
                "FROM lineitem_struct l,\n" +
                "    orders o\n" +
                "WHERE\n" +
                "    l.l_orderkey = o.orderkey";

        Object expected = computeActual(noAriaSession(), sql).getOnlyValue();
        Object result = computeActual(ariaSession(), sql).getOnlyValue();
        assertEquals(result, expected);
    }

    @Test
    public void testStructWithNullsType()
    {
        String sql = "SELECT\n" +
                "    checksum(partkey_struct)\n" +
//                "    checksum(suppkey_quantity_struct)\n" +
                "FROM lineitem_string_structs_with_nulls l,\n" +
                "    orders o\n" +
                "WHERE\n" +
                "    l.orderkey = o.orderkey";

        Object expected = computeActual(noAriaSession(), sql).getOnlyValue();
        Object result = computeActual(ariaSession(), sql).getOnlyValue();
        assertEquals(result, expected);
    }

    @Test
    public void testMapWithoutHashTables()
    {
        String sql = "SELECT\n" +
                "    checksum(l.l_map)\n" +
                "FROM lineitem_map_array l,\n" +
                "    orders o\n" +
                "WHERE\n" +
                "    l.l_orderkey = o.orderkey";

        Object expected = computeActual(noAriaSession(), sql).getOnlyValue();
        Object result = computeActual(ariaSession(), sql).getOnlyValue();
        assertEquals(result, expected);
    }

    @Test
    public void testMapWithHashTables()
    {
        String sql = "SELECT\n" +
                "    checksum(l.l_map)\n" +
                "FROM lineitem_map_array l,\n" +
                "    orders o\n" +
                "WHERE\n" +
                "    l.l_orderkey = o.orderkey\n" +
                "    AND l.l_map[1] IS NOT NULL";

        Object expected = computeActual(noAriaSession(), sql).getOnlyValue();
        Object result = computeActual(ariaSession(), sql).getOnlyValue();
        assertEquals(result, expected);
    }

    @Test
    public void testQueenOfTheNight()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    COUNT(*),\n" +
                "    SUM(l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost)\n" +
                "FROM lineitem l, partsupp p\n" +
                "WHERE\n" +
                "    l.partkey = p.partkey\n" +
                "    AND l.suppkey = p.suppkey\n" +
                "    AND p.availqty < 1000");
    }
}
