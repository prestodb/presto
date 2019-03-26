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

import static com.facebook.presto.SystemSessionProperties.ARIA_FLAGS;
import static com.facebook.presto.SystemSessionProperties.ARIA_REORDER;
import static com.facebook.presto.SystemSessionProperties.ARIA_REUSE_PAGES;
import static com.facebook.presto.SystemSessionProperties.ARIA_SCAN;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.tpch.TpchTable.getTables;
import static io.airlift.units.Duration.nanosSince;

public class TestAriaHiveDistributedQueries
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestAriaHiveDistributedQueries.class);

    protected TestAriaHiveDistributedQueries()
    {
        super(() -> createQueryRunner());
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner(ImmutableList.of());

        Session noAria = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(ARIA_SCAN, "false")
                .build();

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, noAria, getTables());

        createTable(queryRunner, noAria, "lineitem_aria", "CREATE TABLE lineitem_aria AS\n" +
                "SELECT\n" +
                "    orderkey,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    linenumber,\n" +
                "    quantity,\n" +
                "    extendedprice,\n" +
                "    shipmode,\n" +
                "    shipdate,\n" +
                "    comment,\n" +
                "    returnflag = 'R' AS is_returned,\n" +
                "    CAST(quantity + 1 AS REAL) AS float_quantity,\n" +
                "    CAST(discount AS decimal(5, 2)) AS short_decimal_discount,\n" +
                "    CAST(discount AS decimal(38, 2)) AS long_decimal_discount,\n" +
                "    CAST(array_position(array['SHIP','REG AIR','AIR','FOB','MAIL','RAIL','TRUCK'], shipmode) AS tinyint) AS tinyint_shipmode,\n" +
                "    date_add('second', suppkey, cast(shipdate as timestamp)) AS timestamp_shipdate,\n" +
                "    MAP(\n" +
                "        ARRAY[1, 2, 3],\n" +
                "        ARRAY[orderkey, partkey, suppkey]\n" +
                "    ) AS order_part_supp_map,\n" +
                "    ARRAY[ARRAY[orderkey, partkey, suppkey]] AS order_part_supp_array\n" +
                "FROM tpch.tiny.lineitem");

        createTable(queryRunner, noAria, "lineitem_aria_nulls", "CREATE TABLE lineitem_aria_nulls AS\n" +
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

        createTable(queryRunner, noAria, "lineitem_aria_strings", "CREATE TABLE lineitem_aria_strings AS\n" +
                "SELECT\n" +
                "    orderkey,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    linenumber,\n" +
                "    comment,\n" +
                "    CONCAT(CAST(partkey AS VARCHAR), comment) AS partkey_comment,\n" +
                "    CONCAT(CAST(suppkey AS VARCHAR), comment) AS suppkey_comment,\n" +
                "    CONCAT(CAST(quantity AS VARCHAR), comment) AS quantity_comment\n" +
                "FROM tpch.tiny.lineitem\n" +
                "WHERE orderkey < 100000");

        createTable(queryRunner, noAria, "lineitem_aria_string_structs", "CREATE TABLE lineitem_aria_string_structs AS\n" +
                "SELECT\n" +
                "    orderkey,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    linenumber,\n" +
                "    CAST(\n" +
                "        ROW(\n" +
                "            comment,\n" +
                "            CONCAT(CAST(partkey AS VARCHAR), comment)\n" +
                "        ) AS ROW(\n" +
                "            comment VARCHAR,\n" +
                "            partkey_comment VARCHAR\n" +
                "        )\n" +
                "    ) AS partkey_struct,\n" +
                "    CAST(\n" +
                "        ROW(\n" +
                "            CONCAT(CAST(suppkey AS VARCHAR), comment),\n" +
                "            CONCAT(CAST(quantity AS VARCHAR), comment)\n" +
                "        ) AS ROW(\n" +
                "            suppkey_comment VARCHAR,\n" +
                "            quantity_comment VARCHAR\n" +
                "        )\n" +
                "    ) AS suppkey_quantity_struct\n" +
                "FROM tpch.tiny.lineitem\n" +
                "WHERE orderkey < 100000");

        createTable(queryRunner, noAria, "lineitem_aria_string_structs_with_nulls", "CREATE TABLE lineitem_aria_string_structs_with_nulls AS\n" +
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
                "    ) AS partkey_struct,\n" +
                "    CAST(\n" +
                "        IF(mod(suppkey, 7) = 0, NULL,\n" +
                "            ROW(\n" +
                "                IF (mod(suppkey, 17) = 0, NULL,\n" +
                "                    CONCAT(CAST(suppkey AS VARCHAR), comment)\n" +
                "                ),\n" +
                "                CONCAT(CAST(quantity AS VARCHAR), comment)\n" +
                "            )\n" +
                "        ) AS ROW(\n" +
                "            suppkey_comment VARCHAR,\n" +
                "            quantity_comment varchar\n" +
                "        )\n" +
                "    ) AS suppkey_quantity_struct\n" +
                "FROM tpch.tiny.lineitem\n" +
                "WHERE orderkey < 100000");

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
                .setSystemProperty(ARIA_SCAN, "true")
                .setSystemProperty(ARIA_REORDER, "true")
                .setSystemProperty(ARIA_REUSE_PAGES, "true")
                .setSystemProperty(ARIA_FLAGS, "127")
                .build();
    }

    // filters1.sql
    // filters1_nulls.sql
    @Test
    public void testFilters()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    comment\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    orderkey BETWEEN 100000 AND 200000\n" +
                "    AND partkey BETWEEN 10000 AND 30000\n" +
                "    AND suppkey BETWEEN 1000 AND 5000\n" +
                "    AND comment > 'f'");

        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    comment\n" +
                "FROM lineitem_aria_nulls\n" +
                "WHERE\n" +
                "    orderkey BETWEEN 100000 AND 200000\n" +
                "    AND partkey BETWEEN 10000 AND 30000\n" +
                "    AND suppkey BETWEEN 1000 AND 5000\n" +
                "    AND comment > 'f'");

        // SliceDictionaryStreamReader for shipinstruct
        assertQuery(ariaSession(), "SELECT\n" +
                "    linenumber,\n" +
                "    orderkey,\n" +
                "    shipinstruct\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    shipinstruct = 'TAKE BACK RETURN'");

        // SliceDictionaryStreamReader for shipinstruct and shipmode
        assertQuery(ariaSession(), "SELECT\n" +
                "    linenumber,\n" +
                "    orderkey,\n" +
                "    shipinstruct,\n" +
                "    shipmode\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    shipinstruct = 'TAKE BACK RETURN' AND shipmode = 'AIR'");

        assertQuery(ariaSession(), "SELECT\n" +
                "    linenumber,\n" +
                "    orderkey\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    shipinstruct = 'TAKE BACK RETURN' AND orderkey < 10");

        // BooleanStreamReader
        assertQuery(ariaSession(), "SELECT linenumber FROM lineitem_aria WHERE is_returned = TRUE",
                "SELECT linenumber FROM lineitem WHERE returnflag = 'R'");

        assertQuery(ariaSession(), "SELECT linenumber, is_returned FROM lineitem_aria WHERE is_returned = TRUE",
                "SELECT linenumber, true FROM lineitem WHERE returnflag = 'R'");

        // Filter on a float column
        assertQuery(ariaSession(),
                "SELECT float_quantity FROM lineitem_aria WHERE float_quantity < 5",
                "SELECT quantity + 1 FROM lineitem WHERE quantity < 4");

        assertQuery(ariaSession(),
                "SELECT orderkey, float_quantity FROM lineitem_aria WHERE float_quantity = 4",
                "SELECT orderkey, quantity + 1 FROM lineitem WHERE quantity = 3");

        // Filter on a tinyint column
        assertQuery(ariaSession(),
                "SELECT linenumber, shipmode, tinyint_shipmode FROM lineitem_aria WHERE tinyint_shipmode in (3, 5, 6)",
                "SELECT linenumber, shipmode, CASE shipmode WHEN 'AIR' THEN 3 WHEN 'MAIL' THEN 5 WHEN 'RAIL' THEN 6 END FROM lineitem WHERE shipmode in ('AIR','MAIL','RAIL')");

        assertQuery(ariaSession(),
                "SELECT linenumber, shipmode FROM lineitem_aria WHERE tinyint_shipmode in (3, 5, 6)",
                "SELECT linenumber, shipmode FROM lineitem WHERE shipmode in ('AIR','MAIL','RAIL')");

        // Filter on a timestamp column
        assertQuery(ariaSession(), "SELECT shipdate, timestamp_shipdate FROM lineitem_aria WHERE timestamp_shipdate < date '1997-11-29'",
                "SELECT shipdate, dateadd('second', suppkey, shipdate) FROM lineitem where shipdate < date '1997-11-29'");

        assertQuery(ariaSession(), "SELECT shipdate FROM lineitem_aria WHERE timestamp_shipdate < date '1997-11-29'",
                "SELECT shipdate FROM lineitem where shipdate < date '1997-11-29'");

        // Filter on a decimal column
        assertQuery(ariaSession(), "SELECT shipdate, short_decimal_discount FROM lineitem_aria WHERE short_decimal_discount < decimal '0.3'",
                "SELECT shipdate, discount FROM lineitem where discount < 0.3");

        assertQuery(ariaSession(), "SELECT shipdate FROM lineitem_aria WHERE short_decimal_discount < decimal '0.3'",
                "SELECT shipdate FROM lineitem where discount < 0.3");

        assertQuery(ariaSession(), "SELECT shipdate, long_decimal_discount FROM lineitem_aria WHERE long_decimal_discount < decimal '0.3'",
                "SELECT shipdate, discount FROM lineitem where discount < 0.3");

        assertQuery(ariaSession(), "SELECT shipdate FROM lineitem_aria WHERE long_decimal_discount < decimal '0.3'",
                "SELECT shipdate FROM lineitem where discount < 0.3");
    }

    // nulls1.sql
    // nulls2_sql
    @Test
    public void testNulls()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey,\n" +
                "    suppkey\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    partkey IS NULL\n" +
                "    AND suppkey IS NULL");

        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    quantity,\n" +
                "    comment\n" +
                "FROM lineitem\n" +
                "WHERE (\n" +
                "        partkey IS NULL\n" +
                "        OR partkey BETWEEN 1000 AND 2000\n" +
                "        OR partkey BETWEEN 10000 AND 11000\n" +
                "    )\n" +
                "    AND (\n" +
                "        suppkey IS NULL\n" +
                "        OR suppkey BETWEEN 1000 AND 2000\n" +
                "        OR suppkey BETWEEN 3000 AND 4000\n" +
                "    )\n" +
                "    AND (\n" +
                "        quantity IS NULL\n" +
                "        OR quantity BETWEEN 5 AND 10\n" +
                "        OR quantity BETWEEN 20 AND 40\n" +
                "    )");
    }

    @Test
    public void testStrings()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    comment,\n" +
                "    partkey_comment,\n" +
                "    suppkey_comment,\n" +
                "    quantity_comment\n" +
                "FROM lineitem_aria_strings\n" +
                "WHERE\n" +
                "    comment > 'f'\n" +
                "    AND partkey_comment > '1'\n" +
                "    AND suppkey_comment > '1'\n" +
                "    AND quantity_comment > '2'");

        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey_struct.comment,\n" +
                "    partkey_struct.partkey_comment,\n" +
                "    suppkey_quantity_struct.suppkey_comment,\n" +
                "    suppkey_quantity_struct.quantity_comment\n" +
                "FROM lineitem_aria_string_structs",
                "SELECT\n" +
                        "    orderkey,\n" +
                        "    linenumber,\n" +
                        "    comment,\n" +
                        "    partkey_comment,\n" +
                        "    suppkey_comment,\n" +
                        "    quantity_comment\n" +
                        "FROM lineitem_aria_strings");

        assertQuery(ariaSession(), "SELECT\n" +
                        "    orderkey,\n" +
                        "    linenumber,\n" +
                        "    partkey_struct.comment,\n" +
                        "    partkey_struct.partkey_comment,\n" +
                        "    suppkey_quantity_struct.suppkey_comment,\n" +
                        "    suppkey_quantity_struct.quantity_comment\n" +
                        "FROM lineitem_aria_string_structs_with_nulls",
                "SELECT\n" +
                        "    orderkey,\n" +
                        "    linenumber,\n" +
                        "    CASEWHEN(mod(partkey, 5) = 0, null, comment),\n" +
                        "    CASEWHEN(mod(partkey, 5) = 0 OR mod(partkey, 13) = 0, null, partkey_comment),\n" +
                        "    CASEWHEN(mod(suppkey, 7) = 0 OR mod(suppkey, 17) = 0, null, suppkey_comment),\n" +
                        "    CASEWHEN(mod(suppkey, 7) = 0, null, quantity_comment)\n" +
                        "FROM lineitem_aria_strings");

        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey_struct.comment,\n" +
                "    partkey_struct.partkey_comment,\n" +
                "    suppkey_quantity_struct.suppkey_comment,\n" +
                "    suppkey_quantity_struct.quantity_comment\n" +
                "FROM lineitem_aria_string_structs_with_nulls\n" +
                "WHERE\n" +
                "    partkey_struct.comment > 'f'\n" +
                "    AND partkey_struct.partkey_comment > '1'\n" +
                "    AND suppkey_quantity_struct.suppkey_comment > '1'\n" +
                "    AND suppkey_quantity_struct.quantity_comment > '2'",
                "SELECT\n" +
                        "    orderkey,\n" +
                        "    linenumber,\n" +
                        "    comment,\n" +
                        "    partkey_comment,\n" +
                        "    suppkey_comment,\n" +
                        "    quantity_comment\n" +
                        "FROM lineitem_aria_strings\n" +
                        "WHERE\n" +
                        "    mod(partkey, 5) <> 0 AND mod(partkey, 13) <> 0" +
                        "    AND mod(suppkey, 7) <> 0 AND mod(suppkey, 17) <> 0" +
                        "    AND comment > 'f'\n" +
                        "    AND partkey_comment > '1'\n" +
                        "    AND suppkey_comment > '1'\n" +
                        "    AND quantity_comment > '2'");
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

    @Test
    public void testJoins()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    p.supplycost\n" +
                "FROM lineitem l\n" +
                "JOIN partsupp p\n" +
                "    ON p.suppkey = l.suppkey AND p.partkey = l.partkey");
    }
}
