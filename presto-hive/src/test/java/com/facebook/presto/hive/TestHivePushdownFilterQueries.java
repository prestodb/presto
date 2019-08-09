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
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static io.airlift.tpch.TpchTable.getTables;
import static java.lang.String.format;

public class TestHivePushdownFilterQueries
        extends AbstractTestQueryFramework
{
    private static final Pattern ARRAY_SUBSCRIPT_PATTERN = Pattern.compile("([a-z_+]+)((\\[[0-9]+\\])+)");

    private static final String WITH_LINEITEM_EX = "WITH lineitem_ex AS (\n" +
            "SELECT linenumber, orderkey, partkey, suppkey, quantity, extendedprice, tax, \n" +
            "   CASE WHEN linenumber % 5 = 0 THEN null ELSE shipmode = 'AIR' END AS ship_by_air, \n" +
            "   CASE WHEN linenumber % 7 = 0 THEN null ELSE returnflag = 'R' END AS is_returned, \n" +
            "   CASE WHEN linenumber % 4 = 0 THEN null ELSE CAST(day(shipdate) AS TINYINT) END AS ship_day, " +
            "   CASE WHEN linenumber % 6 = 0 THEN null ELSE CAST(month(shipdate) AS TINYINT) END AS ship_month, " +
            "   CASE WHEN linenumber % 3 = 0 THEN null ELSE CAST(shipdate AS TIMESTAMP) END AS ship_timestamp, \n" +
            "   CASE WHEN orderkey % 3 = 0 THEN null ELSE CAST(commitdate AS TIMESTAMP) END AS commit_timestamp, \n" +
            "   CASE WHEN orderkey % 5 = 0 THEN null ELSE CAST(discount AS REAL) END AS discount_real, \n" +
            "   CASE WHEN orderkey % 43  = 0 THEN null ELSE discount END as discount, \n" +
            "   CASE WHEN orderkey % 7 = 0 THEN null ELSE CAST(tax AS REAL) END AS tax_real, \n" +
            "   CASE WHEN linenumber % 2 = 0 THEN null ELSE (CAST(day(shipdate) AS TINYINT) , CAST(month(shipdate) AS TINYINT)) END AS ship_day_month, " +
            "   CASE WHEN orderkey % 11 = 0 THEN null ELSE (orderkey, partkey, suppkey) END AS keys, \n" +
            "   CASE WHEN orderkey % 41 = 0 THEN null ELSE (extendedprice, discount, tax) END AS doubles, \n" +
            "   CASE WHEN orderkey % 13 = 0 THEN null ELSE ((orderkey, partkey), (suppkey,), CASE WHEN orderkey % 17 = 0 THEN null ELSE (orderkey, partkey) END) END AS nested_keys, \n" +
            "   CASE WHEN orderkey % 17 = 0 THEN null ELSE (shipmode = 'AIR', returnflag = 'R') END as flags, \n" +
            "   CASE WHEN orderkey % 19 = 0 THEN null ELSE (CAST(discount AS REAL), CAST(tax AS REAL)) END as reals, \n" +
            "   CASE WHEN orderkey % 23 = 0 THEN null ELSE (orderkey, linenumber, (CAST(day(shipdate) as TINYINT), CAST(month(shipdate) AS TINYINT), CAST(year(shipdate) AS INTEGER))) END AS info, \n" +
            "   CASE WHEN orderkey % 31 = 0 THEN null ELSE (" +
            "       (CAST(day(shipdate) AS TINYINT), CAST(month(shipdate) AS TINYINT), CAST(year(shipdate) AS INTEGER)), " +
            "       (CAST(day(commitdate) AS TINYINT), CAST(month(commitdate) AS TINYINT), CAST(year(commitdate) AS INTEGER)), " +
            "       (CAST(day(receiptdate) AS TINYINT), CAST(month(receiptdate) AS TINYINT), CAST(year(receiptdate) AS INTEGER))) END AS dates, \n" +
            "   CASE WHEN orderkey % 37 = 0 THEN null ELSE (CAST(shipdate AS TIMESTAMP), CAST(commitdate AS TIMESTAMP)) END AS timestamps \n" +

            "FROM lineitem)\n";

    protected TestHivePushdownFilterQueries()
    {
        super(TestHivePushdownFilterQueries::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(getTables(),
                ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"),
                "sql-standard",
                ImmutableMap.of("hive.pushdown-filter-enabled", "true"),
                Optional.empty());

        queryRunner.execute(noPushdownFilter(queryRunner.getDefaultSession()),
                "CREATE TABLE lineitem_ex (linenumber, orderkey, partkey, suppkey, quantity, extendedprice, tax, ship_by_air, is_returned, ship_day, ship_month, ship_timestamp, commit_timestamp, discount_real, discount, tax_real, ship_day_month, keys, doubles, nested_keys, flags, reals, info, dates, timestamps) AS " +
                        "SELECT linenumber, orderkey, partkey, suppkey, quantity, extendedprice, tax, " +
                        "   IF (linenumber % 5 = 0, null, shipmode = 'AIR') AS ship_by_air, " +
                        "   IF (linenumber % 7 = 0, null, returnflag = 'R') AS is_returned, " +
                        "   IF (linenumber % 4 = 0, null, CAST(day(shipdate) AS TINYINT)) AS ship_day, " +
                        "   IF (linenumber % 6 = 0, null, CAST(month(shipdate) AS TINYINT)) AS ship_month, " +
                        "   IF (linenumber % 3 = 0, null, CAST(shipdate AS TIMESTAMP)) AS ship_timestamp, " +
                        "   IF (orderkey % 3 = 0, null, CAST(commitdate AS TIMESTAMP)) AS commit_timestamp, " +
                        "   IF (orderkey % 5 = 0, null, CAST(discount AS REAL)) AS discount_real, " +
                        "   IF (orderkey % 43 = 0, null, discount) AS discount, " +
                        "   IF (orderkey % 7 = 0, null, CAST(tax AS REAL)) AS tax_real, " +
                        "   IF (linenumber % 2 = 0, null, ARRAY[CAST(day(shipdate) AS TINYINT), CAST(month(shipdate) AS TINYINT)]) AS ship_day_month, " +
                        "   IF (orderkey % 11 = 0, null, ARRAY[orderkey, partkey, suppkey]) AS keys, " +
                        "   IF (orderkey % 41 = 0, null, ARRAY[extendedprice, discount, tax]) AS doubles, " +
                        "   IF (orderkey % 13 = 0, null, ARRAY[ARRAY[orderkey, partkey], ARRAY[suppkey], IF (orderkey % 17 = 0, null, ARRAY[orderkey, partkey])]) AS nested_keys, " +
                        "   IF (orderkey % 17 = 0, null, ARRAY[shipmode = 'AIR', returnflag = 'R']) AS flags, " +
                        "   IF (orderkey % 19 = 0, null, ARRAY[CAST(discount AS REAL), CAST(tax AS REAL)]), " +
                        "   IF (orderkey % 23 = 0, null, CAST(ROW(orderkey, linenumber, ROW(day(shipdate), month(shipdate), year(shipdate))) " +
                        "       AS ROW(orderkey BIGINT, linenumber INTEGER, shipdate ROW(ship_day TINYINT, ship_month TINYINT, ship_year INTEGER)))), " +
                        "   IF (orderkey % 31 = 0, NULL, ARRAY[" +
                        "       CAST(ROW(day(shipdate), month(shipdate), year(shipdate)) AS ROW(day TINYINT, month TINYINT, year INTEGER)), " +
                        "       CAST(ROW(day(commitdate), month(commitdate), year(commitdate)) AS ROW(day TINYINT, month TINYINT, year INTEGER)), " +
                        "       CAST(ROW(day(receiptdate), month(receiptdate), year(receiptdate)) AS ROW(day TINYINT, month TINYINT, year INTEGER))]), " +
                        "   IF (orderkey % 37 = 0, NULL, ARRAY[CAST(shipdate AS TIMESTAMP), CAST(commitdate AS TIMESTAMP)]) AS timestamps " +
                        "FROM lineitem");
        return queryRunner;
    }

    @Test
    public void testBooleans()
    {
        // Single boolean column
        assertQueryUsingH2Cte("SELECT is_returned FROM lineitem_ex");

        assertQueryUsingH2Cte("SELECT is_returned FROM lineitem_ex WHERE is_returned = true");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE is_returned is not null");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE is_returned = false");

        // Two boolean columns
        assertQueryUsingH2Cte("SELECT ship_by_air, is_returned FROM lineitem_ex");

        assertQueryUsingH2Cte("SELECT ship_by_air, is_returned FROM lineitem_ex WHERE ship_by_air = true");

        assertQueryUsingH2Cte("SELECT ship_by_air, is_returned FROM lineitem_ex WHERE ship_by_air = true AND is_returned = false");

        assertQueryUsingH2Cte("SELECT COUNT(*) FROM lineitem_ex WHERE ship_by_air is null");

        assertQueryUsingH2Cte("SELECT COUNT(*) FROM lineitem_ex WHERE ship_by_air is not null AND is_returned = true");
    }

    @Test
    public void testBytes()
    {
        // Single tinyint column
        assertQueryUsingH2Cte("SELECT ship_day FROM lineitem_ex");

        assertQueryUsingH2Cte("SELECT ship_day FROM lineitem_ex WHERE ship_day < 15");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE ship_day > 15");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE ship_day is null");

        // Two tinyint columns
        assertQueryUsingH2Cte("SELECT ship_day, ship_month FROM lineitem_ex");

        assertQueryUsingH2Cte("SELECT ship_day, ship_month FROM lineitem_ex WHERE ship_month = 1");

        assertQueryUsingH2Cte("SELECT ship_day, ship_month FROM lineitem_ex WHERE ship_day = 1 AND ship_month = 1");

        assertQueryUsingH2Cte("SELECT COUNT(*) FROM lineitem_ex WHERE ship_month is null");

        assertQueryUsingH2Cte("SELECT COUNT(*) FROM lineitem_ex WHERE ship_day is not null AND ship_month = 1");

        assertQueryUsingH2Cte("SELECT ship_day, ship_month FROM lineitem_ex WHERE ship_day > 15 AND ship_month < 5 AND (ship_day + ship_month) < 20");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE ship_day_month[2] = 12");
    }

    @Test
    public void testNumeric()
    {
        assertQuery("SELECT orderkey, custkey, orderdate, shippriority FROM orders");

        assertQuery("SELECT count(*) FROM orders WHERE orderkey BETWEEN 100 AND 1000 AND custkey BETWEEN 500 AND 800");

        assertQuery("SELECT custkey, orderdate, shippriority FROM orders WHERE orderkey BETWEEN 100 AND 1000 AND custkey BETWEEN 500 AND 800");

        assertQuery("SELECT orderkey, orderdate FROM orders WHERE orderdate BETWEEN date '1994-01-01' AND date '1997-03-30'");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE orderkey < 30000 AND ship_by_air = true");

        assertQueryUsingH2Cte("SELECT linenumber, orderkey, ship_by_air, is_returned FROM lineitem_ex WHERE orderkey < 30000 AND ship_by_air = true");

        assertQueryUsingH2Cte("SELECT linenumber, ship_by_air, is_returned FROM lineitem_ex WHERE orderkey < 30000 AND ship_by_air = true");
    }

    @Test
    public void testTimestamps()
    {
        // Single timestamp column
        assertQueryUsingH2Cte("SELECT ship_timestamp FROM lineitem_ex");

        assertQueryUsingH2Cte("SELECT ship_timestamp FROM lineitem_ex WHERE ship_timestamp < TIMESTAMP '1993-01-01 01:00:00'");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE ship_timestamp IS NOT NULL");

        assertQueryUsingH2Cte("SELECT count(*) FROM lineitem_ex WHERE ship_timestamp = TIMESTAMP '2012-08-08 01:00:00'");

        // Two timestamp columns
        assertQueryUsingH2Cte("SELECT commit_timestamp, ship_timestamp FROM lineitem_ex");

        assertQueryUsingH2Cte("SELECT commit_timestamp, ship_timestamp FROM lineitem_ex WHERE ship_timestamp > TIMESTAMP '1993-08-08 01:00:00' AND commit_timestamp < TIMESTAMP '1993-08-08 01:00:00'");

        assertQueryReturnsEmptyResult("SELECT commit_timestamp, ship_timestamp FROM lineitem_ex WHERE year(ship_timestamp) - year(commit_timestamp) > 1");

        assertQueryUsingH2Cte("SELECT commit_timestamp, ship_timestamp, orderkey FROM lineitem_ex WHERE year(commit_timestamp) > 1993 and year(ship_timestamp) > 1993 and year(ship_timestamp) - year(commit_timestamp) = 1");

        assertQueryUsingH2Cte("SELECT count(*) from lineitem_ex where timestamps[1] > TIMESTAMP '1993-08-08 01:00:00'");

        assertQueryUsingH2Cte("SELECT count(*) from lineitem_ex where year(timestamps[1]) != year(timestamps[2])");
    }

    @Test
    public void testDouble()
    {
        assertQuery("SELECT quantity, extendedprice, discount, tax FROM lineitem");

        assertQueryUsingH2Cte("SELECT count(discount) FROM lineitem_ex");

        assertFilterProject("discount IS NULL", "count(*)");

        assertFilterProject("discount IS NOT NULL", "sum(quantity), sum(discount)");

        assertFilterProject("is_returned = true", "quantity, extendedprice, discount");

        assertFilterProject("quantity = 4", "orderkey, tax");

        assertFilterProject("quantity = 4 AND discount = 0", "extendedprice, discount");

        assertFilterProject("quantity = 4 AND discount = 0 AND tax = .05", "orderkey");

        assertFilterProject("(discount + tax) < (quantity / 10)", "tax");

        assertFilterProject("doubles[1] > 0.01", "count(*)");

        // Compact values
        assertFilterProject("discount + tax  > .05 AND discount > .01 AND tax > .01", "tax");

        // SucceedingPositionsToFail > 0 in readWithFilter
        assertFilterProject("is_returned AND doubles[2] = .01 AND doubles[1] + discount > 0.10", "count(*)");
    }

    @Test
    public void testFloats()
    {
        assertQueryUsingH2Cte("SELECT discount_real, tax_real FROM lineitem_ex");

        assertFilterProject("tax_real IS NOT NULL", "count(*)");

        assertFilterProject("tax_real IS NULL", "count(*)");

        assertFilterProject("tax_real > 0.1", "count(*)");

        assertFilterProject("tax_real < 0.03", "discount_real, tax_real");

        assertFilterProject("tax_real < 0.05  AND discount_real > 0.05", "discount_real");

        assertFilterProject("tax_real = discount_real", "discount_real");

        assertFilterProject("discount_real > 0.01 AND tax_real > 0.01 AND (discount_real + tax_real) < 0.08", "discount_real");

        assertFilterProject("reals[1] > 0.01", "count(*)");
    }

    @Test
    public void testArrays()
    {
        // read all positions
        assertQueryUsingH2Cte("SELECT * FROM lineitem_ex");

        // top-level IS [NOT] NULL filters
        assertFilterProject("keys IS NULL", "orderkey, flags");
        assertFilterProject("nested_keys IS NULL", "keys, flags");

        assertFilterProject("flags IS NOT NULL", "keys, orderkey");
        assertFilterProject("nested_keys IS NOT NULL", "keys, flags");

        // mid-level IS [NOR] NULL filters
        assertFilterProject("nested_keys[3] IS NULL", "keys, flags");
        assertFilterProject("nested_keys[3] IS NOT NULL", "keys, flags");

        // read selected positions
        assertQueryUsingH2Cte("SELECT * FROM lineitem_ex WHERE orderkey = 1");

        // read all positions; extract selected positions
        assertQueryUsingH2Cte("SELECT * FROM lineitem_ex WHERE orderkey % 3 = 1");

        // filter
        assertFilterProject("keys[2] = 1", "orderkey, flags");
        assertFilterProject("nested_keys[1][2] = 1", "orderkey, flags");

        // filter function
        assertFilterProject("keys[2] % 3 = 1", "orderkey, flags");
        assertFilterProject("nested_keys[1][2] % 3 = 1", "orderkey, flags");

        // less selective filter
        assertFilterProject("keys[1] < 1000", "orderkey, flags");
        assertFilterProject("nested_keys[1][1] < 1000", "orderkey, flags");

        // filter plus filter function
        assertFilterProject("keys[1] < 1000 AND keys[2] % 3 = 1", "orderkey, flags");
        assertFilterProject("nested_keys[1][1] < 1000 AND nested_keys[1][2] % 3 = 1", "orderkey, flags");

        // filter function on multiple columns
        assertFilterProject("keys[1] % 3 = 1 AND (orderkey + keys[2]) % 5 = 1", "orderkey, flags");
        assertFilterProject("nested_keys[1][1] % 3 = 1 AND (orderkey + nested_keys[1][2]) % 5 = 1", "orderkey, flags");

        // filter on multiple columns, plus filter function
        assertFilterProject("keys[1] < 1000 AND flags[2] = true AND keys[2] % 2 = if(flags[1], 0, 1)", "orderkey, flags");
        assertFilterProject("nested_keys[1][1] < 1000 AND flags[2] = true AND nested_keys[1][2] % 2 = if(flags[1], 0, 1)", "orderkey, flags");

        // filters at different levels
        assertFilterProject("nested_keys IS NOT NULL AND nested_keys[1][1] > 0", "keys");
        assertFilterProject("nested_keys[3] IS NULL AND nested_keys[2][1] > 10", "keys, flags");
        assertFilterProject("nested_keys[3] IS NOT NULL AND nested_keys[1][2] > 10", "keys, flags");
        assertFilterProject("nested_keys IS NOT NULL AND nested_keys[3] IS NOT NULL AND nested_keys[1][1] > 0", "keys");
        assertFilterProject("nested_keys IS NOT NULL AND nested_keys[3] IS NULL AND nested_keys[1][1] > 0", "keys");

        assertFilterProjectFails("keys[5] > 0", "orderkey", "Array subscript out of bounds");
        assertFilterProjectFails("nested_keys[5][1] > 0", "orderkey", "Array subscript out of bounds");
        assertFilterProjectFails("nested_keys[1][5] > 0", "orderkey", "Array subscript out of bounds");
        assertFilterProjectFails("nested_keys[2][5] > 0", "orderkey", "Array subscript out of bounds");
    }

    @Test
    public void testStructs()
    {
        assertQueryUsingH2Cte("SELECT orderkey, info, dates FROM lineitem_ex");

        Function<String, String> rewriter = query -> query.replaceAll("info.orderkey", "info[1]")
                .replaceAll("info.linenumber", "info[2]")
                .replaceAll("info.shipdate.ship_year", "info[3][3]")
                .replaceAll("info.shipdate", "info[3]")
                .replaceAll("dates\\[1\\].day", "dates[1][1]");

        assertQueryUsingH2Cte("SELECT info.orderkey FROM lineitem_ex", rewriter);
        assertQueryUsingH2Cte("SELECT info.orderkey, info.linenumber FROM lineitem_ex", rewriter);

        assertQueryUsingH2Cte("SELECT info.linenumber, info.shipdate.ship_year FROM lineitem_ex WHERE orderkey < 1000", rewriter);

        assertQueryUsingH2Cte("SELECT info.orderkey FROM lineitem_ex WHERE info IS NULL", rewriter);
        assertQueryUsingH2Cte("SELECT info.orderkey FROM lineitem_ex WHERE info IS NOT NULL", rewriter);

        assertQueryUsingH2Cte("SELECT info, dates FROM lineitem_ex WHERE info.orderkey % 7 = 0", rewriter);
        assertQueryUsingH2Cte("SELECT info.orderkey, info.shipdate FROM lineitem_ex WHERE info.orderkey % 7 = 0", rewriter);

        assertQueryUsingH2Cte("SELECT dates FROM lineitem_ex WHERE dates[1].day % 2 = 0", rewriter);
    }

    private void assertFilterProject(String filter, String projections)
    {
        assertQueryUsingH2Cte(format("SELECT * FROM lineitem_ex WHERE %s", filter));
        assertQueryUsingH2Cte(format("SELECT %s FROM lineitem_ex WHERE %s", projections, filter));
    }

    private void assertFilterProjectFails(String filter, String projections, String expectedMessageRegExp)
    {
        assertQueryFails(format("SELECT * FROM lineitem_ex WHERE %s", filter), expectedMessageRegExp);
        assertQueryFails(format("SELECT %s FROM lineitem_ex WHERE %s", projections, filter), expectedMessageRegExp);
    }

    @Test
    public void testFilterFunctions()
    {
        // filter function on orderkey; orderkey is projected out
        assertQuery("SELECT custkey, orderkey, orderdate FROM orders WHERE orderkey % 5 = 0");

        // filter function on orderkey; orderkey is not projected out
        assertQuery("SELECT custkey, orderdate FROM orders WHERE orderkey % 5 = 0");

        // filter function and range predicate on orderkey
        assertQuery("SELECT custkey, orderdate FROM orders WHERE orderkey % 5 = 0 AND orderkey > 100");

        // multiple filter functions
        assertQuery("SELECT custkey, orderdate FROM orders WHERE orderkey % 5 = 0 AND custkey % 7 = 0");

        // multi-column filter functions
        assertQuery("SELECT custkey, orderdate FROM orders WHERE (orderkey + custkey) % 5 = 0");

        // filter function with an error
        assertQueryFails("SELECT custkey, orderdate FROM orders WHERE array[1, 2, 3][orderkey % 5 + custkey % 7 + 1] > 0", "Array subscript out of bounds");

        // filter function with "recoverable" error
        assertQuery("SELECT custkey, orderdate FROM orders WHERE array[1, 2, 3][orderkey % 5 + custkey % 7 + 1] > 0 AND orderkey % 5 = 1 AND custkey % 7 = 0", "SELECT custkey, orderdate FROM orders WHERE orderkey % 5 = 1 AND custkey % 7 = 0");

        // filter function on numeric and boolean columns
        assertFilterProject("if(is_returned, linenumber, orderkey) % 5 = 0", "linenumber");

        // filter functions on array columns
        assertFilterProject("keys[1] % 5 = 0", "orderkey");
        assertFilterProject("nested_keys[1][1] % 5 = 0", "orderkey");

        assertFilterProject("keys[1] % 5 = 0 AND keys[2] > 100", "orderkey");
        assertFilterProject("keys[1] % 5 = 0 AND nested_keys[1][2] > 100", "orderkey");

        assertFilterProject("keys[1] % 5 = 0 AND keys[2] % 7 = 0", "orderkey");
        assertFilterProject("keys[1] % 5 = 0 AND nested_keys[1][2] % 7 = 0", "orderkey");

        assertFilterProject("(cast(keys[1] as integer) + keys[3]) % 5 = 0", "orderkey");
        assertFilterProject("(cast(keys[1] as integer) + nested_keys[1][2]) % 5 = 0", "orderkey");

        // subscript out of bounds
        assertQueryFails("SELECT orderkey FROM lineitem_ex WHERE keys[5] % 7 = 0", "Array subscript out of bounds");
        assertQueryFails("SELECT orderkey FROM lineitem_ex WHERE nested_keys[1][5] % 7 = 0", "Array subscript out of bounds");

        assertQueryFails("SELECT * FROM lineitem_ex WHERE nested_keys[1][5] > 0", "Array subscript out of bounds");
        assertQueryFails("SELECT orderkey FROM lineitem_ex WHERE nested_keys[1][5] > 0", "Array subscript out of bounds");
        assertQueryFails("SELECT * FROM lineitem_ex WHERE nested_keys[1][5] > 0 AND orderkey % 5 = 0", "Array subscript out of bounds");

        assertFilterProject("nested_keys[1][5] > 0 AND orderkey % 5 > 10", "keys");
    }

    @Test
    public void testPartitionColumns()
    {
        assertUpdate("CREATE TABLE test_partition_columns WITH (partitioned_by = ARRAY['p']) AS\n" +
                "SELECT * FROM (VALUES (1, 'abc'), (2, 'abc')) as t(x, p)", 2);

        assertQuery("SELECT * FROM test_partition_columns", "SELECT 1, 'abc' UNION ALL SELECT 2, 'abc'");

        assertQuery("SELECT * FROM test_partition_columns WHERE p = 'abc'", "SELECT 1, 'abc' UNION ALL SELECT 2, 'abc'");

        assertQuery("SELECT * FROM test_partition_columns WHERE p LIKE 'a%'", "SELECT 1, 'abc' UNION ALL SELECT 2, 'abc'");

        assertQuery("SELECT * FROM test_partition_columns WHERE substr(p, x, 1) = 'a'", "SELECT 1, 'abc'");

        assertQueryReturnsEmptyResult("SELECT * FROM test_partition_columns WHERE p = 'xxx'");

        assertUpdate("DROP TABLE test_partition_columns");
    }

    @Test
    public void testBucketColumn()
    {
        getQueryRunner().execute("CREATE TABLE test_bucket_column WITH (bucketed_by = ARRAY['orderkey'], bucket_count = 11) AS " +
                "SELECT linenumber, orderkey FROM lineitem");

        assertQuery("SELECT linenumber, \"$bucket\" FROM test_bucket_column", "SELECT linenumber, orderkey % 11 FROM lineitem");
        assertQuery("SELECT linenumber, \"$bucket\" FROM test_bucket_column WHERE (\"$bucket\" + linenumber) % 2 = 1", "SELECT linenumber, orderkey % 11 FROM lineitem WHERE (orderkey % 11 + linenumber) % 2 = 1");

        assertUpdate("DROP TABLE test_bucket_column");
    }

    @Test
    public void testPathColumn()
    {
        Session session = getQueryRunner().getDefaultSession();
        assertQuerySucceeds(session, "SELECT linenumber, \"$path\" FROM lineitem");
        assertQuerySucceeds(session, "SELECT linenumber, \"$path\" FROM lineitem WHERE length(\"$path\") % 2 = linenumber % 2");
    }

    //TODO add a correctness check for the results and move this test to TestOrcSelectiveReader
    @Test
    public void testArraysOfNulls()
    {
        getQueryRunner().execute("CREATE TABLE test_arrays_of_nulls AS " +
                "SELECT orderkey, linenumber, " +
                "   CAST(ARRAY[null, null, null] AS ARRAY(BIGINT)) bigints, " +
                "   CAST(ARRAY[null, null, null] AS ARRAY(INTEGER)) integers, " +
                "   CAST(ARRAY[null, null, null] AS ARRAY(SMALLINT)) smallints, " +
                "   CAST(ARRAY[null, null, null] AS ARRAY(TINYINT)) tinyints, " +
                "   CAST(ARRAY[null, null, null] AS ARRAY(BOOLEAN)) booleans, " +
                "   CAST(ARRAY[null, null, null] AS ARRAY(TIMESTAMP)) timestamps, " +
                "   CAST(ARRAY[null, null, null] AS ARRAY(REAL)) floats, " +
                "   CAST(ARRAY[null, null, null] AS ARRAY(DOUBLE)) doubles " +
                "FROM lineitem");

        List<String> columnNames = ImmutableList.of(
                "bigints", "integers", "tinyints", "smallints", "booleans", "floats", "doubles");
        try {
            for (String columnName : columnNames) {
                assertQuerySucceeds(getSession(), format("SELECT count(*) FROM test_arrays_of_nulls WHERE %s[1] IS NOT NULL", columnName));
                assertQuerySucceeds(getSession(), format("SELECT count(*) FROM test_arrays_of_nulls WHERE %s[1] IS NULL", columnName));
            }
        }
        finally {
            getQueryRunner().execute("DROP TABLE test_arrays_of_nulls");
        }
    }

    private void assertQueryUsingH2Cte(String query)
    {
        assertQueryUsingH2Cte(query, Function.identity());
    }

    private void assertQueryUsingH2Cte(String query, Function<String, String> rewriter)
    {
        assertQuery(query, WITH_LINEITEM_EX + toH2(rewriter.apply(query)));
    }

    private static String toH2(String query)
    {
        return replaceArraySubscripts(query).replaceAll(" if\\(", " casewhen(");
    }

    private static String replaceArraySubscripts(String query)
    {
        Matcher matcher = ARRAY_SUBSCRIPT_PATTERN.matcher(query);

        StringBuilder builder = new StringBuilder();
        int offset = 0;
        while (matcher.find()) {
            String expression = matcher.group(1);
            List<String> indices = Splitter.onPattern("[^0-9]").omitEmptyStrings().splitToList(matcher.group(2));
            for (int i = 0; i < indices.size(); i++) {
                expression = format("array_get(%s, %s)", expression, indices.get(i));
            }

            builder.append(query, offset, matcher.start()).append(expression);
            offset = matcher.end();
        }
        builder.append(query.substring(offset));
        return builder.toString();
    }

    private static Session noPushdownFilter(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "false")
                .build();
    }
}
