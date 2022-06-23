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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.DATE;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.REAL;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PARTIAL_AGGREGATION_PUSHDOWN_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveStorageFormat.RCTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.tpch.TpchTable.getTables;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.joining;

public class TestHivePushdownFilterQueries
        extends AbstractTestQueryFramework
{
    private static final Pattern ARRAY_SUBSCRIPT_PATTERN = Pattern.compile("([a-z_+]+)((\\[[0-9]+\\])+)");

    private static final String WITH_LINEITEM_EX = "WITH lineitem_ex AS (\n" +
            "SELECT linenumber, orderkey, partkey, suppkey, quantity, extendedprice, tax, shipinstruct, shipmode, \n" +
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
            "   CASE WHEN orderkey % 37 = 0 THEN null ELSE CAST(discount AS DECIMAL(20, 8)) END AS discount_long_decimal, " +
            "   CASE WHEN orderkey % 41 = 0 THEN null ELSE CAST(tax AS DECIMAL(3, 2)) END AS tax_short_decimal, " +
            "   CASE WHEN orderkey % 43 = 0 THEN null ELSE (CAST(discount AS DECIMAL(20, 8)), CAST(tax AS DECIMAL(20, 8))) END AS long_decimals, " +
            "   CASE WHEN orderkey % 11 = 0 THEN null ELSE (orderkey, partkey, suppkey) END AS keys, \n" +
            "   CASE WHEN orderkey % 41 = 0 THEN null ELSE (extendedprice, discount, tax) END AS doubles, \n" +
            "   CASE WHEN orderkey % 13 = 0 THEN null ELSE ARRAY[ARRAY[orderkey, partkey], ARRAY[suppkey], CASE WHEN orderkey % 17 = 0 THEN null ELSE ARRAY[orderkey, partkey] END] END AS nested_keys, \n" +
            "   CASE WHEN orderkey % 17 = 0 THEN null ELSE (shipmode = 'AIR', returnflag = 'R') END as flags, \n" +
            "   CASE WHEN orderkey % 19 = 0 THEN null ELSE (CAST(discount AS REAL), CAST(tax AS REAL)) END as reals, \n" +
            "   CASE WHEN orderkey % 23 = 0 THEN null ELSE (orderkey, linenumber, (CAST(day(shipdate) as TINYINT), CAST(month(shipdate) AS TINYINT), CAST(year(shipdate) AS INTEGER))) END AS info, \n" +
            "   CASE WHEN orderkey % 31 = 0 THEN null ELSE (" +
            "       (CAST(day(shipdate) AS TINYINT), CAST(month(shipdate) AS TINYINT), CAST(year(shipdate) AS INTEGER)), " +
            "       (CAST(day(commitdate) AS TINYINT), CAST(month(commitdate) AS TINYINT), CAST(year(commitdate) AS INTEGER)), " +
            "       (CAST(day(receiptdate) AS TINYINT), CAST(month(receiptdate) AS TINYINT), CAST(year(receiptdate) AS INTEGER))) END AS dates, \n" +
            "   CASE WHEN orderkey % 37 = 0 THEN null ELSE (CAST(shipdate AS TIMESTAMP), CAST(commitdate AS TIMESTAMP)) END AS timestamps, \n" +
            "   CASE WHEN orderkey % 43 = 0 THEN null ELSE comment END AS comment, \n" +
            "   CASE WHEN orderkey % 43 = 0 THEN null ELSE upper(comment) END AS uppercase_comment, \n" +
            "   CAST('' as VARBINARY) AS empty_comment, \n" +
            "   CASE WHEN orderkey % 47 = 0 THEN null ELSE CAST(comment AS CHAR(5)) END AS fixed_comment, \n" +
            "   CASE WHEN orderkey % 49 = 0 THEN null ELSE (CAST(comment AS CHAR(4)), CAST(comment AS CHAR(3)), CAST(SUBSTR(comment,length(comment) - 4) AS CHAR(4))) END AS char_array, \n" +
            "   CASE WHEN orderkey % 49 = 0 THEN null ELSE (comment, comment) END AS varchar_array \n" +

            "FROM lineitem)\n";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(getTables(),
                ImmutableMap.of("experimental.pushdown-subfields-enabled", "true",
                        "experimental.pushdown-dereference-enabled", "true"),
                "sql-standard",
                ImmutableMap.of("hive.pushdown-filter-enabled", "true",
                        "hive.enable-parquet-dereference-pushdown", "true",
                        "hive.partial_aggregation_pushdown_enabled", "true",
                        "hive.partial_aggregation_pushdown_for_variable_length_datatypes_enabled", "true"),
                Optional.empty());

        queryRunner.execute(noPushdownFilter(queryRunner.getDefaultSession()),
                "CREATE TABLE lineitem_ex (linenumber, orderkey, partkey, suppkey, quantity, extendedprice, tax, shipinstruct, shipmode, ship_by_air, is_returned, ship_day, ship_month, ship_timestamp, commit_timestamp, discount_real, discount, tax_real, ship_day_month, discount_long_decimal, tax_short_decimal, long_decimals, keys, doubles, nested_keys, flags, reals, info, dates, timestamps, comment, uppercase_comment, empty_comment, fixed_comment, char_array, varchar_array) AS " +
                        "SELECT linenumber, orderkey, partkey, suppkey, quantity, extendedprice, tax, shipinstruct, shipmode, " +
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
                        "   IF (orderkey % 37 = 0, null, CAST(discount AS DECIMAL(20, 8))) AS discount_long_decimal, " +
                        "   IF (orderkey % 41 = 0, null, CAST(tax AS DECIMAL(3, 2))) AS tax_short_decimal, " +
                        "   IF (orderkey % 43 = 0, null, ARRAY[CAST(discount AS DECIMAL(20, 8)), CAST(tax AS DECIMAL(20, 8))]) AS long_decimals, " +
                        "   IF (orderkey % 11 = 0, null, ARRAY[orderkey, partkey, suppkey]) AS keys, " +
                        "   IF (orderkey % 41 = 0, null, ARRAY[extendedprice, discount, tax]) AS doubles, " +
                        "   IF (orderkey % 13 = 0, null, ARRAY[ARRAY[orderkey, partkey], ARRAY[suppkey], IF (orderkey % 17 = 0, null, ARRAY[orderkey, partkey])]) AS nested_keys, " +
                        "   IF (orderkey % 17 = 0, null, ARRAY[shipmode = 'AIR', returnflag = 'R']) AS flags, " +
                        "   IF (orderkey % 19 = 0, null, ARRAY[CAST(discount AS REAL), CAST(tax AS REAL)]), " +
                        "   IF (orderkey % 23 = 0, null, CAST(ROW(orderkey, linenumber, ROW(day(shipdate), month(shipdate), year(shipdate))) AS ROW(orderkey BIGINT, linenumber INTEGER, shipdate ROW(ship_day TINYINT, ship_month TINYINT, ship_year INTEGER)))), " +
                        "   IF (orderkey % 31 = 0, NULL, ARRAY[" +
                        "       CAST(ROW(day(shipdate), month(shipdate), year(shipdate)) AS ROW(day TINYINT, month TINYINT, year INTEGER)), " +
                        "       CAST(ROW(day(commitdate), month(commitdate), year(commitdate)) AS ROW(day TINYINT, month TINYINT, year INTEGER)), " +
                        "       CAST(ROW(day(receiptdate), month(receiptdate), year(receiptdate)) AS ROW(day TINYINT, month TINYINT, year INTEGER))]), " +
                        "   IF (orderkey % 37 = 0, NULL, ARRAY[CAST(shipdate AS TIMESTAMP), CAST(commitdate AS TIMESTAMP)]) AS timestamps, " +
                        "   IF (orderkey % 43 = 0, NULL, comment) AS comment, " +
                        "   IF (orderkey % 43 = 0, NULL, upper(comment)) AS uppercase_comment, " +
                        "   CAST('' as VARBINARY) AS empty_comment, \n" +
                        "   IF (orderkey % 47 = 0, NULL, CAST(comment AS CHAR(5))) AS fixed_comment, " +
                        "   IF (orderkey % 49 = 0, NULL, ARRAY[CAST(comment AS CHAR(4)), CAST(comment AS CHAR(3)), CAST(SUBSTR(comment,length(comment) - 4) AS CHAR(4))]) AS char_array, " +
                        "   IF (orderkey % 49 = 0, NULL, ARRAY[comment, comment]) AS varchar_array " +
                        "FROM lineitem");
        return queryRunner;
    }

    @Test
    public void testTableSampling()
    {
        assertQuerySucceeds("SELECT * FROM lineitem TABLESAMPLE BERNOULLI (1)");

        assertQuerySucceeds("SELECT * FROM lineitem TABLESAMPLE BERNOULLI (1) WHERE orderkey > 1000");

        assertQuerySucceeds("SELECT * FROM lineitem TABLESAMPLE BERNOULLI (1) WHERE orderkey % 2 = 0");

        assertQueryReturnsEmptyResult("SELECT * FROM lineitem WHERE rand() > 1");

        // error in filter function with no inputs
        assertQueryFails("SELECT * FROM lineitem WHERE array[1, 2, 3][cast(floor(rand()) as integer)] > 0", "SQL array indices start at 1");

        // error in filter function with no inputs is masked by another filter
        assertQuerySucceeds("SELECT * FROM lineitem WHERE array[1, 2, 3][cast(floor(rand()) as integer)] > 0 AND linenumber < 0");
        assertQuerySucceeds("SELECT * FROM lineitem WHERE array[1, 2, 3][cast(floor(rand()) as integer)] > 0 AND linenumber % 2 < 0");

        // error in filter function with no inputs is masked by an error in another filter
        assertQueryFails("SELECT * FROM lineitem WHERE array[1, 2, 3][cast(floor(rand()) as integer)] > 0 AND array[1, 2, 3][cast(floor(rand() * linenumber) as integer) - linenumber] > 0", "Array subscript is negative");
    }

    @Test
    public void testLegacyUnnest()
    {
        Session legacyUnnest = Session.builder(getSession()).setSystemProperty("legacy_unnest", "true").build();

        assertQuery(legacyUnnest, "SELECT orderkey, date.day FROM lineitem_ex CROSS JOIN UNNEST(dates) t(date)",
                "SELECT orderkey, day(shipdate) FROM lineitem WHERE orderkey % 31 <> 0 UNION ALL " +
                        "SELECT orderkey, day(commitdate) FROM lineitem WHERE orderkey % 31 <> 0 UNION ALL " +
                        "SELECT orderkey, day(receiptdate) FROM lineitem WHERE orderkey % 31 <> 0");
    }

    @Test
    public void testPushdownWithDisjointFilters()
    {
        assertQueryUsingH2Cte("SELECT * FROM lineitem_ex where orderkey = 1 and orderkey = 2");

        assertQueryUsingH2Cte("SELECT count(*) FROM orders WHERE orderkey = 100 and orderkey = 101");
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
    public void testMaps()
    {
        getQueryRunner().execute("CREATE TABLE test_maps AS " +
                "SELECT orderkey, " +
                "   linenumber, " +
                "   IF (keys IS NULL, null, MAP(ARRAY[1, 2, 3], keys)) AS map_keys, " +
                "   IF (flags IS NULL, null, MAP(ARRAY[1, 2], flags)) AS map_flags " +
                "FROM lineitem_ex");

        Function<String, String> rewriter = query -> query.replaceAll("map_keys", "keys")
                .replaceAll("map_flags", "flags")
                .replaceAll("test_maps", "lineitem_ex")
                .replaceAll("cardinality", "array_length");
        try {
            //filter on nested columns
            assertQueryUsingH2Cte("SELECT * FROM test_maps WHERE map_keys[1] > 10 and map_keys[1] < 20", rewriter);

            assertQueryUsingH2Cte("SELECT map_keys[1] FROM test_maps WHERE linenumber < 3", rewriter);

            assertQueryUsingH2Cte("SELECT cardinality(map_keys) FROM test_maps", rewriter);
            assertQueryUsingH2Cte("SELECT cardinality(map_keys) FROM test_maps WHERE map_keys[1] % 2 = 0", rewriter);

            assertQueryUsingH2Cte("SELECT map_keys[1] FROM test_maps", rewriter);
            assertQueryUsingH2Cte("SELECT map_keys[2] FROM test_maps", rewriter);
            assertQueryUsingH2Cte("SELECT map_keys[1], map_keys[3] FROM test_maps", rewriter);

            assertQueryUsingH2Cte("SELECT map_keys[1] FROM test_maps WHERE map_keys[1] % 2 = 0", rewriter);

            assertQueryUsingH2Cte("SELECT map_keys[2] FROM test_maps WHERE map_keys[1] % 2 = 0", rewriter);
            assertQueryUsingH2Cte("SELECT map_keys[1], map_keys[3] FROM test_maps WHERE map_keys[1] % 2 = 0", rewriter);

            assertQueryUsingH2Cte("SELECT map_keys[1], map_flags[2] FROM test_maps WHERE map_keys IS NOT NULL AND map_flags IS NOT NULL AND map_keys[1] % 2 = 0", rewriter);

            // filter-only map
            assertQueryUsingH2Cte("SELECT linenumber FROM test_maps WHERE map_keys IS NOT NULL", rewriter);

            // equality filter
            assertQuery("SELECT orderkey FROM test_maps WHERE map_flags = MAP(ARRAY[1, 2], ARRAY[true, true])", "SELECT orderkey FROM lineitem WHERE orderkey % 17 <> 0 AND shipmode = 'AIR' AND returnflag = 'R'");

            assertQueryFails("SELECT map_keys[5] FROM test_maps WHERE map_keys[1] % 2 = 0", "Key not present in map: 5");
            assertQueryFails("SELECT map_keys[5] FROM test_maps WHERE map_keys[4] % 2 = 0", "Key not present in map: 4");
        }
        finally {
            getQueryRunner().execute("DROP TABLE test_maps");
        }
    }

    @Test
    public void testDecimals()
    {
        assertQueryUsingH2Cte("SELECT discount_long_decimal, tax_short_decimal FROM lineitem_ex");

        assertFilterProject("discount_long_decimal IS NOT NULL", "count(*)");

        assertFilterProject("discount_long_decimal IS NULL", "discount_long_decimal");

        assertFilterProject("discount_long_decimal > 0.05", "discount_long_decimal");

        assertFilterProject("tax_short_decimal < 0.03", "tax_short_decimal");

        assertFilterProject("tax_short_decimal < 0.05  AND discount_long_decimal > 0.05", "discount_long_decimal");

        assertFilterProject("tax_short_decimal < discount_long_decimal", "discount_long_decimal");

        assertFilterProject("discount_long_decimal > 0.01 AND tax_short_decimal > 0.01 AND (discount_long_decimal + tax_short_decimal) < 0.03", "discount_long_decimal");

        assertFilterProject("long_decimals[1] > 0.01", "count(*)");

        assertFilterProject("tax_real > 0.01 and tax_short_decimal > 0.02 and (discount_long_decimal + tax_short_decimal) < 0.05", "tax_short_decimal, discount_long_decimal");
    }

    @Test
    public void testStrings()
    {
        assertFilterProject("comment < 'a' OR comment BETWEEN 'c' AND 'd'", "empty_comment");
        //char
        assertFilterProject("orderkey = 8480", "char_array");
        assertFilterProject("orderkey < 1000", "fixed_comment");

        //varchar/char direct
        assertFilterProject("comment is not NULL and linenumber=1 and orderkey<10", "comment");
        assertFilterProject("comment is NULL", "count(*)");
        assertFilterProject("length(comment) > 14 and orderkey < 150 and linenumber=2", "count(*)");
        assertFilterProject("comment like '%fluf%'", "comment");
        assertFilterProject("orderkey = 8480", "comment, fixed_comment");

        //varchar/char dictionary
        assertFilterProject("orderkey < 5000", "shipinstruct");
        assertFilterProject("shipinstruct IN ('NONE')", "comment, fixed_comment, char_array");
        assertFilterProject("trim(char_array[1]) = char_array[2]", "count(*)");
        assertFilterProject("char_array[1] IN ('along') and shipinstruct IN ('NONE')", "char_array");
        assertFilterProject("length(varchar_array[1]) > 10", "varchar_array");
        assertFilterProject("shipmode in ('AIR', 'MAIL', 'RAIL')\n" +
                "AND shipinstruct in ('TAKE BACK RETURN', 'DELIVER IN PERSON')\n" +
                "AND substr(shipinstruct, 2, 1) = substr(shipmode, 2, 1)\n" +
                "AND shipmode = if(linenumber % 2 = 0, 'RAIL', 'MAIL')", "orderkey");

        assertFilterProject("varchar_array[1] BETWEEN 'd' AND 'f'", "orderkey");
        assertFilterProject("comment between 'd' and 'f' AND uppercase_comment between 'D' and 'E' and length(comment) % 2  = linenumber % 2 and length(uppercase_comment) % 2  = linenumber % 2", "orderkey");

        assertQueryUsingH2Cte("select shipmode from lineitem_ex where shipmode in ('AIR', 'MAIL', 'RAIL') and orderkey < 1000 and linenumber < 5 order by orderkey limit 20");

        assertQueryUsingH2Cte("SELECT comment, varchar_array FROM lineitem_ex " +
                "WHERE comment between 'a' and 'd' AND (length(comment) + orderkey + length(varchar_array[1])) % 2 = 0");
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

        // equality filter
        assertQuery("SELECT orderkey FROM lineitem_ex WHERE keys = ARRAY[1, 22, 48]", "SELECT orderkey FROM lineitem WHERE orderkey = 1 AND partkey = 22 AND suppkey = 48");

        // subfield pruning
        assertQueryUsingH2Cte("SELECT nested_keys[2][1], nested_keys[1] FROM lineitem_ex");

        assertFilterProjectFails("keys[5] > 0", "orderkey", "Array subscript out of bounds");
        assertFilterProjectFails("nested_keys[5][1] > 0", "orderkey", "Array subscript out of bounds");
        assertFilterProjectFails("nested_keys[1][5] > 0", "orderkey", "Array subscript out of bounds");
        assertFilterProjectFails("nested_keys[2][5] > 0", "orderkey", "Array subscript out of bounds");
    }

    @Test
    public void testArraySubfieldPruning()
    {
        Function<String, String> rewriter = query -> query.replaceAll("cardinality", "array_length");

        // filter uses full column; that column is projected in full, partially or not at all

        // always-false filter
        assertQueryUsingH2Cte("SELECT keys FROM lineitem_ex WHERE cardinality(keys) = 1", rewriter);

        assertQueryUsingH2Cte("SELECT keys[1] FROM lineitem_ex WHERE cardinality(keys) = 1", rewriter);

        assertQueryUsingH2Cte("SELECT orderkey FROM lineitem_ex WHERE cardinality(keys) = 1", rewriter);

        // some rows pass the filter
        assertQueryUsingH2Cte("SELECT keys FROM lineitem_ex WHERE cardinality(keys) = 3", rewriter);

        assertQueryUsingH2Cte("SELECT keys[1] FROM lineitem_ex WHERE cardinality(keys) = 3", rewriter);

        assertQueryUsingH2Cte("SELECT orderkey FROM lineitem_ex WHERE cardinality(keys) = 3", rewriter);

        assertQueryUsingH2Cte("SELECT keys FROM lineitem_ex WHERE cardinality(keys) = 3 AND keys[1] < 1000", rewriter);

        assertQueryUsingH2Cte("SELECT keys[1] FROM lineitem_ex WHERE cardinality(keys) = 3 AND keys[1] < 1000", rewriter);

        assertQueryUsingH2Cte("SELECT orderkey FROM lineitem_ex WHERE cardinality(keys) = 3 AND keys[1] < 1000", rewriter);

        // range filter
        assertQueryUsingH2Cte("SELECT keys FROM lineitem_ex WHERE keys is NOT NULL");

        assertQueryUsingH2Cte("SELECT keys[1] FROM lineitem_ex WHERE keys is NOT NULL");

        assertQueryUsingH2Cte("SELECT linenumber FROM lineitem_ex WHERE keys is NOT NULL");

        // filter uses partial column; that column is projected in full, partially or not at all
        assertQueryUsingH2Cte("SELECT keys FROM lineitem_ex WHERE keys[1] < 1000");
        assertQueryUsingH2Cte("SELECT keys FROM lineitem_ex WHERE keys[1] % 2 = 0");
        assertQueryUsingH2Cte("SELECT keys FROM lineitem_ex WHERE keys[1] < 1000 AND keys[2] % 2 = 0");

        assertQueryUsingH2Cte("SELECT keys[1] FROM lineitem_ex WHERE keys[1] < 1000");
        assertQueryUsingH2Cte("SELECT keys[1] FROM lineitem_ex WHERE keys[1] % 2 = 0");
        assertQueryUsingH2Cte("SELECT keys[1] FROM lineitem_ex WHERE keys[1] < 1000 AND keys[2] % 2 = 0");

        assertQueryUsingH2Cte("SELECT keys[2] FROM lineitem_ex WHERE keys[1] < 1000");
        assertQueryUsingH2Cte("SELECT keys[2] FROM lineitem_ex WHERE keys[1] % 2 = 0");
        assertQueryUsingH2Cte("SELECT keys[2] FROM lineitem_ex WHERE keys[1] < 1000 AND keys[2] % 2 = 0");

        assertQueryUsingH2Cte("SELECT keys[1], keys[2] FROM lineitem_ex WHERE keys[1] < 1000");
        assertQueryUsingH2Cte("SELECT keys[1], keys[2] FROM lineitem_ex WHERE keys[1] % 2 = 0");
        assertQueryUsingH2Cte("SELECT keys[1], keys[2] FROM lineitem_ex WHERE keys[1] < 1000 AND keys[2] % 2 = 0");

        assertQueryUsingH2Cte("SELECT linenumber FROM lineitem_ex WHERE keys[1] < 1000");
        assertQueryUsingH2Cte("SELECT linenumber FROM lineitem_ex WHERE keys[1] % 2 = 0");
        assertQueryUsingH2Cte("SELECT linenumber FROM lineitem_ex WHERE keys[1] < 1000 AND keys[2] % 2 = 0");

        // no filter on array column; column is projected in full or partially
        assertQueryUsingH2Cte("SELECT keys FROM lineitem_ex WHERE orderkey < 1000");

        assertQueryUsingH2Cte("SELECT keys[1] FROM lineitem_ex WHERE orderkey < 1000");

        assertQueryUsingH2Cte("SELECT keys[2], keys[3] FROM lineitem_ex WHERE orderkey < 1000");
    }

    @Test
    public void testArrayOfMaps()
    {
        getQueryRunner().execute("CREATE TABLE test_arrays_of_maps AS\n" +
                "SELECT orderkey, ARRAY[MAP(ARRAY[1, 2, 3], ARRAY[orderkey, partkey, suppkey]), MAP(ARRAY[1, 2, 3], ARRAY[orderkey + 1, partkey + 1, suppkey + 1])] as array_of_maps\n" +
                "FROM lineitem");

        try {
            assertQuery("SELECT t.maps[1] FROM test_arrays_of_maps CROSS JOIN UNNEST(array_of_maps) AS t(maps)", "SELECT orderkey FROM lineitem UNION ALL SELECT orderkey + 1 FROM lineitem");

            assertQuery("SELECT cardinality(array_of_maps[1]) > 0, t.maps[1] FROM test_arrays_of_maps CROSS JOIN UNNEST(array_of_maps) AS t(maps)", "SELECT true, orderkey FROM lineitem UNION ALL SELECT true, orderkey + 1 FROM lineitem");
        }
        finally {
            getQueryRunner().execute("DROP TABLE test_arrays_of_maps");
        }
    }

    @Test
    public void testMapsOfArrays()
    {
        getQueryRunner().execute("CREATE TABLE test_maps_of_arrays AS\n" +
                "SELECT orderkey, map_from_entries(array_agg(row(linenumber, array[quantity, discount, tax]))) items\n" +
                "FROM lineitem\n" +
                "GROUP BY 1");

        try {
            assertQuery("SELECT t.doubles[2] FROM test_maps_of_arrays CROSS JOIN UNNEST(items) AS t(linenumber, doubles)", "SELECT discount FROM lineitem");

            assertQuery("SELECT t.linenumber, t.doubles[2] FROM test_maps_of_arrays CROSS JOIN UNNEST(items) AS t(linenumber, doubles)", "SELECT linenumber, discount FROM lineitem");

            assertQuery("SELECT cardinality(items[1]) > 0, t.doubles[2] FROM test_maps_of_arrays CROSS JOIN UNNEST(items) AS t(linenumber, doubles)", "SELECT true, discount FROM lineitem");
        }
        finally {
            getQueryRunner().execute("DROP TABLE test_maps_of_arrays");
        }
    }

    @Test
    public void testStructs()
    {
        assertQueryUsingH2Cte("SELECT orderkey, info, dates FROM lineitem_ex");

        Function<String, String> rewriter = query -> query.replaceAll("info.orderkey", "info[1]")
                .replaceAll("info.linenumber", "info[2]")
                .replaceAll("info.shipdate.ship_day", "info[3][1]")
                .replaceAll("info.shipdate.ship_year", "info[3][3]")
                .replaceAll("info.shipdate", "info[3]")
                .replaceAll("dates\\[1\\].day", "dates[1][1]");

        assertQueryUsingH2Cte("SELECT info.orderkey FROM lineitem_ex", rewriter);
        assertQueryUsingH2Cte("SELECT info.orderkey, info.linenumber FROM lineitem_ex", rewriter);

        assertQueryUsingH2Cte("SELECT info.linenumber, info.shipdate.ship_year FROM lineitem_ex WHERE orderkey < 1000", rewriter);
        assertQueryUsingH2Cte("SELECT info.orderkey FROM lineitem_ex WHERE orderkey = 16515", rewriter);

        assertQueryUsingH2Cte("SELECT info.orderkey FROM lineitem_ex WHERE info IS NULL", rewriter);
        assertQueryUsingH2Cte("SELECT info.orderkey FROM lineitem_ex WHERE info IS NOT NULL", rewriter);
        assertQueryUsingH2Cte("SELECT info.orderkey FROM lineitem_ex WHERE info IS NOT NULL AND orderkey = 16515", rewriter);

        assertQueryUsingH2Cte("SELECT info, dates FROM lineitem_ex WHERE info.orderkey % 7 = 0", rewriter);
        assertQueryUsingH2Cte("SELECT info.orderkey, dates FROM lineitem_ex WHERE info.orderkey % 7 = 0", rewriter);
        assertQueryUsingH2Cte("SELECT info.linenumber, dates FROM lineitem_ex WHERE info.orderkey % 7 = 0", rewriter);

        assertQueryUsingH2Cte("SELECT dates FROM lineitem_ex WHERE info.orderkey % 7 = 0", rewriter);
        assertQueryUsingH2Cte("SELECT info.orderkey FROM lineitem_ex WHERE info IS NOT NULL", rewriter);

        assertQueryUsingH2Cte("SELECT info.orderkey, info.shipdate FROM lineitem_ex WHERE info.orderkey % 7 = 0", rewriter);

        assertQueryUsingH2Cte("SELECT dates FROM lineitem_ex WHERE dates[1].day % 2 = 0", rewriter);

        assertQueryUsingH2Cte("SELECT info.orderkey, dates FROM lineitem_ex WHERE info IS NOT NULL AND dates IS NOT NULL AND info.orderkey % 7 = 0", rewriter);

        // filter-only struct
        assertQueryUsingH2Cte("SELECT orderkey FROM lineitem_ex WHERE info IS NOT NULL");
        assertQueryUsingH2Cte("SELECT orderkey FROM lineitem_ex WHERE info IS NOT NULL AND info.orderkey = 16515", rewriter);
        assertQueryReturnsEmptyResult("SELECT orderkey FROM lineitem_ex WHERE info IS NOT NULL AND info.orderkey = 16515 and info.orderkey = 16516");
        assertQueryUsingH2Cte("SELECT orderkey FROM lineitem_ex WHERE info IS NOT NULL AND info.orderkey + 1 = 16514", rewriter);

        // filters on subfields
        assertQueryUsingH2Cte("SELECT info.orderkey, info.linenumber FROM lineitem_ex WHERE info.linenumber = 2", rewriter);
        assertQueryUsingH2Cte("SELECT linenumber FROM lineitem_ex WHERE info.linenumber = 2", rewriter);
        assertQueryUsingH2Cte("SELECT linenumber FROM lineitem_ex WHERE info IS NULL OR info.linenumber = 2", rewriter);

        assertQueryUsingH2Cte("SELECT info.shipdate.ship_day FROM lineitem_ex WHERE info.shipdate.ship_day < 15", rewriter);
        assertQueryUsingH2Cte("SELECT info.linenumber FROM lineitem_ex WHERE info.shipdate.ship_day < 15", rewriter);

        // case sensitivity
        assertQuery("SELECT INFO.orderkey FROM lineitem_ex", "SELECT CASE WHEN orderkey % 23 = 0 THEN null ELSE orderkey END FROM lineitem");
        assertQuery("SELECT INFO.ORDERKEY FROM lineitem_ex", "SELECT CASE WHEN orderkey % 23 = 0 THEN null ELSE orderkey END FROM lineitem");
        assertQuery("SELECT iNfO.oRdErKeY FROM lineitem_ex", "SELECT CASE WHEN orderkey % 23 = 0 THEN null ELSE orderkey END FROM lineitem");
    }

    @Test
    public void testAllNullsInStruct()
    {
        List<String> types = ImmutableList.of(BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, REAL, VARCHAR, DATE);

        String query = String.format("SELECT orderkey, CAST(ROW(%s, orderkey) AS ROW(%s, orderkey INTEGER)) as struct FROM orders",
                types.stream()
                        .map(type -> "null")
                        .collect(joining(", ")),
                types.stream()
                        .map(type -> String.format("null_%s %s", type.toLowerCase(getSession().getLocale()), type.toUpperCase()))
                        .collect(joining(", ")));

        getQueryRunner().execute("CREATE TABLE test_all_nulls_in_struct AS " + query);

        try {
            for (String type : types) {
                assertQuery(
                        format(
                                "SELECT struct.orderkey, struct.null_%s FROM test_all_nulls_in_struct WHERE struct IS NOT NULL AND orderkey %% 2 = 0",
                                type.toLowerCase(getSession().getLocale())),
                        "SELECT orderkey, null FROM orders WHERE orderkey % 2 = 0");

                assertQuery(format("SELECT orderkey from test_all_nulls_in_struct WHERE struct.null_%s is NULL AND struct.orderkey > 3000 " +
                                "AND length(CAST(struct.null_%s AS VARCHAR)) IS NULL", type.toLowerCase(getSession().getLocale()), type.toLowerCase(getSession().getLocale())),
                        "SELECT orderkey FROM orders WHERE orderkey > 3000");
            }
        }
        finally {
            getQueryRunner().execute("DROP TABLE test_all_nulls_in_struct");
        }
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

        // filter functions with join predicate pushdown
        assertQueryReturnsEmptyResult("SELECT * FROM orders o, lineitem_ex l " +
                "WHERE o.orderkey <> 100 AND cardinality(l.keys) >= 5 AND l.keys[5] <> 1 AND l.keys[5] = o.orderkey");

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
    public void testNestedFilterFunctions()
    {
        // This query forces the shape aggregation, filter, project, filter, table scan. This ensures the outer filter is used in the query result.
        assertQueryUsingH2Cte(
                "select distinct shipmode from " +
                        "(select * from lineitem_ex where orderkey % 5 = 0)" +
                        "where ((case when (shipmode in ('RAIL', '2')) then '2' when (shipmode = 'AIR') then 'air' else 'Other' end) = '2')");
    }

    @Test
    public void testPushdownComposition()
    {
        // Tests composing two pushdowns each with a range filter and filter function.
        assertQuery(
                "WITH data AS (" +
                        "    SELECT l.suppkey, l.linenumber, l.shipmode, MAX(o.orderdate)" +
                        "    FROM lineitem l,  orders o WHERE" +
                        "        o.orderkey = l.orderkey AND linenumber IN (2, 3, 4, 6) AND shipmode LIKE '%AIR%'" +
                        "        GROUP BY l.suppkey, l.linenumber, l.shipmode)" +
                        "SELECT COUNT(*) FROM data WHERE suppkey BETWEEN 10 AND 30 AND shipmode LIKE '%REG%'");
    }

    @Test
    public void testPartitionColumns()
    {
        assertUpdate("CREATE TABLE test_partition_columns WITH (partitioned_by = ARRAY['p', 'q', 'ds']) AS\n" +
                "SELECT * FROM (VALUES (1, 'abc', 'cba', '2020-01-01'), (2, 'abc', 'def', '2020-01-01')) as t(x, p, q, ds)", 2);

        assertQuery("SELECT * FROM test_partition_columns", "SELECT 1, 'abc', 'cba', '2020-01-01' UNION ALL SELECT 2, 'abc', 'def', '2020-01-01'");

        assertQuery("SELECT x FROM test_partition_columns", "SELECT 1 UNION ALL SELECT 2");

        assertQuery("SELECT * FROM test_partition_columns WHERE p = 'abc'", "SELECT 1, 'abc', 'cba', '2020-01-01' UNION ALL SELECT 2, 'abc', 'def', '2020-01-01'");

        assertQuery("SELECT * FROM test_partition_columns WHERE p LIKE 'a%'", "SELECT 1, 'abc', 'cba', '2020-01-01' UNION ALL SELECT 2, 'abc', 'def', '2020-01-01'");

        assertQuery("SELECT * FROM test_partition_columns WHERE substr(p, x, 1) = 'a' and substr(q, 1, 1) = 'c'", "SELECT 1, 'abc', 'cba', '2020-01-01'");

        assertQueryReturnsEmptyResult("SELECT * FROM test_partition_columns WHERE p = 'xxx'");

        assertQueryReturnsEmptyResult("SELECT * FROM test_partition_columns WHERE p = 'abc' and p='def'");

        assertUpdate("INSERT into test_partition_columns values (3, 'abc', NULL, '2020-01-01')", 1);

        assertQuerySucceeds(getSession(), "select * from test_partition_columns");

        assertQueryFails("SELECT * FROM test_partition_columns WHERE DATE_DIFF( 'day', PARSE_DATETIME( '2020-01-08', 'YYYY-MM-dd' ), PARSE_DATETIME( ds, 'yyyy-MM-dd HH:mm:ss.SSS' ) ) = 7", "Invalid format: \"2020-01-01\" is too short");

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

    @Test
    public void testTextfileFormatWithPushdown()
    {
        assertUpdate("CREATE TABLE textfile (id BIGINT) WITH (format = 'TEXTFILE')");
        assertUpdate("INSERT INTO textfile VALUES (1), (2), (3)", 3);
        assertQuery("SELECT id FROM textfile WHERE id = 1", "SELECT 1");
        assertQuery("SELECT id FROM textfile", "SELECT 1 UNION SELECT 2 UNION SELECT 3 ");
        assertUpdate("DROP TABLE textfile");
    }

    @Test
    public void testSchemaEvolution()
    {
        assertUpdate("CREATE TABLE test_schema_evolution WITH (partitioned_by = ARRAY['regionkey']) AS SELECT nationkey, regionkey FROM nation", 25);
        assertUpdate("ALTER TABLE test_schema_evolution ADD COLUMN nation_plus_region BIGINT");

        // constant filter function errors
        assertQueryFails("SELECT * FROM test_schema_evolution WHERE coalesce(nation_plus_region, fail('constant filter error')) is not null", "constant filter error");
        assertQuerySucceeds("SELECT * FROM test_schema_evolution WHERE nationkey < 0 AND coalesce(nation_plus_region, fail('constant filter error')) is not null");
        assertQueryFails("SELECT * FROM test_schema_evolution WHERE nationkey % 2 = 0 AND coalesce(nation_plus_region, fail('constant filter error')) is not null", "constant filter error");

        // non-deterministic filter function with constant inputs
        assertQueryReturnsEmptyResult("SELECT * FROM test_schema_evolution WHERE nation_plus_region * rand() < 0");
        assertQuery("SELECT nationkey FROM test_schema_evolution WHERE nation_plus_region * rand() IS NULL", "SELECT nationkey FROM nation");
        assertQuerySucceeds("SELECT nationkey FROM test_schema_evolution WHERE coalesce(nation_plus_region, 1) * rand() < 0.5");

        assertUpdate("INSERT INTO test_schema_evolution SELECT nationkey, nationkey + regionkey, regionkey FROM nation", 25);
        assertUpdate("ALTER TABLE test_schema_evolution ADD COLUMN nation_minus_region BIGINT");
        assertUpdate("INSERT INTO test_schema_evolution SELECT nationkey, nationkey + regionkey, nationkey - regionkey, regionkey FROM nation", 25);

        String cte = "WITH test_schema_evolution AS (" +
                "SELECT nationkey, null AS nation_plus_region, null AS nation_minus_region, regionkey FROM nation " +
                "UNION ALL SELECT nationkey, nationkey + regionkey, null, regionkey FROM nation " +
                "UNION ALL SELECT nationkey, nationkey + regionkey, nationkey - regionkey, regionkey FROM nation)";

        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution", cte);
        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution WHERE nation_plus_region IS NULL", cte);
        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution WHERE nation_plus_region > 10", cte);
        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution WHERE nation_plus_region + 1 > 10", cte);
        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution WHERE nation_plus_region + nation_minus_region > 20", cte);
        assertQueryUsingH2Cte("select * from test_schema_evolution where nation_plus_region = regionkey", cte);
        assertUpdate("DROP TABLE test_schema_evolution");
    }

    @Test
    public void testStructSchemaEvolution()
            throws IOException
    {
        getQueryRunner().execute("CREATE TABLE test_struct(x) AS SELECT CAST(ROW(1, 2) AS ROW(a int, b int)) AS x");
        getQueryRunner().execute("CREATE TABLE test_struct_add_column(x) AS SELECT CAST(ROW(1, 2, 3) AS ROW(a int, b int, c int)) AS x");
        Path oldFilePath = getOnlyPath("test_struct");
        Path newDirectoryPath = getOnlyPath("test_struct_add_column").getParent();
        Files.move(oldFilePath, Paths.get(newDirectoryPath.toString(), "old_file"), ATOMIC_MOVE);
        assertQuery("SELECT * FROM test_struct_add_column", "SELECT (1, 2, 3) UNION ALL SELECT (1, 2, null)");
        assertQuery("SELECT x.a FROM test_struct_add_column", "SELECT 1 UNION ALL SELECT 1");
        assertQuery("SELECT count(*) FROM test_struct_add_column where x.c = 1", "SELECT 0");
    }

    @Test
    public void testUpperCaseStructFields()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_struct_with_uppercase_field(field0) WITH (FORMAT = 'DWRF') AS " +
                "SELECT CAST((1, 1) AS ROW(SUBFIELDCAP BIGINT, subfieldsmall BIGINT))", 1);

        try {
            assertQuery("SELECT * FROM test_struct_with_uppercase_field", "SELECT (CAST(1 AS BIGINT), CAST(1 AS BIGINT))");
            assertQuery("SELECT field0.SUBFIELDCAP FROM test_struct_with_uppercase_field", "SELECT CAST(1 AS BIGINT)");
            assertQuery("SELECT field0.subfieldcap FROM test_struct_with_uppercase_field", "SELECT CAST(1 AS BIGINT)");

            // delete the file written by Presto and corresponding crc file
            Path prestoFile = getOnlyPath("test_struct_with_uppercase_field");
            Files.delete(prestoFile);
            Files.deleteIfExists(prestoFile.getParent().resolve("." + prestoFile.getFileName() + ".crc"));

            // copy the file written by Spark
            Path sparkFile = Paths.get(this.getClass().getClassLoader().getResource("struct_with_uppercase_field.dwrf").toURI());
            Files.copy(sparkFile, prestoFile);

            assertQuery("SELECT * FROM test_struct_with_uppercase_field", "SELECT (CAST(1 AS BIGINT), CAST(1 AS BIGINT))");
            assertQuery("SELECT field0.SUBFIELDCAP FROM test_struct_with_uppercase_field", "SELECT CAST(1 AS BIGINT)");
            assertQuery("SELECT field0.subfieldcap FROM test_struct_with_uppercase_field", "SELECT CAST(1 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE test_struct_with_uppercase_field");
        }
    }

    @Test
    public void testRcAndTextFormats()
            throws IOException
    {
        getQueryRunner().execute("CREATE TABLE lineitem_ex_partitioned WITH (format = 'ORC', partitioned_by = ARRAY['ds']) AS\n" +
                "SELECT\n" +
                "    linenumber,\n" +
                "    orderkey,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    quantity,\n" +
                "    extendedprice,\n" +
                "    tax,\n" +
                "    shipinstruct,\n" +
                "    shipmode,\n" +
                "    ship_by_air,\n" +
                "    is_returned,\n" +
                "    ship_day,\n" +
                "    ship_month,\n" +
                "    ship_timestamp,\n" +
                "    commit_timestamp,\n" +
                "    discount_real,\n" +
                "    discount,\n" +
                "    tax_real,\n" +
                "    ship_day_month,\n" +
                "    discount_long_decimal,\n" +
                "    tax_short_decimal,\n" +
                "    long_decimals,\n" +
                "    keys,\n" +
                "    doubles,\n" +
                "    nested_keys,\n" +
                "    flags,\n" +
                "    reals,\n" +
                "    info,\n" +
                "    dates,\n" +
                "    timestamps,\n" +
                "    comment,\n" +
                "    uppercase_comment,\n" +
                "    fixed_comment,\n" +
                "    char_array,\n" +
                "    varchar_array,\n" +
                "    '2019-11-01' AS ds\n" +
                "FROM lineitem_ex");
        try {
            for (HiveStorageFormat format : ImmutableList.of(RCBINARY, RCTEXT, TEXTFILE)) {
                assertFileFormat(format);
            }
        }
        finally {
            assertUpdate("DROP TABLE lineitem_ex_partitioned");
        }
    }

    private void assertFileFormat(HiveStorageFormat storageFormat)
            throws IOException
    {
        // Make an ORC table backed by file of some other format
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE TABLE test_file_format_orc WITH (format='ORC', partitioned_by=ARRAY['ds']) AS " +
                "SELECT * FROM lineitem_ex_partitioned LIMIT 1");
        try {
            queryRunner.execute(format("CREATE TABLE test_file_format WITH (format='%s', partitioned_by=ARRAY['ds']) AS " +
                    "SELECT * FROM lineitem_ex_partitioned", storageFormat));
            Path orcDirectory = getPartitionDirectory("test_file_format_orc", "ds='2019-11-01'");
            deleteRecursively(orcDirectory, ALLOW_INSECURE);

            Path otherDirectory = getPartitionDirectory("test_file_format", "ds='2019-11-01'");
            Files.move(otherDirectory, orcDirectory, REPLACE_EXISTING);

            String cte = WITH_LINEITEM_EX + ", test_file_format_orc AS " +
                    "(SELECT\n" +
                    "    linenumber,\n" +
                    "    orderkey,\n" +
                    "    partkey,\n" +
                    "    suppkey,\n" +
                    "    quantity,\n" +
                    "    extendedprice,\n" +
                    "    tax,\n" +
                    "    shipinstruct,\n" +
                    "    shipmode,\n" +
                    "    ship_by_air,\n" +
                    "    is_returned,\n" +
                    "    ship_day,\n" +
                    "    ship_month,\n" +
                    "    ship_timestamp,\n" +
                    "    commit_timestamp,\n" +
                    "    discount_real,\n" +
                    "    discount,\n" +
                    "    tax_real,\n" +
                    "    ship_day_month,\n" +
                    "    discount_long_decimal,\n" +
                    "    tax_short_decimal,\n" +
                    "    long_decimals,\n" +
                    "    keys,\n" +
                    "    doubles,\n" +
                    "    nested_keys,\n" +
                    "    flags,\n" +
                    "    reals,\n" +
                    "    info,\n" +
                    "    dates,\n" +
                    "    timestamps,\n" +
                    "    comment,\n" +
                    "    uppercase_comment,\n" +
                    "    fixed_comment,\n" +
                    "    char_array,\n" +
                    "    varchar_array,\n" +
                    "    '2019-11-01' AS ds\n" +
                    "FROM lineitem_ex)";

            // no filter
            assertQueryUsingH2Cte("SELECT * FROM test_file_format_orc", cte);
            assertQueryUsingH2Cte("SELECT comment FROM test_file_format_orc", cte);
            assertQueryFails("SELECT COUNT(*) FROM test_file_format_orc", "Partial aggregation pushdown only supported for ORC/Parquet files. Table tpch.test_file_format_orc has file ((.*?)) of format (.*?). Set session property hive.pushdown_partial_aggregations_into_scan=false and execute query again");
            assertQueryUsingH2Cte(noPartialAggregationPushdown(queryRunner.getDefaultSession()), "SELECT COUNT(*) FROM test_file_format_orc", cte, Function.identity());

            // filter on partition column
            assertQueryUsingH2Cte("SELECT comment from test_file_format_orc WHERE ds='2019-11-01'", cte);
            assertQueryReturnsEmptyResult("SELECT comment FROM test_file_format_orc WHERE ds='2019-11-02'");

            // range filters and filter functions
            assertQueryUsingH2Cte("SELECT orderkey from test_file_format_orc WHERE orderkey < 1000", cte);
            assertQueryUsingH2Cte("SELECT orderkey, comment from test_file_format_orc WHERE orderkey < 1000 AND comment LIKE '%final%'", cte);
            assertQueryUsingH2Cte("SELECT COUNT(*) from test_file_format_orc WHERE orderkey < 1000", cte);

            assertQueryUsingH2Cte("SELECT COUNT(*) FROM test_file_format_orc WHERE concat(ds,'*') = '2019-11-01*'", cte);
            assertQueryUsingH2Cte("SELECT orderkey FROM test_file_format_orc WHERE comment LIKE '%final%'", cte);

            assertQueryUsingH2Cte("SELECT discount FROM test_file_format_orc WHERE discount > 0.01", cte);
            assertQueryUsingH2Cte("SELECT * FROM test_file_format_orc WHERE discount > 0.01 and discount + tax > 0.03", cte);
            assertQueryUsingH2Cte("SELECT COUNT(*) FROM test_file_format_orc WHERE discount = 0.0", cte);

            assertQueryUsingH2Cte("SELECT COUNT(*) FROM test_file_format_orc WHERE discount_real > 0.01", cte);
            assertQueryUsingH2Cte("SELECT * FROM test_file_format_orc WHERE tax_real > 0.01 and discount_real > 0.01", cte);

            assertQueryUsingH2Cte("SELECT keys FROM test_file_format_orc WHERE keys IS NOT NULL", cte);
            assertQueryUsingH2Cte("SELECT keys FROM test_file_format_orc WHERE keys IS NULL", cte);
            assertQueryUsingH2Cte("SELECT linenumber FROM test_file_format_orc WHERE keys[1] % 5 = 0 AND keys[2] > 100", cte);

            assertQueryUsingH2Cte("SELECT * FROM test_file_format_orc WHERE is_returned=false", cte);
            assertQueryUsingH2Cte("SELECT * FROM test_file_format_orc WHERE is_returned is NULL", cte);

            assertQueryUsingH2Cte("SELECT ship_day FROM test_file_format_orc WHERE ship_day > 2", cte);

            assertQueryUsingH2Cte("SELECT discount_long_decimal FROM test_file_format_orc WHERE discount_long_decimal > 0.05", cte);
            assertQueryUsingH2Cte("SELECT tax_short_decimal FROM test_file_format_orc WHERE tax_short_decimal < 0.03", cte);
            assertQueryUsingH2Cte("SELECT discount_long_decimal FROM test_file_format_orc WHERE discount_long_decimal > 0.01 AND tax_short_decimal > 0.01 AND (discount_long_decimal + tax_short_decimal) < 0.03", cte);

            Function<String, String> rewriter = query -> query.replaceAll("info.orderkey", "info[1]")
                    .replaceAll("dates\\[1\\].day", "dates[1][1]");

            assertQueryUsingH2Cte("SELECT dates FROM test_file_format_orc WHERE dates[1].day % 2 = 0", cte, rewriter);
            assertQueryUsingH2Cte("SELECT info.orderkey, dates FROM test_file_format_orc WHERE info IS NOT NULL AND dates IS NOT NULL AND info.orderkey % 7 = 0", cte, rewriter);

            // empty result
            assertQueryReturnsEmptyResult("SELECT comment FROM test_file_format_orc WHERE orderkey < 0");
            assertQueryReturnsEmptyResult("SELECT comment FROM test_file_format_orc WHERE comment LIKE '???'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_file_format");
            assertUpdate("DROP TABLE test_file_format_orc");
        }
    }

    @Test
    public void testNans()
    {
        assertUpdate("CREATE TABLE test_nan (double_value DOUBLE, float_value REAL)");
        try {
            assertUpdate("INSERT INTO test_nan VALUES (cast('NaN' as DOUBLE), cast('NaN' as REAL)), ((1, 1)), ((2, 2))", 3);
            assertQuery("SELECT double_value FROM test_nan WHERE double_value != 1", "SELECT cast('NaN' as DOUBLE) UNION SELECT 2");
            assertQuery("SELECT float_value FROM test_nan WHERE float_value != 1", "SELECT CAST('NaN' as REAL) UNION SELECT 2");
            assertQuery("SELECT double_value FROM test_nan WHERE double_value NOT IN (1, 2)", "SELECT CAST('NaN' as DOUBLE)");
        }
        finally {
            assertUpdate("DROP TABLE test_nan");
        }
    }

    @Test
    public void testFilterFunctionsWithOptimization()
    {
        assertQuery("SELECT partkey FROM lineitem WHERE orderkey > 10 OR if(json_extract(json_parse('{}'), '$.a') IS NOT NULL, quantity * discount) > 0",
                "SELECT partkey FROM lineitem WHERE orderkey > 10");
    }

    private Path getPartitionDirectory(String tableName, String partitionClause)
    {
        String filePath = ((String) computeActual(noPushdownFilter(getSession()), format("SELECT \"$path\" FROM %s WHERE %s LIMIT 1", tableName, partitionClause)).getOnlyValue())
                .replace("file:", "");
        return Paths.get(filePath).getParent();
    }

    private Path getOnlyPath(String tableName)
    {
        return Paths.get(((String) computeActual(noPushdownFilter(getSession()), format("SELECT \"$path\" FROM %s LIMIT 1", tableName)).getOnlyValue())
                .replace("file:", ""));
    }

    private void assertQueryUsingH2Cte(String query, String cte)
    {
        assertQueryUsingH2Cte(query, cte, Function.identity());
    }

    private void assertQueryUsingH2Cte(String query, String cte, Function<String, String> rewriter)
    {
        assertQuery(query, cte + toH2(rewriter.apply(query)));
    }

    private void assertQueryUsingH2Cte(String query)
    {
        assertQueryUsingH2Cte(query, Function.identity());
    }

    private void assertQueryUsingH2Cte(Session session, String query, String cte, Function<String, String> rewriter)
    {
        assertQuery(session, query, cte + toH2(rewriter.apply(query)));
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

    private static Session noPartialAggregationPushdown(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(HIVE_CATALOG, PARTIAL_AGGREGATION_PUSHDOWN_ENABLED, "false")
                .build();
    }
}
