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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedLineitemAndOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createEmptyTable;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPart;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartitionedNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPrestoBenchTables;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createSupplier;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestNativeGeneralQueries
        extends AbstractTestQueryFramework
{
    private static final String[] TABLE_FORMATS = {"DWRF"};

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createCustomer(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createNation(queryRunner);
        createPartitionedNation(queryRunner);
        createSupplier(queryRunner);
        createBucketedCustomer(queryRunner);
        createPart(queryRunner);
        createRegion(queryRunner);
        createEmptyTable(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);

        createPrestoBenchTables(queryRunner);
    }

    @Test
    public void testCatalogWithCacheEnabled()
    {
        Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("hive.storage-format", "DWRF")
                .put("hive.pushdown-filter-enabled", "true")
                .build();

        getQueryRunner().createCatalog("hivecached", "hive", hiveProperties);

        Session session = Session.builder(getSession())
                .setCatalog("hivecached")
                .setCatalogSessionProperty("hivecached", "orc_compression_codec", "ZSTD")
                .setCatalogSessionProperty("hivecached", "collect_column_statistics_on_write", "false")
                .build();
        try {
            getQueryRunner().execute(session, "CREATE TABLE tmp AS SELECT * FROM nation");
            assertQuery("SELECT * FROM tmp");
        }
        finally {
            dropTableIfExists("tmp");
        }
    }

    @Test
    public void testFiltersAndProjections()
    {
        assertQuery("SELECT * FROM nation");
        assertQuery("SELECT * FROM nation WHERE nationkey = 4");
        assertQuery("SELECT * FROM nation WHERE nationkey <> 4");
        assertQuery("SELECT * FROM nation WHERE nationkey < 4");
        assertQuery("SELECT * FROM nation WHERE nationkey <= 4");
        assertQuery("SELECT * FROM nation WHERE nationkey > 4");
        assertQuery("SELECT * FROM nation WHERE nationkey >= 4");
        assertQuery("SELECT * FROM nation WHERE nationkey BETWEEN 3 AND 7");
        assertQuery("SELECT * FROM nation WHERE nationkey IN (1, 3, 5)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (1, 3, 5)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (1, 8, 11)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (1, 2, 3)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (-14, 2)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (1, 2, 3, 4, 5, 10, 11, 12, 13)");
        // Java coordinator/workers causes these queries to fail, even though the INT_MAX ones work on cpp
        // "SELECT * FROM nation WHERE nationkey NOT IN (2, 33, " + Long.MAX_VALUE + ")"
        // "SELECT * FROM nation WHERE nationkey NOT IN (" + Long.MIN_VALUE + ", 2, 33)"
        // "SELECT * FROM nation WHERE nationkey NOT IN (" + Long.MIN_VALUE + ", " + Long.MAX_VALUE + ")"
        assertQuery("SELECT * FROM nation WHERE nationkey NOT BETWEEN 3 AND 7");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT BETWEEN -10 AND 5");
        assertQuery("SELECT * FROM nation WHERE nationkey < 5 OR nationkey > 10");
        assertQuery("SELECT nationkey * 10, nationkey % 5, -nationkey, nationkey / 3 FROM nation");
        assertQuery("SELECT *, nationkey / 3 FROM nation");
        assertQuery("SELECT nationkey IS NULL FROM nation");
        assertQuery("SELECT * FROM nation WHERE name <> 'SAUDI ARABIA'");
        assertQuery("SELECT * FROM nation WHERE name NOT IN ('RUSSIA', 'UNITED STATES', 'CHINA')");
        assertQuery("SELECT * FROM nation WHERE name NOT IN ('aaa', 'UniteD StateS', 'UNITED STATEs', 'uNITED STATES')");
        assertQuery("SELECT * FROM nation WHERE name NOT IN ('', ';', 'new country w1th $p3c1@l ch@r@c73r5')");
        assertQuery("SELECT * FROM nation WHERE name NOT BETWEEN 'A' AND 'K'"); // should produce NegatedBytesRange
        assertQuery("SELECT * FROM nation WHERE name <= 'B' OR 'G' <= name");
        assertQuery("SELECT * FROM lineitem WHERE shipmode <> 'FOB'");
        assertQuery("SELECT * FROM lineitem WHERE shipmode NOT IN ('RAIL', 'AIR')");
        assertQuery("SELECT * FROM lineitem WHERE shipmode NOT IN ('', 'TRUCK', 'FOB', 'RAIL')");
        assertQuery("SELECT x IS DISTINCT FROM y, y IS NOT DISTINCT FROM x FROM (SELECT shipinstruct AS x, IF(shipinstruct='NONE', NULL, shipinstruct) AS y FROM lineitem)");

        assertQuery("SELECT rand() < 1, random() < 1 FROM nation", "SELECT true, true FROM nation");

        assertQuery("SELECT * FROM lineitem");
        assertQuery("SELECT ceil(discount), ceiling(discount), floor(discount), abs(discount) FROM lineitem");
        assertQuery("SELECT linenumber IN (2, 4, 6) FROM lineitem");
        assertQuery("SELECT orderdate FROM orders WHERE cast(orderdate as DATE) IN (cast('1997-07-29' as DATE), cast('1993-03-13' as DATE)) ORDER BY orderdate LIMIT 10");

        assertQuery("SELECT * FROM orders");

        assertQuery("SELECT coalesce(linenumber, -1) FROM lineitem");

        assertQuery("SELECT * FROM lineitem WHERE linenumber = 1");
        assertQuery("SELECT * FROM lineitem WHERE linenumber > 3");

        assertQuery("SELECT * FROM lineitem WHERE linenumber_as_smallint = 3");
        assertQuery("SELECT * FROM lineitem WHERE linenumber_as_smallint > 5 AND linenumber_as_smallint < 2");

        assertQuery("SELECT * FROM lineitem WHERE linenumber_as_tinyint > 5");
        assertQuery("SELECT * FROM lineitem WHERE linenumber_as_tinyint IN (1, 2)");

        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE discount > 0.02");
        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE discount BETWEEN 0.01 AND 0.02");

        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE discount_as_real > 0.02");
        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE discount_as_real BETWEEN 0.01 AND 0.02");
        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE tax_as_real < 0.02");
        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE tax_as_real BETWEEN 0.02 AND 0.06");

        assertQuery("SELECT * FROM lineitem WHERE is_open=true");
        assertQuery("SELECT * FROM lineitem WHERE is_open<>true");
        assertQuery("SELECT * FROM lineitem WHERE is_open");
        assertQuery("SELECT * FROM lineitem WHERE is_open=false");
        assertQuery("SELECT * FROM lineitem WHERE is_open=true or is_open is null");
        assertQuery("SELECT * FROM lineitem WHERE is_open<>true or is_open is null");
        assertQuery("SELECT * FROM lineitem WHERE is_open or is_open is null");
        assertQuery("SELECT * FROM lineitem WHERE is_open=false or is_open is null");
        assertQuery("SELECT * FROM lineitem WHERE NOT is_open");
        assertQuery("SELECT * FROM lineitem WHERE is_returned=true");
        assertQuery("SELECT * FROM lineitem WHERE is_returned");
        assertQuery("SELECT * FROM lineitem WHERE is_returned=false");
        assertQuery("SELECT * FROM lineitem WHERE NOT is_returned");
        assertQuery("SELECT * FROM lineitem WHERE is_returned and is_open");
        assertQuery("SELECT * FROM lineitem WHERE is_returned and NOT is_open");
        assertQuery("SELECT * FROM lineitem WHERE NOT is_returned and is_open");
        assertQuery("SELECT * FROM lineitem WHERE NOT is_returned and  NOT is_open");

        // query with filter using like
        assertQuery("SELECT * FROM lineitem WHERE shipinstruct like 'TAKE BACK%'");
        assertQuery("SELECT * FROM lineitem WHERE shipinstruct like 'TAKE BACK#%' escape '#'");

        // no row passes the filter
        assertQuery(
                "SELECT linenumber, orderkey, discount FROM lineitem WHERE discount > 0.2");

        // remaining filter
        assertQuery("SELECT count(*) FROM orders_ex WHERE contains(map_keys(quantity_by_linenumber), 1)");

        // Double and float inequality filter
        assertQuery("SELECT SUM(discount) FROM lineitem WHERE discount != 0.04");
        assertQuery("SELECT SUM(discount_as_real) FROM lineitem WHERE discount_as_real != cast(0.1 as REAL)");

        // When else clause is a null constant with Map type.
        assertQuery("SELECT if(orderkey % 2 = 0, quantity_by_linenumber) FROM orders_ex");
    }

    @Test
    public void testAnalyzeStats()
    {
        assertUpdate("ANALYZE region", 5);

        // Show stats returns the following stats for each column in region table:
        // column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
        assertQuery("SHOW STATS FOR region",
                "SELECT * FROM (VALUES" +
                        "('regionkey', NULL, 5.0, 0.0, NULL, '0', '4')," +
                        "('name', 54.0, 5.0, 0.0, NULL, NULL, NULL)," +
                        "('comment', 350.0, 5.0, 0.0, NULL, NULL, NULL)," +
                        "(NULL, NULL, NULL, NULL, 5.0, NULL, NULL))");

        // Create a partitioned table and run analyze on it.
        String tmpTableName = generateRandomTableName();
        try {
            Session writeSession = buildSessionForTableWrite();
            getQueryRunner().execute(writeSession, String.format("CREATE TABLE %s (name VARCHAR, regionkey BIGINT," +
                    "nationkey BIGINT) WITH (partitioned_by = ARRAY['regionkey','nationkey'])", tmpTableName));
            getQueryRunner().execute(writeSession,
                    String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName),
                    "SELECT name, regionkey, nationkey FROM nation");
            assertUpdate(String.format("ANALYZE %s", tmpTableName), 25);
            assertQuery(String.format("SHOW STATS for %s", tmpTableName),
                    "SELECT * FROM (VALUES" +
                            "('name', 277.0, 1.0, 0.0, NULL, NULL, NULL)," +
                            "('regionkey', NULL, 5.0, 0.0, NULL, '0', '4')," +
                            "('nationkey', NULL, 25.0, 0.0, NULL, '0', '24')," +
                            "(NULL, NULL, NULL, NULL, 25.0, NULL, NULL))");
            // @TODO Add test for Analyze on table partitions. Refer: https://github.com/prestodb/presto/issues/20232
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testTableSample()
    {
        // At best we can check for query success for the TABLESAMPLE based queries as the number of rows returned
        // has some randomness.
        assertQuerySucceeds("SELECT * FROM nation TABLESAMPLE BERNOULLI (20)");
        assertQuerySucceeds("SELECT * FROM lineitem TABLESAMPLE BERNOULLI (1) WHERE orderkey > 1000");
        assertQuerySucceeds("SELECT * FROM lineitem TABLESAMPLE BERNOULLI (1) WHERE orderkey % 2 = 0");

        assertQuerySucceeds("SELECT * FROM nation TABLESAMPLE SYSTEM (45)");
        assertQuerySucceeds("SELECT * FROM lineitem TABLESAMPLE SYSTEM (1) WHERE orderkey > 1000");
        assertQuerySucceeds("SELECT * FROM lineitem TABLESAMPLE SYSTEM (1) WHERE orderkey % 2 = 0");

        assertQuerySucceeds("SELECT o.*, i.* FROM orders o TABLESAMPLE SYSTEM (10) " +
                "JOIN lineitem i TABLESAMPLE BERNOULLI (40) ON o.orderkey = i.orderkey");
    }

    @Test(groups = {"parquet"})
    public void testDateFilter()
    {
        String tmpTableName = generateRandomTableName();

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "parquet_pushdown_filter_enabled", "true")
                .setCatalogSessionProperty("hive", "orc_compression_codec", "ZSTD")
                .build();

        try {
            computeExpected(String.format("CREATE TABLE %s (c0 DATE) WITH (format = 'PARQUET')", tmpTableName), ImmutableList.of());
            computeExpected(String.format("INSERT INTO %s VALUES (DATE '1996-01-02'), (DATE '1996-12-01')", tmpTableName), ImmutableList.of());

            assertQueryResultCount(session, String.format("SELECT * from %s where c0 in (select c0 from %s) ", tmpTableName, tmpTableName), 2);
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testOrderBy()
    {
        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY nationkey");
        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY nationkey DESC");
        assertQueryOrdered("SELECT nationkey, name, regionkey FROM nation ORDER BY name");
        assertQueryOrdered("SELECT nationkey, name, regionkey FROM nation ORDER BY name DESC");

        assertQueryOrdered(
                "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax "
                        + "FROM lineitem ORDER BY orderkey, linenumber");
        assertQueryOrdered(
                "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax "
                        + "FROM lineitem ORDER BY orderkey, linenumber DESC");

        assertQueryOrdered(
                "SELECT returnflag, linestatus, count(*) FROM lineitem "
                        + "GROUP BY returnflag, linestatus ORDER BY returnflag, linestatus");

        assertQuery("SELECT linenumber, NULL FROM lineitem GROUP BY 1 ORDER BY 1");

        assertQueryOrdered("SELECT * FROM nation ORDER BY nationkey OFFSET 7 LIMIT 5");
        assertQueryOrdered("SELECT * FROM nation ORDER BY nationkey OFFSET 7 LIMIT 100");
        assertQueryReturnsEmptyResult("SELECT * FROM nation ORDER BY nationkey OFFSET 700 LIMIT 5");
    }

    @Test
    public void testEmptyTable()
    {
        assertQueryOrdered("SELECT COUNT(1) FROM empty_table WHERE orderkey > 4");
    }

    @Test
    public void testTopN()
    {
        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY nationkey LIMIT 5");

        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY nationkey LIMIT 50");

        assertQueryOrdered(
                "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax "
                        + "FROM lineitem ORDER BY orderkey, linenumber DESC LIMIT 10");

        assertQueryOrdered(
                "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax "
                        + "FROM lineitem ORDER BY orderkey, linenumber DESC LIMIT 2000");

        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY name LIMIT 15");
        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY name DESC LIMIT 15");

        assertQuery("SELECT linenumber, NULL FROM lineitem ORDER BY 1 LIMIT 23");
    }

    @Test
    public void testNullIf()
    {
        assertQuery("SELECT NULLIF(totalprice, 0) FROM (SELECT SUM(extendedprice) AS totalprice FROM lineitem WHERE shipdate >= '1995-09-01')");
        assertQuery("SELECT NULLIF(totalprice, 0) FROM (SELECT SUM(extendedprice) AS totalprice FROM lineitem WHERE shipdate >= '9999-99-99')");
        assertQuery("SELECT NULLIF(totalprice, 0.5) FROM (SELECT SUM(extendedprice) AS totalprice FROM lineitem WHERE shipdate >= '1995-09-01')");
        assertQuery("SELECT NULLIF(totalprice, 0.5) FROM (SELECT SUM(extendedprice) AS totalprice FROM lineitem WHERE shipdate >= '9999-99-99')");
        assertQuery("SELECT NULLIF(totalprice, 0) FROM (SELECT COUNT(1) AS totalprice FROM lineitem WHERE shipdate >= '1995-09-01')");
        assertQuery("SELECT NULLIF(totalprice, 0) FROM (SELECT COUNT(1) AS totalprice FROM lineitem WHERE shipdate >= '9999-99-99')");
        assertQuery("SELECT NULLIF(totalprice, 0.5) FROM (SELECT COUNT(1) AS totalprice FROM lineitem WHERE shipdate >= '1995-09-01')");
        assertQuery("SELECT NULLIF(totalprice, 0.5) FROM (SELECT COUNT(1) AS totalprice FROM lineitem WHERE shipdate >= '9999-99-99')");
    }

    @Test
    public void testCast()
    {
        assertQuery("SELECT CAST(linenumber as TINYINT), CAST(linenumber AS SMALLINT), "
                + "CAST(linenumber AS INTEGER), CAST(linenumber AS BIGINT), CAST(quantity AS REAL), "
                + "CAST(orderkey AS DOUBLE), CAST(orderkey AS VARCHAR) FROM lineitem");

        assertQuery("SELECT CAST(true as VARCHAR), CAST(false as VARCHAR)");
        assertQuery("SELECT CAST(0.0 as VARCHAR)");

        assertQuery("SELECT try_cast(linenumber as TINYINT), try_cast(linenumber AS SMALLINT), "
                + "try_cast(linenumber AS INTEGER), try_cast(linenumber AS BIGINT), try_cast(quantity AS REAL), "
                + "try_cast(orderkey AS DOUBLE), try_cast(orderkey AS VARCHAR) FROM lineitem");

        // Casts to varbinary.
        assertQuery("SELECT cast(null as varbinary)");
        assertQuery("SELECT cast('' as varbinary)");
        assertQuery("SELECT cast('string_longer_than_12_characters' as varbinary)");
        assertQuery("SELECT cast(comment as varbinary) from orders");

        // Some values are too large and would trigger "Out of range for tinyint" for a regular cast.
        assertQuery("SELECT try_cast(orderkey as TINYINT) FROM lineitem");

        // Ensure timestamp casts are correct.
        assertQuery("SELECT cast(cast(shipdate as varchar) as timestamp) FROM lineitem ORDER BY 1");

        // Ensure date casts are correct.
        assertQuery("SELECT cast(cast(orderdate as varchar) as date) FROM orders ORDER BY 1");

        // Cast to Json type.
        assertQuery("SELECT cast(regionkey = 2 as JSON) FROM nation");
        assertQuery("SELECT cast(size as JSON), cast(partkey as JSON), cast(brand as JSON), cast(name as JSON) FROM part");
        assertQuery("SELECT cast(nationkey + 0.01 as JSON), cast(array[suppkey, nationkey] as JSON), cast(map(array[name, address, phone], array[1.1, 2.2, 3.3]) as JSON), cast(row(name, suppkey) as JSON), cast(array[map(array[name, address], array[1, 2]), map(array[name, phone], array[3, 4])] as JSON), cast(map(array[name, address, phone], array[array[1, 2], array[3, 4], array[5, 6]]) as JSON), cast(map(array[suppkey], array[name]) as JSON), cast(row(array[name, address], array[], array[null], map(array[phone], array[null])) as JSON) from supplier");
        assertQuery("SELECT cast(orderdate as JSON) FROM orders");
        assertQueryFails("SELECT cast(map(array[from_unixtime(suppkey)], array[1]) as JSON) from supplier", "Cannot cast .* to JSON");

        assertQuery("SELECT try_cast(regionkey = 2 as JSON) FROM nation");
        assertQuery("SELECT try_cast(size as JSON), try_cast(partkey as JSON), try_cast(brand as JSON), try_cast(name as JSON) FROM part");
        assertQuery("SELECT try_cast(nationkey + 0.01 as JSON), try_cast(array[suppkey, nationkey] as JSON), try_cast(map(array[name, address, phone], array[1.1, 2.2, 3.3]) as JSON), try_cast(row(name, suppkey) as JSON), try_cast(array[map(array[name, address], array[1, 2]), map(array[name, phone], array[3, 4])] as JSON), try_cast(map(array[name, address, phone], array[array[1, 2], array[3, 4], array[5, 6]]) as JSON), try_cast(map(array[suppkey], array[name]) as JSON), try_cast(row(array[name, address], array[], array[null], map(array[phone], array[null])) as JSON) from supplier");
        assertQuery("SELECT try_cast(orderdate as JSON) FROM orders");
        assertQueryFails("SELECT try_cast(map(array[from_unixtime(suppkey)], array[1]) as JSON) from supplier", "Cannot cast .* to JSON");

        // Cast from Json type.
        assertQuery("SELECT cast(json_parse(json_format(cast(array[nationkey, regionkey] as json))) as array(smallint)) FROM nation");
        assertQuery("SELECT cast(json_parse(json_format(cast(map(array[1, 2], array[nationkey, regionkey]) as json))) as map(tinyint, smallint)) FROM nation");
        assertQuery("SELECT cast(json_parse(json_format(cast(row(nationkey, name) as json))) as row(smallint, varchar)) FROM nation");

        // Round-trip tests of casts for Json.
        assertQuery("SELECT cast(cast(name as JSON) as VARCHAR), cast(cast(size as JSON) as INTEGER), cast(cast(size + 0.01 as JSON) as DOUBLE), cast(cast(size > 5 as JSON) as BOOLEAN) FROM part");
        assertQuery("SELECT cast(cast(array[suppkey, nationkey] as JSON) as ARRAY(INTEGER)), cast(cast(map(array[name, address, phone], array[1.1, 2.2, 3.3]) as JSON) as MAP(VARCHAR(40), DOUBLE)), cast(cast(map(array[name], array[phone]) as JSON) as MAP(VARCHAR(25), JSON)), cast(cast(array[array[suppkey], array[nationkey]] as JSON) as ARRAY(JSON)) from supplier");

        // Cast from date to timestamp
        assertQuery("SELECT CAST(date(shipdate) AS timestamp) FROM lineitem");
        Session session = Session.builder(getSession())
                .setSystemProperty("legacy_timestamp", "false")
                .build();
        assertQuery(session, "SELECT CAST(date(shipdate) AS timestamp) FROM lineitem");

        // Cast all integer types to short decimal
        assertQuery("SELECT CAST(linenumber_as_tinyint as DECIMAL(2, 0)) FROM lineitem");
        assertQuery("SELECT CAST(linenumber_as_smallint as DECIMAL(8, 4)) FROM lineitem");
        assertQuery("SELECT CAST(CAST(linenumber as INTEGER) as DECIMAL(15, 6)) FROM lineitem");
        assertQuery("SELECT CAST(nationkey as DECIMAL(18, 6)) FROM nation_partitioned");

        // Cast all integer types to long decimal
        assertQuery("SELECT CAST(linenumber_as_tinyint as DECIMAL(25, 0)) FROM lineitem");
        assertQuery("SELECT CAST(linenumber_as_smallint as DECIMAL(19, 4)) FROM lineitem");
        assertQuery("SELECT CAST(CAST(linenumber as INTEGER) as DECIMAL(20, 6)) FROM lineitem");
        assertQuery("SELECT CAST(nationkey as DECIMAL(22, 6)) FROM nation_partitioned");

        // Cast short decimal to integer types
        assertQuery("SELECT CAST(c0 as TINYINT) FROM (VALUES (DECIMAL'1.23'), " +
                "(NULL), (DECIMAL'1.'), (DECIMAL'0.0')) as l (c0)");
        assertQuery("SELECT CAST(c0 as SMALLINT) FROM (VALUES (DECIMAL'123.04'), " +
                "(NULL), (DECIMAL'32.760'), (DECIMAL'0.0')) as l (c0)");
        assertQuery("SELECT CAST(c0 as INTEGER) FROM (VALUES (DECIMAL'12345.678'), " +
                "(NULL), (DECIMAL'123456.7890'), (DECIMAL'1234567.0'), (DECIMAL'0.0')) as l (c0)");
        assertQuery("SELECT CAST(c0 as BIGINT) FROM (VALUES (DECIMAL'1234567.89012134'), " +
                "(NULL), (DECIMAL'12345678.901'), (DECIMAL'123456789.01214234')) as l (c0)");

        // Cast long decimal to integer types
        assertQuery("SELECT CAST(c0 as INTEGER) FROM (VALUES (DECIMAL'12345.67890121416182022234'), " +
                "(NULL), (DECIMAL'123456.78901214161822234'), (DECIMAL'1234567.890121416234')) as l (c0)");
        assertQuery("SELECT CAST(c0 as BIGINT) FROM (VALUES (DECIMAL'1234567.890121416182022234'), " +
                "(NULL), (DECIMAL'12345678.901214161822234'), (DECIMAL'123456789.0121416234')) as l (c0)");

        // Cast short decimal to double/float.
        assertQuery("SELECT CAST(c0 as DOUBLE) FROM (VALUES (DECIMAL'1.234'), (NULL), (DECIMAL'12.12345'), " +
                "(DECIMAL'12345.1234'), (DECIMAL'123456789.1234567'), (DECIMAL'1234567890121418.0')) as l (c0)");
        assertQuery("SELECT CAST(c0 as REAL) FROM (VALUES (DECIMAL'1.234'), (NULL), (DECIMAL'12.12345'), " +
                "(DECIMAL'12345.1234'), (DECIMAL'123456789.1234567'), (DECIMAL'1234567890121418.0')) as l (c0)");

        // Cast long decimal to double/float.
        assertQuery("SELECT CAST(c0 as DOUBLE) FROM (VALUES (DECIMAL'1234567890121416182022.234'), (NULL), " +
                "(DECIMAL'12345678920222426.1234'), (DECIMAL'12345678901214161830.1234567')) as l (c0)");
        assertQuery("SELECT CAST(c0 as REAL) FROM (VALUES (DECIMAL'1234567890121416182022.234'), (NULL), " +
                "(DECIMAL'12345678920222426.1234'), (DECIMAL'12345678901214162830.1234567')) as l (c0)");

        // Cast to ROW.
        assertQuery("SELECT cast(row(orderkey, comment) as row(\"123\" varchar, \"456\" varchar)) FROM orders");
    }

    @Test
    public void testTry()
    {
        assertQuery("SELECT try(orderkey / (linenumber - 2)) FROM lineitem");
        assertQuery("SELECT try(cast(if(linenumber % 3 = 0, '123', comment) as integer)) FROM lineitem");
    }

    @Test
    public void testIf()
    {
        assertQuery("SELECT if(linenumber % 5 = 0, 10, 20) FROM lineitem");
        assertQuery("SELECT if(linenumber % 5 = 0, orderkey * 3, discount / 2) FROM lineitem");
    }

    @Test
    public void testSwitch()
    {
        assertQuery("SELECT case linenumber % 10 when orderkey % 3 then orderkey + 1 when 2 then orderkey + 2 else 0 end FROM lineitem");
        assertQuery("SELECT case linenumber when 1 then 'one' when 2 then 'two' else '...' end FROM lineitem");
        assertQuery("SELECT case when linenumber = 1 then 'one' when linenumber = 2 then 'two' else '...' end FROM lineitem");
    }

    @Test
    public void testRegexp()
    {
        assertQuery("SELECT regexp_extract(key, '[^\\.]+'), regexp_extract(key, '([^\\.]+)\\.([^\\.]+)', 1), regexp_extract(key, '([^\\.]+)\\.([^\\.]+)', 2) FROM (" +
                "SELECT concat(name, '.', cast(regionkey AS VARCHAR), '.', cast(nationkey AS VARCHAR)) AS key FROM nation" +
                ")");
        assertQuery("SELECT regexp_extract_all(key, '[^\\.]+'), regexp_extract_all(key, '([^\\.]+)\\.([^\\.]+)', 1), regexp_extract_all(key, '([^\\.]+)\\.([^\\.]+)', 2) FROM (" +
                "SELECT concat(name, '.', cast(regionkey AS VARCHAR), '.', cast(nationkey AS VARCHAR)) AS key FROM nation" +
                ")");
        assertQuery("SELECT regexp_like(key, '\\.1\\.') FROM (" +
                "SELECT concat(name, '.', cast(regionkey AS VARCHAR), '.', cast(nationkey AS VARCHAR)) AS key FROM nation" +
                ")");
    }

    @Test
    public void testJsonExtract()
    {
        assertQuery("SELECT json_extract_scalar(cast(x as json), '$[1]') " +
                "FROM (SELECT '[' || array_join(array[nationkey, regionkey], ',') || ']' as x FROM nation)");
    }

    @Test
    public void testValues()
    {
        assertQuery("SELECT 1, 0.24, ceil(4.5), 'A not too short ASCII string'");
        assertQuery("SELECT NULL");
        assertQuery("SELECT * FROM (VALUES NULL, NULL)");
        assertQuery("SELECT cast(NULL as bigint), cast(NULL as integer), cast(NULL as smallint), cast(NULL as tinyint)");
        assertQuery("SELECT cast(NULL as varchar)");

        assertQuery("SELECT array[1, 23, 456]");
        assertQuery("SELECT array[1, 23, NULL, 456]");

        assertQuery("SELECT array['apple', 'banana', 'carrot', 'A new kind of fruit with a very long name containing special characters ,./;']");
        assertQuery("SELECT array['apple', NULL, 'banana', 'carrot', 'A new kind of fruit with a very long name containing special characters ,./;']");

        assertQuery("SELECT array[0.1, 2.3, 45.6]");
        assertQuery("SELECT array[0.1, 2.3, 45.6, NULL]");

        assertQuery("SELECT array[NULL, NULL, NULL, NULL]");

        assertQuery("SELECT array[1, 2, 3], array[0.1, NULL, 0.23, 0.00004], array['x', 'y', 'zetta']");

        assertQuery("SELECT * FROM (VALUES (array[1, 23, 456])) as t(a)");
        assertQuery("SELECT * FROM (VALUES (array[1, NULL, 23, 456])) as t(a)");

        assertQuery("SELECT * FROM (VALUES (map(array[1, 2, 3], array[10, 20, 30]))) as t(a)");

        assertQuery("SELECT BIGINT '12345', INTEGER '1234', SMALLINT '123', TINYINT '12', TRUE, FALSE, DOUBLE '1.234', REAL '1.23', 'ABC', 'Somewhat longish string', NULL, array[1, 2, 3]");

        // Test ValuesNode with expressions.
        assertQuery("SELECT * FROM UNNEST(sequence(1, 10000), sequence(5, 10000)) as t(x, y)");
    }

    @Test
    public void testLiterals()
    {
        // Large arrays are converted into $literal$array(varchar(22))(from_base64(VARCHAR'DgAAAFZBUk....')) during planning.
        assertQuery("SELECT\n" +
                "    ARRAY[\n" +
                "        '2021-01-01::2021-01-03',\n" +
                "        '2021-01-04::2021-01-10',\n" +
                "        '2021-01-11::2021-01-17',\n" +
                "        '2021-01-18::2021-01-24',\n" +
                "        '2021-01-25::2021-01-31',\n" +
                "        '2021-02-01::2021-02-07',\n" +
                "        '2021-02-08::2021-02-14',\n" +
                "        '2021-02-15::2021-02-21',\n" +
                "        '2021-02-22::2021-02-28',\n" +
                "        '2021-03-01::2021-03-07',\n" +
                "        '2021-03-08::2021-03-14',\n" +
                "        '2021-03-15::2021-03-21',\n" +
                "        '2021-03-22::2021-03-28',\n" +
                "        '2021-03-29::2021-04-04',\n" +
                "        '2021-04-05::2021-04-11',\n" +
                "        '2021-04-12::2021-04-18',\n" +
                "        '2021-04-19::2021-04-25',\n" +
                "        '2021-04-26::2021-05-02',\n" +
                "        '2021-05-03::2021-05-09',\n" +
                "        '2021-05-10::2021-05-16',\n" +
                "        '2021-05-17::2021-05-23',\n" +
                "        '2021-05-24::2021-05-30',\n" +
                "        '2021-05-31::2021-06-06',\n" +
                "        '2021-06-07::2021-06-13',\n" +
                "        '2021-06-14::2021-06-20',\n" +
                "        '2021-06-21::2021-06-27',\n" +
                "        '2021-06-28::2021-07-04',\n" +
                "        '2021-07-05::2021-07-11',\n" +
                "        '2021-07-12::2021-07-18',\n" +
                "        '2021-07-19::2021-07-25',\n" +
                "        '2021-07-26::2021-08-01',\n" +
                "        '2021-08-02::2021-08-08',\n" +
                "        '2021-08-09::2021-08-15',\n" +
                "        '2021-08-16::2021-08-22',\n" +
                "        '2021-08-23::2021-08-29',\n" +
                "        '2021-08-30::2021-09-05',\n" +
                "        '2021-09-06::2021-09-12',\n" +
                "        '2021-09-13::2021-09-19',\n" +
                "        '2021-09-20::2021-09-23'\n" +
                "    ][linenumber]\n" +
                "FROM lineitem");
    }

    @Test
    public void testDecimalLiterals()
    {
        Session enableDecimalParsing = enableDecimalParsing();
        // Single SHORT_DECIMAL literal.
        assertQuery(enableDecimalParsing, "SELECT CAST('123.1234' as DECIMAL(8,4))");
        // Single LONG_DECIMAL literal.
        assertQuery(enableDecimalParsing, "SELECT CAST('123456789012345.123456' as DECIMAL(22,6))");
        // Multiple SHORT_DECIMAL literals with NULLs.
        assertQuery(enableDecimalParsing, "SELECT * from (values decimal'123.12', decimal'-0.0004', NULL," +
                "CAST('922337203685477580' as DECIMAL(18,0)), decimal'1123', NULL)");
        // Multiple SHORT_DECIMAL and LONG_DECIMAL literals with NULLs.
        assertQuery(enableDecimalParsing, "SELECT * from (values CAST('-123456789012345.123456' as DECIMAL(21,6))," +
                "NULL, CAST('9999999999999999999999999' as DECIMAL(25, 0))," +
                "CAST('-999999999.999999' as DECIMAL(15, 6)), NULL)");
        // Array of decimals.
        assertQuery(enableDecimalParsing, "SELECT ARRAY[decimal'1.2', decimal'123.123'," +
                "decimal'100000000.0', NULL, NULL]");
    }

    private Session enableDecimalParsing()
    {
        return Session.builder(getSession())
                .setSystemProperty("parse_decimal_literals_as_double", "false")
                .build();
    }

    @Test
    public void testDecimalArithmetic()
    {
        // Addition of two Long Decimal columns with inferred types DECIMAL(35,20) and DECIMAL(29,4)
        // and also contains NULLs.
        assertQuery(
                "SELECT n + m from (values " +
                        "(DECIMAL'999999999999999.999' , DECIMAL'1')," +
                        "(DECIMAL'-123456789012345.123456', DECIMAL'-9999999999999999')," +
                        "(DECIMAL'1.23', DECIMAL'-0.0005')," +
                        "(DECIMAL'1.33333333333333333333', DECIMAL'-0.0005')," +
                        "(NULL, NULL), (decimal'1.23', NULL)) t(n, m)");

        // Addition of two short decimal columns of type DECIMAL(10,7) and DECIMAL(10,5)
        assertQuery("SELECT n + m from (values (decimal'1.1', decimal'-1.1')," +
                "(decimal'-0.0000004', decimal'-0.12345')," +
                "(decimal'123', decimal'13245')) t(n, m)");

        // numeric limits
        assertQueryFails("SELECT n + m from (values (DECIMAL'99999999999999999999999999999999999999'," +
                        "CAST('1' as DECIMAL(2,0)))) t(n, m)",
                ".*Decimal.*");
        assertQueryFails(
                "SELECT n + m from (values (CAST('-99999999999999999999999999999999999999' as DECIMAL(38,0))," +
                        "CAST('-1' as DECIMAL(15,0)))) t(n,m)",
                ".*Decimal.*");

        // Subtraction of long decimals.
        assertQuery(
                "SELECT n - m from (values " +
                        "(CAST('999999999999999.999' as decimal(18,3)), CAST('1' as decimal(1,0)))," +
                        "(CAST('-123456789012345.123456' as DECIMAL(21,6)), CAST('-9999999999999999' as DECIMAL(25, 0)))," +
                        "(CAST('1.23' as DECIMAL(3,2)), CAST('-0.0005' as DECIMAL(5,4)))," +
                        "(CAST('1.33333333333333333333' as DECIMAL(23,20)), CAST('-0.0005' as DECIMAL(5,4)))," +
                        "(NULL, NULL)," +
                        "(decimal'1.23', NULL)) t(n, m)");

        // Subtraction of short decimals.
        assertQuery("SELECT n - m from (values (decimal'1.1', decimal'-1.1')," +
                "(decimal'-0.0000004', decimal'-0.12345')," +
                "(decimal'123', decimal'13245')) t(n, m)");
        // Subtraction Overflow
        assertQueryFails(
                "SELECT n - m from (values (DECIMAL'-99999999999999999999999999999999999999', decimal'1')) " +
                        "t(n,m)", ".*Decimal.*");
        // Multiplication.
        assertQuery("SELECT n * m from (values (DECIMAL'99999999999999999999', DECIMAL'-0.000003')," +
                "(DECIMAL'-0.00000000000000001', DECIMAL'10000000000'),(DECIMAL'-12345678902345.124', DECIMAL'-0.275')," +
                "(NULL, NULL), (NULL, DECIMAL'2')) t(n, m)");
        assertQuery("SELECT n*m from(values (DECIMAL '100', DECIMAL '299'),(DECIMAL '5.4', DECIMAL '-125')," +
                "(DECIMAL '-3.4', DECIMAL '-625'), (DECIMAL '-0.0004', DECIMAL '-0.0123')) t(n,m)");
        // Multiplication overflow.
        assertQueryFails("SELECT n*m from (values (DECIMAL'14621507953634074601941877663083790335', DECIMAL'10')) " +
                "t(n,m)", ".*Decimal.*");
        // Division long decimals.
        assertQuery("SELECT n/m from(values " +
                "(CAST('10000000000000000.00' as decimal(19, 2)), DECIMAL'30000000000000.00')," +
                "(CAST('-1255555555555' as decimal(19, 0)), DECIMAL'50000000000')," +
                "(CAST('-0.55555555' as decimal(19, 6)), DECIMAL'-111111111.222')," +
                "(CAST('123456789123456789' as decimal(18, 0)), DECIMAL '-999232342342344234')" +
                ") t(n, m)");
        // Divide by zero error.
        assertQueryFails("SELECT n/m from(values (DECIMAL'100', DECIMAL'0.0')) t(n,m)",
                ".*Division by zero.*");

        // Division short decimals.
        assertQuery("SELECT n/m from(values (DECIMAL'100', DECIMAL'299'),(DECIMAL'5.4', DECIMAL'-125')," +
                "(DECIMAL'-3.4', DECIMAL'0.6'), (DECIMAL'-0.0004', DECIMAL'-0.0123')) t(n,m)");

        // Division overflow.
        assertQueryFails("SELECT n/m from(values (DECIMAL'99999999999999999999999999999999999999', DECIMAL'0.01'))" +
                " t(n,m)", ".*Decimal.*");
    }

    @Test
    public void testDecimalLogicalFunctions()
    {
        // Between.
        assertQuery("SELECT c0 from (values DECIMAL'2.5', DECIMAL'2.4232', DECIMAL'3', NULL, DECIMAL'5000') t(c0) " +
                "where c0 between DECIMAL'2.0' and DECIMAL'3.0'");
        assertQuery("SELECT c0 from (values DECIMAL'-1.54455555555555555555'," +
                "DECIMAL'3.141592653589793238', NULL, DECIMAL'2.718281828459045') t(c0) " +
                "where c0 between DECIMAL'-2.0' and DECIMAL'3.0'");
        assertQuery("SELECT c0 from (values DECIMAL'99999999999999999999999999999999999999', DECIMAL'-99999999999999999999999999999999999999'," +
                "DECIMAL'99999999999999999999999999999999999998', DECIMAL'-99999999999999999999999999999999999998') " +
                " t(c0) where c0 between DECIMAL'-99999999999999999999999999999999999999' and DECIMAL'99999999999999999999999999999999999999'");
        // Equals.
        assertQuery("SELECT c0 from (values DECIMAL'2.5', DECIMAL'2.4232', DECIMAL'3', NULL, DECIMAL'5000') t(c0) " +
                "where c0=DECIMAL'2.5'");
        assertQuery("SELECT c0 from (values DECIMAL'2.5555555555555555555', DECIMAL'3.141592653589793238', NULL," +
                "DECIMAL'2.718281828459045') t(c0) " +
                "where c0=DECIMAL'2.5555555555555555555'");
        assertQuery("SELECT c0 from (values (DECIMAL'2.5555555555555555555',DECIMAL'2.5555555555555555555')," +
                "(DECIMAL'3.141592653589793238', NULL), (DECIMAL'-1.54455555555555555555', DECIMAL'-1.54455555555555555555')," +
                "(NULL, NULL )) t(c0, c1) where c0 = c1");

        // Greater-than.
        assertQuery("SELECT c0 from (values (DECIMAL'2.5555555555555555555',DECIMAL'2.5555555555555555551')," +
                "(DECIMAL'3.141592653589793238', NULL), (DECIMAL'-1.54455555555555555551', DECIMAL'-1.54455555555555555555')," +
                "(NULL, NULL )) t(c0, c1) where c0 > c1");
        // Less-than.
        assertQuery("SELECT c0 from (values (DECIMAL'2.5555555555555555555',DECIMAL'2.5555555555555555551')," +
                "(DECIMAL'3.141592653589793238', NULL), (DECIMAL'-1.54455555555555555551', DECIMAL'-1.54455555555555555555')," +
                "(NULL, NULL )) t(c0, c1) where c0 < c1");

        // Greater-than-equal
        assertQuery("SELECT c0 from (values (DECIMAL'2.5555555555555555555',DECIMAL'2.5555555555555555551')," +
                "(DECIMAL'3.141592653589793238', NULL), (DECIMAL'-1.54455555555555555551', DECIMAL'-1.54455555555555555555')," +
                "(NULL, NULL )) t(c0, c1) where c0 >= c1");
        // Less-than-equal.
        assertQuery("SELECT c0 from (values (DECIMAL'2.5555555555555555555',DECIMAL'2.5555555555555555551')," +
                "(DECIMAL'3.141592653589793238', NULL), (DECIMAL'-1.54455555555555555551', DECIMAL'-1.54455555555555555555')," +
                "(NULL, NULL )) t(c0, c1) where c0 <= c1");
    }

    @Test
    public void testStringFunctions()
    {
        // Substr, length, trim.
        assertQuery("SELECT substr(comment, 1, 10), length(comment), trim(comment) FROM orders");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), ltrim(comment) FROM orders");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), rtrim(comment) FROM orders");

        assertQuery("SELECT trim(comment, ' ns'), ltrim(comment, 'a b c'), rtrim(comment, 'l y') FROM orders");

        // Split
        assertQueryOrdered("SELECT shipmode, comment, split(comment, 'ly') FROM lineitem order by 1,2");
        assertQueryOrdered("SELECT shipmode, comment, split(comment, 'i', 3) FROM lineitem order by 1,2");
        assertQuery("SELECT shipmode, comment, split(comment, 'i', linenumber) FROM lineitem order by 1,2");

        // Split_part
        assertQuery("SELECT shipmode, comment, split_part(comment, 'ly', 1) FROM lineitem order by 1,2");

        // Reverse
        assertQuery("SELECT comment, reverse(comment) FROM orders");
    }

    @Test
    public void testBinaryFunctions()
    {
        // crc32.
        assertQuery("SELECT crc32(cast(comment as varbinary)) FROM orders");

        // from_base64, to_base64.
        assertQuery("SELECT from_base64(to_base64(cast(comment as varbinary))) FROM orders");

        // from_big_endian_32, to_big_endian_32.
        assertQuery("SELECT to_big_endian_32(null)");
        assertQuery("SELECT to_big_endian_32(1)");
        assertQuery("SELECT to_big_endian_32(-1)");
        assertQuery("SELECT to_big_endian_32(12345678)");
        assertQuery("SELECT to_big_endian_32(-12345678)");
        assertQuery("SELECT to_big_endian_32(cast(orderkey as INT)) FROM orders");

        assertQuery("SELECT from_big_endian_32(to_big_endian_32(null))");
        assertQuery("SELECT from_big_endian_32(to_big_endian_32(0))");
        assertQuery("SELECT from_big_endian_32(to_big_endian_32(1))");
        assertQuery("SELECT from_big_endian_32(to_big_endian_32(-1))");
        assertQuery("SELECT from_big_endian_32(to_big_endian_32(12345678))");
        assertQuery("SELECT from_big_endian_32(to_big_endian_32(-12345678))");
        assertQuery("SELECT from_big_endian_32(to_big_endian_32(INT '" + Integer.MAX_VALUE + "'))");
        assertQuery("SELECT from_big_endian_32(to_big_endian_32(INT '" + Integer.MIN_VALUE + "'))");
        assertQuery("SELECT from_big_endian_32(to_big_endian_32(cast(orderkey as INT))) FROM orders");

        // from_big_endian_64, to_big_endian_64.
        assertQuery("SELECT to_big_endian_64(null)");
        assertQuery("SELECT to_big_endian_64(1)");
        assertQuery("SELECT to_big_endian_64(-1)");
        assertQuery("SELECT to_big_endian_64(12345678)");
        assertQuery("SELECT to_big_endian_64(-12345678)");
        assertQuery("SELECT to_big_endian_64(orderkey) FROM orders");

        assertQuery("SELECT from_big_endian_64(to_big_endian_64(null))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(0))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(1))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(-1))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(12345678))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(-12345678))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(BIGINT '" + Integer.MAX_VALUE + "'))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(BIGINT '" + Integer.MIN_VALUE + "'))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(BIGINT '" + Long.MAX_VALUE + "'))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(BIGINT '" + Long.MIN_VALUE + "'))");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(orderkey)) FROM orders");
        assertQuery("SELECT from_big_endian_64(to_big_endian_64(custkey)) FROM orders");

        // from_hex, to_hex.
        assertQuery("SELECT from_hex(to_hex(cast(comment as varbinary))) FROM orders");

        // hmac_sha1, hmac_sha256, hmac_sha512.
        assertQuery("SELECT hmac_sha1(cast(comment as varbinary), cast(clerk as varbinary)) FROM orders");
        assertQuery("SELECT hmac_sha256(cast(comment as varbinary), cast(clerk as varbinary)) FROM orders");
        assertQuery("SELECT hmac_sha512(cast(comment as varbinary), cast(clerk as varbinary)) FROM orders");

        // md5.
        assertQuery("SELECT md5(cast(comment as varbinary)) FROM orders");

        // sha1, sha256, sha512.
        assertQuery("SELECT sha1(cast(comment as varbinary)) FROM orders");
        assertQuery("SELECT sha256(cast(comment as varbinary)) FROM orders");
        assertQuery("SELECT sha512(cast(comment as varbinary)) FROM orders");

        // spooky_hash_v2_32, spooky_hash_v2_64.
        assertQuery("SELECT spooky_hash_v2_32(cast(comment as varbinary)) FROM orders");
        assertQuery("SELECT spooky_hash_v2_64(cast(comment as varbinary)) FROM orders");

        // xxhash64.
        assertQuery("SELECT xxhash64(cast(comment as varbinary)) FROM orders");

        // from_base64url, to_base64url
        assertQuery("SELECT from_base64url(to_base64url(cast(comment as varbinary))) FROM orders");
    }

    @Test
    public void testArrayAndMapFunctions()
    {
        assertQuery("SELECT array[orderkey, partkey] FROM lineitem");

        assertQuery("SELECT cardinality(quantities), cardinality(quantity_by_linenumber) FROM orders_ex");

        assertQuery("SELECT map_keys(quantity_by_linenumber), map_values(quantity_by_linenumber) FROM orders_ex");

        assertQuery("SELECT filter(quantities, q -> q > 10) FROM orders_ex");

        assertQuery("SELECT transform(array[1, 2, 3], x -> x * regionkey + nationkey) FROM nation");
        assertQuery("SELECT transform(array[1, 2, 3], x -> x + nationkey) FROM nation");

        assertQuery("SELECT reduce(array[nationkey, regionkey], 103, (s, x) -> s + x, s -> s) FROM nation");

        assertQuery("SELECT array_distinct(quantities) FROM orders_ex");
        assertQuery("SELECT map(u, u) FROM (SELECT array_duplicates(cast(quantities as array(bigint))) as u FROM orders_ex)");

        assertQuery("SELECT array_join(array[orderkey, partkey], ',') FROM lineitem");

        assertQuery("SELECT quantities[2] FROM orders_ex WHERE cardinality(quantities) >= 2");
        assertQuery("SELECT element_at(quantities, 2) FROM orders_ex");
        assertQuery("SELECT slice(quantities, 2, 4) FROM orders_ex");

        assertQuery("SELECT quantity_by_linenumber[2] FROM orders_ex WHERE cardinality(quantities) >= 2");
        assertQuery("SELECT element_at(quantity_by_linenumber, 2) FROM orders_ex");
        assertQuery("SELECT cast(zip(quantities, map_values(quantity_by_linenumber)) as array(row(a double, b integer))) FROM orders_ex");
    }

    @Test
    public void testWidthBucket()
    {
        assertQuery("SELECT width_bucket(to_unixtime(cast(ds as timestamp)), array[1609487900, 1619740800, 1622419200]) FROM customer_bucketed");
        assertQuery("SELECT width_bucket(to_unixtime(cast(ds as timestamp)), array[1609487900.1, 1619740800.2, 1622419200.3]) FROM customer_bucketed");
    }

    @Test
    public void testPartitionedTable()
    {
        assertQuery("SELECT * from nation_partitioned", "SELECT nationkey, name, comment, cast(regionkey as VARCHAR) FROM nation");
        assertQuery("SELECT * from nation_partitioned WHERE regionkey='2'", "SELECT nationkey, name, comment, cast(regionkey as VARCHAR) FROM nation WHERE regionkey=2");
        assertQuery("SELECT * from nation_partitioned WHERE regionkey in ('2', '4')", "SELECT nationkey, name, comment, cast(regionkey as VARCHAR) FROM nation WHERE regionkey in (2, 4)");

        assertQuery("SELECT nationkey,regionkey FROM nation_partitioned WHERE CAST(nationkey AS VARCHAR)=regionkey");

        // This triggers output partitioning by a constant key.
        assertQuery("SELECT T.nationkey FROM \n" +
                "(SELECT nationkey FROM nation_partitioned_ds WHERE ds = '2022-04-09' GROUP BY nationkey, ds) T\n" +
                "JOIN (SELECT nationkey FROM nation_partitioned_ds WHERE ds = '2022-03-18' GROUP BY nationkey, ds) U\n" +
                "ON T.nationkey = U.nationkey");
    }

    @Test
    public void testPath()
    {
        assertQuery("SELECT \"$path\", * from orders");

        // Fetch one of the file paths and use it in a filter
        String path = (String) computeActual("SELECT \"$path\" from orders LIMIT 1").getOnlyValue();

        assertQuery(format("SELECT * from orders WHERE \"$path\"='%s'", path));
    }

    @Test
    public void testBucket()
    {
        assertQuery("SELECT \"$bucket\", * from customer_bucketed");

        // Fetch one of the buckets and use it in a filter
        Integer bucket = (Integer) computeActual("SELECT \"$bucket\" from customer_bucketed LIMIT 1").getOnlyValue();

        assertQuery(format("SELECT * from customer_bucketed WHERE \"$bucket\"=%d", bucket));
    }

    @Test
    public void testLimit()
    {
        assertQueryResultCount("SELECT * FROM lineitem LIMIT 1", 1);
        assertQueryResultCount("SELECT * FROM lineitem LIMIT 37", 37);
        assertQueryResultCount("SELECT * FROM lineitem LIMIT 12345", 12345);
        assertQueryResultCount("SELECT * FROM lineitem LIMIT 123456", 60175);

        // test LIMIT 0
        assertQueryReturnsEmptyResult("SELECT * FROM lineitem LIMIT 0");
    }

    @Test
    public void testIsNullIsNotNull()
    {
        assertQuery("SELECT count(*) FROM orders where clerk is not null");
        assertQuery("SELECT count(*) FROM orders where clerk is null");
        assertQuery("select count(*) from orders_ex where quantities is null");
        assertQuery("select count(*) from orders_ex where quantities is not null");
        assertQuery("select count(*) from orders_ex where quantity_by_linenumber is null");
        assertQuery("select count(*) from orders_ex where quantity_by_linenumber is not null");
    }

    @Test
    public void testUnnest()
    {
        assertQuery("SELECT orderkey, quantity FROM orders_ex CROSS JOIN UNNEST (quantities) as t(quantity)");
        assertQuery("SELECT orderkey, linenumber, quantity FROM orders_ex CROSS JOIN UNNEST (quantity_by_linenumber) as t(linenumber, quantity)");
        assertQuery("SELECT orderkey, linenumber, quantity, numbers FROM orders_ex CROSS JOIN UNNEST (quantity_by_linenumber, array[20, 10]) as t(linenumber, quantity, numbers)");

        assertQuery("SELECT orderkey, quantity, quantity_ordinality FROM orders_ex CROSS JOIN UNNEST (quantities) WITH ORDINALITY as t(quantity, quantity_ordinality)");
        assertQuery("SELECT orderkey, linenumber, quantity, quantity_ordinality FROM orders_ex CROSS JOIN UNNEST (quantity_by_linenumber) WITH ORDINALITY as t(linenumber, quantity, quantity_ordinality)");
        assertQuery("SELECT orderkey, linenumber, quantity, numbers, ordinal FROM orders_ex CROSS JOIN UNNEST (quantity_by_linenumber, array[20, 10]) WITH ORDINALITY as t(linenumber, quantity, numbers, ordinal)");
    }

    @Test
    public void testInformationSchemaTables()
    {
        assertQuery("select lower(table_name) from information_schema.tables "
                + "where table_name = 'lineitem' or table_name = 'LINEITEM' ");
    }

    @Test
    public void testShowAndDescribe()
    {
        assertQuery("SHOW functions");
        assertQuery("SHOW tables");
        assertQuery("DESCRIBE lineitem");
    }

    @Test
    public void testBucketedExecution()
    {
        // Run aggregation query that groups by a bucketed column.
        assertQuery("SELECT SUM(acctbal), COUNT(1), name FROM customer_bucketed WHERE '2021-01-30' != ds GROUP BY 3 order by 3");
        // Test join query using bucketed/grouped execution.
        assertQuery("SELECT sum(quantity) FROM lineitem_bucketed l, orders_bucketed o WHERE l.orderkey = o.orderkey group by l.ds");

        // Test with concurrent lifespans > 1. Test with numbers <, == and > number of buckets.
        // Test group by and join on bucketed-by column,

        // Concurrent lifespans < number of buckets.
        assertQuery(concurrentLifeSpansPerTask(5), "SELECT sum(acctbal), count(1), name FROM customer_bucketed WHERE '2021-01-30' != ds GROUP BY 3");
        assertQuery(concurrentLifeSpansPerTask(5), "SELECT count(1) FROM lineitem_bucketed l, orders_bucketed o WHERE l.orderkey = o.orderkey");

        // Concurrent lifespans == number of buckets.
        assertQuery(concurrentLifeSpansPerTask(10), "SELECT sum(acctbal), count(1), name FROM customer_bucketed WHERE '2021-01-30' != ds GROUP BY 3");
        assertQuery(concurrentLifeSpansPerTask(10), "SELECT count(1) FROM lineitem_bucketed l, orders_bucketed o WHERE l.orderkey = o.orderkey");

        // Concurrent lifespans > number of buckets.
        assertQuery(concurrentLifeSpansPerTask(20), "SELECT sum(acctbal), count(1), name FROM customer_bucketed WHERE '2021-01-30' != ds GROUP BY 3");
        assertQuery(concurrentLifeSpansPerTask(20), "SELECT count(1) FROM lineitem_bucketed l, orders_bucketed o WHERE l.orderkey = o.orderkey");
    }

    private Session concurrentLifeSpansPerTask(int value)
    {
        return Session.builder(getSession())
                .setSystemProperty("concurrent_lifespans_per_task", String.format("%s", value))
                .build();
    }

    private String generateRandomTableName()
    {
        String tableName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
        // Clean up if the temporary named table already exists.
        dropTableIfExists(tableName);
        return tableName;
    }

    @Test
    public void testCreateTableWithUnsupportedFormats()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        String[] unsupportedTableFormats = {"ORC", "JSON"};
        for (String unsupportedTableFormat : unsupportedTableFormats) {
            assertQueryFails(String.format("CREATE TABLE %s WITH (format = '" + unsupportedTableFormat + "') AS SELECT * FROM nation", tmpTableName), " Unsupported file format in TableWrite: \"" + unsupportedTableFormat + "\".");
        }
    }

    @Test
    public void testReadTableWithUnsupportedFormats()
    {
        assertQueryFails("SELECT * FROM nation_json", ".*ReaderFactory is not registered for format json.*");
        assertQueryFails("SELECT * FROM nation_text", ".*ReaderFactory is not registered for format text.*");
    }

    @Test
    public void testCreateUnpartitionedTableAsSelect()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        for (String tableFormat : TABLE_FORMATS) {
            try {
                getQueryRunner().execute(session, String.format("CREATE TABLE %s WITH (format = '" + tableFormat + "') AS SELECT * FROM nation", tmpTableName));
                assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT * FROM nation");
            }
            finally {
                dropTableIfExists(tmpTableName);
            }
        }

        try {
            getQueryRunner().execute(session, String.format("CREATE TABLE %s AS SELECT linenumber, count(*) as cnt FROM lineitem GROUP BY 1", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT linenumber, count(*) FROM lineitem GROUP BY 1");
        }
        finally {
            dropTableIfExists(tmpTableName);
        }

        try {
            getQueryRunner().execute(session, String.format("CREATE TABLE %s AS SELECT orderkey, count(*) as cnt FROM lineitem GROUP BY 1", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT orderkey, count(*) FROM lineitem GROUP BY 1");
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testCreatePartitionedTableAsSelect()
    {
        {
            Session session = buildSessionForTableWrite();
            // Generate temporary table name for created partitioned table.
            String partitionedOrdersTableName = generateRandomTableName();

            for (String tableFormat : TABLE_FORMATS) {
                try {
                    getQueryRunner().execute(session, String.format(
                            "CREATE TABLE %s WITH (format = '" + tableFormat + "', " +
                                    "partitioned_by = ARRAY[ 'orderstatus' ]) " +
                                    "AS SELECT custkey, comment, orderstatus FROM orders", partitionedOrdersTableName));
                    assertQuery(String.format("SELECT * FROM %s", partitionedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders");
                }
                finally {
                    dropTableIfExists(partitionedOrdersTableName);
                }
            }
        }
    }

    @Test
    public void testInsertIntoPartitionedTable()
    {
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        Session writeSession = buildSessionForTableWrite();

        try {
            getQueryRunner().execute(writeSession, String.format("CREATE TABLE %s (name VARCHAR, regionkey BIGINT, nationkey BIGINT) WITH (partitioned_by = ARRAY['regionkey','nationkey'])", tmpTableName));
            // Test insert into an empty table.
            getQueryRunner().execute(writeSession, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT name, regionkey, nationkey FROM nation");

            // Test failure on insert into existing partitions.
            assertQueryFails(writeSession, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName),
                    ".*Cannot insert into an existing partition of Hive table: regionkey=.*/nationkey=.*");

            // Test insert into existing partitions if insert_existing_partitions_behavior is set to OVERWRITE.
            Session overwriteSession = Session.builder(writeSession)
                    .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                    .build();
            getQueryRunner().execute(overwriteSession, String.format("INSERT INTO %s SELECT CONCAT(name, '.test'), regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT CONCAT(name, '.test'), regionkey, nationkey FROM nation");
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testInsertIntoSpecialPartitionName()
    {
        Session writeSession = buildSessionForTableWrite();
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        try {
            getQueryRunner().execute(writeSession, String.format("CREATE TABLE %s (name VARCHAR, nationkey VARCHAR) WITH (partitioned_by = ARRAY['nationkey'])", tmpTableName));

            // For special character in partition name, without correct handling, it would throw errors like 'Invalid partition spec: nationkey=A/B'
            // In this test, verify those partition names can be successfully created
            String[] specialCharacters = new String[] {"\"", "#", "%", "''", "*", "/", ":", "=", "?", "\\", "\\x7F", "{", "[", "]", "^"}; // escape single quote for sql
            for (String specialCharacter : specialCharacters) {
                getQueryRunner().execute(writeSession, String.format("INSERT INTO %s VALUES ('name', 'A%sB')", tmpTableName, specialCharacter));
                assertQuery(String.format("SELECT nationkey FROM %s", tmpTableName), String.format("VALUES('A%sB')", specialCharacter));
                getQueryRunner().execute(writeSession, String.format("DELETE FROM %s", tmpTableName));
            }
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testCreateBucketTableAsSelect()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name for bucketed table.
        String bucketedOrdersTableName = generateRandomTableName();

        for (String tableFormat : TABLE_FORMATS) {
            try {
                getQueryRunner().execute(session, String.format(
                        "CREATE TABLE %s WITH (format = '" + tableFormat + "', " +
                                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                                "bucketed_by = ARRAY[ 'custkey' ], " +
                                "bucket_count = 1) " +
                                "AS SELECT custkey, comment, orderstatus FROM orders", bucketedOrdersTableName));
                assertQuery(String.format("SELECT * FROM %s", bucketedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders");
            }
            finally {
                dropTableIfExists(bucketedOrdersTableName);
            }
        }
    }

    @Test
    public void testCreateBucketSortedTableAsSelect()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name.
        String badBucketTableName = generateRandomTableName();

        // TODO: update this test condition after bucket sort write is supported by native worker.
        try {
            this.assertQueryFails(session, String.format(
                    "CREATE TABLE %s WITH (" +
                            "partitioned_by = ARRAY[ 'orderstatus' ], " +
                            "bucketed_by=array['orderkey'], " +
                            "bucket_count=11, " +
                            "sorted_by=array['orderkey']) " +
                            "AS SELECT orderkey, orderstatus FROM orders", badBucketTableName), ".*Bucketed sorted table is not supported.*");
        }
        finally {
            dropTableIfExists(badBucketTableName);
        }
    }

    private Session buildSessionForTableWrite()
    {
        // TODO: enable this after column stats collection is enabled.
        return Session.builder(getSession())
                .setSystemProperty("table_writer_merge_operator_enabled", "true")
                .setSystemProperty("task_writer_count", "4")
                .setSystemProperty("task_partitioned_writer_count", "2")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .setCatalogSessionProperty("hive", "optimized_partition_update_serialization_enabled", "false")
                .setCatalogSessionProperty("hive", "orc_compression_codec", "ZSTD")
                .build();
    }

    private void dropTableIfExists(String tableName)
    {
        // An ugly workaround for the lack of getExpectedQueryRunner()
        computeExpected(String.format("DROP TABLE IF EXISTS %s", tableName), ImmutableList.of(BIGINT));
    }

    @Test
    public void testUnionAll()
    {
        assertQuery("SELECT distinct orderkey FROM (" +
                "SELECT orderkey FROM lineitem WHERE linenumber = 5 " +
                "UNION ALL SELECT orderkey FROM lineitem WHERE linenumber = 6)");

        assertQuery("WITH t AS (SELECT null as a, null as b UNION ALL SELECT 'xxx' as a, 12 as b) " +
                "SELECT * FROM t, t as u WHERE t.a = u.a and t.b = u.b");
    }

    @Test
    public void testSubqueries()
    {
        assertQuery("SELECT name FROM nation WHERE regionkey = (SELECT max(regionkey) FROM region)");

        // Subquery returns zero rows.
        assertQuery("SELECT name FROM nation WHERE regionkey = (SELECT regionkey FROM region WHERE regionkey < 0)");

        // Subquery returns more than one row.
        assertQueryFails("SELECT name FROM nation WHERE regionkey = (SELECT regionkey FROM region)", ".*Expected single row of input. Received 5 rows.*");
    }

    @Test
    public void testArithmetic()
    {
        assertQuery("SELECT mod(orderkey, linenumber) FROM lineitem");
        assertQuery("SELECT discount * 0.123 FROM lineitem");
        assertQuery("SELECT ln(totalprice) FROM orders");
        assertQuery("SELECT sqrt(totalprice) FROM orders");
        assertQuery("SELECT radians(totalprice) FROM orders");
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        assertQuery("SELECT from_unixtime(orderkey, '+01:00'), from_unixtime(orderkey, '-05:00'), from_unixtime(orderkey, 'Europe/Moscow') FROM orders");
        assertQuery("SELECT from_unixtime(orderkey, '+01:00'), count(1) FROM orders GROUP BY 1");

        assertQuery("SELECT parse_datetime(cast(1970 + nationkey as varchar) || '-01-02+00:' || cast(10 + (3 * nationkey) % 50 as varchar), 'YYYY-MM-dd+HH:mm'), parse_datetime(cast(1970 + nationkey as varchar) || '-01-02+00:' || cast(10 + (3 * nationkey) % 50 as varchar) || '+14:00', 'YYYY-MM-dd+HH:mmZZ') FROM nation");

        assertQuery("SELECT to_unixtime(from_unixtime(orderkey, '+01:00')), to_unixtime(from_unixtime(orderkey, '-05:00')), to_unixtime(from_unixtime(orderkey, 'Europe/Moscow')) FROM orders");
        assertQuery("SELECT to_unixtime(from_unixtime(orderkey, '+01:00')), count(1) FROM orders GROUP BY 1");
        assertQuery("SELECT to_unixtime(parse_datetime(cast(1970 + nationkey as varchar) || '-01-02+00:' || cast(10 + (3 * nationkey) % 50 as varchar), 'YYYY-MM-dd+HH:mm')), to_unixtime(parse_datetime(cast(1970 + nationkey as varchar) || '-01-02+00:' || cast(10 + (3 * nationkey) % 50 as varchar) || '+14:00', 'YYYY-MM-dd+HH:mmZZ')) FROM nation");
        assertQuery("SELECT timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles'");
        assertQuery("SELECT ARRAY[timestamp '2018-02-06 23:00:00.000 Australia/Melbourne', null, timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles']");

        assertQuery("SELECT orderkey, year(from_unixtime(orderkey, '+01:00')), quarter(from_unixtime(orderkey, '-07:00')), month(from_unixtime(orderkey, '+00:00')), day(from_unixtime(orderkey, '-13:00')), day_of_week(from_unixtime(orderkey, '+03:00')), day_of_year(from_unixtime(orderkey, '-13:00')), year_of_week(from_unixtime(orderkey, '+14:00')), hour(from_unixtime(orderkey, '+01:00')), minute(from_unixtime(orderkey, '+01:00')), second(from_unixtime(orderkey, '-07:00')), millisecond(from_unixtime(orderkey, '+03:00')) FROM orders");
        assertQuery("SELECT orderkey, date_trunc('year', from_unixtime(orderkey, '-03:00')), date_trunc('quarter', from_unixtime(orderkey, '+14:00')), date_trunc('month', from_unixtime(orderkey, '+03:00')), date_trunc('day', from_unixtime(orderkey, '-07:00')), date_trunc('hour', from_unixtime(orderkey, '-09:30')), date_trunc('minute', from_unixtime(orderkey, '+05:30')), date_trunc('second', from_unixtime(orderkey, '+00:00')) FROM orders");

        assertQuery("SELECT timezone_hour(from_unixtime(orderkey, 'Asia/Oral')) FROM orders");
        assertQuery("SELECT timezone_minute(from_unixtime(orderkey, 'Asia/Kolkata')) FROM orders");
    }

    @Test
    public void testTimestampFunctions()
    {
        assertQuery("SELECT orderkey, from_unixtime(if (orderkey % 2 = 0, infinity(), orderkey)) FROM orders");
        assertQuery("SELECT orderkey, to_unixtime(from_unixtime(if (orderkey % 2 = 0, -infinity(), orderkey))) FROM orders");
        assertQuery("SELECT orderkey, from_unixtime(if (orderkey % 2 = 0, nan(), orderkey)) FROM orders");
        assertQuery("SELECT orderkey, from_unixtime(if (orderkey % 2 = 0, 3.87111e+37, orderkey)) FROM orders");

        assertQuery("SELECT orderkey, hour(from_unixtime(orderkey)), minute(from_unixtime(orderkey)), second(from_unixtime(orderkey)), millisecond(from_unixtime(orderkey)) FROM orders");
        assertQuery("SELECT orderkey, year(from_unixtime(orderkey)), quarter(from_unixtime(orderkey)), month(from_unixtime(orderkey)), day(from_unixtime(orderkey)) FROM orders");
        assertQuery("SELECT orderkey, day_of_week(from_unixtime(orderkey)), day_of_month(from_unixtime(orderkey)), day_of_year(from_unixtime(orderkey)), year_of_week(from_unixtime(orderkey)) FROM orders");
        assertQuery("SELECT orderkey, date_trunc('year', from_unixtime(orderkey)), date_trunc('quarter', from_unixtime(orderkey)), date_trunc('month', from_unixtime(orderkey)), date_trunc('day', from_unixtime(orderkey)), date_trunc('hour', from_unixtime(orderkey)), date_trunc('minute', from_unixtime(orderkey)), date_trunc('second', from_unixtime(orderkey)) FROM orders");
    }

    @Test
    public void testPrestoBenchTables()
    {
        assertQuery("SELECT name from prestobench_nation");
        assertQuery("SELECT partkey from prestobench_part");
        assertQuery("SELECT custkey from prestobench_customer");
        assertQuery("SELECT custkey from prestobench_orders");
    }

    @Test
    public void testGreatestLeast()
    {
        assertQuery("SELECT greatest(linenumber, suppkey, partkey) from lineitem");
        assertQuery("SELECT least(shipdate, commitdate) from lineitem");
    }

    @Test
    public void testSign()
    {
        assertQuery("SELECT sign(totalprice) from orders");
        assertQuery("SELECT sign(-totalprice) from orders");
        assertQuery("SELECT sign(custkey) from orders");
        assertQuery("SELECT sign(-custkey) from orders");
        assertQuery("SELECT sign(shippriority) from orders");
    }

    @Test
    public void testPad()
    {
        assertQuery("SELECT lpad(name, nationkey % 100, comment) from nation");
        assertQuery("SELECT rpad(name, nationkey % 100, comment) from nation");
    }

    @Test
    public void testRow()
    {
        assertQuery("SELECT cast(row(nationkey, regionkey) as row(a bigint, b bigint)) FROM nation");
        assertQuery("SELECT row(name, null, cast(row(nationkey, regionkey) as row(a bigint, b bigint))) FROM nation");
    }

    @Test(groups = {"parquet"})
    public void testDecimalRangeFilters()
    {
        // Actual session is for the native query runner.
        // It is required to have "parquet_pushdown_filter_enabled" enabled, that is the only supported mode.
        Session currentSession = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "parquet_pushdown_filter_enabled", "true")
                .build();

        // Expected session is for the Java query runner.
        // The Java runner does not support Parquet filter pushdown yet, so we have to explicitly disable it.
        Session expectedSession = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "parquet_pushdown_filter_enabled", "false")
                .build();

        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();

        String shortDecimalMin = "DECIMAL '-999999999999999999'";
        String shortDecimalMax = "DECIMAL '999999999999999999'";
        // Cannot convert from DECIMAL(38,0) to DECIMAL(38,2) so we keep the max values as DECIMAL(38,2).
        String longDecimalMin = "DECIMAL '-999999999999999999999999999999999999.99'";
        String longDecimalMax = "DECIMAL '999999999999999999999999999999999999.99'";

        try {
            // Create a Parquet table with decimal types and test data.
            getExpectedQueryRunner().execute(expectedSession, String.format("CREATE TABLE %s (c0 DECIMAL(15,2), c1 DECIMAL(38,2)) WITH (format = 'PARQUET')", tmpTableName), ImmutableList.of());
            getExpectedQueryRunner().execute(expectedSession, String.format("INSERT INTO %s VALUES (DECIMAL '0', DECIMAL '0'), (DECIMAL '1.2', DECIMAL '3.4'), (DECIMAL '1000000.12', DECIMAL '28239823232323.57'), (DECIMAL '-542392.89', DECIMAL '-6723982392109.29')", tmpTableName), ImmutableList.of());

            String[] queries = {
                    String.format("SELECT * FROM %s WHERE c0 > DECIMAL '1.1' and c1 < DECIMAL '5.2'", tmpTableName),
                    String.format("SELECT * FROM %s WHERE c0 >= DECIMAL '1.2' and c1 <= DECIMAL '5.2'", tmpTableName),
                    String.format("SELECT * FROM %s WHERE c0 = DECIMAL '1.2' and c1 = DECIMAL '3.4'", tmpTableName),
                    String.format("SELECT * FROM %s WHERE c0 > DECIMAL '-542392.89' and c1 <= DECIMAL '28239823232323.57'", tmpTableName),
                    String.format("SELECT * FROM %s WHERE c0 >= DECIMAL '1.2'", tmpTableName),
                    String.format("SELECT * FROM %s WHERE c1 < DECIMAL '5.2'", tmpTableName),
                    String.format("SELECT * FROM %s WHERE c1 <= DECIMAL '3.4'", tmpTableName),
                    String.format("SELECT * FROM %s WHERE c0 = DECIMAL '1.2'", tmpTableName),
                    String.format("SELECT * FROM %s WHERE c1 = DECIMAL '3.4'", tmpTableName),

                    // Test short decimal min/max values.
                    String.format("SELECT * FROM %s WHERE c0 > " + shortDecimalMin, tmpTableName),
                    String.format("SELECT * FROM %s WHERE c0 < " + shortDecimalMax, tmpTableName),
                    String.format("SELECT * FROM %s WHERE c0 >= " + shortDecimalMin, tmpTableName),
                    String.format("SELECT * FROM %s WHERE c0 <= " + shortDecimalMax, tmpTableName),

                    // Test long decimal min/max values.
                    String.format("SELECT * FROM %s WHERE c1 > " + longDecimalMin, tmpTableName),
                    String.format("SELECT * FROM %s WHERE c1 < " + longDecimalMax, tmpTableName),
                    String.format("SELECT * FROM %s WHERE c1 >= " + longDecimalMin, tmpTableName),
                    String.format("SELECT * FROM %s WHERE c1 <= " + longDecimalMax, tmpTableName)
            };

            for (String query : queries) {
                assertQuery(currentSession, query, expectedSession, query);
            }
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testLambda()
    {
        assertQuery("select transform(x, i->i*y) from (select x, y*y as y from (values row(array[1], 2)) t(x, y))");

        // test nested lambda
        assertQuery("select transform(transform(x, i->i*z), i->i*y) from (select x, y*y as y, z*z as z from (values row(array[1], 2, 3)) t(x, y, z))");
        assertQuery("select transform(x, i->transform(i, j->j*y)) from (select x, y*y as y from (values row(array[array[1]], 2)) t(x, y))");
    }

    @Test
    public void testMergeEmptyHll()
    {
        assertQuery("select cardinality(merge(empty_approx_set())) from orders");
        assertQuery("select cardinality(merge(empty_approx_set(0.1))) from orders");
    }

    @Test
    public void testDereference()
    {
        assertQuery("SELECT transform(array[row(orderkey, comment)], x -> x[2]) FROM orders");
        assertQuery("SELECT transform(array[row(orderkey, orderkey * 10)], x -> x[2]) FROM orders");
        assertQuery("SELECT r[2] FROM (VALUES (ROW (ROW (1, 'a', true)))) AS v(r)");
    }

    private void assertQueryResultCount(String sql, int expectedResultCount)
    {
        assertEquals(getQueryRunner().execute(sql).getRowCount(), expectedResultCount);
    }

    private void assertQueryResultCount(Session session, String sql, int expectedResultCount)
    {
        assertEquals(getQueryRunner().execute(session, sql).getRowCount(), expectedResultCount);
    }
}
