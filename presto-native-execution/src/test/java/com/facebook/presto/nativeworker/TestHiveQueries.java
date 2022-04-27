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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

abstract class TestHiveQueries
        extends AbstractTestHiveQueries
{
    protected TestHiveQueries(boolean useThrift)
    {
        super(useThrift);
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
        assertQuery("SELECT nationkey * 10, nationkey % 5, -nationkey, nationkey / 3 FROM nation");
        assertQuery("SELECT *, nationkey / 3 FROM nation");
        assertQuery("SELECT nationkey IS NULL FROM nation");

        assertQuery("SELECT rand() < 1, random() < 1 FROM nation", "SELECT true, true FROM nation");

        assertQuery("SELECT * FROM lineitem");
        assertQuery("SELECT ceil(discount), ceiling(discount), floor(discount), abs(discount) FROM lineitem");
        assertQuery("SELECT linenumber IN (2, 4, 6) FROM lineitem");

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
        assertQuery("SELECT * FROM lineitem WHERE is_open");
        assertQuery("SELECT * FROM lineitem WHERE is_open=false");
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

        assertQuery("SELECT BIGINT '12345', INTEGER '1234', SMALLINT '123', TINYINT '12', TRUE, FALSE, DOUBLE '1.234', REAL '1.23', 'ABC', 'Somewhat longish string', NULL, array[1, 2, 3]");
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
    public void testStringFunctions()
    {
        // Substr, length, trim.
        assertQuery("SELECT substr(comment, 1, 10), length(comment), trim(comment) FROM orders");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), ltrim(comment) FROM orders");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), rtrim(comment) FROM orders");

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
        assertQuery("SELECT width_bucket(to_unixtime(ds), array[1609487900, 1619740800, 1622419200]) FROM customer_bucketed");
        assertQuery("SELECT width_bucket(to_unixtime(ds), array[1609487900.1, 1619740800.2, 1622419200.3]) FROM customer_bucketed");
    }

    @Test
    public void testPartitionedTable()
    {
        assertQuery("SELECT * from nation_partitioned", "SELECT nationkey, name, comment, cast(regionkey as VARCHAR) FROM nation");
        assertQuery("SELECT * from nation_partitioned WHERE regionkey='2'", "SELECT nationkey, name, comment, cast(regionkey as VARCHAR) FROM nation WHERE regionkey=2");
        assertQuery("SELECT * from nation_partitioned WHERE regionkey in ('2', '4')", "SELECT nationkey, name, comment, cast(regionkey as VARCHAR) FROM nation WHERE regionkey in (2, 4)");
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

    @Test
    public void testCreateTableAsSelect()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .build();

        getQueryRunner().execute(session, "CREATE TABLE tmp AS SELECT * FROM nation");
        assertQuery("SELECT * FROM tmp", "SELECT * FROM nation");
        dropTable("tmp");

        getQueryRunner().execute(session, "CREATE TABLE tmp AS SELECT linenumber, count(*) as cnt FROM lineitem GROUP BY 1");
        assertQuery("SELECT * FROM tmp", "SELECT linenumber, count(*) FROM lineitem GROUP BY 1");
        dropTable("tmp");

        getQueryRunner().execute(session, "CREATE TABLE tmp AS SELECT orderkey, count(*) as cnt FROM lineitem GROUP BY 1");
        assertQuery("SELECT * FROM tmp", "SELECT orderkey, count(*) FROM lineitem GROUP BY 1");
        dropTable("tmp");
    }

    private void dropTable(String tableName)
    {
        // An ugly workaround for the lack of getExpectedQueryRunner()
        computeExpected("DROP TABLE IF EXISTS " + tableName, ImmutableList.of(BIGINT));
    }

    @Test
    public void testUnionAll()
    {
        assertQuery("SELECT distinct orderkey FROM (" +
                "SELECT orderkey FROM lineitem WHERE linenumber = 5 " +
                "UNION ALL SELECT orderkey FROM lineitem WHERE linenumber = 6)");
    }

    @Test
    public void testSubqueries()
    {
        // TODO(gaoge): Fix the broken test case
        // assertQuery("SELECT name FROM nation WHERE regionkey = (SELECT max(regionkey) FROM region)");

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
        assertQuery("select ARRAY[timestamp '2018-02-06 23:00:00.000 Australia/Melbourne', null, timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles']");
    }

    @Test
    public void testTimestampFunctions()
    {
        assertQuery("SELECT orderkey, from_unixtime(if (orderkey % 2 = 0, infinity(), orderkey)) FROM orders");
        assertQuery("SELECT orderkey, to_unixtime(from_unixtime(if (orderkey % 2 = 0, -infinity(), orderkey))) FROM orders");
        assertQuery("SELECT orderkey, from_unixtime(if (orderkey % 2 = 0, nan(), orderkey)) FROM orders");
        assertQuery("SELECT orderkey, from_unixtime(if (orderkey % 2 = 0, 3.87111e+37, orderkey)) FROM orders");

        assertQuery("SELECT orderkey, hour(from_unixtime(orderkey, '+01:00')) FROM orders");
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
    }

    private void assertQueryResultCount(String sql, int expectedResultCount)
    {
        assertEquals(getQueryRunner().execute(sql).getRowCount(), expectedResultCount);
    }
}
