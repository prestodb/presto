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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import io.airlift.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.security.SecureRandom;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClickHouseDistributedQueries
            extends AbstractTestDistributedQueries
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = new TestingClickHouseServer();
        return createClickHouseQueryRunner(clickhouseServer,
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (clickhouseServer != null) {
            clickhouseServer.close();
        }
    }

    @Test
    @Override
    public void testLargeIn()
    {
        String longValues = range(0, 1000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");

        String varcharValues = range(0, 1000)
                .mapToObj(i -> "'" + i + "'")
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) IN (" + varcharValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) NOT IN (" + varcharValues + ")");
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    public void testRenameColumn()
    {
        // ClickHouse need resets all data in a column for specified column which to be renamed
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDelete()
    {
        // ClickHouse need resets all data in a column for specified column which to be renamed
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Test
    @Override
    public void testInsert()
    {
        @Language("SQL") String query = "SELECT orderdate, orderkey, totalprice FROM orders";

        assertUpdate("CREATE TABLE test_insert AS " + query + " WITH NO DATA", 0);
        assertQuery("SELECT count(*) FROM test_insert", "SELECT 0");

        assertUpdate("INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM test_insert", query);

        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (-1)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (null)", 1);
        assertUpdate("INSERT INTO test_insert (orderdate) VALUES (DATE '2001-01-01')", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, orderdate) VALUES (-2, DATE '2001-01-02')", 1);
        assertUpdate("INSERT INTO test_insert (orderdate, orderkey) VALUES (DATE '2001-01-03', -3)", 1);
        assertUpdate("INSERT INTO test_insert (totalprice) VALUES (1234)", 1);

        assertQuery("SELECT * FROM test_insert", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT null, null, null"
                + " UNION ALL SELECT DATE '2001-01-01', null, null"
                + " UNION ALL SELECT DATE '2001-01-02', -2, null"
                + " UNION ALL SELECT DATE '2001-01-03', -3, null"
                + " UNION ALL SELECT null, null, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO test_insert (orderkey, orderdate, totalprice) " +
                        "SELECT orderkey, orderdate, totalprice FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT 2 * count(*) FROM orders");

        assertUpdate("DROP TABLE test_insert");

        assertUpdate("CREATE TABLE test_insert (a DOUBLE, b BIGINT)");

        assertUpdate("INSERT INTO test_insert (a) VALUES (null)", 1);
        assertUpdate("INSERT INTO test_insert (a) VALUES (1234)", 1);
        assertQuery("SELECT a FROM test_insert", "VALUES (null), (1234)");

        assertQueryFails("INSERT INTO test_insert (b) VALUES (1.23E1)", "line 1:37: Mismatch at column 1.*");

        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        skipTestUnless(supportsNotNullColumns());

        String catalog = getSession().getCatalog().get();
        String createTableFormat = "CREATE TABLE %s.tpch.test_not_null_with_insert (\n" +
                "   %s date,\n" +
                "   %s date NOT NULL,\n" +
                "   %s bigint NOT NULL\n" +
                ")";
        @Language("SQL") String createTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                "column_a",
                "column_b",
                "column_c");
        @Language("SQL") String expectedCreateTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                "\"column_a\"",
                "\"column_b\"",
                "\"column_c\"");
        assertUpdate(createTableSql);
        assertEquals(computeScalar("SHOW CREATE TABLE test_not_null_with_insert"), expectedCreateTableSql);

        assertQueryFails("INSERT INTO test_not_null_with_insert (column_a) VALUES (date '2012-12-31')", "(?s).*NULL.*column_b.*");
        assertQueryFails("INSERT INTO test_not_null_with_insert (column_a, column_b) VALUES (date '2012-12-31', null)", "(?s).*NULL.*column_b.*");

        assertQueryFails("INSERT INTO test_not_null_with_insert (column_b) VALUES (date '2012-12-31')", "(?s).*NULL.*column_c.*");
        assertQueryFails("INSERT INTO test_not_null_with_insert (column_b, column_c) VALUES (date '2012-12-31', null)", "(?s).*NULL.*column_c.*");

        assertUpdate("INSERT INTO test_not_null_with_insert (column_b, column_c) VALUES (date '2012-12-31', 1)", 1);
        assertUpdate("INSERT INTO test_not_null_with_insert (column_a, column_b, column_c) VALUES (date '2013-01-01', date '2013-01-02', 2)", 1);
        assertQuery(
                "SELECT * FROM test_not_null_with_insert",
                "VALUES ( NULL, CAST ('2012-12-31' AS DATE), 1 ), ( CAST ('2013-01-01' AS DATE), CAST ('2013-01-02' AS DATE), 2 );");

        assertUpdate("DROP TABLE test_not_null_with_insert");
    }

    @Test
    public void testInsertAndSelectFromDateTimeTables()
    {
        // ----- Table T - No milliseconds -----
        ZonedDateTime originalTimestamp = ZonedDateTime.parse("2025-01-08T12:34:56Z", ISO_ZONED_DATE_TIME);
        // the test session is Pacific/Apia
        ZonedDateTime adjustedTimestamp = originalTimestamp.withZoneSameInstant(
                ZoneId.of(DEFAULT_TIME_ZONE_KEY.getId()));

        // Pacific/Apia becomes 2025-01-09 01:34:56
        String adjustedTimestampString = adjustedTimestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        assertUpdate("CREATE TABLE t (ts timestamp not null)");
        assertUpdate("INSERT INTO t (ts) VALUES (timestamp '" + adjustedTimestampString + "')", 1);
        assertQuery(
                "SELECT * FROM t LIMIT 100",
                "VALUES (timestamp  '" + adjustedTimestampString + "')");
        assertUpdate("DROP TABLE IF EXISTS t");
        // ----- End of Table T - No milliseconds -----

        // ----- Table T1 - 1 digit of milliseconds -----
        originalTimestamp = ZonedDateTime.parse("2025-01-08T12:34:56.7Z", ISO_ZONED_DATE_TIME);
        // the test session is Pacific/Apia
        adjustedTimestamp = originalTimestamp.withZoneSameInstant(ZoneId.of(DEFAULT_TIME_ZONE_KEY.getId()));

        // Pacific/Apia becomes 2025-01-09 01:34:56.7
        adjustedTimestampString = adjustedTimestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));

        assertUpdate("CREATE TABLE t1 (ts timestamp not null)");
        assertUpdate("INSERT INTO t1 (ts) VALUES (timestamp '" + adjustedTimestampString + "')", 1);
        assertQuery(
                "SELECT * FROM t1 LIMIT 100",
                "VALUES (timestamp  '" + adjustedTimestampString + "')");
        assertUpdate("DROP TABLE IF EXISTS t1");
        // ----- End of Table T1 - 1 digit of milliseconds -----

        // ----- Table T2 - 2 digits of milliseconds -----
        originalTimestamp = ZonedDateTime.parse("2025-01-08T12:34:56.75Z", ISO_ZONED_DATE_TIME);
        // the test session is Pacific/Apia
        adjustedTimestamp = originalTimestamp.withZoneSameInstant(ZoneId.of(DEFAULT_TIME_ZONE_KEY.getId()));

        // Pacific/Apia becomes 2025-01-09 01:34:56.75
        adjustedTimestampString = adjustedTimestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"));

        assertUpdate("CREATE TABLE t2 (ts timestamp not null)");
        assertUpdate("INSERT INTO t2 (ts) VALUES (timestamp '" + adjustedTimestampString + "')", 1);
        assertQuery(
                "SELECT * FROM t2 LIMIT 100",
                "VALUES (timestamp  '" + adjustedTimestampString + "')");
        assertUpdate("DROP TABLE IF EXISTS t2");
        // ----- End of Table T2 - 2 digits of milliseconds -----

        // ----- Table T3 - 3 digits of milliseconds -----
        originalTimestamp = ZonedDateTime.parse("2025-01-08T12:34:56.759Z", ISO_ZONED_DATE_TIME);
        // the test session is Pacific/Apia
        adjustedTimestamp = originalTimestamp.withZoneSameInstant(ZoneId.of(DEFAULT_TIME_ZONE_KEY.getId()));

        // Pacific/Apia becomes 2025-01-09 01:34:56.759
        adjustedTimestampString = adjustedTimestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

        assertUpdate("CREATE TABLE t3 (ts timestamp not null)");
        assertUpdate("INSERT INTO t3 (ts) VALUES (timestamp '" + adjustedTimestampString + "')", 1);
        assertQuery(
                "SELECT * FROM t3 LIMIT 100",
                "VALUES (timestamp  '" + adjustedTimestampString + "')");
        assertUpdate("DROP TABLE IF EXISTS t3");
        // ----- End of Table T3 - 3 digits of milliseconds -----
    }

    @Override
    public void testDropColumn()
    {
        String tableName = "test_drop_column_" + randomTableSuffix();

        // only MergeTree engine table can drop column
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int) WITH (engine = 'MergeTree', order_by = ARRAY['x'])");
        assertUpdate("INSERT INTO " + tableName + "(x,y,a) SELECT 123, 456, 111", 1);

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");

        assertUpdate("DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // the columns are referenced by order_by/order_by property can not be dropped
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int NOT NULL) WITH " +
                "(engine = 'MergeTree', order_by = ARRAY['x'], partition_by = ARRAY['a'])");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN x", "(?s).* Missing columns: 'x' while processing query: 'x', required columns: 'x' 'x'.*\\n");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", "(?s).* Missing columns: 'a' while processing query: 'a', required columns: 'a' 'a'.*\\n");
    }

    @Override
    public void testAddColumn()
    {
        String tableName = "test_add_column_" + randomTableSuffix();
        // Only MergeTree engine table can add column
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertUpdate("INSERT INTO " + tableName + " (id, x) VALUES(1, 'first')", 1);

        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN X bigint", ".* Column 'X' already exists");
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN a varchar");
        assertUpdate("INSERT INTO " + tableName + " SELECT 2, 'second', 'xxx'", 1);
        assertQuery(
                "SELECT x, a FROM " + tableName,
                "VALUES ('first', NULL), ('second', 'xxx')");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b double");
        assertUpdate("INSERT INTO " + tableName + " SELECT 3, 'third', 'yyy', 33.3E0", 1);
        assertQuery(
                "SELECT x, a, b FROM " + tableName,
                "VALUES ('first', NULL, NULL), ('second', 'xxx', NULL), ('third', 'yyy', 33.3)");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("INSERT INTO " + tableName + " SELECT 4, 'fourth', 'zzz', 55.3E0, 'newColumn'", 1);
        assertQuery(
                "SELECT x, a, b, c FROM " + tableName,
                "VALUES ('first', NULL, NULL, NULL), ('second', 'xxx', NULL, NULL), ('third', 'yyy', 33.3, NULL), ('fourth', 'zzz', 55.3, 'newColumn')");
        assertUpdate("DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN IF NOT EXISTS x bigint");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE clickhouse.tpch.orders (\n" +
                        "   \"orderkey\" bigint,\n" +
                        "   \"custkey\" bigint,\n" +
                        "   \"orderstatus\" varchar,\n" +
                        "   \"totalprice\" double,\n" +
                        "   \"orderdate\" date,\n" +
                        "   \"orderpriority\" varchar,\n" +
                        "   \"clerk\" varchar,\n" +
                        "   \"shippriority\" integer,\n" +
                        "   \"comment\" varchar\n" +
                        ")");
    }

    @Override
    public void testDescribeOutput()
    {
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", 19L, null, null)
                .row("custkey", "bigint", "", "", 19L, null, null)
                .row("orderstatus", "varchar", "", "", null, null, 2147483647L)
                .row("totalprice", "double", "", "", 53L, null, null)
                .row("orderdate", "date", "", "", null, null, null)
                .row("orderpriority", "varchar", "", "", null, null, 2147483647L)
                .row("clerk", "varchar", "", "", null, null, 2147483647L)
                .row("shippriority", "integer", "", "", 10L, null, null)
                .row("comment", "varchar", "", "", null, null, 2147483647L)
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testDifferentEngine()
    {
        String tableName = "test_add_column_" + randomTableSuffix();
        // MergeTree
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'mergetree', order_by = ARRAY['id'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);
        // MergeTree without order by
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree')", "The property of order_by is required for table engine MergeTree\\(\\)");

        // MergeTree with optional
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR, logdate DATE NOT NULL) WITH " +
                "(engine = 'MergeTree', order_by = ARRAY['id'], partition_by = ARRAY['toYYYYMM(logdate)'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        //Log families
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'log')");
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'tinylog')");
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'stripelog')");
        assertUpdate("DROP TABLE " + tableName);

        //NOT support engine
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'bad_engine')", "Unable to set table property 'engine' to.*");
    }

    /**
     * test clickhouse table properties
     * <p>
     * Because the current connector does not support the `show create table` statement to display all the table properties,
     * so we cannot use this statement to test whether the properties of the created table meet our expectations,
     * and we will modify the test case after the `show create table` is full supported
     */
    @Test
    public void testTableProperty()
    {
        String tableName = "test_add_column_" + randomTableSuffix();
        // no table property, it should create a table with default Log engine table
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        // one required property
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log')");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'StripeLog')");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'TinyLog')");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Log engine DOES NOT any property
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', order_by=ARRAY['id'])", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', partition_by=ARRAY['id'])", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', sample_by='id')", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n");

        // optional properties
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        // the column refers by order by must be not null
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x'])", ".*Sorting key contains nullable columns, but merge tree setting `allow_nullable_key` is disabled.*\\n.*");

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = ARRAY['id'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x', 'y'], primary_key = ARRAY['id', 'x'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x boolean NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x'], primary_key = ARRAY['id','x'], sample_by = 'x' )");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Primary key must be a prefix of the sorting key,
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], sample_by = ARRAY['x', 'y'])",
                "Invalid value for table property 'sample_by': .*");

        // wrong property type
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = 'id')",
                "Invalid value for table property 'order_by': .*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = 'id')",
                "Invalid value for table property 'primary_key': .*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = ARRAY['id'], partition_by = 'id')",
                "Invalid value for table property 'partition_by': .*");
    }

    @Override
    public void testStringFilters()
    {
        assertUpdate("CREATE TABLE test_varcharn_filter (shipmode VARCHAR(85))");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_varcharn_filter"));
        assertTableColumnNames("test_varcharn_filter", "shipmode");
        assertUpdate("INSERT INTO test_varcharn_filter SELECT shipmode FROM lineitem", 60175);

        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR'", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR    '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR       '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR            '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'NONEXIST'", "VALUES (0)");
    }

    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // not supported
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // not supported
    }

    @Override
    public void testPayloadJoinApplicability()
    {
        // not supported
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
        // test not supported
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        // test not supported
    }

    @Override
    public void testSubfieldAccessControl()
    {
        // test not supported
    }

    @Override
    public void testPreProcessMetastoreCalls()
    {
        //not supported
    }

    @Override
    public void testCorrelatedExistsSubqueries()
    {
        //not supported
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregation()
    {
        //not supported
    }

    private static String randomTableSuffix()
    {
        SecureRandom random = new SecureRandom();
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(5, randomSuffix.length()));
    }
    protected static final class DataMappingTestSetup
    {
        private final String trinoTypeName;
        private final String sampleValueLiteral;
        private final String highValueLiteral;

        private final boolean unsupportedType;

        public DataMappingTestSetup(String trinoTypeName, String sampleValueLiteral, String highValueLiteral)
        {
            this(trinoTypeName, sampleValueLiteral, highValueLiteral, false);
        }

        private DataMappingTestSetup(String trinoTypeName, String sampleValueLiteral, String highValueLiteral, boolean unsupportedType)
        {
            this.trinoTypeName = requireNonNull(trinoTypeName, "trinoTypeName is null");
            this.sampleValueLiteral = requireNonNull(sampleValueLiteral, "sampleValueLiteral is null");
            this.highValueLiteral = requireNonNull(highValueLiteral, "highValueLiteral is null");
            this.unsupportedType = unsupportedType;
        }

        public String getTrinoTypeName()
        {
            return trinoTypeName;
        }

        public String getSampleValueLiteral()
        {
            return sampleValueLiteral;
        }

        public String getHighValueLiteral()
        {
            return highValueLiteral;
        }

        public boolean isUnsupportedType()
        {
            return unsupportedType;
        }

        public DataMappingTestSetup asUnsupported()
        {
            return new DataMappingTestSetup(
                    trinoTypeName,
                    sampleValueLiteral,
                    highValueLiteral,
                    true);
        }

        @Override
        public String toString()
        {
            // toString is brief because it's used for test case labels in IDE
            return trinoTypeName + (unsupportedType ? "!" : "");
        }
    }
}
