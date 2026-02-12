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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import io.airlift.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.ITERATIVE_OPTIMIZER_TIMEOUT;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZER_USE_HISTOGRAMS;
import static com.facebook.presto.SystemSessionProperties.REDISTRIBUTE_WRITES;
import static com.facebook.presto.SystemSessionProperties.SHARDED_JOINS_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestOracleDistributedQueries
        extends AbstractTestDistributedQueries
{
    private final OracleServerTester oracleServer;
    private final QueryRunner queryRunner;

    protected TestOracleDistributedQueries()
            throws Exception
    {
        this.oracleServer = new OracleServerTester();
        this.queryRunner = createOracleQueryRunner(oracleServer, TpchTable.getTables());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean isLegacyTimestampEnabled()
    {
        return false;
    }

    @Test
    @Override
    public void testLargeIn()
    {
        int numberOfElements = 1000;
        String longValues = range(0, numberOfElements)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");

        String arrayValues = range(0, numberOfElements)
                .mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + 2))
                .collect(joining(", "));
        assertQuery("SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery("SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    @Test
    @Override
    public void testCreateTable()
    {
        assertUpdate("CREATE TABLE test_create (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create"));
        assertTableColumnNames("test_create", "a", "b", "c");

        assertUpdate("DROP TABLE test_create");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        assertQueryFails("CREATE TABLE test_create (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        // Replace test_create_table_if_not_exists with test_create_table_if_not_exist to fetch max size naming on oracle
        assertUpdate("CREATE TABLE test_create_table_if_not_exist (a bigint, b varchar, c double)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exist"));
        assertTableColumnNames("test_create_table_if_not_exist", "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_create_table_if_not_exist (d bigint, e varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exist"));
        assertTableColumnNames("test_create_table_if_not_exist", "a", "b", "c");

        assertUpdate("DROP TABLE test_create_table_if_not_exist");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exist"));

        // Test CREATE TABLE LIKE
        assertUpdate("CREATE TABLE test_create_original (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_original"));
        assertTableColumnNames("test_create_original", "a", "b", "c");

        assertUpdate("CREATE TABLE test_create_like (LIKE test_create_original, d boolean, e varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_like"));
        assertTableColumnNames("test_create_like", "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE test_create_original");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_original"));

        assertUpdate("DROP TABLE test_create_like");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_like"));
    }

    @Test
    @Override
    public void testSymbolAliasing()
    {
        // Replace tablename to less than 30chars, max size naming on oracle
        String tableName = "symbol_aliasing" + System.currentTimeMillis();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 foo_1, 2 foo_2_4", 1);
        assertQuery("SELECT foo_1, foo_2_4 FROM " + tableName, "SELECT 1, 2");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testRenameColumn()
    {
        // Replace tablename to less than 30chars, max size naming on oracle
        String tableName = "test_renamecol_" + System.currentTimeMillis();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 'some value' x", 1);

        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO y");
        assertQuery("SELECT y FROM " + tableName, "VALUES 'some value'");

        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN y TO Z"); // 'Z' is upper-case, not delimited
        assertQuery(
                "SELECT z FROM " + tableName, // 'z' is lower-case, not delimited
                "VALUES 'some value'");

        // There should be exactly one column
        assertQuery("SELECT * FROM " + tableName, "VALUES 'some value'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testShowColumns(@Optional("PARQUET") String storageFormat)
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        ///Added Literal 'L' suffix to match expected values.
        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", 19L, null, null)
                .row("custkey", "bigint", "", "", 19L, null, null)
                .row("orderstatus", "varchar(1)", "", "", null, null, 1L)
                .row("totalprice", "double", "", "", 53L, null, null)
                .row("orderdate", "timestamp", "", "", null, null, null)
                .row("orderpriority", "varchar(15)", "", "", null, null, 15L)
                .row("clerk", "varchar(15)", "", "", null, null, 15L)
                .row("shippriority", "bigint", "", "", 19L, null, null)
                .row("comment", "varchar(79)", "", "", null, null, 79L)
                .build();

        // Until we migrate all connectors to parametrized varchar we check two options
        assertEquals(actual, expectedParametrizedVarchar, format("%s does not matches %s", actual, expectedParametrizedVarchar));
    }

    @Test
    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        //Catalog oracle only supports writes using autocommit
    }

    @Test
    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        //Catalog oracle only supports writes using autocommit
    }

    @Test
    @Override
    public void testDelete()
    {
        //This connector does not support deletes
    }

    @Test
    @Override
    public void testAddColumn()
    {
        ///Added Literal 'L' suffix to match expected values.
        assertUpdate("CREATE TABLE test_add_column AS SELECT 123 x", 1);
        assertUpdate("CREATE TABLE test_add_column_a AS SELECT 234 x, 111 a", 1);
        assertUpdate("CREATE TABLE test_add_column_ab AS SELECT 345 x, 222 a, 33.3E0 b", 1);
        assertUpdate("CREATE TABLE test_add_column_abc AS SELECT 456 x, 333 a, 66.6E0 b, 'fourth' c", 1);

        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN x bigint", ".* Column 'x' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN X bigint", ".* Column 'X' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN a bigint");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_a", 1);
        MaterializedResult materializedRows = computeActual("SELECT x, a FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123L);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234L);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b double");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_ab", 1);
        materializedRows = computeActual("SELECT x, a, b FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123L);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertNull(materializedRows.getMaterializedRows().get(0).getField(2));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234L);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);
        assertNull(materializedRows.getMaterializedRows().get(1).getField(2));
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(0), 345L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(1), 222L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(2), 33.3);

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("ALTER TABLE test_add_column ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_abc", 1);
        materializedRows = computeActual("SELECT x, a, b, c FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123L);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertNull(materializedRows.getMaterializedRows().get(0).getField(2));
        assertNull(materializedRows.getMaterializedRows().get(0).getField(3));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234L);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);
        assertNull(materializedRows.getMaterializedRows().get(1).getField(2));
        assertNull(materializedRows.getMaterializedRows().get(1).getField(3));
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(0), 345L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(1), 222L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(2), 33.3);
        assertNull(materializedRows.getMaterializedRows().get(2).getField(3));
        assertEquals(materializedRows.getMaterializedRows().get(3).getField(0), 456L);
        assertEquals(materializedRows.getMaterializedRows().get(3).getField(1), 333L);
        assertEquals(materializedRows.getMaterializedRows().get(3).getField(2), 66.6);
        assertEquals(materializedRows.getMaterializedRows().get(3).getField(3), "fourth");

        assertUpdate("DROP TABLE test_add_column");
        assertUpdate("DROP TABLE test_add_column_a");
        assertUpdate("DROP TABLE test_add_column_ab");
        assertUpdate("DROP TABLE test_add_column_abc");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_a"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_ab"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_abc"));

        assertUpdate("ALTER TABLE IF EXISTS test_add_column ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS test_add_column ADD COLUMN IF NOT EXISTS x bigint");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column"));
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }
    @Override
    public void testPayloadJoinApplicability()
    {
        // no op -- test not supported due to lack of support for map(integer, integer) column type.
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
        // no op -- test not supported due to lack of support for map(integer, integer) column type.
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        // no op -- test not supported due to lack of support for map(integer, integer) column type.
    }

    @Override
    public void testSubfieldAccessControl()
    {
        // no op -- test not supported due to lack of support for column type: row("f1" integer, "f2" integer, "f3" array(row("ff1" integer, "ff2" integer))).
    }

    @Override
    public void testRenameTable()
    {
        // Added Literal 'L' suffix to match expected values.
        assertUpdate("CREATE TABLE test_rename AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename RENAME TO test_rename_new");
        MaterializedResult materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertUpdate("ALTER TABLE IF EXISTS test_rename_new RENAME TO test_rename");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertUpdate("ALTER TABLE IF EXISTS test_rename RENAME TO test_rename_new");
        materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        // provide new table name in uppercase
        assertUpdate("ALTER TABLE test_rename_new RENAME TO TEST_RENAME");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertUpdate("DROP TABLE test_rename");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_new"));

        assertUpdate("ALTER TABLE IF EXISTS test_rename RENAME TO test_rename_new");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_new"));
    }

    @Override
    public void testInsert()
    {
        Session session = sessionWithLegacyTimestamp();
        @Language("SQL") String query = "SELECT orderdate, orderkey, totalprice FROM orders";

        assertUpdate(session, "CREATE TABLE test_insert AS " + query + " WITH NO DATA", 0);
        assertQuery(session, "SELECT count(*) FROM test_insert", "SELECT 0");

        assertUpdate(session, "INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertQuery(session, "SELECT * FROM test_insert", query);

        assertUpdate(session, "INSERT INTO test_insert (orderkey) VALUES (-1)", 1);
        assertUpdate(session, "INSERT INTO test_insert (orderkey) VALUES (null)", 1);
        assertUpdate(session, "INSERT INTO test_insert (orderdate) VALUES (DATE '2001-01-01')", 1);
        assertUpdate(session, "INSERT INTO test_insert (orderkey, orderdate) VALUES (-2, DATE '2001-01-02')", 1);
        assertUpdate(session, "INSERT INTO test_insert (orderdate, orderkey) VALUES (DATE '2001-01-03', -3)", 1);
        assertUpdate(session, "INSERT INTO test_insert (totalprice) VALUES (1234)", 1);

        assertQuery(session, "SELECT * FROM test_insert", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT null, null, null"
                + " UNION ALL SELECT DATE '2001-01-01', null, null"
                + " UNION ALL SELECT DATE '2001-01-02', -2, null"
                + " UNION ALL SELECT DATE '2001-01-03', -3, null"
                + " UNION ALL SELECT null, null, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(session,
                "INSERT INTO test_insert (orderkey, orderdate, totalprice) " +
                        "SELECT orderkey, orderdate, totalprice FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT 2 * count(*) FROM orders");

        assertUpdate(session, "DROP TABLE test_insert");

        // Refactored the code because of Unsupported column type: array(double)
        assertUpdate(session, "CREATE TABLE test_insert (a DOUBLE, b BIGINT)");

        assertUpdate(session, "INSERT INTO test_insert (a) VALUES (null)", 1);
        assertUpdate(session, "INSERT INTO test_insert (a) VALUES (1234)", 1);
        assertQuery(session, "SELECT a FROM test_insert", "VALUES (null), (1234)");

        assertQueryFails(session, "INSERT INTO test_insert (b) VALUES (1.23E1)", "line 1:37: Mismatch at column 1.*");

        assertUpdate(session, "DROP TABLE test_insert");
    }

    @Override
    public void testStringFilters()
    {
        // For CHAR(10), values are padded with spaces; shipmode = 'AIR' compares exact and returns 0 if trailing spaces exist.
        // Using TRIM(shipmode) = 'AIR' removes trailing/leading spaces, matching Oracle CHAR behavior and returning the correct count.
        assertUpdate("CREATE TABLE test_charn_filter (shipmode CHAR(10))");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_charn_filter"));
        assertTableColumnNames("test_charn_filter", "shipmode");
        assertUpdate("INSERT INTO test_charn_filter SELECT shipmode FROM lineitem", 60175);

        assertQuery("SELECT count(*) FROM test_charn_filter WHERE TRIM(shipmode) = 'AIR'", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE TRIM(shipmode) = 'AIR    '", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE TRIM(shipmode) = 'AIR       '", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE TRIM(shipmode) = 'AIR            '", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'NONEXIST'", "VALUES (0)");

        assertUpdate("CREATE TABLE test_varcharn_filter (shipmode VARCHAR(10))");
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
    public void testCreateTableAsSelect()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .setSystemProperty(REDISTRIBUTE_WRITES, "false")
                .build();

        assertUpdate("CREATE TABLE IF NOT EXISTS test_ctas AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames("test_ctas", "name", "regionkey");
        assertUpdate("DROP TABLE test_ctas");

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT orderkey, discount FROM lineitem", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(session,
                "test_select",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        assertCreateTableAsSelect(
                "test_join",
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "test_limit",
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");

       // Overriding the test : assertCreateTableAsSelect("test_unicode").
       //com.facebook.presto.spi.PrestoException: ORA-12899: value too large for column

        assertCreateTableAsSelect(session,
                "test_with_data",
                "SELECT * FROM orders WITH DATA",
                "SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(session,
                "test_with_no_data",
                "SELECT * FROM orders WITH NO DATA",
                "SELECT * FROM orders LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(session,
                "test_union_all",
                "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 0 UNION ALL " +
                        "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 1",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(session,
                "test_union_all",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertCreateTableAsSelect(session,
                "test_union_all",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE analyze_test AS SELECT orderstatus FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders");
        assertUpdate("DROP TABLE analyze_test");
    }

    @Override
    public void testInsertIntoNotNullColumn()
    {
        //When writing to Oracle, the value is stored as a DATE type, but while reading from Oracle to Presto, DATE includes both date and time components and hence read as a TIMESTAMP in Presto.
        //Link to the documentation : https://docs.oracle.com/en/database/oracle/oracle-database/26/nlspg/datetime-data-types-and-time-zone-support.html#GUID-4D95F6B2-8F28-458A-820D-6C05F848CA23
    }

    @Override
    public void testLargeInWithHistograms()
    {
        //Changing variable(longvalues) range to avoid ORA-01795: maximum number of expressions in a list is 1000.
        String longValues = range(0, 1000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        String query = "select orderpriority, sum(totalprice) from lineitem join orders on lineitem.orderkey = orders.orderkey where orders.orderkey in (" + longValues + ") group by 1";
        Session session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "30000ms")
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                .build();
        assertQuerySucceeds(session, query);
        session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "20000ms")
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "false")
                .build();
        assertQuerySucceeds(session, query);
    }

    @Override
    public void testShardedJoinOptimization()
    {
        Session defaultSession = getSession();
        Session defaultSessionLegacyTimestampDisabled = sessionWithLegacyTimestamp();

        Session session = Session.builder(defaultSession)
                .setSystemProperty(SHARDED_JOINS_STRATEGY, "ALWAYS")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();

        String[] queries = {
                "select * from lineitem l join orders o on (l.orderkey=o.orderkey)",
                "select * from lineitem l join orders o on (l.orderkey=o.orderkey) join part p on (l.partkey=p.partkey)",
                "select * from lineitem l LEFT JOIN orders o on (l.orderkey=o.orderkey)"
        };

        for (String query : queries) {
            MaterializedResult resultExplainQuery = computeActual(session, "EXPLAIN " + query);
            assert (((String) resultExplainQuery.getOnlyValue()).contains("random"));
            assertQuery(session, query);
        }

        String[] notSupportedQueries = {
                "select * from lineitem l right join orders o on (l.orderkey=o.orderkey)",
                "select * from lineitem l full join orders o on (l.orderkey=o.orderkey)"
        };

        for (String query : notSupportedQueries) {
            MaterializedResult resultExplainQuery = computeActual(session, "EXPLAIN " + query);
            assert (!((String) resultExplainQuery.getOnlyValue()).contains("random"));

            assertQueryWithSameQueryRunner(session, query, defaultSessionLegacyTimestampDisabled);
        }
    }
}
