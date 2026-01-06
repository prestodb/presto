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
import static com.facebook.presto.tests.QueryAssertions.assertContains;
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
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
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
    public void testCaseInsensitiveAliasedRelation()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "SELECT A.* FROM orders a");
    }

    @Override
    public void testInsertIntoNotNullColumn()
    {
        //When writing to Oracle, the value is stored as a DATE type, but while reading from Oracle to Presto, DATE includes both date and time components and hence read as a TIMESTAMP in Presto.
        //Link to the documentation : https://docs.oracle.com/en/database/oracle/oracle-database/26/nlspg/datetime-data-types-and-time-zone-support.html#GUID-4D95F6B2-8F28-458A-820D-6C05F848CA23
    }

    @Override
    public void testWithAliased()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                        .build();
        assertQuery(session, "WITH a AS (SELECT * FROM orders) SELECT * FROM a x", "SELECT * FROM orders");
    }

    @Override
    public void testWith()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "" +
                        "WITH a AS (SELECT * FROM orders) " +
                        "SELECT * FROM a",
                "SELECT * FROM orders");
        assertQuerySucceeds("WITH t(x, y, z) AS (TABLE region) SELECT * FROM t");
    }

    @Override
    public void testWildcard()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "SELECT * FROM orders");
    }
    @Override
    public void testUnionWithFilterNotInSelect()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "SELECT orderkey, orderdate FROM orders WHERE custkey < 1000 UNION ALL SELECT orderkey, shipdate FROM lineitem WHERE linenumber < 2000");
        assertQuery(session, "SELECT orderkey, orderdate FROM orders UNION ALL SELECT orderkey, shipdate FROM lineitem WHERE linenumber < 2000");
        assertQuery(session, "SELECT orderkey, orderdate FROM orders WHERE custkey < 1000 UNION ALL SELECT orderkey, shipdate FROM lineitem");
    }

    @Override
    public void testUnionWithAggregation()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(
                "SELECT regionkey, count(*) FROM (" +
                        "   SELECT regionkey FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM (VALUES 2, 100) t(regionkey)) " +
                        "GROUP BY regionkey",
                "SELECT * FROM (VALUES  (0, 5), (1, 5), (2, 6), (3, 5), (4, 5), (100, 1))");

        assertQuery(session,
                "SELECT ds, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY ds");
        assertQuery(session,
                "SELECT ds, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY ds");
        assertQuery(session,
                "SELECT ds, count(DISTINCT orderkey) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY ds");
        assertQuery(
                "SELECT clerk, count(DISTINCT orderstatus) FROM (" +
                        "SELECT * FROM orders WHERE orderkey=0 " +
                        " UNION ALL " +
                        "SELECT * FROM orders WHERE orderkey<>0) " +
                        "GROUP BY clerk");
        assertQuery(
                "SELECT count(clerk) FROM (" +
                        "SELECT clerk FROM orders WHERE orderkey=0 " +
                        " UNION ALL " +
                        "SELECT clerk FROM orders WHERE orderkey<>0) " +
                        "GROUP BY clerk");
        assertQuery(
                "SELECT count(orderkey), sum(sc) FROM (" +
                        "    SELECT sum(custkey) sc, orderkey FROM (" +
                        "        SELECT custkey,orderkey, orderkey+1 FROM orders WHERE orderkey=0" +
                        "        UNION ALL " +
                        "        SELECT custkey,orderkey,orderkey+1 FROM orders WHERE orderkey<>0) " +
                        "    GROUP BY orderkey)");

        assertQuery(
                "SELECT count(orderkey), sum(sc) FROM (\n" +
                        "    SELECT sum(custkey) sc, orderkey FROM (\n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey=0\n" +
                        "        UNION ALL \n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey<>0) \n" +
                        "    GROUP BY GROUPING SETS ((orderkey, orderstatus), (orderkey)))",
                "SELECT count(orderkey), sum(sc) FROM (\n" +
                        "    SELECT sum(custkey) sc, orderkey FROM (\n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey=0\n" +
                        "        UNION ALL \n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey<>0) \n" +
                        "    GROUP BY orderkey, orderstatus \n" +
                        "    \n" +
                        "    UNION ALL \n" +
                        "    \n" +
                        "    SELECT sum(custkey) sc, orderkey FROM (\n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey=0\n" +
                        "        UNION ALL \n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey<>0) \n" +
                        "    GROUP BY orderkey)");
    }

    @Override
    public void testTableQueryOrderLimit()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQueryOrdered(session, "TABLE orders ORDER BY orderkey LIMIT 10", "SELECT * FROM orders ORDER BY orderkey LIMIT 10");
    }

    @Override
    public void testTableQueryInUnion()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "(SELECT * FROM orders ORDER BY orderkey LIMIT 10) UNION ALL TABLE orders", "(SELECT * FROM orders ORDER BY orderkey LIMIT 10) UNION ALL SELECT * FROM orders");
    }
    @Override
    public void testTableQuery()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "TABLE orders", "SELECT * FROM orders");
    }
    @Override
    public void testTableAsSubquery()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQueryOrdered(session, "(TABLE orders) ORDER BY orderkey", "(SELECT * FROM orders) ORDER BY orderkey");
    }
    @Override
    public void testRepeatedOutputs2()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.

        // this test exposed a bug that wasn't caught by other tests that resulted in the execution engine
        // trying to read orderkey as the second field, causing a type mismatch
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "SELECT orderdate, orderdate, orderkey FROM orders");
    }

    @Override
    public void testReferenceToWithQueryInFromClause()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session,
                "WITH a AS (SELECT * FROM orders)" +
                        "SELECT * FROM (" +
                        "   SELECT * FROM a" +
                        ")",
                "SELECT * FROM orders");
    }
    @Override
    public void testQualifiedWildcardFromAlias()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "SELECT T.* FROM orders T");
    }
    @Override
    public void testQualifiedWildcard()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "SELECT orders.* FROM orders");
    }
    @Override
    public void testMultipleWildcards()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "SELECT *, 123, * FROM orders");
    }
    @Override
    public void testMultiColumnUnionAll()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "SELECT * FROM orders UNION ALL SELECT * FROM orders");
    }
    @Override
    public void testMixedWildcards()
    {
        // Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery(session, "SELECT *, orders.*, orderkey FROM orders");
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
    public void testCorrelatedExistsSubqueries()
    {
        //Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        // projection
        assertQuery(
                "SELECT EXISTS(SELECT 1 FROM (VALUES 1, 1, 1, 2, 2, 3, 4) i(a) WHERE i.a < o.a AND i.a < 4) " +
                        "FROM (VALUES 0, 3, 3, 5) o(a)",
                "VALUES false, true, true, true");
        assertQuery(
                "SELECT EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) " +
                        "FROM lineitem l LIMIT 1");

        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 1000 = 0)",
                "VALUES 14999"); // h2 is slow
        assertQuery(
                "SELECT count(*) FROM lineitem l " +
                        "WHERE EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

        // order by
        assertQuery(
                "SELECT orderkey FROM orders o ORDER BY " +
                        "EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "LIMIT 1",
                "VALUES 60000"); // h2 is slow
        assertQuery(
                "SELECT orderkey FROM lineitem l ORDER BY " +
                        "EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

        // group by
        assertQuery(session,
                "SELECT max(o.orderdate), o.orderkey, " +
                        "EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) " +
                        "FROM orders o GROUP BY o.orderkey ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1, true)"); // h2 is slow
        assertQuery(session,
                "SELECT max(o.orderdate), o.orderkey " +
                        "FROM orders o " +
                        "GROUP BY o.orderkey " +
                        "HAVING EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1)"); // h2 is slow
        assertQuery(session,
                "SELECT max(o.orderdate), o.orderkey FROM orders o " +
                        "GROUP BY o.orderkey, EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1)"); // h2 is slow
        assertQuery(
                "SELECT max(l.quantity), l.orderkey, EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) FROM lineitem l " +
                        "GROUP BY l.orderkey");
        assertQuery(
                "SELECT max(l.quantity), l.orderkey FROM lineitem l " +
                        "GROUP BY l.orderkey " +
                        "HAVING EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");
        assertQuery(
                "SELECT max(l.quantity), l.orderkey FROM lineitem l " +
                        "GROUP BY l.orderkey, EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT EXISTS(SELECT 1 FROM orders i WHERE o1.orderkey < o2.orderkey AND i.orderkey % 10000 = 0)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT EXISTS(SELECT 1 FROM orders i WHERE o1.orderkey < o2.orderkey)", "(?s)line .*: Correlated subquery in given context is not supported.*");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)))",
                "VALUES 14999"); // h2 is slow
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 WHERE o.orderkey > 10 OR o.orderkey != 3)))",
                "VALUES 14999");
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere()
    {
        //Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery("SELECT EXISTS(SELECT 1 WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT EXISTS(SELECT null WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE EXISTS(SELECT 1 WHERE o.orderkey = 0)");
        assertQuery(session, "SELECT * FROM orders o ORDER BY EXISTS(SELECT 1 WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS (SELECT avg(l.orderkey) FROM lineitem l WHERE o.orderkey = l.orderkey)");
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS (SELECT avg(l.orderkey) FROM lineitem l WHERE o.orderkey = l.orderkey GROUP BY l.linenumber)");
        assertQueryFails(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS (SELECT count(*) FROM lineitem l WHERE o.orderkey = l.orderkey HAVING count(*) > 3)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // with duplicated rows
        assertQuery(
                "SELECT EXISTS(SELECT 1 WHERE a = 1) FROM (VALUES 1, 1, 2, 3) t(a)",
                "VALUES true, true, false, false");

        // group by
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey, EXISTS(SELECT 1 WHERE o.orderkey = 0) " +
                        "FROM orders o GROUP BY o.orderkey");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey " +
                        "FROM orders o GROUP BY o.orderkey HAVING EXISTS (SELECT 1 WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey " +
                        "FROM orders o GROUP BY o.orderkey, EXISTS (SELECT 1 WHERE o.orderkey = 0)");

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT EXISTS(SELECT 1 WHERE o1.orderkey = o2.orderkey)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT EXISTS(SELECT 1 WHERE o1.orderkey = o2.orderkey)",
                "(?s)line .*: Correlated subquery in given context is not supported.*");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 WHERE o.orderkey = 0)))",
                "SELECT count(*) FROM orders o WHERE o.orderkey = 0");

        // not exists
        assertQuery(
                "SELECT count(*) FROM customer WHERE NOT EXISTS(SELECT * FROM orders WHERE orders.custkey=customer.custkey)",
                "VALUES 500");
    }
    @Override
    public void testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols()
    {
        //Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery("SELECT EXISTS(SELECT o.orderkey) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE EXISTS(SELECT o.orderkey)");
        assertQuery(session, "SELECT * FROM orders o ORDER BY EXISTS(SELECT o.orderkey)");

        // group by
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey, EXISTS(SELECT o.orderkey) FROM orders o GROUP BY o.orderkey");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey " +
                        "FROM orders o GROUP BY o.orderkey HAVING EXISTS (SELECT o.orderkey)");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey FROM orders o GROUP BY o.orderkey, EXISTS (SELECT o.orderkey)");

        // join
        assertQuery(
                "SELECT * FROM orders o JOIN (SELECT * FROM lineitem ORDER BY orderkey LIMIT 2) l " +
                        "ON NOT EXISTS(SELECT o.orderkey = l.orderkey)");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o WHERE (SELECT * FROM (SELECT EXISTS(SELECT o.orderkey)))",
                "VALUES 15000");
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregation()
    {
        //Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        // projection
        assertQuery(
                "SELECT (SELECT round(3 * avg(i.a)) FROM (VALUES 1, 1, 1, 2, 2, 3, 4) i(a) WHERE i.a < o.a AND i.a < 4) " +
                        "FROM (VALUES 0, 3, 3, 5) o(a)",
                "VALUES null, 4, 4, 5");

        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE (SELECT avg(i.orderkey) FROM orders i " +
                        "WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) > 100",
                "VALUES 14999"); // h2 is slow

        // order by
        assertQuery(
                "SELECT orderkey FROM orders o " +
                        "ORDER BY " +
                        "   (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0), " +
                        "   orderkey " +
                        "LIMIT 1",
                "VALUES 1"); // h2 is slow

        // group by
        assertQuery(session,
                "SELECT max(o.orderdate), o.orderkey, " +
                        "(SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) " +
                        "FROM orders o GROUP BY o.orderkey ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1, 40000)"); // h2 is slow
        assertQuery(session,
                "SELECT max(o.orderdate), o.orderkey " +
                        "FROM orders o " +
                        "GROUP BY o.orderkey " +
                        "HAVING 40000 < (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-07-24', 20000)"); // h2 is slow
        assertQuery(session,
                "SELECT max(o.orderdate), o.orderkey FROM orders o " +
                        "GROUP BY o.orderkey, (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1)"); // h2 is slow

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT 1 = (SELECT avg(i.orderkey) FROM orders i WHERE o1.orderkey < o2.orderkey AND i.orderkey % 10000 = 0)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT 1 = (SELECT avg(i.orderkey) FROM orders i WHERE o1.orderkey < o2.orderkey)", "(?s)line .*: Correlated subquery in given context is not supported.*");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE 100 < (SELECT * " +
                        "FROM (SELECT (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)))",
                "VALUES 14999"); // h2 is slow

        // consecutive correlated subqueries with scalar aggregation
        assertQuery("SELECT " +
                "(SELECT avg(regionkey) " +
                " FROM nation n2" +
                " WHERE n2.nationkey = n1.nationkey)," +
                "(SELECT avg(regionkey)" +
                " FROM nation n3" +
                " WHERE n3.nationkey = n1.nationkey)" +
                "FROM nation n1");
        assertQuery("SELECT" +
                "(SELECT avg(regionkey)" +
                " FROM nation n2 " +
                " WHERE n2.nationkey = n1.nationkey)," +
                "(SELECT avg(regionkey)+1 " +
                " FROM nation n3 " +
                " WHERE n3.nationkey = n1.nationkey)" +
                "FROM nation n1");

        //count in subquery
        assertQuery("SELECT * " +
                        "FROM (VALUES (0),( 1), (2), (7)) AS v1(c1) " +
                        "WHERE v1.c1 > (SELECT count(c1) FROM (VALUES (0),( 1), (2)) AS v2(c1) WHERE v1.c1 = v2.c1)",
                "VALUES (2), (7)");
    }
    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere()
    {
        //Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery("SELECT (SELECT count(*) WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE 1 = (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery(session, "SELECT * FROM orders o ORDER BY (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT count(*) FROM nation n WHERE " +
                        "(SELECT count(*) FROM region r WHERE n.regionkey = r.regionkey) > 1");
        assertQueryFails(
                "SELECT count(*) FROM nation n WHERE " +
                        "(SELECT avg(a) FROM (SELECT count(*) FROM region r WHERE n.regionkey = r.regionkey) t(a)) > 1",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // with duplicated rows
        assertQuery(
                "SELECT (SELECT count(*) WHERE a = 1) FROM (VALUES 1, 1, 2, 3) t(a)",
                "VALUES true, true, false, false");

        // group by
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey, (SELECT count(*) WHERE o.orderkey = 0) " +
                        "FROM orders o GROUP BY o.orderkey");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey " +
                        "FROM orders o GROUP BY o.orderkey HAVING 1 = (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey FROM orders o " +
                        "GROUP BY o.orderkey, (SELECT count(*) WHERE o.orderkey = 0)");

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT 1 = (SELECT count(*) WHERE o1.orderkey = o2.orderkey)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT 1 = (SELECT count(*) WHERE o1.orderkey = o2.orderkey)", "(?s)line .*: Correlated subquery in given context is not supported.*");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE 1 = (SELECT * FROM (SELECT (SELECT count(*) WHERE o.orderkey = 0)))",
                "SELECT count(*) FROM orders o WHERE o.orderkey = 0");
    }
    @Override
    public void testScalarSubquery()
    {
        //Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // aggregation
        assertQuery(session, "SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT max(orderkey) FROM orders)");

        // no output
        assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT orderkey FROM orders WHERE 0=1)");

        // no output matching with null test
        assertQuery(session, "SELECT * FROM lineitem WHERE \n" +
                "(SELECT orderkey FROM orders WHERE 0=1) " +
                "is null");
        assertQuery("SELECT * FROM lineitem WHERE \n" +
                "(SELECT orderkey FROM orders WHERE 0=1) " +
                "is not null");

        // subquery results and in in-predicate
        assertQuery("SELECT (SELECT 1) IN (1, 2, 3)");
        assertQuery("SELECT (SELECT 1) IN (   2, 3)");

        // multiple subqueries
        assertQuery("SELECT (SELECT 1) = (SELECT 3)");
        assertQuery("SELECT (SELECT 1) < (SELECT 3)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "(SELECT min(orderkey) FROM orders)" +
                "<" +
                "(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT (SELECT 1), (SELECT 2), (SELECT 3)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                "WHERE orderkey BETWEEN" +
                "   (SELECT avg(orderkey) FROM orders) - 10 " +
                "   AND" +
                "   (SELECT avg(orderkey) FROM orders) + 10");

        // subqueries with joins
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM orders o1 " +
                "INNER JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 " +
                "LEFT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM orders o1 RIGHT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT DISTINCT COUNT(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 " +
                        "FULL JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                        "ON o1.orderkey " +
                        "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                        "GROUP BY o1.orderkey",
                "VALUES 1, 10");

        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY (SELECT 2)");

        // subquery returns multiple rows
        String multipleRowsErrorMsg = "(?s)Scalar sub-query has returned multiple rows.*";
        assertQueryFails("SELECT * FROM lineitem WHERE orderkey = (\n" +
                        "SELECT orderkey FROM orders ORDER BY totalprice)",
                multipleRowsErrorMsg);
        assertQueryFails("SELECT orderkey, totalprice FROM orders ORDER BY (VALUES 1, 2)",
                multipleRowsErrorMsg);

        // exposes a bug in optimize hash generation because EnforceSingleNode does not
        // support more than one column from the underlying query
        assertQuery("SELECT custkey, (SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 1) FROM orders");

        // cast scalar sub-query
        assertQuery("SELECT 1.0/(SELECT 1), CAST(1.0 AS REAL)/(SELECT 1), 1/(SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1) AND 1 = (SELECT 1), 2.0 = (SELECT 1) WHERE 1.0 = (SELECT 1) AND 1 = (SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1), 2.0 = (SELECT 1), CAST(2.0 AS REAL) = (SELECT 1) WHERE 1.0 = (SELECT 1)");

        // coerce correlated symbols
        assertQuery("SELECT * FROM (VALUES 1) t(a) WHERE 1=(SELECT count(*) WHERE 1.0 = a)", "SELECT 1");
        assertQuery("SELECT * FROM (VALUES 1.0) t(a) WHERE 1=(SELECT count(*) WHERE 1 = a)", "SELECT 1.0");
    }
    @Test
    public void testUnnest()
    {
        //Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a)", "SELECT 1");
        assertQuery("SELECT x[1] FROM UNNEST(ARRAY[ARRAY[1, 2, 3]]) t(x)", "SELECT 1");
        assertQuery("SELECT x[1][2] FROM UNNEST(ARRAY[ARRAY[ARRAY[1, 2, 3]]]) t(x)", "SELECT 2");
        assertQuery("SELECT x[2] FROM UNNEST(ARRAY[MAP(ARRAY[1,2], ARRAY['hello', 'hi'])]) t(x)", "SELECT 'hi'");
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3])", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3]) t(a)", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY[1, 2], ARRAY[3, 4]) t(a, b)", "SELECT * FROM VALUES (1, 3), (2, 4)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES (1, 4), (2, 5), (3, NULL)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES 1, 2, 3");
        assertQuery("SELECT b FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES 4, 5, NULL");
        assertQuery("SELECT count(*) FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5])", "SELECT 3");
        assertQuery("SELECT a FROM UNNEST(ARRAY['kittens', 'puppies']) t(a)", "SELECT * FROM VALUES ('kittens'), ('puppies')");
        assertQuery(
                "WITH unioned AS ( SELECT 1 UNION ALL SELECT 2 ) SELECT * FROM unioned CROSS JOIN UNNEST(ARRAY[3]) steps (step)",
                "SELECT * FROM (VALUES (1, 3), (2, 3))");
        assertQuery("" +
                        "SELECT c " +
                        "FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b) " +
                        "CROSS JOIN (values (8), (9)) t2(c)",
                "SELECT * FROM VALUES 8, 8, 8, 9, 9, 9");
        assertQuery("" +
                        "SELECT a.custkey, t.e " +
                        "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a " +
                        "CROSS JOIN UNNEST(my_array) t(e)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (1), (2), (3))");
        assertQuery("" +
                        "SELECT a.custkey, t.e " +
                        "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a, " +
                        "UNNEST(my_array) t(e)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (1), (2), (3))");
        assertQuery("SELECT * FROM UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 1, 1)");
        assertQuery("SELECT * FROM UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 1, 1)");
        assertQuery("SELECT a, b FROM UNNEST(MAP(ARRAY[1,2], ARRAY['cat', 'dog'])) t(a, b)", "SELECT * FROM VALUES (1, 'cat'), (2, 'dog')");
        assertQuery("SELECT a, b FROM UNNEST(MAP(ARRAY[1,2], ARRAY['cat', NULL])) t(a, b)", "SELECT * FROM VALUES (1, 'cat'), (2, NULL)");

        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a) WITH ORDINALITY", "SELECT 1");
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY", "SELECT * FROM VALUES (1, 1), (2, 2), (3, 3)");
        assertQuery("SELECT b FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY t(a, b)", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a, b, c FROM UNNEST(ARRAY[10, 20, 30], ARRAY[4, 5]) WITH ORDINALITY t(a, b, c)", "SELECT * FROM VALUES (10, 4, 1), (20, 5, 2), (30, NULL, 3)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY['kittens', 'puppies']) WITH ORDINALITY t(a, b)", "SELECT * FROM VALUES ('kittens', 1), ('puppies', 2)");
        assertQuery("" +
                        "SELECT c " +
                        "FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) WITH ORDINALITY t(a, b, c) " +
                        "CROSS JOIN (values (8), (9)) t2(d)",
                "SELECT * FROM VALUES 1, 1, 2, 2, 3, 3");
        assertQuery("" +
                        "SELECT a.custkey, t.e, t.f " +
                        "FROM (SELECT custkey, ARRAY[10, 20, 30] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a " +
                        "CROSS JOIN UNNEST(my_array) WITH ORDINALITY t(e, f)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (10, 1), (20, 2), (30, 3))");
        assertQuery("" +
                        "SELECT a.custkey, t.e, t.f " +
                        "FROM (SELECT custkey, ARRAY[10, 20, 30] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a, " +
                        "UNNEST(my_array) WITH ORDINALITY t(e, f)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (10, 1), (20, 2), (30, 3))");

        assertQuery(session, "SELECT * FROM orders, UNNEST(ARRAY[1])", "SELECT orders.*, 1 FROM orders");
        assertQuery("SELECT a FROM (" +
                        "    SELECT l.arr AS arr FROM (" +
                        "        SELECT orderkey, ARRAY[1,2,3] AS arr FROM orders ORDER BY orderkey LIMIT 1) l" +
                        "    FULL OUTER JOIN (" +
                        "        SELECT orderkey, ARRAY[1,2,3] AS arr FROM orders ORDER BY orderkey LIMIT 1) o" +
                        "    ON l.orderkey = o.orderkey) " +
                        "CROSS JOIN UNNEST(arr) AS t (a)",
                "SELECT * FROM (VALUES (1), (2), (3))");

        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN UNNEST(x) ON true",
                "(?s)line .*: UNNEST on other than the right side of CROSS JOIN is not supported.*");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN UNNEST(x) ON true",
                "(?s)line .*: UNNEST on other than the right side of CROSS JOIN is not supported.*");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN UNNEST(x) ON true",
                "(?s)line .*: UNNEST on other than the right side of CROSS JOIN is not supported.*");
    }
    @Override
    public void testLimitPushDown()
    {
        //Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
        MaterializedResult actual = computeActual(session,
                "(TABLE orders ORDER BY orderkey) UNION ALL " +
                        "SELECT * FROM orders WHERE orderstatus = 'F' UNION ALL " +
                        "(TABLE orders ORDER BY orderkey LIMIT 20) UNION ALL " +
                        "(TABLE orders LIMIT 5) UNION ALL " +
                        "TABLE orders LIMIT 10");
        MaterializedResult all = computeExpected(session, "SELECT * FROM orders", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Override
    public void testShardedJoinOptimization()
    {
        //Setting session property 'LEGACY_TIMESTAMP' to 'false' for fixing timestamp mapping errors between expected and actual values.
        Session defaultSession = getSession();
        Session defaultSessionLegacyTimestampDisabled = Session.builder(defaultSession)
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();

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
