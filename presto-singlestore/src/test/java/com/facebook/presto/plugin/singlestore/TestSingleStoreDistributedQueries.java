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
package com.facebook.presto.plugin.singlestore;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestSingleStoreDistributedQueries
        extends AbstractTestDistributedQueries
{
    private final DockerizedSingleStoreServer singleStoreServer;

    public TestSingleStoreDistributedQueries()
    {
        this.singleStoreServer = new DockerizedSingleStoreServer();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SingleStoreQueryRunner.createSingleStoreQueryRunner(singleStoreServer, ImmutableMap.of(), TpchTable.getTables());
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        singleStoreServer.close();
    }

    @Override
    public void testShowColumns(@Optional("PARQUET") String storageFormat)
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", 19L, null, null)
                .row("custkey", "bigint", "", "", 19L, null, null)
                .row("orderstatus", "varchar(85)", "", "", null, null, 85L)//utf8
                .row("totalprice", "double", "", "", 53L, null, null)
                .row("orderdate", "date", "", "", null, null, null)
                .row("orderpriority", "varchar(85)", "", "", null, null, 85L)
                .row("clerk", "varchar(85)", "", "", null, null, 85L)
                .row("shippriority", "integer", "", "", 10L, null, null)
                .row("comment", "varchar(85)", "", "", null, null, 85L)
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Test
    public void testStringFilters()
    {
        assertUpdate("CREATE TABLE test_charn_filter (shipmode CHAR(85))");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_charn_filter"));
        assertTableColumnNames("test_charn_filter", "shipmode");
        assertUpdate("INSERT INTO test_charn_filter SELECT shipmode FROM lineitem", 60175);

        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'AIR'", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'AIR    '", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'AIR       '", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'AIR            '", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_charn_filter WHERE shipmode = 'NONEXIST'", "VALUES (0)");

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

    @Test
    public void testLargeIn()
    {
        String longValues = range(0, 100)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");

        String varcharValues = range(0, 100)
                .mapToObj(i -> "'" + i + "'")
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) IN (" + varcharValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) NOT IN (" + varcharValues + ")");

        String arrayValues = range(0, 100)
                .mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + 2))
                .collect(joining(", "));
        assertQuery("SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery("SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        skipTestUnless(supportsNotNullColumns());

        String catalog = getSession().getCatalog().get();
        String createTableStatement = "CREATE TABLE " + catalog + ".tpch.test_not_null_with_insert (\n" +
                "   \"column_a\" date,\n" +
                "   \"column_b\" date NOT NULL\n" +
                ")";
        assertUpdate("CREATE TABLE test_not_null_with_insert (column_a DATE, column_b DATE NOT NULL)");
        assertQuery(
                "SHOW CREATE TABLE test_not_null_with_insert",
                "VALUES '" + createTableStatement + "'");

        assertQueryFails("INSERT INTO test_not_null_with_insert (column_a) VALUES (date '2012-12-31')", "NULL value not allowed for NOT NULL column: column_b");
        assertQueryFails("INSERT INTO test_not_null_with_insert (column_a, column_b) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_b");

        assertUpdate("ALTER TABLE test_not_null_with_insert ADD COLUMN column_c BIGINT NOT NULL");
        assertQuery(
                "SHOW CREATE TABLE test_not_null_with_insert",
                "VALUES 'CREATE TABLE " + catalog + ".tpch.test_not_null_with_insert (\n" +
                        "   \"column_a\" date,\n" +
                        "   \"column_b\" date NOT NULL,\n" +
                        "   \"column_c\" bigint NOT NULL\n" +
                        ")'");

        assertQueryFails("INSERT INTO test_not_null_with_insert (column_b) VALUES (date '2012-12-31')", "NULL value not allowed for NOT NULL column: column_c");
        assertQueryFails("INSERT INTO test_not_null_with_insert (column_b, column_c) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_c");

        assertUpdate("INSERT INTO test_not_null_with_insert (column_b, column_c) VALUES (date '2012-12-31', 1)", 1);
        assertUpdate("INSERT INTO test_not_null_with_insert (column_a, column_b, column_c) VALUES (date '2013-01-01', date '2013-01-02', 2)", 1);
        assertQuery(
                "SELECT * FROM test_not_null_with_insert",
                "VALUES ( NULL, CAST ('2012-12-31' AS DATE), 1 ), ( CAST ('2013-01-01' AS DATE), CAST ('2013-01-02' AS DATE), 2 );");

        assertUpdate("DROP TABLE test_not_null_with_insert");
    }

    @Override
    public void testDescribeOutput()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // Catalog singlestore only supports writes using autocommit
    }

    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // Catalog singlestore only supports writes using autocommit
    }

    @Override
    public void testInsert()
    {
        // no op -- test not supported due to lack of support for array types.  See
        // TestSingleStoreIntegrationSmokeTest for insertion tests.
    }

    @Override
    public void testSubfieldAccessControl()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testPayloadJoinApplicability()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Test
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testDelete()
    {
        // Delete is currently unsupported
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }
}
