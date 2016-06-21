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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestMySqlDistributedQueries
        extends AbstractTestQueries
{
    private final TestingMySqlServer mysqlServer;

    public TestMySqlDistributedQueries()
            throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass", "tpch"));
    }

    public TestMySqlDistributedQueries(TestingMySqlServer mysqlServer)
            throws Exception
    {
        super(createMySqlQueryRunner(mysqlServer, TpchTable.getTables()));
        this.mysqlServer = mysqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        closeAllRuntimeException(mysqlServer);
    }

    @Test
    public void testDropTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_drop AS SELECT 123 x", 1);
        assertTrue(queryRunner.tableExists(getSession(), "test_drop"));

        assertUpdate("DROP TABLE test_drop");
        assertFalse(queryRunner.tableExists(getSession(), "test_drop"));
    }

    @Test
    public void testViews()
            throws Exception
    {
        execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");

        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");

        execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testPrestoCreatedParameterizedVarchar()
            throws Exception
    {
        assertUpdate("CREATE TABLE presto_test_parameterized_varchar AS SELECT " +
                "CAST('a' AS varchar(10)) text_a," +
                "CAST('b' AS varchar(255)) text_b," +
                "CAST('c' AS varchar(256)) text_c," +
                "CAST('d' AS varchar(65535)) text_d," +
                "CAST('e' AS varchar(65536)) text_e," +
                "CAST('f' AS varchar(16777215)) text_f," +
                "CAST('g' AS varchar(16777216)) text_g," +
                "CAST('h' AS varchar(" + VarcharType.MAX_LENGTH + ")) text_h," +
                "CAST('unbounded' AS varchar) text_unbounded", 1);
        assertTrue(queryRunner.tableExists(getSession(), "presto_test_parameterized_varchar"));
        assertTableColumnNames(
                "presto_test_parameterized_varchar",
                "text_a",
                "text_b",
                "text_c",
                "text_d",
                "text_e",
                "text_f",
                "text_g",
                "text_h",
                "text_unbounded");

        MaterializedResult materializedRows = computeActual("SELECT * from presto_test_parameterized_varchar");
        assertEquals(materializedRows.getTypes().get(0), createVarcharType(255));
        assertEquals(materializedRows.getTypes().get(1), createVarcharType(255));
        assertEquals(materializedRows.getTypes().get(2), createVarcharType(65535));
        assertEquals(materializedRows.getTypes().get(3), createVarcharType(65535));
        assertEquals(materializedRows.getTypes().get(4), createVarcharType(16777215));
        assertEquals(materializedRows.getTypes().get(5), createVarcharType(16777215));
        assertEquals(materializedRows.getTypes().get(6), createUnboundedVarcharType());
        assertEquals(materializedRows.getTypes().get(7), createUnboundedVarcharType());
        assertEquals(materializedRows.getTypes().get(8), createUnboundedVarcharType());

        MaterializedRow row = getOnlyElement(materializedRows);
        assertEquals(row.getField(0), "a");
        assertEquals(row.getField(1), "b");
        assertEquals(row.getField(2), "c");
        assertEquals(row.getField(3), "d");
        assertEquals(row.getField(4), "e");
        assertEquals(row.getField(5), "f");
        assertEquals(row.getField(6), "g");
        assertEquals(row.getField(7), "h");
        assertEquals(row.getField(8), "unbounded");
        assertUpdate("DROP TABLE presto_test_parameterized_varchar");
    }

    @Test
    public void testMySqlCreatedParameterizedVarchar()
            throws Exception
    {
        execute("CREATE TABLE tpch.mysql_test_parameterized_varchar (" +
                "text_a tinytext, " +
                "text_b text, " +
                "text_c mediumtext, " +
                "text_d longtext, " +
                "text_e varchar(32), " +
                "text_f varchar(20000)" +
                ")");
        execute("INSERT INTO tpch.mysql_test_parameterized_varchar VALUES('a', 'b', 'c', 'd', 'e', 'f')");

        MaterializedResult materializedRows = computeActual("SELECT * from mysql_test_parameterized_varchar");
        assertEquals(materializedRows.getTypes().get(0), createVarcharType(255));
        assertEquals(materializedRows.getTypes().get(1), createVarcharType(65535));
        assertEquals(materializedRows.getTypes().get(2), createVarcharType(16777215));
        assertEquals(materializedRows.getTypes().get(3), createUnboundedVarcharType());
        assertEquals(materializedRows.getTypes().get(4), createVarcharType(32));
        assertEquals(materializedRows.getTypes().get(5), createVarcharType(20000));

        MaterializedRow row = getOnlyElement(materializedRows);
        assertEquals(row.getField(0), "a");
        assertEquals(row.getField(1), "b");
        assertEquals(row.getField(2), "c");
        assertEquals(row.getField(3), "d");
        assertEquals(row.getField(4), "e");
        assertEquals(row.getField(5), "f");
        assertUpdate("DROP TABLE mysql_test_parameterized_varchar");
    }

    @Override
    public void testShowColumns()
            throws Exception
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "")
                .row("custkey", "bigint", "")
                .row("orderstatus", "varchar(255)", "")
                .row("totalprice", "double", "")
                .row("orderdate", "date", "")
                .row("orderpriority", "varchar(255)", "")
                .row("clerk", "varchar(255)", "")
                .row("shippriority", "integer", "")
                .row("comment", "varchar(255)", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(mysqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
