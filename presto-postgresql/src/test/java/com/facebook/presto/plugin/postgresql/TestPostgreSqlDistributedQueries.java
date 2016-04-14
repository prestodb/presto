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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestPostgreSqlDistributedQueries
        extends AbstractTestQueries
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestPostgreSqlDistributedQueries()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    public TestPostgreSqlDistributedQueries(TestingPostgreSqlServer postgreSqlServer)
            throws Exception
    {
        super(createPostgreSqlQueryRunner(postgreSqlServer, TpchTable.getTables()));
        this.postgreSqlServer = postgreSqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        closeAllRuntimeException(postgreSqlServer);
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
                "CAST('a' AS varchar(255)) text_a," +
                "CAST('b' AS varchar(20000)) text_b," +
                "CAST('unbounded' AS varchar) text_unbounded", 1);
        assertTrue(queryRunner.tableExists(getSession(), "presto_test_parameterized_varchar"));
        assertTableColumnNames("presto_test_parameterized_varchar", "text_a", "text_b", "text_unbounded");

        MaterializedResult materializedRows = computeActual("SELECT * from presto_test_parameterized_varchar");
        assertEquals(materializedRows.getTypes().get(0), createVarcharType(255));
        assertEquals(materializedRows.getTypes().get(1), createVarcharType(20000));
        assertEquals(materializedRows.getTypes().get(2), createUnboundedVarcharType());

        MaterializedRow row = getOnlyElement(materializedRows);
        assertEquals(row.getField(0), "a");
        assertEquals(row.getField(1), "b");
        assertEquals(row.getField(2), "unbounded");
        assertUpdate("DROP TABLE presto_test_parameterized_varchar");
    }

    @Test
    public void testPostgreSqlCreatedParameterizedVarchar()
            throws Exception
    {
        execute("CREATE TABLE tpch.postgresql_test_parameterized_varchar (" +
                "text_a varchar(32), " +
                "text_b varchar(20000), " +
                "text_c text)");
        execute("INSERT INTO tpch.postgresql_test_parameterized_varchar VALUES('a', 'b', 'c')");

        MaterializedResult materializedRows = computeActual("SELECT * from postgresql_test_parameterized_varchar");
        assertEquals(materializedRows.getTypes().get(0), createVarcharType(32));
        assertEquals(materializedRows.getTypes().get(1), createVarcharType(20000));
        assertEquals(materializedRows.getTypes().get(2), createUnboundedVarcharType());

        MaterializedRow row = getOnlyElement(materializedRows);
        assertEquals(row.getField(0), "a");
        assertEquals(row.getField(1), "b");
        assertEquals(row.getField(2), "c");
        assertUpdate("DROP TABLE postgresql_test_parameterized_varchar");
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
