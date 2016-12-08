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

import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertTrue;

@Test
public class TestPostgreSqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestPostgreSqlIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    public TestPostgreSqlIntegrationSmokeTest(TestingPostgreSqlServer postgreSqlServer)
            throws Exception
    {
        super(PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ORDERS));
        this.postgreSqlServer = postgreSqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        closeAllRuntimeException(postgreSqlServer);
    }

    @Test
    public void testTimestampTz()
            throws Exception
    {
        queryRunner.execute("CREATE TABLE tpch.test_timestamptz AS SELECT CAST('2016-12-07 00:00:00-03:00' AS TIMESTAMP WITH TIME ZONE) as sometime");
        assertQuery("SELECT * FROM test_timestamptz", "SELECT CAST('2016-12-07 00:00:00-03:00' AS TIMESTAMP)");
    }

    @Test
    public void testTimestamp()
            throws Exception
    {
        queryRunner.execute("CREATE TABLE tpch.test_timestamp AS SELECT CAST('2016-12-07 00:00:00' AS TIMESTAMP) as sometime");
        assertQuery("SELECT * FROM test_timestamp", "SELECT CAST('2016-12-07 00:00:00' AS TIMESTAMP)");
    }

    @Test
    public void testTimeTz()
            throws Exception
    {
        queryRunner.execute("CREATE TABLE tpch.test_timetz AS SELECT CAST('00:00:00-03:00' AS TIME WITH TIME ZONE) as sometime");
        assertQuery("SELECT * FROM test_timetz", "SELECT CAST(CAST('1970-01-01 00:00:00-03:00' AS TIMESTAMP) AS TIME)");
    }
    @Test
    public void testTime()
            throws Exception
    {
        queryRunner.execute("CREATE TABLE tpch.test_time AS SELECT CAST('00:00:00' AS TIME) as sometime");
        assertQuery("SELECT * FROM test_time", "SELECT CAST('00:00:00' AS TIME)");
    }

    @Test
    public void testDate()
            throws Exception
    {
        queryRunner.execute("CREATE TABLE tpch.test_date AS SELECT CAST('2016-12-07' AS DATE) as sometime");
        assertQuery("SELECT * FROM test_date", "SELECT CAST('2016-12-07' AS DATE)");
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert (x bigint, y varchar(100), z timestamp with time zone)");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test', CAST('2016-12-07 12:34:56-07:00' AS TIMESTAMP WITH TIME ZONE))", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y, CAST('2016-12-07 12:34:56-07:00' AS TIMESTAMP)");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testMaterializedView()
            throws Exception
    {
        execute("CREATE MATERIALIZED VIEW tpch.test_mv as SELECT * FROM tpch.orders");
        assertTrue(queryRunner.tableExists(getSession(), "test_mv"));
        assertQuery("SELECT orderkey FROM test_mv", "SELECT orderkey FROM orders");
        execute("DROP MATERIALIZED VIEW tpch.test_mv");
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
