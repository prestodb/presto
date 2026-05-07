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
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestSqlServerProcedure
        extends AbstractTestQueryFramework
{
    private MSSQLServerContainer mssqlServerContainer;

    public TestSqlServerProcedure()
    {
        this.mssqlServerContainer = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2025-latest")
                .acceptLicense();
        this.mssqlServerContainer.start();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        mssqlServerContainer.stop();
    }

    @Test
    public void testExecuteProcedure()
    {
        String tableName = "test_execute";
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;
        try {
            assertUpdate("CALL system.execute('CREATE TABLE " + schemaTableName + "(id BIGINT IDENTITY(1,1) PRIMARY KEY, a int)')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
            assertUpdate("CALL system.execute('INSERT INTO " + schemaTableName + "(a) VALUES (1)')");
            assertUpdate("CALL system.execute('INSERT INTO " + schemaTableName + "(a) VALUES (21)')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 1), (2, 21)");

            assertUpdate("CALL system.execute('UPDATE " + schemaTableName + " SET a = 2 where id = 1')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 2), (2, 21)");

            assertUpdate("CALL system.execute('DELETE FROM " + schemaTableName + "')");
            assertQueryReturnsEmptyResult("SELECT * FROM " + schemaTableName);

            assertUpdate("CALL system.execute('DROP TABLE " + schemaTableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaTableName);
        }
    }

    @Test
    public void testExecuteProcedureWithNamedArgument()
    {
        String tableName = "test_execute_named";
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;
        try {
            assertUpdate("CALL system.execute(QUERY => 'CREATE TABLE " + schemaTableName + "(id BIGINT IDENTITY(1,1) PRIMARY KEY, a int)')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
            assertUpdate("CALL system.execute(QUERY => 'INSERT INTO " + schemaTableName + "(a) VALUES (1)')");
            assertUpdate("CALL system.execute(QUERY => 'INSERT INTO " + schemaTableName + "(a) VALUES (21)')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 1), (2, 21)");

            assertUpdate("CALL system.execute(QUERY => 'UPDATE " + schemaTableName + " SET a = 2 where id = 1')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 2), (2, 21)");

            assertUpdate("CALL system.execute(QUERY => 'DELETE FROM " + schemaTableName + "')");
            assertQueryReturnsEmptyResult("SELECT * FROM " + schemaTableName);

            assertUpdate("CALL system.execute(QUERY => 'DROP TABLE " + schemaTableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaTableName);
        }
    }

    @Test
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertQueryFails("CALL system.execute('SELECT 1')", "(?s)Failed to execute query.*");
        assertQueryFails("CALL system.execute('invalid')", "(?s)Failed to execute query.*");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SqlServerQueryRunner.createSqlServerQueryRunner(
                mssqlServerContainer.getJdbcUrl(),
                ImmutableMap.of("allow-drop-table", "true"),
                mssqlServerContainer.getUsername(),
                mssqlServerContainer.getPassword());
    }
}
