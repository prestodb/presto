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

import com.facebook.airlift.testing.postgresql.TestingPostgreSqlServer;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestPostgreSqlCaseSensitiveMapping
        extends AbstractTestQueryFramework
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestPostgreSqlCaseSensitiveMapping()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    public TestPostgreSqlCaseSensitiveMapping(TestingPostgreSqlServer postgreSqlServer)
    {
        this.postgreSqlServer = postgreSqlServer;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PostgreSqlQueryRunner.createPostgreSqlQueryRunner(
                postgreSqlServer,
                ImmutableMap.of("case-sensitive-name-matching", "true"),
                ImmutableSet.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        postgreSqlServer.close();
    }

    @Test
    public void testCreateAllTablesWithMixedCaseScenarios()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("\"postgresmixedcase\"");
                AutoCloseable ignore2 = withSchema("\"POSTGRESMIXEDCASE\"");
                AutoCloseable ignore3 = withSchema("\"PostgresMixedCase\"");
                AutoCloseable ignore4 = withTable("\"postgresmixedcase\".\"testtable\"", "(c varchar(5))");
                AutoCloseable ignore5 = withTable("\"postgresmixedcase\".\"TestTable\"", "(c varchar(5))");
                AutoCloseable ignore6 = withTable("\"postgresmixedcase\".\"TESTTABLE\"", "(c varchar(5))");
                AutoCloseable ignore9 = withTable("\"POSTGRESMIXEDCASE\".\"TESTTABLE\"", "(c varchar(5))");
                AutoCloseable ignore11 = withTable("\"PostgresMixedCase\".\"TestTable\"", "(c varchar(5))")) {
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn())
                        .containsOnly(
                            "tpch",
                            "public",
                            "pg_catalog",
                            "information_schema",
                            "postgresmixedcase",
                            "POSTGRESMIXEDCASE",
                            "PostgresMixedCase");

            assertQuery("SHOW SCHEMAS LIKE 'postgresm%'", "VALUES 'postgresmixedcase'");
            assertQuery("SHOW SCHEMAS LIKE 'POSTGR%'", "VALUES 'POSTGRESMIXEDCASE'");
            assertQuery("SHOW SCHEMAS LIKE 'PostgresMixed%'", "VALUES 'PostgresMixedCase'");
            assertQueryReturnsEmptyResult("SHOW SCHEMAS LIKE 'postGres%'");

            assertQuery("SHOW TABLES FROM postgresmixedcase", "VALUES 'testtable', 'TestTable', 'TESTTABLE'");
            assertQuery("SHOW TABLES FROM POSTGRESMIXEDCASE", "VALUES 'TESTTABLE'");
            assertQuery("SHOW TABLES FROM PostgresMixedCase", "VALUES 'TestTable'");
            assertQueryReturnsEmptyResult("SHOW TABLES FROM postgresmixedcase LIKE 'TEstTable%'");
        }
    }

    @Test
    public void testInsertDataIntoExistingMixedCaseTables()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("\"postgresmixedcase\"");
                AutoCloseable ignore2 = withSchema("\"POSTGRESMIXEDCASE\"");
                AutoCloseable ignore3 = withSchema("\"PostgresMixedCase\"");
                AutoCloseable ignore4 = withTable("\"postgresmixedcase\".\"testtable\"", "(c varchar(5))");
                AutoCloseable ignore9 = withTable("\"POSTGRESMIXEDCASE\".\"TESTTABLE\"", "(c varchar(5))");
                AutoCloseable ignore11 = withTable("\"PostgresMixedCase\".\"TestTable\"", "(c varchar(10))")) {
            assertUpdate("INSERT INTO postgresmixedcase.testtable VALUES ('lower')", 1);
            assertUpdate("INSERT INTO POSTGRESMIXEDCASE.TESTTABLE VALUES ('UPPER')", 1);
            assertUpdate("INSERT INTO PostgresMixedCase.TestTable VALUES ('MixedCase')", 1);

            assertQuery("SELECT * FROM postgresmixedcase.testtable", "VALUES 'lower'");
            assertQuery("SELECT * FROM POSTGRESMIXEDCASE.TESTTABLE", "VALUES 'UPPER'");
            assertQuery("SELECT * FROM PostgresMixedCase.TestTable", "VALUES 'MixedCase'");
        }
    }

    @Test
    public void testTableAlterWithMixedCaseNames()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("\"postgresmixedcase\"");
                AutoCloseable ignore2 = withSchema("\"POSTGRESMIXEDCASE\"");
                AutoCloseable ignore3 = withSchema("\"PostgresMixedCase\"");
                AutoCloseable ignore4 = withTable("\"postgresmixedcase\".\"testtable\"", "(c varchar(5))");
                AutoCloseable ignore9 = withTable("\"POSTGRESMIXEDCASE\".\"TESTTABLE\"", "(c varchar(5))");
                AutoCloseable ignore11 = withTable("\"PostgresMixedCase\".\"TestTable\"", "(c varchar(10))")) {
            assertUpdate("ALTER TABLE postgresmixedcase.testtable ADD COLUMN new_col VARCHAR");
            assertUpdate("ALTER TABLE POSTGRESMIXEDCASE.TESTTABLE ADD COLUMN new_col2 VARCHAR");
            assertUpdate("ALTER TABLE PostgresMixedCase.TestTable ADD COLUMN new_col3 VARCHAR");

            assertQuery("DESCRIBE postgresmixedcase.testtable",
                    "VALUES " +
                            "('c', 'varchar(5)', '', ''), " +
                            "('new_col', 'varchar', '', '')");
            assertQuery("DESCRIBE POSTGRESMIXEDCASE.TESTTABLE",
                    "VALUES " +
                            "('c', 'varchar(5)', '', ''), " +
                            "('new_col2', 'varchar', '', '')");
            assertQuery("DESCRIBE PostgresMixedCase.TestTable",
                    "VALUES " +
                            "('c', 'varchar(10)', '', ''), " +
                            "('new_col3', 'varchar', '', '')");
        }
    }

    private AutoCloseable withSchema(String schemaName)
    {
        execute("CREATE SCHEMA " + schemaName);
        return () -> execute("DROP SCHEMA " + schemaName + " CASCADE");
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
    {
        execute(String.format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> execute(String.format("DROP TABLE %s", tableName));
    }

    private void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }
}
