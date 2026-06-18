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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestPostgreSqlCaseSensitiveMapping
        extends AbstractTestQueryFramework
{
    private final PostgreSQLContainer postgresContainer;

    public TestPostgreSqlCaseSensitiveMapping()
    {
        this.postgresContainer = new PostgreSQLContainer("postgres:14")
                .withDatabaseName("tpch")
                .withUsername("testuser")
                .withPassword("testpass");
        this.postgresContainer.start();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PostgreSqlQueryRunner.createPostgreSqlQueryRunner(
                postgresContainer.getJdbcUrl(),
                ImmutableMap.of("case-sensitive-name-matching", "true"),
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        postgresContainer.stop();
    }

    @Test
    public void testCreateAllTablesWithMixedCaseScenarios()
            throws Exception
    {
        try (AutoCloseable ignore = withSchemasAndTables()) {
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
        try (AutoCloseable ignore = withSchemasAndTables()) {
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
        try (AutoCloseable ignore = withSchemasAndTables()) {
            assertUpdate("ALTER TABLE postgresmixedcase.testtable ADD COLUMN New_Col VARCHAR");
            assertUpdate("ALTER TABLE postgresmixedcase.testtable ADD COLUMN NEW_COL1 VARCHAR");
            assertUpdate("ALTER TABLE POSTGRESMIXEDCASE.TESTTABLE ADD COLUMN New_Col2 VARCHAR");
            assertUpdate("ALTER TABLE POSTGRESMIXEDCASE.TESTTABLE ADD COLUMN NEW_COL1 VARCHAR");
            assertUpdate("ALTER TABLE PostgresMixedCase.TestTable ADD COLUMN New_Col3 VARCHAR");
            assertUpdate("ALTER TABLE PostgresMixedCase.TestTable ADD COLUMN NEW_COL1 VARCHAR");

            assertQuery("DESCRIBE postgresmixedcase.testtable",
                    "VALUES " +
                            "('c', 'varchar(5)', '', '', null, null, 5L), " +
                            "('New_Col', 'varchar', '', '', null, null, 2147483647L), " +
                            "('NEW_COL1', 'varchar', '', '', null, null, 2147483647L)");
            assertQuery("DESCRIBE POSTGRESMIXEDCASE.TESTTABLE",
                    "VALUES " +
                            "('c', 'varchar(5)', '', '', null, null, 5), " +
                            "('New_Col2', 'varchar', '', '', null, null, 2147483647L), " +
                            "('NEW_COL1', 'varchar', '', '', null, null, 2147483647L)");
            assertQuery("DESCRIBE PostgresMixedCase.TestTable",
                    "VALUES " +
                            "('c', 'varchar(10)', '', '', null, null, 10L), " +
                            "('New_Col3', 'varchar', '', '', null, null, 2147483647L), " +
                            "('NEW_COL1', 'varchar', '', '', null, null, 2147483647L)");
        }
    }

    private AutoCloseable withSchemasAndTables()
            throws Exception
    {
        AutoCloseable schema1 = withSchema("\"postgresmixedcase\"");
        AutoCloseable schema2 = withSchema("\"POSTGRESMIXEDCASE\"");
        AutoCloseable schema3 = withSchema("\"PostgresMixedCase\"");

        AutoCloseable table1 = withTable("\"postgresmixedcase\".\"testtable\"", "(c varchar(5))");
        AutoCloseable table2 = withTable("\"postgresmixedcase\".\"TestTable\"", "(c varchar(5))");
        AutoCloseable table3 = withTable("\"postgresmixedcase\".\"TESTTABLE\"", "(c varchar(5))");
        AutoCloseable table4 = withTable("\"POSTGRESMIXEDCASE\".\"TESTTABLE\"", "(c varchar(5))");
        AutoCloseable table5 = withTable("\"PostgresMixedCase\".\"TestTable\"", "(c varchar(10))");

        return () -> {
            try {
                table5.close();
                table4.close();
                table3.close();
                table2.close();
                table1.close();
                schema3.close();
                schema2.close();
                schema1.close();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to clean up test schemas/tables", e);
            }
        };
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
        try (Connection connection = DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }
}
