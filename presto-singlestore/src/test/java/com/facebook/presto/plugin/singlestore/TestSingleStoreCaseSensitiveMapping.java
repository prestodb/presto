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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Test
public class TestSingleStoreCaseSensitiveMapping
        extends AbstractTestQueryFramework
{
    private final DockerizedSingleStoreServer singleStoreServer;

    public TestSingleStoreCaseSensitiveMapping()
    {
        this.singleStoreServer = new DockerizedSingleStoreServer();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SingleStoreQueryRunner.createSingleStoreQueryRunner(
                singleStoreServer,
                ImmutableMap.of("case-sensitive-name-matching", "true"),
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        singleStoreServer.close();
    }

    @Test
    public void testCreateAllTablesWithMixedCaseScenarios()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("\"singlestoremixedcase\"");
                 AutoCloseable ignore2 = withSchema("\"SINGLESTOREMIXEDCASE\"");
                 AutoCloseable ignore3 = withSchema("\"SingleStoreMixedCase\"");
                 AutoCloseable ignore4 = withTable("`singlestoremixedcase`.`testtable`", "(c varchar(5))");
                 AutoCloseable ignore5 = withTable("`singlestoremixedcase`.`TestTable`", "(c varchar(5))");
                 AutoCloseable ignore6 = withTable("`singlestoremixedcase`.`TESTTABLE`", "(c varchar(5))");
                 AutoCloseable ignore9 = withTable("`SINGLESTOREMIXEDCASE`.`TESTTABLE`", "(c varchar(5))");
                 AutoCloseable ignore11 = withTable("`SingleStoreMixedCase`.`TestTable`", "(c varchar(5))")) {
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn())
                    .containsOnly(
                            "SINGLESTOREMIXEDCASE",
                            "SingleStoreMixedCase",
                            "information_schema",
                            "singlestoremixedcase",
                            "tpch");
            assertQuery("SHOW SCHEMAS LIKE 'singlestorem%'", "VALUES 'singlestoremixedcase'");
            assertQuery("SHOW SCHEMAS LIKE 'SINGLESTO%'", "VALUES 'SINGLESTOREMIXEDCASE'");
            assertQuery("SHOW SCHEMAS LIKE 'SingleStoreMi%'", "VALUES 'SingleStoreMixedCase'");
            assertQueryReturnsEmptyResult("SHOW SCHEMAS LIKE 'pingleStoreMi%'");

            assertQuery("SHOW TABLES FROM singlestoremixedcase", "VALUES 'testtable', 'TestTable', 'TESTTABLE'");
            assertQuery("SHOW TABLES FROM SINGLESTOREMIXEDCASE", "VALUES 'TESTTABLE'");
            assertQuery("SHOW TABLES FROM SingleStoreMixedCase", "VALUES 'TestTable'");
            assertQueryReturnsEmptyResult("SHOW TABLES FROM SingleStoreMixedCase LIKE 'TEstTable%'");
        }
    }

    @Test
    public void testInsertDataIntoExistingMixedCaseTables()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("\"singlestoremixedcase\"");
                AutoCloseable ignore2 = withSchema("\"SINGLESTOREMIXEDCASE\"");
                AutoCloseable ignore3 = withSchema("\"SingleStoreMixedCase\"");
                AutoCloseable ignore4 = withTable("`singlestoremixedcase`.`testtable`", "(c varchar(5))");
                AutoCloseable ignore5 = withTable("`SINGLESTOREMIXEDCASE`.`TESTTABLE`", "(c varchar(5))");
                AutoCloseable ignore6 = withTable("`SingleStoreMixedCase`.`TestTable`", "(c varchar(10))")) {
            assertUpdate("INSERT INTO singlestoremixedcase.testtable VALUES ('lower')", 1);
            assertUpdate("INSERT INTO SINGLESTOREMIXEDCASE.TESTTABLE VALUES ('UPPER')", 1);
            assertUpdate("INSERT INTO SingleStoreMixedCase.TestTable VALUES ('MixedCase')", 1);

            assertQuery("SELECT * FROM singlestoremixedcase.testtable", "VALUES 'lower'");
            assertQuery("SELECT * FROM SINGLESTOREMIXEDCASE.TESTTABLE", "VALUES 'UPPER'");
            assertQuery("SELECT * FROM SingleStoreMixedCase.TestTable", "VALUES 'MixedCase'");
        }
    }

    @Test
    public void testTableAlterWithMixedCaseNames()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("\"singlestoremixedcase\"");
                AutoCloseable ignore2 = withSchema("\"SINGLESTOREMIXEDCASE\"");
                AutoCloseable ignore3 = withSchema("\"SingleStoreMixedCase\"");
                AutoCloseable ignore4 = withTable("`singlestoremixedcase`.`testtable`", "(c varchar(5))");
                AutoCloseable ignore5 = withTable("`SINGLESTOREMIXEDCASE`.`TESTTABLE`", "(c varchar(5))");
                AutoCloseable ignore6 = withTable("`SingleStoreMixedCase`.`TestTable`", "(c varchar(10))")) {
            assertUpdate("ALTER TABLE singlestoremixedcase.testtable ADD COLUMN new_col varchar(85)");
            assertUpdate("ALTER TABLE SINGLESTOREMIXEDCASE.TESTTABLE ADD COLUMN new_col2 varchar(85)");
            assertUpdate("ALTER TABLE SingleStoreMixedCase.TestTable ADD COLUMN new_col3 varchar(85)");

            assertQuery("DESCRIBE singlestoremixedcase.testtable",
                    "VALUES " +
                            "('c', 'varchar(5)', '', ''), " +
                            "('new_col', 'varchar(85)', '', '')");
            assertQuery("DESCRIBE SINGLESTOREMIXEDCASE.TESTTABLE",
                    "VALUES " +
                            "('c', 'varchar(5)', '', ''), " +
                            "('new_col2', 'varchar(85)', '', '')");
            assertQuery("DESCRIBE SingleStoreMixedCase.TestTable",
                    "VALUES " +
                            "('c', 'varchar(10)', '', ''), " +
                            "('new_col3', 'varchar(85)', '', '')");
        }
    }

    private AutoCloseable withSchema(String schemaName)
    {
        execute("CREATE SCHEMA " + schemaName);
        return () -> execute("DROP SCHEMA " + schemaName);
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
    {
        execute(String.format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> execute(String.format("DROP TABLE %s", tableName));
    }

    private void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(singleStoreServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }
}
