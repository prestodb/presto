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
package com.facebook.presto.tests.sqlserver;

import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.SQL_SERVER_MIXED_CASE_OFF;
import static com.facebook.presto.tests.utils.QueryExecutors.onSqlServer;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestSqlServerMixedCaseSupportOff
        extends ProductTest
{
    private static final String CATALOG = "sqlserver";
    private static final String SCHEMA_NAME = "sqlservermixedcase";
    private static final String SCHEMA_NAME_UPPER = "SQLSERVERMIXEDCASE1";
    private static final String SCHEMA_NAME_MIXED = "SqlServerMixedCase2";
    private static final String TABLE_NAME = "testtable";
    private static final String TABLE_NAME_0 = "testtable0";
    private static final String TABLE_NAME_MIXED_1 = "TestTable1";
    private static final String TABLE_NAME_UPPER_2 = "TESTTABLE2";
    private static final String TABLE_NAME_UPPER_SCHEMA_3 = "TESTTABLE3";
    private static final String TABLE_NAME_UPPER_SCHEMA_4 = "TESTTABLE4";
    private static final String TABLE_NAME_UPPER_SCHEMA_02 = "testtable02";

    @Test(groups = {SQL_SERVER_MIXED_CASE_OFF})
    public void testCreateSchemasDirectlyInSqlServer()
    {
        onSqlServer().executeQuery(
                "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" + SCHEMA_NAME + "') " +
                        "BEGIN EXEC('CREATE SCHEMA " + SCHEMA_NAME + "') END");

        onSqlServer().executeQuery(
                "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" + SCHEMA_NAME_UPPER + "') " +
                        "BEGIN EXEC('CREATE SCHEMA " + SCHEMA_NAME_UPPER + "') END");

        onSqlServer().executeQuery(
                "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" + SCHEMA_NAME_MIXED + "') " +
                        "BEGIN EXEC('CREATE SCHEMA " + SCHEMA_NAME_MIXED + "') END");

        assertThat(query("SHOW SCHEMAS FROM " + CATALOG))
                .contains(
                        row("sqlservermixedcase"),
                        row("sqlservermixedcase1"),
                        row("sqlservermixedcase2"));
    }

    @Test(groups = {SQL_SERVER_MIXED_CASE_OFF}, dependsOnMethods = "testCreateSchemasDirectlyInSqlServer")
    public void testCreateTablesWithMixedCaseNames()
    {
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_0 + " (name VARCHAR(50), id INT)");
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " (name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_MIXED_1 + " (Name VARCHAR(50), id INT)");
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_4 + " (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " (name VARCHAR(50), id INT, num DOUBLE)");
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_3 + " (name VARCHAR(50), id INT, num DOUBLE)");

        assertThat(query("SHOW TABLES FROM " + CATALOG + "." + SCHEMA_NAME))
                .containsOnly(row("testtable0"), row("testtable"), row("testtable1"), row("testtable2"));

        assertThat(query("SHOW TABLES FROM " + CATALOG + "." + SCHEMA_NAME_UPPER))
                .containsOnly(row("testtable4"), row("testtable02"), row("testtable3"));
    }

    @Test(groups = {SQL_SERVER_MIXED_CASE_OFF}, dependsOnMethods = "testCreateTablesWithMixedCaseNames")
    public void testInsertDataWithMixedCaseNames()
    {
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " VALUES ('amy', 112), ('mia', 123)");
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " VALUES ('ann', 112), ('mary', 123)");
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_3 + " VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
                .containsOnly(row("amy", 112), row("mia", 123));

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2))
                .containsOnly(row("ann", 112), row("mary", 123));

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_3))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));
    }

    @Test(groups = {SQL_SERVER_MIXED_CASE_OFF}, dependsOnMethods = "testInsertDataWithMixedCaseNames")
    public void testTableAlterWithMixedCaseNames()
    {
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " ADD COLUMN num REAL");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " ADD COLUMN num1 REAL");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " ADD COLUMN num01 REAL");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02.toUpperCase() + " ADD COLUMN num2 REAL");

        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
                .contains(row("num", "real", "", ""));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02))
                .contains(row("num1", "real", "", ""));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2))
                .contains(row("num01", "real", "", ""));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02.toUpperCase()))
                .contains(row("num2", "real", "", ""));

        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " RENAME COLUMN num TO numb");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " RENAME COLUMN num1 TO numb01");

        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
                .contains(row("numb", "real", "", ""));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02))
                .contains(row("numb01", "real", "", ""));
    }

    @Test(groups = {SQL_SERVER_MIXED_CASE_OFF}, dependsOnMethods = "testTableAlterWithMixedCaseNames")
    public void testDropMixedCaseTablesAndSchemas()
    {
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".testtable");
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".testtable0");
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".TestTable1");
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".TESTTABLE2");
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + ".TESTTABLE3");
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + ".TESTTABLE4");
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + ".testtable02");

        onSqlServer().executeQuery("DROP SCHEMA IF EXISTS " + SCHEMA_NAME);
        onSqlServer().executeQuery("DROP SCHEMA IF EXISTS " + SCHEMA_NAME_UPPER);
        onSqlServer().executeQuery("DROP SCHEMA IF EXISTS " + SCHEMA_NAME_MIXED);
    }
}
