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

import static com.facebook.presto.tests.TestGroups.SQL_SERVER_MIXED_CASE_ON;
import static com.facebook.presto.tests.utils.QueryExecutors.onSqlServer;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestSqlServerMixedCaseSupportOn
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
    private static final String TABLE_NAME_FROM_LOWER = "createdfromlowercase";
    private static final String TABLE_NAME_FROM_MIXED = "createdFromMixedCase";
    private static final String TABLE_NAME_JOIN_LOWER = "createdwithjoinlower";

    @Test(groups = {SQL_SERVER_MIXED_CASE_ON})
    public void testCreateSchemasInSqlServer()
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
                        row(SCHEMA_NAME),
                        row(SCHEMA_NAME_UPPER),
                        row(SCHEMA_NAME_MIXED));
    }

    @Test(groups = {SQL_SERVER_MIXED_CASE_ON}, dependsOnMethods = "testCreateSchemasInSqlServer")
    public void testCreateAllTablesWithMixedCaseScenarios()
    {
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_0 + "\" (name VARCHAR(50), id INT)");
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" (name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_MIXED_1 + "\" (Name VARCHAR(50), id INT)");
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_UPPER_2 + "\" (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_4 + "\" (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" (name VARCHAR(50), id INT, num DOUBLE)");
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_3 + "\" (name VARCHAR(50), id INT, num DOUBLE)");
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_FROM_LOWER + "\" AS SELECT * FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\"");
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME_MIXED + "\".\"" + TABLE_NAME_FROM_MIXED + "\" AS SELECT * FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\"");
        query("CREATE TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_JOIN_LOWER + "\" AS " +
                "SELECT d.* FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" d " +
                "INNER JOIN " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" m " +
                "ON d.id = m.id WHERE d.id = 1");

        assertThat(query("SHOW TABLES FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\""))
                .containsOnly(row("testtable0"), row("testtable"), row("TestTable1"), row("TESTTABLE2"), row("createdfromlowercase"), row("createdwithjoinlower"));

        assertThat(query("SHOW TABLES FROM " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\""))
                .containsOnly(row("TESTTABLE4"), row("testtable02"), row("TESTTABLE3"));

        assertThat(query("SHOW TABLES FROM " + CATALOG + ".\"" + SCHEMA_NAME_MIXED + "\""))
                .containsOnly(row("createdFromMixedCase"));
    }

    @Test(groups = {SQL_SERVER_MIXED_CASE_ON}, dependsOnMethods = "testCreateAllTablesWithMixedCaseScenarios")
    public void testInsertDataIntoExistingMixedCaseTables()
    {
        query("INSERT INTO " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" VALUES ('eva', 301), ('lisa', 302)");
        query("INSERT INTO " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_MIXED_1 + "\" VALUES ('ivan', 401), ('nora', 402)");
        query("INSERT INTO " + CATALOG + ".\"" + SCHEMA_NAME_MIXED + "\".\"" + TABLE_NAME_FROM_MIXED + "\" VALUES ('kate', 501), ('leo', 502)");
        query("INSERT INTO " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" VALUES ('kate', 501, 200), ('leo', 502, 201)");
        query("INSERT INTO " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_4 + "\" VALUES ('kate', 501), ('leo', 502)");
        query("INSERT INTO " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_3 + "\" VALUES ('zack', 601, 100.1), ('jane', 602, 200.2)");
        query("INSERT INTO " + CATALOG + ".\"" + SCHEMA_NAME_MIXED + "\".\"" + TABLE_NAME_FROM_MIXED + "\" VALUES ('ruby', 701), ('ted', 702)");
        query("INSERT INTO " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_MIXED_1 + "\" " +
                "SELECT name, id FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\"");
        query("INSERT INTO " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" " +
                "SELECT Name, id FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_MIXED_1 + "\"");

        assertThat(query("SELECT COUNT(*) FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\""))
                .hasRowsCount(1);
        assertThat(query("SELECT COUNT(*) FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_MIXED_1 + "\""))
                .hasRowsCount(1);
        assertThat(query("SELECT COUNT(*) FROM " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_3 + "\""))
                .hasRowsCount(1);
        assertThat(query("SELECT COUNT(*) FROM " + CATALOG + ".\"" + SCHEMA_NAME_MIXED + "\".\"" + TABLE_NAME_FROM_MIXED + "\""))
                .hasRowsCount(1);
    }

    @Test(groups = {SQL_SERVER_MIXED_CASE_ON}, dependsOnMethods = "testInsertDataIntoExistingMixedCaseTables")
    public void testSelectDataWithMixedCaseNames()
    {
        assertThat(query("SELECT * FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\""))
                .containsOnly(row("eva", 301), row("lisa", 302), row("ivan", 401), row("nora", 402), row("eva", 301), row("lisa", 302));

        assertThat(query("SELECT name FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_MIXED_1 + "\" WHERE id = 401"))
                .containsOnly(row("ivan"));

        assertThat(query("SELECT name FROM " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" WHERE id = 501"))
                .containsOnly(row("kate"));

        assertThat(query("SELECT name FROM " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_3 + "\" WHERE id = 601"))
                .containsOnly(row("zack"));

        assertThat(query("SELECT Name, ID " +
                "FROM " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_4 + "\" " +
                "WHERE name IN (" +
                "SELECT Name FROM " + CATALOG + ".\"" + SCHEMA_NAME_MIXED + "\".\"" + TABLE_NAME_FROM_MIXED + "\" WHERE id = 501)"))
                .containsOnly(row("kate", 501));
    }

    @Test(groups = {SQL_SERVER_MIXED_CASE_ON}, dependsOnMethods = "testSelectDataWithMixedCaseNames")
    public void testTableAlterWithMixedCaseNames()
    {
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" ADD COLUMN num REAL");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" ADD COLUMN num1 REAL");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_UPPER_2 + "\" ADD COLUMN num01 REAL");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" ADD COLUMN num2 REAL");

        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\""))
                .contains(row("num", "real", "", ""));

        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\""))
                .contains(row("num1", "real", "", ""));

        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_UPPER_2 + "\""))
                .contains(row("num01", "real", "", ""));

        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\""))
                .contains(row("num2", "real", "", ""));

        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" RENAME COLUMN num TO numb");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" RENAME COLUMN num1 TO numb01");

        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\""))
                .contains(row("numb", "real", "", ""));

        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\""))
                .contains(row("numb01", "real", "", ""));
    }

    @Test(groups = {SQL_SERVER_MIXED_CASE_ON}, dependsOnMethods = "testTableAlterWithMixedCaseNames")
    public void testDropMixedCaseTablesAndSchemas()
    {
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + TABLE_NAME);
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + TABLE_NAME_0);
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + TABLE_NAME_MIXED_1);
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2);
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_3);
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_4);
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02);
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + TABLE_NAME_FROM_LOWER);
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME_MIXED + "." + TABLE_NAME_FROM_MIXED);
        onSqlServer().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + TABLE_NAME_JOIN_LOWER);

        onSqlServer().executeQuery("DROP SCHEMA IF EXISTS " + SCHEMA_NAME);
        onSqlServer().executeQuery("DROP SCHEMA IF EXISTS " + SCHEMA_NAME_UPPER);
        onSqlServer().executeQuery("DROP SCHEMA IF EXISTS " + SCHEMA_NAME_MIXED);
    }
}
