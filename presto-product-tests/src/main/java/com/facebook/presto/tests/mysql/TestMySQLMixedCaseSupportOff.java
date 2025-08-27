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
package com.facebook.presto.tests.mysql;

import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.MYSQL;
import static com.facebook.presto.tests.utils.QueryExecutors.onMySql;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestMySQLMixedCaseSupportOff
        extends ProductTest
{
    private static final String CATALOG = "mysql";
    private static final String SCHEMA_NAME = "mysqlmixedcase";
    private static final String SCHEMA_NAME_UPPER = "MYSQLMIXEDCASE1";

    private static final String TABLE_NAME = "testtable";
    private static final String TABLE_NAME_0 = "testtable0";
    private static final String TABLE_NAME_MIXED_1 = "TestTable1";
    private static final String TABLE_NAME_UPPER_2 = "TESTTABLE2";
    private static final String TABLE_NAME_UPPER_SCHEMA_3 = "TESTTABLE3";
    private static final String TABLE_NAME_UPPER_SCHEMA_4 = "TESTTABLE4";
    private static final String TABLE_NAME_UPPER_SCHEMA_02 = "testtable02";

    /*
     * Test cases for creating tables with different naming conventions in MySQL.
     *
     * This test verifies table creation, schema behavior, and column definitions
     * when using different case variations for table and column names.
     */
    @Test(groups = {MYSQL})
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

    /*
     * This test validates inserting data into tables with different case variations in schema and table names.
     * It ensures that data is inserted and retrieved correctly regardless of case sensitivity.
     */
    @Test(groups = {MYSQL}, dependsOnMethods = "testCreateTablesWithMixedCaseNames")
    public void testInsertDataWithMixedCaseNames()
    {
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " VALUES ('amy', 112), ('mia', 123)");
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " VALUES ('ann', 112), ('mary', 123)");
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_3 + " VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
                .containsOnly(row("amy", 112), row("mia", 123));

        // Even if we define a column as ID, we can refer to it as id, ID, or anything else in any case — with or without backticks.
        assertThat(query("SELECT name, id FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
                .containsOnly(row("amy", 112), row("mia", 123));

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2))
                .containsOnly(row("ann", 112), row("mary", 123));

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_3))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));
    }

    /*
     * This test verifies altering tables with different case variations in schema and table names.
     *
     * Scenarios covered:
     * 1. Adding columns to tables with lowercase, mixed-case, and uppercase names.
     * 2. Renaming columns in tables with various case patterns.
     *
     * The test ensures:
     * - Column addition works correctly across cases.
     * - Column renaming functions as expected.
     * - Case variations do not impact alter operations.
     */
    @Test(groups = {MYSQL}, dependsOnMethods = "testInsertDataWithMixedCaseNames")
    public void testTableAlterWithMixedCaseNames()
    {
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " ADD COLUMN num REAL");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " ADD COLUMN NuM2 REAL");

        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " ADD COLUMN num01 REAL");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " ADD COLUMN NuM2 REAL");

        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " ADD COLUMN num1 REAL");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02.toUpperCase() + " ADD COLUMN num2 REAL");
        // Negative test: Creating a duplicate column with different case should fail in mysql
        assertThat(() -> query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " ADD COLUMN NuM2 REAL"))
                .failsWithMessage("line 1:1: Column 'NuM2' already exists");

        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME)).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "real", "", "", 24L, null, null),
                row("num2", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2)).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("num01", "real", "", "", 24L, null, null),
                row("num2", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02)).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("num1", "real", "", "", 24L, null, null),
                row("num2", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02.toUpperCase())).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("num1", "real", "", "", 24L, null, null),
                row("num2", "real", "", "", 24L, null, null));

        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " RENAME COLUMN num TO numb");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " RENAME COLUMN NuM2 TO NuM02");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " RENAME COLUMN num1 TO numb01");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " RENAME COLUMN NuM2 TO NuM02");

        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME)).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("numb", "real", "", "", 24L, null, null),
                row("num02", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02)).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("numb01", "real", "", "", 24L, null, null),
                row("num02", "real", "", "", 24L, null, null));

        // drop column
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " DROP COLUMN NuM02");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02 + " DROP COLUMN numb01");

        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME)).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("numb", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME_UPPER + "." + TABLE_NAME_UPPER_SCHEMA_02)).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("num02", "real", "", "", 24L, null, null));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testTableAlterWithMixedCaseNames")
    public void testDropMixedCaseTablesAndSchemas()
    {
        onMySql().executeQuery("DROP TABLE IF EXISTS mysqlmixedcase.testtable");
        onMySql().executeQuery("DROP TABLE IF EXISTS mysqlmixedcase.testtable0");
        onMySql().executeQuery("DROP TABLE IF EXISTS mysqlmixedcase.TestTable1");
        onMySql().executeQuery("DROP TABLE IF EXISTS mysqlmixedcase.TESTTABLE2");
        onMySql().executeQuery("DROP TABLE IF EXISTS MYSQLMIXEDCASE1.TESTTABLE3");
        onMySql().executeQuery("DROP TABLE IF EXISTS MYSQLMIXEDCASE1.TESTTABLE4");
        onMySql().executeQuery("DROP TABLE IF EXISTS MYSQLMIXEDCASE1.testtable02");
    }
}
