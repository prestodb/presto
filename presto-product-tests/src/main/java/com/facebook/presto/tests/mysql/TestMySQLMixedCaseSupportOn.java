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

import static com.facebook.presto.tests.TestGroups.MYSQL_MIXED_CASE;
import static com.facebook.presto.tests.utils.QueryExecutors.onMySql;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestMySQLMixedCaseSupportOn
        extends ProductTest
{
    private static final String CATALOG = "mysql";
    private static final String SCHEMA_NAME = "mysqlmixedcase";
    private static final String SCHEMA_NAME_UPPER = "MYSQLMIXEDCASE1";
    private static final String SCHEMA_NAME_MIXED = "MySqlMixedCase2";
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
    private static final String TABLE_NAME_JOIN_MIXED = "createdwithjoinmixed";

    /**
     * This comprehensive test covers various scenarios for creating tables with different combinations
     * of schema, table, and column casing. The objective is to validate Presto's handling of case sensitivity
     * with MySQL when mixed-case support is enabled.
     *
     * Covered scenarios:
     * - Lowercase schema with lowercase table and columns
     * - Lowercase schema with lowercase table and uppercase columns
     * - Lowercase schema with mixed-case table name
     * - Lowercase schema with uppercase table name and columns
     * - Uppercase schema with uppercase table name and columns
     * - Uppercase schema with lowercase table and columns
     * - Uppercase schema with uppercase table and columns
     * - Mixed-case schema with new table
     * - Lowercase schema: CREATE TABLE AS SELECT from lowercase table
     * - Mixed-case schema: CREATE TABLE AS SELECT from lowercase table
     * - Join queries using lowercase aliases
     */
    @Test(groups = {MYSQL_MIXED_CASE})
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

    /**
     * This test validates inserting data into tables using various combinations
     * of schema names and table names with different casing. It ensures that
     * Presto honors case sensitivity when `lower_case_table_names=0` and
     * mixed-case support is enabled in MySQL.
     *
     * Covered scenarios:
     * 1. Inserting into a table using lowercase schema and lowercase table name.
     * 2. Inserting into a table using lowercase schema and mixed-case table name.
     * 3. Inserting into a table using mixed-case schema and lowercase table name.
     * 4. Inserting into a table using uppercase schema and uppercase table name with mixed-case columns.
     * 5. Inserting into a table using mixed-case schema and mixed-case table name.
     * 6. Inserting into a table with mixed-case name from another table with lowercase name.
     * 7. Inserting into a table with lowercase name from another table with mixed-case name.
     */
    @Test(groups = {MYSQL_MIXED_CASE}, dependsOnMethods = "testCreateAllTablesWithMixedCaseScenarios")
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

    /**
     * This test verifies that selecting data from MySQL tables with various combinations
     * of case-sensitive schema names, table names, and column names works correctly
     * when mixed-case support is enabled in Presto.
     *
     * Covered scenarios:
     * 1. Select all data from a lowercase schema and lowercase table.
     * 2. Select filtered data from a mixed-case table in a lowercase schema.
     * 3. Select filtered data from a lowercase table in a mixed-case schema.
     * 4. Select filtered data from a mixed-case table in a mixed-case schema.
     * 5. Select (with subquery) from a mixed-case schema and table using mixed-case columns.
     */
    @Test(groups = {MYSQL_MIXED_CASE}, dependsOnMethods = "testInsertDataIntoExistingMixedCaseTables")
    public void testSelectDataWithMixedCaseNames()
    {
        assertThat(query("SELECT name, ID FROM " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\""))
                .containsOnly(row("eva", 301), row("lisa", 302), row("ivan", 401), row("nora", 402), row("eva", 301), row("lisa", 302));

        // Even if we define a column as ID, we can refer to it as id, ID, or anything else in any case — with or without backticks.
        assertThat(query("SELECT name, id FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
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

    /**
     * This test verifies that altering MySQL tables with various combinations
     * of case-sensitive schema names and table names works correctly
     * when mixed-case support is enabled in Presto.
     *
     * Covered scenarios:
     * 1. Add a new column to a table in a lowercase schema with a lowercase table name.
     * 2. Add new columns to a table in an uppercase schema with an uppercase table name.
     * 3. Add a new column to a table in a lowercase schema with an uppercase table name.
     * 4. Rename columns in tables with different schema and table casing.
     * 5. Verify added and renamed columns using DESCRIBE queries.
     */

    @Test(groups = {MYSQL_MIXED_CASE}, dependsOnMethods = "testSelectDataWithMixedCaseNames")
    public void testTableAlterWithMixedCaseNames()
    {
        // add column
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" ADD COLUMN num REAL");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" ADD COLUMN NuM2 REAL");

        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_UPPER_2 + "\" ADD COLUMN num01 REAL");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_UPPER_2 + "\" ADD COLUMN NuM2 REAL");

        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" ADD COLUMN num1 REAL");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" ADD COLUMN NuM2 REAL");
        // Negative test: Creating a duplicate column with different case should fail in mysql
        assertThat(() -> query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" ADD COLUMN num2 REAL"))
                .failsWithMessage("Duplicate column name 'num2'");

        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\"")).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("ID", "integer", "", "", 10L, null, null),
                row("num", "real", "", "", 24L, null, null),
                row("NuM2", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME_UPPER_2 + "\"")).containsOnly(
                row("Name", "varchar(255)", "", "", null, null, 255L),
                row("ID", "integer", "", "", 10L, null, null),
                row("num01", "real", "", "", 24L, null, null),
                row("NuM2", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\"")).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("num1", "real", "", "", 24L, null, null),
                row("NuM2", "real", "", "", 24L, null, null));

        // rename column
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" RENAME COLUMN num TO numb");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" RENAME COLUMN NuM2 TO NuM02");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" RENAME COLUMN num1 TO numb01");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" RENAME COLUMN NuM2 TO NuM02");

        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\"")).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("ID", "integer", "", "", 10L, null, null),
                row("numb", "real", "", "", 24L, null, null),
                row("NuM02", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\"")).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("numb01", "real", "", "", 24L, null, null),
                row("NuM02", "real", "", "", 24L, null, null));

        // drop column
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\" DROP COLUMN NuM02");
        query("ALTER TABLE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\" DROP COLUMN numb01");

        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME + "\".\"" + TABLE_NAME + "\"")).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("ID", "integer", "", "", 10L, null, null),
                row("numb", "real", "", "", 24L, null, null));
        assertThat(query("DESCRIBE " + CATALOG + ".\"" + SCHEMA_NAME_UPPER + "\".\"" + TABLE_NAME_UPPER_SCHEMA_02 + "\"")).containsOnly(
                row("name", "varchar(255)", "", "", null, null, 255L),
                row("id", "integer", "", "", 10L, null, null),
                row("num", "double", "", "", 53L, null, null),
                row("NuM02", "real", "", "", 24L, null, null));
    }

    @Test(groups = {MYSQL_MIXED_CASE}, dependsOnMethods = "testTableAlterWithMixedCaseNames")
    public void testDropMixedCaseTablesAndSchemas()
    {
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME + "`.`" + TABLE_NAME + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME + "`.`" + TABLE_NAME_0 + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME + "`.`" + TABLE_NAME_MIXED_1 + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME + "`.`" + TABLE_NAME_UPPER_2 + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME_UPPER + "`.`" + TABLE_NAME_UPPER_SCHEMA_3 + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME_UPPER + "`.`" + TABLE_NAME_UPPER_SCHEMA_4 + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME_UPPER + "`.`" + TABLE_NAME_UPPER_SCHEMA_02 + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME + "`.`" + TABLE_NAME_FROM_LOWER + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME_MIXED + "`.`" + TABLE_NAME_FROM_MIXED + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME + "`.`" + TABLE_NAME_JOIN_LOWER + "`");
        onMySql().executeQuery("DROP TABLE IF EXISTS `" + SCHEMA_NAME + "`.`" + TABLE_NAME_JOIN_MIXED + "`");
    }
}
